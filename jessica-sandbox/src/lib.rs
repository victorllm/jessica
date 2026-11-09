//! Deterministic WebAssembly sandbox primitives.
//!
//! Task 3.2 builds on the Task 3.1 loading path with WASI preview 1 capability
//! control and per-store resource limits. By default a module can reach no host
//! directories and no network. A [`SandboxPolicy`] may grant read-only
//! directories, and every store gets fuel, wall-clock, memory, table, and
//! instance limits. Policy violations and resource exhaustion are returned as
//! structured errors; they are never ignored or allowed to panic the host.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use thiserror::Error;
use wasmtime::{
    Config, Engine, Instance, Linker, Module, ResourceLimiter, Store, StoreLimits,
    StoreLimitsBuilder, Trap,
};
use wasmtime_wasi::WasiCtxBuilder;
use wasmtime_wasi::filesystem::FsPerms;
use wasmtime_wasi::p1::{WasiP1Ctx, add_to_linker_sync};

mod policy;

pub use policy::{ReadOnlyDir, SandboxPolicy};

/// Factory that owns one compiled engine and loads WebAssembly modules.
#[derive(Debug)]
pub struct WasmEngine {
    engine: Engine,
}

impl WasmEngine {
    /// Creates an engine with fuel metering and epoch interruption enabled.
    ///
    /// Both settings are host-side enforcement mechanisms. Enabling them grants
    /// the guest no capability; they make the per-store limits enforceable.
    pub fn new() -> Result<Self, SandboxError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config).map_err(SandboxError::Engine)?;
        Ok(Self { engine })
    }

    /// Loads and compiles a `.wasm` file from disk.
    pub fn load_file(&self, path: impl AsRef<Path>) -> Result<WasmModule, SandboxError> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|source| SandboxError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        self.load_bytes(&bytes)
    }

    /// Compiles an in-memory WebAssembly binary.
    ///
    /// This is the binary form, not WebAssembly text (`.wat`); the crate does
    /// not enable wasmtime's text parser.
    pub fn load_bytes(&self, bytes: &[u8]) -> Result<WasmModule, SandboxError> {
        let module = Module::new(&self.engine, bytes).map_err(SandboxError::Compile)?;
        Ok(WasmModule {
            engine: self.engine.clone(),
            module,
        })
    }
}

/// A compiled WebAssembly module ready to be instantiated.
#[derive(Debug)]
pub struct WasmModule {
    engine: Engine,
    module: Module,
}

impl WasmModule {
    /// Instantiates the module with WASI support governed by `policy`.
    ///
    /// WASI preview 1 imports are always available, but the resulting context
    /// has no preopened directories and no network access unless the policy
    /// grants a read-only directory. Instantiation itself runs under the
    /// policy's fuel and wall-clock budgets.
    ///
    /// This is a blocking call. A module start function may invoke WASI host
    /// functions, so async callers must use `tokio::task::spawn_blocking` (or
    /// a dedicated OS thread).
    pub fn instantiate(&self, policy: &SandboxPolicy) -> Result<WasmInstance, SandboxError> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(policy.max_memory_bytes())
            .table_elements(policy.max_table_elements())
            .instances(policy.max_instances())
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(
            &self.engine,
            SandboxStore {
                wasi: build_wasi_ctx(policy)?,
                limits,
            },
        );
        store.limiter(|state| &mut state.limits);

        let mut linker: Linker<SandboxStore> = Linker::new(&self.engine);
        add_to_linker_sync(&mut linker, |state: &mut SandboxStore| &mut state.wasi)
            .map_err(SandboxError::WasiLinker)?;

        let instance = run_limited(
            &mut store,
            &self.engine,
            policy.fuel(),
            policy.wall_timeout(),
            |store| linker.instantiate(store, &self.module),
        )
        .map_err(RunFailure::into_instantiate)?;

        Ok(WasmInstance {
            store,
            instance,
            engine: self.engine.clone(),
            fuel: policy.fuel(),
            wall_timeout: policy.wall_timeout(),
        })
    }
}

/// An instantiated module and its execution store.
#[derive(Debug)]
pub struct WasmInstance {
    store: Store<SandboxStore>,
    instance: Instance,
    engine: Engine,
    fuel: u64,
    wall_timeout: Duration,
}

impl WasmInstance {
    /// Calls an exported function with the signature `() -> i32`.
    ///
    /// Fuel and the wall-clock deadline are reset for every call. A missing
    /// export, a signature mismatch, a capability denial, resource exhaustion,
    /// and any other WebAssembly trap are reported as distinct errors; none of
    /// them panic the host.
    ///
    /// This is a blocking call. WASI host functions used by the guest run
    /// through a synchronous shim that must not execute directly on a Tokio
    /// worker; async callers must use `tokio::task::spawn_blocking` (or a
    /// dedicated OS thread).
    pub fn call_i32(&mut self, name: &str) -> Result<i32, SandboxError> {
        let function = self
            .instance
            .get_func(&mut self.store, name)
            .ok_or_else(|| SandboxError::UnknownExport {
                name: name.to_owned(),
            })?;
        let typed =
            function
                .typed::<(), i32>(&self.store)
                .map_err(|source| SandboxError::Signature {
                    name: name.to_owned(),
                    source,
                })?;
        run_limited(
            &mut self.store,
            &self.engine,
            self.fuel,
            self.wall_timeout,
            |store| typed.call(store, ()),
        )
        .map_err(|failure| failure.into_call(name, self.wall_timeout))
    }
}

/// Store data shared by WASI host calls and the resource limiter.
struct SandboxStore {
    wasi: WasiP1Ctx,
    limits: StoreLimits,
}

// `WasiP1Ctx` does not implement `Debug`; the WASI state is deliberately not
// exposed here, so a compact placeholder is sufficient.
impl fmt::Debug for SandboxStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandboxStore").finish_non_exhaustive()
    }
}

impl ResourceLimiter for SandboxStore {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.limits.memory_growing(current, desired, maximum)
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.limits.memory_grow_failed(error)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.limits.table_growing(current, desired, maximum)
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.limits.table_grow_failed(error)
    }

    fn instances(&self) -> usize {
        self.limits.instances()
    }

    fn tables(&self) -> usize {
        self.limits.tables()
    }

    fn memories(&self) -> usize {
        self.limits.memories()
    }
}

/// Builds the WASI context for one instance.
///
/// Defaults are deny-by-default: stdin is closed, stdout/stderr are sinks, no
/// arguments or environment variables are inherited, no directories are
/// preopened, and TCP, UDP, and name lookup are disabled. Only read-only
/// directory preopens from the policy are added.
fn build_wasi_ctx(policy: &SandboxPolicy) -> Result<WasiP1Ctx, SandboxError> {
    let mut builder = WasiCtxBuilder::new();
    builder
        .allow_ip_name_lookup(false)
        .allow_tcp(false)
        .allow_udp(false);
    for dir in policy.read_only_dirs() {
        builder
            .preopened_dir(dir.host_path(), dir.guest_path(), FsPerms::ReadOnly)
            .map_err(|source| SandboxError::Capability {
                host_path: dir.host_path().to_path_buf(),
                guest_path: dir.guest_path().to_owned(),
                source,
            })?;
    }
    Ok(builder.build_p1())
}

/// Failure modes shared by instantiation and calls.
enum RunFailure {
    /// The store's fuel budget could not be set.
    FuelConfig(wasmtime::Error),
    /// The wall-clock watchdog thread could not be started.
    Watchdog(std::io::Error),
    /// WebAssembly execution returned an error.
    Wasm(wasmtime::Error),
}

impl RunFailure {
    /// Maps an instantiation failure to a public error.
    fn into_instantiate(self) -> SandboxError {
        match self {
            Self::FuelConfig(source) => SandboxError::FuelConfig(source),
            Self::Watchdog(source) => SandboxError::Watchdog(source),
            Self::Wasm(source) => SandboxError::Instantiate(source),
        }
    }

    /// Maps a call failure to a public error, distinguishing fuel exhaustion
    /// and deadline interruption from other traps.
    fn into_call(self, name: &str, timeout: Duration) -> SandboxError {
        match self {
            Self::FuelConfig(source) => SandboxError::FuelConfig(source),
            Self::Watchdog(source) => SandboxError::Watchdog(source),
            Self::Wasm(source) => match source.downcast_ref::<Trap>() {
                Some(Trap::OutOfFuel) => SandboxError::FuelExhausted {
                    name: name.to_owned(),
                },
                Some(Trap::Interrupt) => SandboxError::DeadlineExceeded {
                    name: name.to_owned(),
                    timeout,
                },
                _ => SandboxError::Trap {
                    name: name.to_owned(),
                    source,
                },
            },
        }
    }
}

/// Resets fuel, arms the wall-clock watchdog, and runs `execute`.
///
/// The watchdog is dropped before this function returns, which cancels and
/// joins it. This prevents a stale epoch increment from interrupting a later
/// call on the same store.
fn run_limited<T>(
    store: &mut Store<SandboxStore>,
    engine: &Engine,
    fuel: u64,
    wall_timeout: Duration,
    execute: impl FnOnce(&mut Store<SandboxStore>) -> wasmtime::Result<T>,
) -> Result<T, RunFailure> {
    store.set_fuel(fuel).map_err(RunFailure::FuelConfig)?;
    store.set_epoch_deadline(1);
    let _watchdog = Watchdog::start(engine.clone(), wall_timeout).map_err(RunFailure::Watchdog)?;
    execute(store).map_err(RunFailure::Wasm)
}

/// One-shot wall-clock watchdog for a single store operation.
///
/// The timer thread increments the engine epoch after `timeout` unless it is
/// cancelled first. `Drop` always cancels and joins the thread.
struct Watchdog {
    cancel: mpsc::Sender<()>,
    handle: Option<JoinHandle<()>>,
}

impl Watchdog {
    /// Starts a watchdog for the next operation.
    fn start(engine: Engine, timeout: Duration) -> std::io::Result<Self> {
        let (cancel, cancelled) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("jessica-wasm-deadline".to_owned())
            .spawn(move || {
                if matches!(
                    cancelled.recv_timeout(timeout),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    engine.increment_epoch();
                }
            })?;
        Ok(Self {
            cancel,
            handle: Some(handle),
        })
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        let _ = self.cancel.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Errors produced while loading, instantiating, or calling a module.
#[derive(Debug, Error)]
pub enum SandboxError {
    /// The wasmtime engine could not be created.
    #[error("failed to create the wasm engine: {0}")]
    Engine(#[source] wasmtime::Error),
    /// A module file could not be read.
    #[error("failed to read wasm module `{path}`: {source}")]
    Read {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The bytes were not a valid WebAssembly binary.
    #[error("failed to compile wasm module: {0}")]
    Compile(#[source] wasmtime::Error),
    /// The module could not be instantiated, including resource-limit or
    /// start-function budget failures.
    #[error("failed to instantiate wasm module: {0}")]
    Instantiate(#[source] wasmtime::Error),
    /// A read-only directory capability could not be applied.
    #[error(
        "failed to grant read-only access to host directory `{host_path}` as guest path \
         `{guest_path}`: {source}"
    )]
    Capability {
        /// Host directory that could not be opened.
        host_path: PathBuf,
        /// Guest-visible path for the failed capability.
        guest_path: String,
        /// Underlying wasmtime error.
        #[source]
        source: wasmtime::Error,
    },
    /// WASI host functions could not be registered with the linker.
    #[error("failed to configure WASI host functions: {0}")]
    WasiLinker(#[source] wasmtime::Error),
    /// The store fuel budget could not be configured.
    #[error("failed to configure the store fuel budget: {0}")]
    FuelConfig(#[source] wasmtime::Error),
    /// The wall-clock watchdog thread could not be started.
    #[error("failed to start the wall-clock watchdog: {0}")]
    Watchdog(#[source] std::io::Error),
    /// The requested export does not exist or is not a function.
    #[error("wasm module has no exported function `{name}`")]
    UnknownExport {
        /// Requested export name.
        name: String,
    },
    /// The export exists but does not match `() -> i32`.
    #[error("wasm export `{name}` has an incompatible signature: {source}")]
    Signature {
        /// Export name.
        name: String,
        /// Underlying wasmtime type-mismatch error.
        #[source]
        source: wasmtime::Error,
    },
    /// The export exhausted its fuel budget.
    #[error("wasm export `{name}` exhausted its fuel budget")]
    FuelExhausted {
        /// Export name.
        name: String,
    },
    /// The export ran past its wall-clock budget and was interrupted.
    #[error("wasm export `{name}` exceeded its wall-clock timeout of {timeout:?}")]
    DeadlineExceeded {
        /// Export name.
        name: String,
        /// Wall-clock budget that was exceeded.
        timeout: Duration,
    },
    /// The export trapped while executing for a reason other than fuel or the
    /// wall-clock deadline.
    #[error("wasm export `{name}` trapped: {source}")]
    Trap {
        /// Export name.
        name: String,
        /// Underlying wasmtime trap.
        #[source]
        source: wasmtime::Error,
    },
}
