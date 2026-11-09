//! Deterministic WebAssembly sandbox primitives.
//!
//! Task 3.1 implements only the compile → instantiate → call path. The engine
//! is created without WASI support and modules are instantiated with an empty
//! linker, so a module cannot reach the host filesystem or network. Capability
//! policy and resource limits arrive in Task 3.2.

use std::path::{Path, PathBuf};

use thiserror::Error;
use wasmtime::{Config, Engine, Instance, Linker, Module, Store};

/// Factory that owns one compiled engine and loads WebAssembly modules.
#[derive(Debug)]
pub struct WasmEngine {
    engine: Engine,
}

impl WasmEngine {
    /// Creates an engine with wasmtime defaults and no WASI capabilities.
    pub fn new() -> Result<Self, SandboxError> {
        let config = Config::new();
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
    /// Instantiates the module with an empty linker.
    ///
    /// The empty linker means a module that declares any import fails to
    /// instantiate. In particular, no WASI imports are available in Task 3.1.
    pub fn instantiate(&self) -> Result<WasmInstance, SandboxError> {
        let mut store = Store::new(&self.engine, ());
        let linker = Linker::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &self.module)
            .map_err(SandboxError::Instantiate)?;
        Ok(WasmInstance { store, instance })
    }
}

/// An instantiated module and its execution store.
#[derive(Debug)]
pub struct WasmInstance {
    store: Store<()>,
    instance: Instance,
}

impl WasmInstance {
    /// Calls an exported function with the signature `() -> i32`.
    ///
    /// A missing export, a signature mismatch, and a WebAssembly trap are
    /// reported as distinct errors; none of them panic the host.
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
        typed
            .call(&mut self.store, ())
            .map_err(|source| SandboxError::Trap {
                name: name.to_owned(),
                source,
            })
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
    /// The module could not be instantiated.
    #[error("failed to instantiate wasm module: {0}")]
    Instantiate(#[source] wasmtime::Error),
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
    /// The exported function trapped while executing.
    #[error("wasm export `{name}` trapped: {source}")]
    Trap {
        /// Export name.
        name: String,
        /// Underlying wasmtime trap.
        #[source]
        source: wasmtime::Error,
    },
}
