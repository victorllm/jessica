//! WebAssembly tools described by a JSON manifest.
//!
//! A manifest names the tool, carries its provider-facing argument schema,
//! points at a compiled `.wasm` module, selects the buffer ABI exports, and
//! declares the sandbox permissions the module runs under. Relative module and
//! read-only directory paths resolve against the manifest's directory.
//!
//! Every execution instantiates a fresh module instance on a blocking thread.
//! Isolation per call means a trap, fuel exhaustion, or a corrupt guest heap
//! cannot poison later calls. Traps are never swallowed: they become error
//! [`ToolResult`]s so the model can observe and recover from them.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolRegistry, ToolRegistryError, ToolSchema};
use jessica_core::types::ToolResult;
use jessica_sandbox::{BufferAbi, SandboxError, SandboxPolicy, WasmEngine, WasmModule};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

use crate::{failure, success};

/// Default maximum bytes a wasm tool may return.
const DEFAULT_MAX_RESULT_BYTES: usize = 64 * 1024;
/// Original manifest schema version. A missing `version` selects this value.
pub const MANIFEST_VERSION_V1: u32 = 1;
/// Manifest schema version that adds advisory descriptor annotations.
pub const MANIFEST_VERSION_V2: u32 = 2;

/// Advisory annotations a v2 manifest declares about its guest.
///
/// Annotations are untrusted provider metadata. The sandbox policy still
/// enforces no write preopens and no network; the host only uses these values
/// to classify the tool more conservatively and to require approval for
/// declared destructive or open-world behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmAnnotations {
    /// Guest claims it does not mutate host state.
    pub read_only: bool,
    /// Guest claims it can destroy or overwrite existing state.
    pub destructive: bool,
    /// Guest claims it interacts with external entities.
    pub open_world: bool,
}

/// Parsed manifest before paths and sandbox policy are resolved.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    /// Manifest schema version; absent means version 1.
    #[serde(default)]
    version: Option<u32>,
    /// Stable tool name exposed to the model.
    name: String,
    /// Human-readable description copied into the provider schema.
    #[serde(default)]
    description: String,
    /// JSON schema for the tool's argument object.
    schema: Value,
    /// Compiled module path, relative to the manifest directory.
    module: PathBuf,
    /// Guest export names implementing the buffer ABI.
    #[serde(default)]
    abi: AbiManifest,
    /// Sandbox permissions and resource limits.
    #[serde(default)]
    permissions: PermissionManifest,
    /// Advisory descriptor annotations; only valid in version 2.
    #[serde(default)]
    annotations: Option<AnnotationManifest>,
}

/// Provider annotations declared by a version 2 manifest.
///
/// All three fields are required when the object is present so a v2 manifest
/// cannot silently omit a classification the host would have to guess.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AnnotationManifest {
    /// Guest claims it does not mutate host state.
    read_only: bool,
    /// Guest claims it can destroy or overwrite existing state.
    destructive: bool,
    /// Guest claims it interacts with external entities.
    open_world: bool,
}

impl Manifest {
    /// Returns the effective manifest version.
    fn version(&self) -> u32 {
        self.version.unwrap_or(MANIFEST_VERSION_V1)
    }

    /// Returns the validated annotations, when declared.
    fn wasm_annotations(&self) -> Option<WasmAnnotations> {
        self.annotations
            .as_ref()
            .map(|annotations| WasmAnnotations {
                read_only: annotations.read_only,
                destructive: annotations.destructive,
                open_world: annotations.open_world,
            })
    }
}

/// Buffer ABI export names declared by a manifest.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AbiManifest {
    /// Allocator export name.
    alloc: String,
    /// Invocation export name.
    invoke: String,
}

impl Default for AbiManifest {
    fn default() -> Self {
        Self {
            alloc: "jessica_alloc".to_owned(),
            invoke: "jessica_invoke".to_owned(),
        }
    }
}

/// Sandbox permissions declared by a manifest.
///
/// `None` limits inherit the [`SandboxPolicy`] defaults. Unknown fields such as
/// a write-directory declaration are rejected by `deny_unknown_fields`.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PermissionManifest {
    /// Fuel budget per guest export call.
    fuel: Option<u64>,
    /// Wall-clock budget per guest export call, in milliseconds.
    wall_timeout_ms: Option<u64>,
    /// Linear-memory cap in bytes.
    max_memory_bytes: Option<usize>,
    /// Table-element cap.
    max_table_elements: Option<usize>,
    /// Instance cap per store.
    max_instances: Option<usize>,
    /// Maximum bytes the guest may return; defaults to 64 KiB.
    max_result_bytes: Option<usize>,
    /// Host directories granted read-only.
    read_only_dirs: Vec<DirectoryPermission>,
}

impl PermissionManifest {
    /// Returns the effective result-size limit.
    fn result_limit(&self) -> usize {
        self.max_result_bytes.unwrap_or(DEFAULT_MAX_RESULT_BYTES)
    }
}

/// One read-only directory preopened into the guest.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryPermission {
    /// Host directory, relative to the manifest directory.
    host: PathBuf,
    /// Guest-visible path.
    guest: String,
}

/// A [`Tool`] backed by a WebAssembly module described by a JSON manifest.
///
/// The manifest is validated and the module is compiled once by
/// [`WasmTool::load`]. Every call then instantiates the compiled module under
/// the declared [`SandboxPolicy`], serializes the JSON arguments into guest
/// memory, invokes the entry point, and parses the returned JSON.
///
/// Calls are serialized per tool: wasmtime's epoch interruption state is shared
/// by every instance created from the same engine, so allowing two calls to
/// overlap could make one call's wall-clock watchdog interrupt the other.
#[derive(Debug)]
pub struct WasmTool {
    name: String,
    description: String,
    parameters: Value,
    module: Arc<WasmModule>,
    abi: BufferAbi,
    policy: Arc<SandboxPolicy>,
    max_result_bytes: usize,
    manifest_version: u32,
    annotations: Option<WasmAnnotations>,
    execution_lock: Arc<Mutex<()>>,
}

impl WasmTool {
    /// Loads a manifest, validates it, and compiles its module.
    ///
    /// Relative module and read-only directory paths resolve against the
    /// manifest's parent directory. Sandbox permission values inherit
    /// [`SandboxPolicy`] defaults when omitted; `max_result_bytes` defaults to
    /// 64 KiB. Declaring a write directory or any other unknown permission
    /// field is rejected.
    ///
    /// This is a blocking call (file I/O and wasm compilation). Async callers
    /// must use `tokio::task::spawn_blocking`.
    pub fn load(manifest_path: impl AsRef<Path>) -> Result<Self, WasmToolError> {
        let manifest_path = manifest_path.as_ref();
        let text = std::fs::read_to_string(manifest_path).map_err(|source| {
            WasmToolError::ReadManifest {
                path: manifest_path.to_path_buf(),
                source,
            }
        })?;
        let manifest: Manifest =
            serde_json::from_str(&text).map_err(|source| WasmToolError::ParseManifest {
                path: manifest_path.to_path_buf(),
                source,
            })?;
        manifest
            .validate()
            .map_err(|message| WasmToolError::InvalidManifest {
                path: manifest_path.to_path_buf(),
                message,
            })?;

        let base = canonical_manifest_dir(manifest_path)?;
        let module_path = resolve(&base, &manifest.module);
        let policy = build_policy(&base, &manifest, manifest_path)?;
        let engine = WasmEngine::new()?;
        let module = engine.load_file(&module_path)?;

        let manifest_version = manifest.version();
        let annotations = manifest.wasm_annotations();
        Ok(Self {
            name: manifest.name,
            description: manifest.description,
            parameters: manifest.schema,
            module: Arc::new(module),
            abi: BufferAbi::new(manifest.abi.alloc, manifest.abi.invoke),
            policy: Arc::new(policy),
            max_result_bytes: manifest.permissions.result_limit(),
            manifest_version,
            annotations,
            execution_lock: Arc::new(Mutex::new(())),
        })
    }

    /// Returns the manifest schema version that produced this tool.
    pub fn manifest_version(&self) -> u32 {
        self.manifest_version
    }

    /// Returns the advisory annotations declared by a v2 manifest.
    ///
    /// `None` means the manifest used the v1 shape or omitted optional v2
    /// annotations; the host then applies the conservative sandbox baseline.
    pub fn annotations(&self) -> Option<WasmAnnotations> {
        self.annotations
    }
}

impl Manifest {
    /// Rejects manifests that would produce an unusable or unsupported tool.
    fn validate(&self) -> Result<(), String> {
        let version = self.version();
        if version != MANIFEST_VERSION_V1 && version != MANIFEST_VERSION_V2 {
            return Err(format!(
                "version must be {MANIFEST_VERSION_V1} or {MANIFEST_VERSION_V2}"
            ));
        }
        if self.annotations.is_some() && version != MANIFEST_VERSION_V2 {
            return Err("annotations require manifest version 2".to_owned());
        }
        if let Some(annotations) = &self.annotations {
            if annotations.read_only && annotations.destructive {
                return Err(
                    "annotations.read_only and annotations.destructive cannot both be true"
                        .to_owned(),
                );
            }
            if annotations.read_only && annotations.open_world {
                return Err(
                    "annotations.read_only and annotations.open_world cannot both be true"
                        .to_owned(),
                );
            }
        }
        if self.name.trim().is_empty() {
            return Err("name must not be empty".to_owned());
        }
        if !self.schema.is_object() {
            return Err("schema must be a JSON object".to_owned());
        }
        if self.module.as_os_str().is_empty() {
            return Err("module must not be empty".to_owned());
        }
        if self.abi.alloc.trim().is_empty() {
            return Err("abi.alloc must not be empty".to_owned());
        }
        if self.abi.invoke.trim().is_empty() {
            return Err("abi.invoke must not be empty".to_owned());
        }
        if self.permissions.fuel == Some(0) {
            return Err("permissions.fuel must be at least 1".to_owned());
        }
        if self.permissions.wall_timeout_ms == Some(0) {
            return Err("permissions.wall_timeout_ms must be at least 1".to_owned());
        }
        if self.permissions.max_memory_bytes == Some(0) {
            return Err("permissions.max_memory_bytes must be at least 1".to_owned());
        }
        if self.permissions.max_instances == Some(0) {
            return Err("permissions.max_instances must be at least 1".to_owned());
        }
        for directory in &self.permissions.read_only_dirs {
            if directory.host.as_os_str().is_empty() {
                return Err("permissions.read_only_dirs[].host must not be empty".to_owned());
            }
            if !directory.guest.starts_with('/') {
                return Err(
                    "permissions.read_only_dirs[].guest must be an absolute guest path".to_owned(),
                );
            }
        }
        Ok(())
    }
}

/// Resolves the manifest's parent directory to an absolute path.
fn canonical_manifest_dir(manifest_path: &Path) -> Result<PathBuf, WasmToolError> {
    let parent = manifest_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::canonicalize(parent).map_err(|source| WasmToolError::InvalidManifest {
        path: manifest_path.to_path_buf(),
        message: format!("failed to resolve the manifest directory: {source}"),
    })
}

/// Resolves `path` against `base` unless it is already absolute.
fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

/// Builds the sandbox policy declared by `manifest`.
fn build_policy(
    base: &Path,
    manifest: &Manifest,
    manifest_path: &Path,
) -> Result<SandboxPolicy, WasmToolError> {
    let permissions = &manifest.permissions;
    let mut policy = SandboxPolicy::new();
    if let Some(fuel) = permissions.fuel {
        policy = policy.with_fuel(fuel);
    }
    if let Some(milliseconds) = permissions.wall_timeout_ms {
        policy = policy.with_wall_timeout(Duration::from_millis(milliseconds));
    }
    if let Some(bytes) = permissions.max_memory_bytes {
        policy = policy.with_memory_limit(bytes);
    }
    if let Some(elements) = permissions.max_table_elements {
        policy = policy.with_table_limit(elements);
    }
    if let Some(instances) = permissions.max_instances {
        policy = policy.with_instance_limit(instances);
    }
    for directory in &permissions.read_only_dirs {
        let host = resolve(base, &directory.host);
        let metadata =
            std::fs::metadata(&host).map_err(|source| WasmToolError::InvalidManifest {
                path: manifest_path.to_path_buf(),
                message: format!(
                    "read-only directory `{}` cannot be used: {source}",
                    host.display()
                ),
            })?;
        if !metadata.is_dir() {
            return Err(WasmToolError::InvalidManifest {
                path: manifest_path.to_path_buf(),
                message: format!("read-only path `{}` is not a directory", host.display()),
            });
        }
        policy = policy.grant_read_only_dir(host, directory.guest.clone());
    }
    Ok(policy)
}

#[async_trait]
impl Tool for WasmTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let input = match serde_json::to_vec(&args) {
            Ok(input) => input,
            Err(source) => {
                return failure(format!(
                    "invalid arguments for wasm tool `{}`: {source}",
                    self.name
                ));
            }
        };

        let module = Arc::clone(&self.module);
        let policy = Arc::clone(&self.policy);
        let abi = self.abi.clone();
        let max_result_bytes = self.max_result_bytes;
        let execution_lock = Arc::clone(&self.execution_lock);
        let outcome = tokio::task::spawn_blocking(move || {
            let _guard = execution_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut instance = module.instantiate(&policy)?;
            instance.call_buffer(&abi, &input, max_result_bytes)
        })
        .await;

        let output = match outcome {
            Ok(Ok(output)) => output,
            Ok(Err(source)) => {
                return failure(format!("wasm tool `{}` failed: {source}", self.name));
            }
            Err(source) => {
                return failure(format!(
                    "wasm tool `{}` failed in its blocking task: {source}",
                    self.name
                ));
            }
        };

        let text = match String::from_utf8(output) {
            Ok(text) => text,
            Err(source) => {
                return failure(format!(
                    "wasm tool `{}` returned invalid UTF-8: {source}",
                    self.name
                ));
            }
        };
        if let Err(source) = serde_json::from_str::<Value>(&text) {
            return failure(format!(
                "wasm tool `{}` returned invalid JSON: {source}",
                self.name
            ));
        }
        success(text)
    }
}

/// Registers a wasm tool described by `manifest_path` into `registry`.
///
/// This is the mixed-registry entry point: native and wasm tools share the
/// same [`ToolRegistry`] and name space, so a duplicate name is rejected by
/// the registry. The call is blocking for the same reasons as
/// [`WasmTool::load`].
pub fn register_wasm_tool(
    registry: &mut ToolRegistry,
    manifest_path: impl AsRef<Path>,
) -> Result<(), WasmToolError> {
    let tool = WasmTool::load(manifest_path)?;
    let name = tool.name().to_owned();
    registry
        .register(Arc::new(tool))
        .map_err(|source| WasmToolError::Registry { name, source })
}

/// Errors produced while loading or registering a wasm tool.
#[derive(Debug, Error)]
pub enum WasmToolError {
    /// The manifest file could not be read.
    #[error("failed to read wasm tool manifest `{path}`: {source}")]
    ReadManifest {
        /// Manifest path.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The manifest was not valid JSON or did not match the expected shape.
    #[error("failed to parse wasm tool manifest `{path}`: {source}")]
    ParseManifest {
        /// Manifest path.
        path: PathBuf,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// The manifest parsed but declared an invalid or unsupported setting.
    #[error("invalid wasm tool manifest `{path}`: {message}")]
    InvalidManifest {
        /// Manifest path.
        path: PathBuf,
        /// Human-readable validation failure.
        message: String,
    },
    /// The module could not be compiled or loaded.
    #[error("failed to load the wasm tool module: {0}")]
    Sandbox(#[from] SandboxError),
    /// The tool name collided with one already registered.
    #[error("failed to register wasm tool `{name}`: {source}")]
    Registry {
        /// Tool name from the manifest.
        name: String,
        /// Underlying registry error.
        #[source]
        source: ToolRegistryError,
    },
}
