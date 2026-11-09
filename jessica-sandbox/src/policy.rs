//! Per-plugin capability and resource policy.
//!
//! A policy is derived from a plugin manifest by the caller. Everything not
//! explicitly granted here stays denied: the sandbox opens no host directories,
//! provides no network access, and applies finite resource limits by default.

use std::path::{Path, PathBuf};
use std::time::Duration;

/// Default fuel budget for one instantiation or call.
const DEFAULT_FUEL: u64 = 10_000_000;
/// Default wall-clock budget for one instantiation or call.
const DEFAULT_WALL_TIMEOUT: Duration = Duration::from_secs(5);
/// Default per-memory byte limit (64 MiB).
const DEFAULT_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;
/// Default per-table element limit.
const DEFAULT_MAX_TABLE_ELEMENTS: usize = 10_000;
/// Default number of instances allowed inside one store.
const DEFAULT_MAX_INSTANCES: usize = 1;

/// A host directory exposed to a plugin under a guest-visible path.
///
/// The guest can only read through this mapping. Write, create, rename, and
/// delete operations are denied by the underlying read-only preopen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOnlyDir {
    host_path: PathBuf,
    guest_path: String,
}

impl ReadOnlyDir {
    /// Declares a read-only directory capability.
    ///
    /// `host_path` is resolved by the host when the module is instantiated;
    /// `guest_path` is the path the guest uses, for example `/data`.
    pub fn new(host_path: impl Into<PathBuf>, guest_path: impl Into<String>) -> Self {
        Self {
            host_path: host_path.into(),
            guest_path: guest_path.into(),
        }
    }

    /// Returns the host-side directory that will be preopened.
    pub fn host_path(&self) -> &Path {
        &self.host_path
    }

    /// Returns the guest-visible path of this preopen.
    pub fn guest_path(&self) -> &str {
        &self.guest_path
    }
}

/// Capabilities and resource limits applied to one plugin instance.
///
/// The default policy grants no host directories and no network access, and
/// applies finite fuel, wall-clock, memory, table, and instance limits to every
/// store. Callers opt into read-only directories with
/// [`SandboxPolicy::grant_read_only_dir`]; write access is intentionally not
/// representable.
#[derive(Clone, Debug)]
pub struct SandboxPolicy {
    fuel: u64,
    wall_timeout: Duration,
    max_memory_bytes: usize,
    max_table_elements: usize,
    max_instances: usize,
    read_only_dirs: Vec<ReadOnlyDir>,
}

impl Default for SandboxPolicy {
    fn default() -> Self {
        Self {
            fuel: DEFAULT_FUEL,
            wall_timeout: DEFAULT_WALL_TIMEOUT,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_table_elements: DEFAULT_MAX_TABLE_ELEMENTS,
            max_instances: DEFAULT_MAX_INSTANCES,
            read_only_dirs: Vec::new(),
        }
    }
}

impl SandboxPolicy {
    /// Creates a policy with no capabilities and the default limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the fuel budget applied to instantiation and to every call.
    ///
    /// Fuel is reset before each operation, so a call that exhausts its budget
    /// does not poison later calls on the same instance.
    pub fn with_fuel(mut self, fuel: u64) -> Self {
        self.fuel = fuel;
        self
    }

    /// Sets the wall-clock budget applied to instantiation and to every call.
    ///
    /// The sandbox interrupts guest execution at the first WebAssembly epoch
    /// check after this duration elapses.
    pub fn with_wall_timeout(mut self, wall_timeout: Duration) -> Self {
        self.wall_timeout = wall_timeout;
        self
    }

    /// Sets the maximum size of each linear memory in bytes.
    pub fn with_memory_limit(mut self, max_memory_bytes: usize) -> Self {
        self.max_memory_bytes = max_memory_bytes;
        self
    }

    /// Sets the maximum number of elements in each table.
    pub fn with_table_limit(mut self, max_table_elements: usize) -> Self {
        self.max_table_elements = max_table_elements;
        self
    }

    /// Sets the maximum number of instances that may exist in one store.
    ///
    /// A value of `0` rejects instantiation entirely.
    pub fn with_instance_limit(mut self, max_instances: usize) -> Self {
        self.max_instances = max_instances;
        self
    }

    /// Grants read-only access to `host_path` under `guest_path`.
    ///
    /// This is the only directory capability the sandbox supports; there is no
    /// API for granting write access.
    pub fn grant_read_only_dir(
        mut self,
        host_path: impl Into<PathBuf>,
        guest_path: impl Into<String>,
    ) -> Self {
        self.read_only_dirs
            .push(ReadOnlyDir::new(host_path, guest_path));
        self
    }

    /// Returns the fuel budget per operation.
    pub fn fuel(&self) -> u64 {
        self.fuel
    }

    /// Returns the wall-clock budget per operation.
    pub fn wall_timeout(&self) -> Duration {
        self.wall_timeout
    }

    /// Returns the per-memory byte limit.
    pub fn max_memory_bytes(&self) -> usize {
        self.max_memory_bytes
    }

    /// Returns the per-table element limit.
    pub fn max_table_elements(&self) -> usize {
        self.max_table_elements
    }

    /// Returns the number of instances allowed in one store.
    pub fn max_instances(&self) -> usize {
        self.max_instances
    }

    /// Returns the declared read-only directory capabilities.
    pub fn read_only_dirs(&self) -> &[ReadOnlyDir] {
        &self.read_only_dirs
    }
}
