//! Behavior tests for WASI capability policy.
//!
//! Denials are observed as WASI errno values returned by the guest module, and
//! grants are observed by the guest reading real host file contents.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_sandbox::{SandboxError, SandboxPolicy, WasmEngine, WasmModule};

/// Precompiled fixture checked in next to its `.wat` source and build script.
const WASI_FIXTURE: &[u8] = include_bytes!("fixtures/wasi_fs.wasm");

fn module() -> WasmModule {
    WasmEngine::new()
        .expect("wasmtime engine must initialize")
        .load_bytes(WASI_FIXTURE)
        .expect("wasi fixture must compile")
}

/// A uniquely named temporary directory removed when the test finishes.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-sandbox-{label}-{}-{id}",
            std::process::id()
        ));
        if path.exists() {
            fs::remove_dir_all(&path).expect("stale temp dir must be removable");
        }
        fs::create_dir_all(&path).expect("temp dir must be creatable");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn default_policy_exposes_no_files_and_the_host_survives_the_denial() {
    let mut instance = module()
        .instantiate(&SandboxPolicy::default())
        .expect("module must instantiate without capabilities");

    let errno = instance
        .call_i32("read_allowed")
        .expect("a denied file open must return a WASI errno, not trap");
    assert_ne!(errno, 0, "no preopen must mean no readable file");
    assert_eq!(
        instance
            .call_i32("ok")
            .expect("the instance must remain usable"),
        7
    );
}

#[test]
fn read_only_grant_permits_reading_a_granted_file() {
    let temp = TempDir::new("read-grant");
    let granted = temp.path().join("granted");
    fs::create_dir(&granted).expect("granted dir must be creatable");
    fs::write(granted.join("allowed.txt"), "SECRET!\n").expect("fixture file must be writable");

    let policy = SandboxPolicy::default().grant_read_only_dir(&granted, "/data");
    let mut instance = module()
        .instantiate(&policy)
        .expect("module must instantiate with a read-only grant");

    assert_eq!(
        instance
            .call_i32("read_allowed")
            .expect("read must execute"),
        0,
        "guest must read the exact granted file contents"
    );
}

#[test]
fn read_only_grant_denies_escaping_the_preopened_directory() {
    let temp = TempDir::new("escape-denied");
    let granted = temp.path().join("granted");
    fs::create_dir(&granted).expect("granted dir must be creatable");
    fs::write(temp.path().join("outside.txt"), "SECRET!\n").expect("outside file must be writable");

    let policy = SandboxPolicy::default().grant_read_only_dir(&granted, "/data");
    let mut instance = module()
        .instantiate(&policy)
        .expect("module must instantiate with a read-only grant");

    let errno = instance
        .call_i32("read_outside")
        .expect("an escaping path must return a WASI errno, not trap");
    assert_ne!(errno, 0, "`../outside.txt` must not escape the preopen");
    assert_eq!(
        instance
            .call_i32("ok")
            .expect("the instance must remain usable"),
        7
    );
}

#[test]
fn read_only_grant_denies_writes_and_creates_no_file() {
    let temp = TempDir::new("write-denied");
    let granted = temp.path().join("granted");
    fs::create_dir(&granted).expect("granted dir must be creatable");

    let policy = SandboxPolicy::default().grant_read_only_dir(&granted, "/data");
    let mut instance = module()
        .instantiate(&policy)
        .expect("module must instantiate with a read-only grant");

    let errno = instance
        .call_i32("write_new")
        .expect("a denied create must return a WASI errno, not trap");
    assert_ne!(errno, 0, "read-only preopen must reject create/write");
    assert!(
        !granted.join("new.txt").exists(),
        "a denied write must not create a host file"
    );
}

#[test]
fn a_missing_granted_directory_is_a_structured_capability_error() {
    let temp = TempDir::new("missing-grant");
    let missing = temp.path().join("does-not-exist");
    let policy = SandboxPolicy::default().grant_read_only_dir(&missing, "/data");

    match module().instantiate(&policy) {
        Err(SandboxError::Capability {
            host_path,
            guest_path,
            ..
        }) => {
            assert_eq!(host_path, missing);
            assert_eq!(guest_path, "/data");
        }
        other => panic!("expected a Capability error, got {other:?}"),
    }
}
