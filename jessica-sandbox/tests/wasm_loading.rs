//! Behavior tests for loading, instantiating, and calling a wasm module.

use std::path::PathBuf;

use jessica_sandbox::{SandboxError, SandboxPolicy, WasmEngine};

/// Precompiled fixture checked in next to its `.wat` source and build script.
const FIXTURE_BYTES: &[u8] = include_bytes!("fixtures/fixed_value.wasm");

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("fixed_value.wasm")
}

fn engine() -> WasmEngine {
    WasmEngine::new().expect("wasmtime engine must initialize")
}

fn policy() -> SandboxPolicy {
    SandboxPolicy::default()
}

#[test]
fn loads_file_instantiates_and_calls_exported_function() {
    let module = engine()
        .load_file(fixture_path())
        .expect("fixture must compile");
    let mut instance = module
        .instantiate(&policy())
        .expect("fixture must instantiate");

    assert_eq!(
        instance.call_i32("answer").expect("answer must execute"),
        42
    );
    assert_eq!(
        instance
            .call_i32("answer")
            .expect("answer must remain callable"),
        42
    );
}

#[test]
fn loads_module_from_compiled_bytes() {
    let module = engine()
        .load_bytes(FIXTURE_BYTES)
        .expect("fixture bytes must compile");
    let mut instance = module
        .instantiate(&policy())
        .expect("fixture must instantiate");

    assert_eq!(
        instance.call_i32("answer").expect("answer must execute"),
        42
    );
}

#[test]
fn missing_file_reports_the_requested_path() {
    let missing = fixture_path().with_file_name("does-not-exist.wasm");

    match engine().load_file(&missing) {
        Err(SandboxError::Read { path, .. }) => assert_eq!(path, missing),
        other => panic!("expected a Read error, got {other:?}"),
    }
}

#[test]
fn invalid_binary_reports_a_compile_error() {
    match engine().load_bytes(b"this is not wasm") {
        Err(SandboxError::Compile(_)) => {}
        other => panic!("expected a Compile error, got {other:?}"),
    }
}

#[test]
fn missing_export_is_reported_by_name() {
    let module = engine()
        .load_file(fixture_path())
        .expect("fixture must compile");
    let mut instance = module
        .instantiate(&policy())
        .expect("fixture must instantiate");

    match instance.call_i32("missing") {
        Err(SandboxError::UnknownExport { name }) => assert_eq!(name, "missing"),
        other => panic!("expected UnknownExport, got {other:?}"),
    }
}

#[test]
fn a_trap_is_returned_as_an_error_and_the_instance_remains_usable() {
    let module = engine()
        .load_file(fixture_path())
        .expect("fixture must compile");
    let mut instance = module
        .instantiate(&policy())
        .expect("fixture must instantiate");

    match instance.call_i32("trap") {
        Err(SandboxError::Trap { name, .. }) => assert_eq!(name, "trap"),
        other => panic!("expected a Trap error, got {other:?}"),
    }
    assert_eq!(
        instance
            .call_i32("answer")
            .expect("a trap must not poison the store"),
        42
    );
}
