//! Behaviour tests for the manifest-driven wasm tool adapter.
//!
//! Every test constructs a real manifest, compiles the checked-in fixture
//! module through the sandbox, and asserts the observable tool result. Trap,
//! fuel-exhaustion, malformed-output, and permission failures must surface as
//! error results rather than panics or silently successful calls.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_core::tool::{Tool, ToolRegistry};
use jessica_tools::{
    MANIFEST_VERSION_V1, MANIFEST_VERSION_V2, ReadFileTool, WasmAnnotations, WasmTool,
    WasmToolError, register_wasm_tool,
};
use serde_json::{Value, json};

const FIXTURE_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/abi_behaviors.wasm"
);
const EXAMPLE_MANIFEST: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../examples/plugins/echo/manifest.json"
);

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

/// Unique temporary directory removed on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        let id = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-wasm-tool-{}-{label}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temporary directory should be created");
        Self { path }
    }

    fn write_manifest(&self, manifest: &Value) -> PathBuf {
        let path = self.path.join("manifest.json");
        let text = serde_json::to_vec_pretty(manifest).expect("manifest should serialize");
        std::fs::write(&path, text).expect("manifest should be written");
        path
    }

    fn write_raw(&self, content: &str) -> PathBuf {
        let path = self.path.join("manifest.json");
        std::fs::write(&path, content).expect("raw manifest should be written");
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Manifest for the shared behaviour fixture.
fn manifest(invoke: &str) -> Value {
    json!({
        "name": "test_tool",
        "description": "Behaviour fixture tool.",
        "schema": {"type": "object"},
        "module": FIXTURE_WASM,
        "abi": {"alloc": "alloc", "invoke": invoke},
        "permissions": {
            "fuel": 5_000_000,
            "wall_timeout_ms": 5000,
            "max_memory_bytes": 131072,
            "max_table_elements": 100,
            "max_instances": 1,
            "max_result_bytes": 65536
        }
    })
}

/// Loads a manifest on a blocking thread, as async callers must.
async fn load(path: PathBuf) -> Result<WasmTool, WasmToolError> {
    tokio::task::spawn_blocking(move || WasmTool::load(path))
        .await
        .expect("manifest loading task must not panic")
}

async fn load_tool(directory: &TempDir, manifest: &Value) -> WasmTool {
    let path = directory.write_manifest(manifest);
    load(path).await.expect("manifest must load")
}

#[tokio::test]
async fn echo_round_trips_json_arguments_through_guest_memory() {
    let directory = TempDir::new("echo");
    let tool = load_tool(&directory, &manifest("echo")).await;

    assert_eq!(tool.name(), "test_tool");
    assert_eq!(
        tool.schema()["function"]["parameters"]["type"],
        json!("object")
    );

    let arguments = json!({"text": "hello wasm"});
    let result = tool.execute(arguments.clone()).await;

    assert!(!result.is_error, "unexpected error: {}", result.content);
    assert_eq!(result.content, arguments.to_string());
}

#[tokio::test]
async fn a_wasm_trap_becomes_an_error_result_instead_of_being_swallowed() {
    let directory = TempDir::new("trap");
    let tool = load_tool(&directory, &manifest("trap")).await;

    let result = tool.execute(json!({})).await;

    assert!(result.is_error);
    assert!(result.content.contains("wasm tool `test_tool` failed"));
    assert!(result.content.contains("trapped"));
    assert!(result.content.contains("wasm export `trap`"));
}

#[tokio::test]
async fn fuel_exhaustion_becomes_an_error_result() {
    let directory = TempDir::new("fuel");
    let mut manifest = manifest("spin");
    manifest["permissions"]["fuel"] = json!(10_000);
    let tool = load_tool(&directory, &manifest).await;

    let result = tool.execute(json!({})).await;

    assert!(result.is_error);
    assert!(result.content.contains("exhausted its fuel budget"));
}

#[tokio::test]
async fn output_above_the_manifest_limit_is_rejected() {
    let directory = TempDir::new("limit");
    let mut manifest = manifest("large");
    manifest["permissions"]["max_result_bytes"] = json!(16);
    let tool = load_tool(&directory, &manifest).await;

    let result = tool.execute(json!({})).await;

    assert!(result.is_error);
    assert!(result.content.contains("exceeds the limit of 16 bytes"));
}

#[tokio::test]
async fn an_output_range_outside_guest_memory_is_rejected() {
    let directory = TempDir::new("bounds");
    let tool = load_tool(&directory, &manifest("out_of_bounds")).await;

    let result = tool.execute(json!({})).await;

    assert!(result.is_error);
    assert!(result.content.contains("outside the guest memory"));
}

#[tokio::test]
async fn invalid_json_output_is_reported_as_a_tool_error() {
    let directory = TempDir::new("bad-json");
    let tool = load_tool(&directory, &manifest("bad_json")).await;

    let result = tool.execute(json!({})).await;

    assert!(result.is_error);
    assert!(result.content.contains("returned invalid JSON"));
}

#[tokio::test]
async fn invalid_utf8_output_is_reported_as_a_tool_error() {
    let directory = TempDir::new("bad-utf8");
    let tool = load_tool(&directory, &manifest("bad_utf8")).await;

    let result = tool.execute(json!({})).await;

    assert!(result.is_error);
    assert!(result.content.contains("returned invalid UTF-8"));
}

#[tokio::test]
async fn a_negative_allocation_pointer_is_reported() {
    let directory = TempDir::new("bad-alloc");
    let mut manifest = manifest("echo");
    manifest["abi"]["alloc"] = json!("bad_alloc");
    let tool = load_tool(&directory, &manifest).await;

    let result = tool.execute(json!({"text": "hi"})).await;

    assert!(result.is_error);
    assert!(result.content.contains("invalid allocation pointer -1"));
}

#[tokio::test]
async fn a_missing_entry_export_is_reported() {
    let directory = TempDir::new("missing-export");
    let tool = load_tool(&directory, &manifest("missing")).await;

    let result = tool.execute(json!({})).await;

    assert!(result.is_error);
    assert!(result.content.contains("no exported function `missing`"));
}

#[tokio::test]
async fn write_permissions_are_explicitly_rejected() {
    let directory = TempDir::new("write-permission");
    let mut manifest = manifest("echo");
    manifest["permissions"]["write_dirs"] = json!([{"host": "data", "guest": "/data"}]);
    let path = directory.write_manifest(&manifest);

    let error = load(path)
        .await
        .expect_err("write permissions must be rejected");

    assert!(matches!(error, WasmToolError::ParseManifest { .. }));
    assert!(error.to_string().contains("write_dirs"));
}

#[tokio::test]
async fn zero_resource_permissions_are_rejected() {
    let directory = TempDir::new("zero-permission");
    let mut manifest = manifest("echo");
    manifest["permissions"]["fuel"] = json!(0);
    let path = directory.write_manifest(&manifest);

    let error = load(path).await.expect_err("zero fuel must fail");
    assert!(matches!(error, WasmToolError::InvalidManifest { .. }));
    assert!(error.to_string().contains("permissions.fuel"));
}

#[tokio::test]
async fn missing_read_only_directories_are_rejected_at_load_time() {
    let directory = TempDir::new("missing-dir");
    let mut manifest = manifest("echo");
    manifest["permissions"]["read_only_dirs"] =
        json!([{"host": "does-not-exist", "guest": "/data"}]);
    let path = directory.write_manifest(&manifest);

    let error = load(path).await.expect_err("missing directory must fail");
    assert!(matches!(error, WasmToolError::InvalidManifest { .. }));
    assert!(error.to_string().contains("does-not-exist"));
    assert!(error.to_string().contains("cannot be used"));
}

#[tokio::test]
async fn guest_directory_paths_must_be_absolute() {
    let directory = TempDir::new("relative-guest");
    let mut manifest = manifest("echo");
    manifest["permissions"]["read_only_dirs"] = json!([{"host": ".", "guest": "data"}]);
    let path = directory.write_manifest(&manifest);

    let error = load(path).await.expect_err("relative guest path must fail");
    assert!(error.to_string().contains("absolute guest path"));
}

#[tokio::test]
async fn missing_manifest_and_missing_module_are_distinct_errors() {
    let directory = TempDir::new("missing-module");
    let error = load(directory.path.join("absent.json"))
        .await
        .expect_err("missing manifest must fail");
    assert!(matches!(error, WasmToolError::ReadManifest { .. }));

    let mut manifest = manifest("echo");
    manifest["module"] = json!(directory.path.join("absent.wasm"));
    let path = directory.write_manifest(&manifest);
    let error = load(path).await.expect_err("missing module must fail");
    assert!(matches!(error, WasmToolError::Sandbox(_)));
    assert!(error.to_string().contains("absent.wasm"));
}

#[tokio::test]
async fn malformed_manifest_json_is_rejected() {
    let directory = TempDir::new("malformed");
    let path = directory.write_raw("{ not json");

    let error = load(path).await.expect_err("malformed JSON must fail");
    assert!(matches!(error, WasmToolError::ParseManifest { .. }));
}

#[tokio::test]
async fn registry_holds_native_and_wasm_tools_side_by_side() {
    let directory = TempDir::new("registry");
    let mut registry = ToolRegistry::new();

    registry
        .register(Arc::new(ReadFileTool::new(&directory.path)))
        .expect("native tool registration should succeed");
    register_wasm_tool(&mut registry, EXAMPLE_MANIFEST)
        .expect("wasm tool registration should succeed");

    let names: Vec<_> = registry.names().collect();
    assert_eq!(names, vec!["echo_json", "read_file"]);
    assert_eq!(registry.len(), 2);

    let schema = registry
        .get("echo_json")
        .expect("wasm tool should be registered")
        .schema();
    assert_eq!(schema["function"]["name"], "echo_json");
    assert_eq!(
        schema["function"]["parameters"]["required"],
        json!(["text"])
    );

    let echo = registry.get("echo_json").expect("wasm tool should exist");
    let result = echo.execute(json!({"text": "registered"})).await;
    assert!(!result.is_error, "unexpected error: {}", result.content);
    assert_eq!(result.content, "{\"text\":\"registered\"}");
}

#[tokio::test]
async fn duplicate_wasm_names_are_rejected_by_the_registry() {
    let mut registry = ToolRegistry::new();
    register_wasm_tool(&mut registry, EXAMPLE_MANIFEST).expect("first registration should succeed");

    let error = register_wasm_tool(&mut registry, EXAMPLE_MANIFEST)
        .expect_err("duplicate registration must fail");

    match error {
        WasmToolError::Registry { name, .. } => assert_eq!(name, "echo_json"),
        other => panic!("expected Registry error, got {other}"),
    }
}

#[tokio::test]
async fn v1_manifests_remain_loadable_without_annotations() {
    let directory = TempDir::new("v1-compat");
    let tool = load_tool(&directory, &manifest("echo")).await;

    assert_eq!(tool.manifest_version(), MANIFEST_VERSION_V1);
    assert_eq!(tool.annotations(), None);
    let result = tool.execute(json!({"text": "still works"})).await;
    assert!(!result.is_error, "v1 behavior changed: {}", result.content);
    assert_eq!(result.content, "{\"text\":\"still works\"}");
}

#[tokio::test]
async fn v2_manifests_expose_validated_annotations() {
    let directory = TempDir::new("v2-annotations");
    let mut manifest = manifest("echo");
    manifest["version"] = json!(2);
    manifest["annotations"] = json!({
        "read_only": true,
        "destructive": false,
        "open_world": false
    });
    let tool = load_tool(&directory, &manifest).await;

    assert_eq!(tool.manifest_version(), MANIFEST_VERSION_V2);
    assert_eq!(
        tool.annotations(),
        Some(WasmAnnotations {
            read_only: true,
            destructive: false,
            open_world: false,
        })
    );
}

#[tokio::test]
async fn annotations_require_v2_and_conflicts_are_rejected() {
    let directory = TempDir::new("annotation-version");
    let mut manifest_v1 = manifest("echo");
    manifest_v1["annotations"] = json!({
        "read_only": true,
        "destructive": false,
        "open_world": false
    });
    let path = directory.write_manifest(&manifest_v1);
    let error = load(path).await.expect_err("v1 annotations must fail");
    assert!(
        error.to_string().contains("require manifest version 2"),
        "unexpected error: {error}"
    );

    let directory = TempDir::new("annotation-conflict");
    let mut conflicting = manifest("echo");
    conflicting["version"] = json!(2);
    conflicting["annotations"] = json!({
        "read_only": true,
        "destructive": true,
        "open_world": false
    });
    let path = directory.write_manifest(&conflicting);
    let error = load(path)
        .await
        .expect_err("conflicting annotations must fail");
    assert!(
        error.to_string().contains("cannot both be true"),
        "unexpected error: {error}"
    );

    let directory = TempDir::new("annotation-open-world-conflict");
    let mut conflicting = manifest("echo");
    conflicting["version"] = json!(2);
    conflicting["annotations"] = json!({
        "read_only": true,
        "destructive": false,
        "open_world": true
    });
    let path = directory.write_manifest(&conflicting);
    let error = load(path)
        .await
        .expect_err("read-only open-world annotations must fail");
    assert!(
        error
            .to_string()
            .contains("read_only and annotations.open_world"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn unsupported_versions_and_unknown_annotation_fields_are_rejected() {
    let directory = TempDir::new("version-unsupported");
    let mut future = manifest("echo");
    future["version"] = json!(3);
    let path = directory.write_manifest(&future);
    let error = load(path).await.expect_err("unknown version must fail");
    assert!(
        error.to_string().contains("version must be 1 or 2"),
        "unexpected error: {error}"
    );

    let directory = TempDir::new("annotation-unknown-field");
    let mut unknown = manifest("echo");
    unknown["version"] = json!(2);
    unknown["annotations"] = json!({
        "read_only": true,
        "destructive": false,
        "open_world": false,
        "network": true
    });
    let path = directory.write_manifest(&unknown);
    let error = load(path)
        .await
        .expect_err("unknown annotation fields must fail");
    assert!(
        matches!(error, WasmToolError::ParseManifest { .. }),
        "unexpected error: {error}"
    );
}

#[test]
fn the_example_plugin_files_exist_and_are_non_empty() {
    let manifest = Path::new(EXAMPLE_MANIFEST);
    assert!(manifest.is_file());
    assert!(
        std::fs::metadata(manifest)
            .expect("manifest metadata")
            .len()
            > 0
    );

    let wasm = manifest.with_file_name("echo.wasm");
    assert!(wasm.is_file());
    assert!(std::fs::metadata(&wasm).expect("wasm metadata").len() > 0);
}
