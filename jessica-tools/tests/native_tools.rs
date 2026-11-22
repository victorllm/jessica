//! Behaviour tests for the workspace-confined file tools.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_core::tool::Tool;
use jessica_tools::{MAX_READ_FILE_BYTES, ReadFileTool, WriteFileTool};
use serde_json::json;

static NEXT_WORKSPACE: AtomicU64 = AtomicU64::new(0);

struct TempWorkspace {
    base: PathBuf,
    root: PathBuf,
    outside: PathBuf,
}

impl TempWorkspace {
    fn new(label: &str) -> Self {
        let id = NEXT_WORKSPACE.fetch_add(1, Ordering::Relaxed);
        let base =
            std::env::temp_dir().join(format!("jessica-tools-{}-{label}-{id}", std::process::id()));
        let root = base.join("root");
        let outside = base.join("outside");
        std::fs::create_dir_all(&root).expect("workspace root should be created");
        std::fs::create_dir_all(&outside).expect("outside directory should be created");
        Self {
            base,
            root,
            outside,
        }
    }

    fn write(&self, relative: &str, content: &[u8]) -> PathBuf {
        let path = self.root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("parent directory should be created");
        }
        std::fs::write(&path, content).expect("fixture file should be written");
        path
    }

    fn outside_file(&self, name: &str, content: &[u8]) -> PathBuf {
        let path = self.outside.join(name);
        std::fs::write(&path, content).expect("outside fixture should be written");
        path
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

#[tokio::test]
async fn read_file_returns_utf8_content() {
    let workspace = TempWorkspace::new("read-ok");
    workspace.write("hello.txt", b"hello\nworld");

    let tool = ReadFileTool::new(workspace.root.clone());
    let result = tool.execute(json!({"path": "hello.txt"})).await;

    assert!(!result.is_error, "unexpected error: {}", result.content);
    assert_eq!(result.content, "1\thello\n2\tworld\n");
}

#[tokio::test]
async fn read_file_rejects_parent_traversal() {
    let workspace = TempWorkspace::new("read-traversal");
    workspace.outside_file("classified.txt", b"top-secret-value");

    let tool = ReadFileTool::new(workspace.root.clone());
    let result = tool
        .execute(json!({"path": "../outside/classified.txt"}))
        .await;

    assert!(result.is_error);
    assert!(result.content.contains("outside the workspace"));
    assert!(!result.content.contains("top-secret-value"));
}

#[tokio::test]
async fn read_file_rejects_absolute_path_outside_root() {
    let workspace = TempWorkspace::new("read-absolute");
    let outside = workspace.outside_file("classified.txt", b"top-secret-value");

    let tool = ReadFileTool::new(workspace.root.clone());
    let result = tool
        .execute(json!({"path": outside.to_string_lossy()}))
        .await;

    assert!(result.is_error);
    assert!(result.content.contains("outside the workspace"));
    assert!(!result.content.contains("top-secret-value"));
}

#[tokio::test]
async fn read_file_rejects_invalid_utf8() {
    let workspace = TempWorkspace::new("read-binary");
    workspace.write("binary.bin", &[0xff, 0xfe, b'a']);

    let tool = ReadFileTool::new(workspace.root.clone());
    let result = tool.execute(json!({"path": "binary.bin"})).await;

    assert!(result.is_error);
    assert!(result.content.contains("not valid UTF-8"));
}

#[tokio::test]
async fn read_file_rejects_oversized_files() {
    let workspace = TempWorkspace::new("read-large");
    let content = vec![b'x'; MAX_READ_FILE_BYTES as usize + 1];
    workspace.write("large.txt", &content);

    let tool = ReadFileTool::new(workspace.root.clone());
    let result = tool.execute(json!({"path": "large.txt"})).await;

    assert!(result.is_error);
    assert!(result.content.contains("too large"));
}

#[tokio::test]
async fn write_file_writes_inside_root() {
    let workspace = TempWorkspace::new("write-ok");
    std::fs::create_dir_all(workspace.root.join("nested")).expect("nested directory should exist");

    let tool = WriteFileTool::new(workspace.root.clone());
    let result = tool
        .execute(json!({"path": "nested/new.txt", "content": "hello"}))
        .await;

    assert!(!result.is_error, "unexpected error: {}", result.content);
    assert!(result.content.contains("wrote 5 bytes"));
    assert_eq!(
        std::fs::read_to_string(workspace.root.join("nested/new.txt"))
            .expect("written file should be readable"),
        "hello"
    );
}

#[tokio::test]
async fn write_file_rejects_parent_traversal() {
    let workspace = TempWorkspace::new("write-traversal");

    let tool = WriteFileTool::new(workspace.root.clone());
    let result = tool
        .execute(json!({"path": "../outside/escaped.txt", "content": "no"}))
        .await;

    assert!(result.is_error);
    assert!(result.content.contains("outside the workspace"));
    assert!(!workspace.outside.join("escaped.txt").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn write_file_rejects_symlink_escape() {
    let workspace = TempWorkspace::new("write-symlink");
    let target = workspace.outside_file("target.txt", b"original");
    let link = workspace.root.join("link.txt");
    tokio::fs::symlink(&target, &link)
        .await
        .expect("symlink should be created");

    let tool = WriteFileTool::new(workspace.root.clone());
    let result = tool
        .execute(json!({"path": "link.txt", "content": "overwritten"}))
        .await;

    assert!(result.is_error);
    assert!(result.content.contains("outside the workspace"));
    assert_eq!(
        std::fs::read_to_string(&target).expect("target should be readable"),
        "original"
    );
}

#[tokio::test]
async fn write_file_reports_a_missing_parent_directory() {
    let workspace = TempWorkspace::new("write-missing-parent");

    let tool = WriteFileTool::new(workspace.root.clone());
    let result = tool
        .execute(json!({"path": "missing/new.txt", "content": "hello"}))
        .await;

    assert!(result.is_error);
    assert!(result.content.contains("parent"));
}

#[test]
fn file_tools_expose_function_schemas() {
    let workspace = TempWorkspace::new("schemas");
    let read = ReadFileTool::new(workspace.root.clone());
    let write = WriteFileTool::new(workspace.root.clone());

    assert_eq!(read.schema()["function"]["name"], "read_file");
    assert_eq!(
        read.schema()["function"]["parameters"]["required"],
        json!(["path"])
    );
    assert_eq!(write.schema()["function"]["name"], "write_file");
    assert_eq!(
        write.schema()["function"]["parameters"]["required"],
        json!(["path", "content"])
    );
}
