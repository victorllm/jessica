//! Workspace-confined file tools.
//!
//! Both tools resolve paths against a workspace root and reject anything whose
//! canonical location escapes it. `write_file` resolves the parent for new
//! files and follows existing symlinks before checking. A time-of-check /
//! time-of-use race remains possible; Phase 2 policy and Phase 3 sandboxing add
//! further containment.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{failure, success};

/// Largest file `read_file` will return, in bytes.
const MAX_READ_BYTES: u64 = 256 * 1024;

/// Reads UTF-8 text files inside a workspace.
pub struct ReadFileTool {
    root: PathBuf,
}

impl ReadFileTool {
    /// Creates a read tool rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[derive(Deserialize)]
struct ReadFileArgs {
    path: String,
}

#[async_trait]
impl Tool for ReadFileTool {
    fn name(&self) -> &str {
        "read_file"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a UTF-8 text file inside the workspace.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path relative to the workspace root."
                        }
                    },
                    "required": ["path"]
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let args: ReadFileArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return failure(format!("invalid read_file arguments: {error}")),
        };
        match self.read(&args.path).await {
            Ok(content) => success(content),
            Err(message) => failure(message),
        }
    }
}

impl ReadFileTool {
    async fn read(&self, requested: &str) -> Result<String, String> {
        let root = canonical_root(&self.root).await?;
        let path = resolve_existing(&root, requested).await?;

        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|error| format!("failed to inspect `{requested}`: {error}"))?;
        if !metadata.is_file() {
            return Err(format!("`{requested}` is not a regular file"));
        }
        if metadata.len() > MAX_READ_BYTES {
            return Err(format!(
                "file `{requested}` is too large ({} bytes, limit {} bytes)",
                metadata.len(),
                MAX_READ_BYTES
            ));
        }

        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|error| format!("failed to read `{requested}`: {error}"))?;
        String::from_utf8(bytes).map_err(|_| format!("file `{requested}` is not valid UTF-8"))
    }
}

/// Writes UTF-8 text files inside a workspace.
pub struct WriteFileTool {
    root: PathBuf,
}

impl WriteFileTool {
    /// Creates a write tool rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[derive(Deserialize)]
struct WriteFileArgs {
    path: String,
    content: String,
}

#[async_trait]
impl Tool for WriteFileTool {
    fn name(&self) -> &str {
        "write_file"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "write_file",
                "description": "Write a UTF-8 text file inside the workspace. The parent directory must exist.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path relative to the workspace root."
                        },
                        "content": {
                            "type": "string",
                            "description": "Full file contents to write."
                        }
                    },
                    "required": ["path", "content"]
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let args: WriteFileArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return failure(format!("invalid write_file arguments: {error}")),
        };
        match self.write(&args.path, args.content).await {
            Ok(message) => success(message),
            Err(message) => failure(message),
        }
    }
}

impl WriteFileTool {
    async fn write(&self, requested: &str, content: String) -> Result<String, String> {
        let root = canonical_root(&self.root).await?;
        let path = resolve_for_write(&root, requested).await?;
        let bytes = content.len();
        tokio::fs::write(&path, content)
            .await
            .map_err(|error| format!("failed to write `{requested}`: {error}"))?;
        Ok(format!("wrote {bytes} bytes to `{requested}`"))
    }
}

/// Resolves the workspace root to an absolute, symlink-free path.
async fn canonical_root(root: &Path) -> Result<PathBuf, String> {
    tokio::fs::canonicalize(root).await.map_err(|error| {
        format!(
            "failed to resolve workspace root `{}`: {error}",
            root.display()
        )
    })
}

/// Resolves an existing path and checks that its canonical form stays inside
/// `root`.
async fn resolve_existing(root: &Path, requested: &str) -> Result<PathBuf, String> {
    let candidate = root.join(requested);
    let canonical = tokio::fs::canonicalize(&candidate)
        .await
        .map_err(|error| format!("failed to resolve `{requested}`: {error}"))?;
    ensure_inside(root, &canonical, requested)?;
    Ok(canonical)
}

/// Resolves a write target, allowing the final component not to exist while
/// still rejecting escapes through `..` or symlinks.
async fn resolve_for_write(root: &Path, requested: &str) -> Result<PathBuf, String> {
    let candidate = root.join(requested);

    match tokio::fs::symlink_metadata(&candidate).await {
        Ok(_) => {
            let canonical = tokio::fs::canonicalize(&candidate)
                .await
                .map_err(|error| format!("failed to resolve `{requested}`: {error}"))?;
            ensure_inside(root, &canonical, requested)?;
            Ok(canonical)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = candidate
                .parent()
                .ok_or_else(|| format!("path `{requested}` has no parent"))?;
            let canonical_parent = tokio::fs::canonicalize(parent).await.map_err(|error| {
                format!("failed to resolve the parent of `{requested}`: {error}")
            })?;
            ensure_inside(root, &canonical_parent, requested)?;
            let name = candidate
                .file_name()
                .ok_or_else(|| format!("path `{requested}` has no file name"))?;
            Ok(canonical_parent.join(name))
        }
        Err(error) => Err(format!("failed to inspect `{requested}`: {error}")),
    }
}

/// Rejects canonical paths outside the workspace root.
fn ensure_inside(root: &Path, path: &Path, requested: &str) -> Result<(), String> {
    if path.starts_with(root) {
        Ok(())
    } else {
        Err(format!("path `{requested}` is outside the workspace"))
    }
}
