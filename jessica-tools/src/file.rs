//! Workspace-confined file tools.
//!
//! Both tools resolve paths against a workspace root and reject anything whose
//! canonical location escapes it. `write_file` resolves the parent for new
//! files and follows existing symlinks before checking. A time-of-check /
//! time-of-use race remains possible; Phase 2 policy and Phase 3 sandboxing add
//! further containment.

use std::path::PathBuf;

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::workspace::{canonical_root, resolve_existing, resolve_for_write};
use crate::{failure, success};

/// Default number of lines returned when `offset` is supplied without `limit`.
pub const DEFAULT_READ_FILE_LINES: u64 = 2_000;

/// Maximum number of lines one `read_file` call may request.
pub const MAX_READ_FILE_LINES: u64 = 10_000;

/// Largest file `read_file` will scan, in bytes.
///
/// Line ranges are counted from the start of the file, so a bounded scan keeps
/// memory and latency predictable. Larger files should be searched with
/// `grep` or read in a later phase that supports random-access offsets.
pub const MAX_READ_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Reads UTF-8 text files inside a workspace.
///
/// Output is always line-numbered. `offset` is a 1-based line number and
/// `limit` is a maximum line count, so reading a range never depends on byte
/// positions inside multi-byte characters. Binary files (those containing NUL
/// bytes) and invalid UTF-8 are rejected. Large selected ranges are handled by
/// the host result manager, not silently dropped here.
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
    offset: Option<u64>,
    limit: Option<u64>,
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
                "description": "Read lines from a UTF-8 text file inside the workspace. Output is line-numbered; binary files and invalid UTF-8 are rejected.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path relative to the workspace root."
                        },
                        "offset": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "First line to return, 1-based. Defaults to 1."
                        },
                        "limit": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAX_READ_FILE_LINES,
                            "description": format!(
                                "Maximum number of lines to return, at most {MAX_READ_FILE_LINES}. Defaults to {DEFAULT_READ_FILE_LINES}."
                            )
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
        match self
            .read(&args.path, args.offset.unwrap_or(1), args.limit)
            .await
        {
            Ok(content) => success(content),
            Err(message) => failure(message),
        }
    }
}

impl ReadFileTool {
    async fn read(
        &self,
        requested: &str,
        offset: u64,
        requested_limit: Option<u64>,
    ) -> Result<String, String> {
        if offset == 0 {
            return Err("read_file offset must be at least 1".to_owned());
        }
        let limit = requested_limit.unwrap_or(DEFAULT_READ_FILE_LINES);
        if limit == 0 || limit > MAX_READ_FILE_LINES {
            return Err(format!(
                "read_file limit must be between 1 and {MAX_READ_FILE_LINES}"
            ));
        }

        let root = canonical_root(&self.root).await?;
        let path = resolve_existing(&root, requested).await?;

        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|error| format!("failed to inspect `{requested}`: {error}"))?;
        if !metadata.is_file() {
            return Err(format!("`{requested}` is not a regular file"));
        }
        if metadata.len() > MAX_READ_FILE_BYTES {
            return Err(format!(
                "file `{requested}` is too large to scan ({} bytes, limit {} bytes); use grep or request a smaller file",
                metadata.len(),
                MAX_READ_FILE_BYTES
            ));
        }

        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|error| format!("failed to read `{requested}`: {error}"))?;
        let mut reader = BufReader::new(file);
        let mut selected: Vec<(u64, String)> = Vec::new();
        let mut line_number = 0_u64;
        let mut buffer = Vec::new();
        let mut has_more = false;

        loop {
            buffer.clear();
            let read = reader
                .read_until(b'\n', &mut buffer)
                .await
                .map_err(|error| format!("failed to read `{requested}`: {error}"))?;
            if read == 0 {
                break;
            }
            line_number += 1;
            if buffer.contains(&0) {
                return Err(format!("file `{requested}` is binary (contains NUL bytes)"));
            }
            if line_number < offset {
                continue;
            }
            if selected.len() as u64 >= limit {
                has_more = true;
                break;
            }
            let text = std::str::from_utf8(trim_line_ending(&buffer))
                .map_err(|_| format!("file `{requested}` is not valid UTF-8"))?;
            selected.push((line_number, text.to_owned()));
        }

        if selected.is_empty() && offset > line_number.max(1) {
            return Err(format!(
                "read_file offset {offset} is beyond the end of `{requested}` ({line_number} lines)"
            ));
        }

        let mut rendered = String::new();
        for (number, text) in &selected {
            rendered.push_str(&number.to_string());
            rendered.push('\t');
            rendered.push_str(text);
            rendered.push('\n');
        }
        if has_more {
            let next = selected
                .last()
                .map_or(offset, |(number, _)| number.saturating_add(1));
            rendered.push_str(&format!(
                "[{limit} lines shown; more lines available; call read_file with offset={next}]\n"
            ));
        }
        Ok(rendered)
    }
}

/// Removes one trailing `\n` or `\r\n` from a line read by `read_until`.
fn trim_line_ending(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;
    use serde_json::json;

    /// Creates a tool over a temporary workspace and writes `name`.
    fn workspace_file(label: &str, name: &str, bytes: &[u8]) -> (TempDir, ReadFileTool) {
        let temp = TempDir::new(label);
        std::fs::write(temp.path().join(name), bytes).expect("test file must be written");
        let tool = ReadFileTool::new(temp.path());
        (temp, tool)
    }

    #[tokio::test]
    async fn a_line_range_returns_one_based_line_numbers() {
        let (_temp, tool) = workspace_file("range", "lines.txt", b"alpha\nbeta\ngamma\n");

        let result = tool
            .execute(json!({"path": "lines.txt", "offset": 2, "limit": 2}))
            .await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(result.content, "2\tbeta\n3\tgamma\n");
    }

    #[tokio::test]
    async fn a_limited_range_reports_the_next_offset_when_more_lines_exist() {
        let (_temp, tool) =
            workspace_file("more-lines", "lines.txt", b"one\ntwo\nthree\nfour\nfive\n");

        let result = tool
            .execute(json!({"path": "lines.txt", "offset": 1, "limit": 2}))
            .await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(result.content.starts_with("1\tone\n2\ttwo\n"));
        assert!(
            result
                .content
                .contains("more lines available; call read_file with offset=3"),
            "the range must advertise its continuation: {}",
            result.content
        );
        assert!(
            !result.content.contains("three"),
            "a limited range must not include later lines: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn an_offset_beyond_eof_is_rejected_with_the_line_count() {
        let (_temp, tool) = workspace_file("beyond", "lines.txt", b"only\n");

        let result = tool
            .execute(json!({"path": "lines.txt", "offset": 9}))
            .await;

        assert!(result.is_error, "an out-of-range offset must fail");
        assert!(
            result.content.contains("beyond the end") && result.content.contains("1 lines"),
            "the error must explain the range: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn an_empty_file_is_read_successfully_at_offset_one() {
        let (_temp, tool) = workspace_file("empty", "empty.txt", b"");

        let result = tool.execute(json!({"path": "empty.txt"})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(result.content, "");
    }

    #[tokio::test]
    async fn binary_files_are_rejected_instead_of_rendered() {
        let (_temp, tool) = workspace_file("binary", "binary.bin", b"before\0after");

        let result = tool.execute(json!({"path": "binary.bin"})).await;

        assert!(result.is_error, "a binary file must fail");
        assert!(
            result.content.contains("binary") && result.content.contains("NUL"),
            "the error must explain binary detection: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn invalid_utf8_is_rejected() {
        let (_temp, tool) = workspace_file("utf8", "invalid.txt", &[0xff, 0xfe, b'a']);

        let result = tool.execute(json!({"path": "invalid.txt"})).await;

        assert!(result.is_error, "invalid UTF-8 must fail");
        assert!(
            result.content.contains("not valid UTF-8"),
            "the error must name the encoding: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn a_path_outside_the_workspace_is_rejected() {
        let temp = TempDir::new("escape-read");
        let outside = TempDir::new("escape-target");
        std::fs::write(outside.path().join("secret.txt"), "secret")
            .expect("outside file must be written");
        let tool = ReadFileTool::new(temp.path());

        let result = tool
            .execute(json!({"path": outside.path().join("secret.txt")}))
            .await;

        assert!(result.is_error, "an outside path must fail");
        assert!(
            result.content.contains("outside the workspace"),
            "the error must name the boundary: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn limit_zero_or_above_the_cap_is_rejected() {
        let (_temp, tool) = workspace_file("limits", "lines.txt", b"line\n");

        for limit in [0_u64, MAX_READ_FILE_LINES + 1] {
            let result = tool
                .execute(json!({"path": "lines.txt", "limit": limit}))
                .await;
            assert!(result.is_error, "limit {limit} must fail");
            assert!(
                result
                    .content
                    .contains(&format!("between 1 and {MAX_READ_FILE_LINES}")),
                "the error must state the accepted range: {}",
                result.content
            );
        }
    }
}
