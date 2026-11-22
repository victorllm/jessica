//! Exact-string editing with explicit occurrence counts.

use std::path::PathBuf;

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::atomic::atomic_replace_existing;
use crate::workspace::{canonical_root, resolve_existing};
use crate::{failure, success};

/// Largest file `edit_file` will load into memory, in bytes.
pub const MAX_EDIT_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Replaces exact text in one UTF-8 file inside a workspace.
///
/// The replacement is literal, not a regular expression. When the target text
/// occurs more than once, the caller must pass `expected_occurrences`; the tool
/// never guesses which occurrence to edit.
pub struct EditFileTool {
    root: PathBuf,
}

impl EditFileTool {
    /// Creates an edit tool rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[derive(Deserialize)]
struct EditFileArgs {
    path: String,
    old: String,
    new: String,
    expected_occurrences: Option<usize>,
}

#[async_trait]
impl Tool for EditFileTool {
    fn name(&self) -> &str {
        "edit_file"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "edit_file",
                "description": "Replace exact text in one existing UTF-8 file inside the workspace. Replacement is literal. When the old text occurs more than once, expected_occurrences must name the exact count; otherwise the edit is rejected.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path relative to the workspace root."
                        },
                        "old": {
                            "type": "string",
                            "description": "Exact text to replace."
                        },
                        "new": {
                            "type": "string",
                            "description": "Replacement text."
                        },
                        "expected_occurrences": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Required when `old` occurs more than once; must equal the actual occurrence count."
                        }
                    },
                    "required": ["path", "old", "new"]
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let args: EditFileArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return failure(format!("invalid edit_file arguments: {error}")),
        };
        match self
            .edit(&args.path, &args.old, &args.new, args.expected_occurrences)
            .await
        {
            Ok(message) => success(message),
            Err(message) => failure(message),
        }
    }
}

impl EditFileTool {
    async fn edit(
        &self,
        requested: &str,
        old: &str,
        new: &str,
        expected: Option<usize>,
    ) -> Result<String, String> {
        if old.is_empty() {
            return Err("edit_file `old` must not be empty".to_owned());
        }
        if expected == Some(0) {
            return Err("edit_file expected_occurrences must be at least 1".to_owned());
        }

        let root = canonical_root(&self.root).await?;
        let path = resolve_existing(&root, requested).await?;
        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|error| format!("failed to inspect `{requested}`: {error}"))?;
        if !metadata.is_file() {
            return Err(format!("`{requested}` is not a regular file"));
        }
        if metadata.len() > MAX_EDIT_FILE_BYTES {
            return Err(format!(
                "file `{requested}` is too large to edit ({} bytes, limit {MAX_EDIT_FILE_BYTES} bytes)",
                metadata.len()
            ));
        }

        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|error| format!("failed to read `{requested}`: {error}"))?;
        let content = String::from_utf8(bytes)
            .map_err(|_| format!("file `{requested}` is not valid UTF-8"))?;
        let occurrences = content.match_indices(old).count();
        if occurrences == 0 {
            return Err(format!("edit_file text was not found in `{requested}`"));
        }
        match expected {
            Some(expected) if expected != occurrences => {
                return Err(format!(
                    "edit_file found {occurrences} occurrences in `{requested}` but expected {expected}"
                ));
            }
            None if occurrences != 1 => {
                return Err(format!(
                    "edit_file found {occurrences} occurrences in `{requested}`; pass expected_occurrences to replace more than one"
                ));
            }
            _ => {}
        }

        let updated = content.replacen(old, new, occurrences);
        atomic_replace_existing(&self.root, requested, updated.as_bytes()).await?;
        Ok(format!(
            "replaced {occurrences} occurrence(s) in `{requested}`"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    /// Writes one workspace file and returns a tool for it.
    fn setup(label: &str, name: &str, content: &str) -> (TempDir, EditFileTool) {
        let temp = TempDir::new(label);
        std::fs::write(temp.path().join(name), content).expect("test file must be written");
        let tool = EditFileTool::new(temp.path());
        (temp, tool)
    }

    async fn run(
        tool: &EditFileTool,
        path: &str,
        old: &str,
        new: &str,
        expected: Option<usize>,
    ) -> ToolResult {
        tool.execute(json!({
            "path": path,
            "old": old,
            "new": new,
            "expected_occurrences": expected,
        }))
        .await
    }

    #[tokio::test]
    async fn a_single_occurrence_is_replaced_without_an_explicit_count() {
        let (temp, tool) = setup("single", "greeting.txt", "hello world\n");

        let result = run(&tool, "greeting.txt", "world", "jessica", None).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(
            std::fs::read_to_string(temp.path().join("greeting.txt"))
                .expect("edited file must remain readable"),
            "hello jessica\n"
        );
        assert!(
            result.content.contains("replaced 1 occurrence"),
            "the result must report the count: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn a_missing_occurrence_is_rejected_without_modifying_the_file() {
        let (temp, tool) = setup("missing", "greeting.txt", "hello world\n");

        let result = run(&tool, "greeting.txt", "absent", "replacement", None).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("not found"),
            "the failure must explain the missing text: {}",
            result.content
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("greeting.txt"))
                .expect("file must remain readable"),
            "hello world\n"
        );
    }

    #[tokio::test]
    async fn multiple_occurrences_require_an_explicit_count() {
        let (temp, tool) = setup("many", "notes.txt", "alpha beta alpha gamma alpha\n");

        let rejected = run(&tool, "notes.txt", "alpha", "A", None).await;
        assert!(rejected.is_error);
        assert!(
            rejected.content.contains("3 occurrences"),
            "the failure must report the observed count: {}",
            rejected.content
        );
        assert!(
            rejected.content.contains("expected_occurrences"),
            "the failure must tell the caller how to disambiguate: {}",
            rejected.content
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("notes.txt"))
                .expect("file must remain readable"),
            "alpha beta alpha gamma alpha\n"
        );

        let accepted = run(&tool, "notes.txt", "alpha", "A", Some(3)).await;
        assert!(
            !accepted.is_error,
            "unexpected failure: {}",
            accepted.content
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("notes.txt"))
                .expect("edited file must remain readable"),
            "A beta A gamma A\n"
        );
    }

    #[tokio::test]
    async fn an_explicit_count_that_does_not_match_is_rejected() {
        let (temp, tool) = setup("mismatch", "notes.txt", "alpha alpha\n");

        let result = run(&tool, "notes.txt", "alpha", "A", Some(3)).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("found 2 occurrences") && result.content.contains("expected 3"),
            "the failure must report both counts: {}",
            result.content
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("notes.txt"))
                .expect("file must remain readable"),
            "alpha alpha\n"
        );
    }

    #[tokio::test]
    async fn multibyte_replacements_preserve_utf8_boundaries() {
        let (temp, tool) = setup("utf8", "text.txt", "前缀🙂中缀🙂后缀");

        let result = run(&tool, "text.txt", "🙂中缀🙂", "🚀", None).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        let updated = std::fs::read(temp.path().join("text.txt")).expect("file must exist");
        assert_eq!(
            String::from_utf8(updated).expect("the file must remain valid UTF-8"),
            "前缀🚀后缀"
        );
    }

    #[tokio::test]
    async fn invalid_utf8_is_rejected_without_modifying_the_file() {
        let temp = TempDir::new("invalid-utf8");
        let original = [0xff, 0xfe, b'a'];
        std::fs::write(temp.path().join("binary.txt"), original).expect("file must exist");
        let tool = EditFileTool::new(temp.path());

        let result = run(&tool, "binary.txt", "a", "b", None).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("not valid UTF-8"),
            "the failure must explain the encoding: {}",
            result.content
        );
        assert_eq!(
            std::fs::read(temp.path().join("binary.txt")).expect("file must exist"),
            original
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_escape_is_rejected() {
        use std::os::unix::fs::symlink;

        let workspace = TempDir::new("edit-escape");
        let outside = TempDir::new("edit-escape-outside");
        std::fs::write(outside.path().join("secret.txt"), "outside\n").expect("file must exist");
        symlink(outside.path(), workspace.path().join("link")).expect("symlink must be created");
        let tool = EditFileTool::new(workspace.path());

        let result = run(&tool, "link/secret.txt", "outside", "changed", None).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("outside the workspace"),
            "the failure must reject the escape: {}",
            result.content
        );
        assert_eq!(
            std::fs::read_to_string(outside.path().join("secret.txt"))
                .expect("outside file must remain readable"),
            "outside\n"
        );
    }

    #[tokio::test]
    async fn a_successful_edit_leaves_no_temporary_file_behind() {
        let (temp, tool) = setup("cleanup", "file.txt", "before\n");

        let result = run(&tool, "file.txt", "before", "after", None).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        let names: Vec<String> = std::fs::read_dir(temp.path())
            .expect("workspace must be readable")
            .map(|entry| {
                entry
                    .expect("directory entry must be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(names, vec!["file.txt".to_owned()]);
    }
}
