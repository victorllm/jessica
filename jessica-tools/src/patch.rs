//! Minimal single-file unified-diff application.
//!
//! The parser intentionally supports a small, explicit subset: one existing
//! UTF-8 regular file per call, LF line endings, `---`/`+++` headers, and
//! standard `@@ -old +new @@` hunks. Renames, file creation or deletion,
//! binary patches, and `\ No newline at end of file` markers are rejected.
//! Everything is validated before the atomic replacement starts.

use std::path::PathBuf;

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::atomic::atomic_replace_existing;
use crate::edit::MAX_EDIT_FILE_BYTES;
use crate::workspace::{canonical_root, resolve_existing};
use crate::{failure, success};

/// Largest patch accepted by `apply_patch`, in bytes.
pub const MAX_PATCH_BYTES: usize = 1024 * 1024;

/// Applies one minimal unified diff to an existing workspace file.
///
/// The tool parses and validates the entire patch first. With `dry_run` it
/// stops after validation; otherwise it computes the complete new file and
/// replaces the target with one atomic rename. A malformed patch or a context
/// mismatch leaves the original file untouched.
pub struct ApplyPatchTool {
    root: PathBuf,
}

impl ApplyPatchTool {
    /// Creates an apply-patch tool rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

/// Facts about a parsed minimal unified diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchSummary {
    /// Workspace-relative target path with any `a/` or `b/` prefix removed.
    pub path: String,
    /// Number of hunks in the patch.
    pub hunks: usize,
    /// Number of added lines across all hunks.
    pub lines_added: usize,
    /// Number of removed lines across all hunks.
    pub lines_removed: usize,
}

/// Parses a minimal unified diff and returns its target and line counts.
///
/// This function has no filesystem side effects. It is used by policy and
/// approval summaries, which need the target path without applying the patch.
pub fn summarize_patch(input: &str) -> Result<PatchSummary, String> {
    let parsed = parse_patch(input)?;
    Ok(PatchSummary {
        path: parsed.path,
        hunks: parsed.hunks.len(),
        lines_added: parsed.lines_added,
        lines_removed: parsed.lines_removed,
    })
}

#[derive(Deserialize)]
struct ApplyPatchArgs {
    patch: String,
    #[serde(default)]
    dry_run: bool,
}

#[async_trait]
impl Tool for ApplyPatchTool {
    fn name(&self) -> &str {
        "apply_patch"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "apply_patch",
                "description": "Apply a minimal unified diff to one existing UTF-8 file inside the workspace. Supports one file with standard ---/+++ headers and @@ hunks, LF line endings, and modifications or insertions. Renames, file creation/deletion, binary patches, and no-newline markers are rejected. Set dry_run to validate without writing.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "patch": {
                            "type": "string",
                            "description": "Unified diff for one file."
                        },
                        "dry_run": {
                            "type": "boolean",
                            "description": "Validate and compute the edit without replacing the file."
                        }
                    },
                    "required": ["patch"]
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let args: ApplyPatchArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return failure(format!("invalid apply_patch arguments: {error}")),
        };
        match self.apply(&args.patch, args.dry_run).await {
            Ok(message) => success(message),
            Err(message) => failure(message),
        }
    }
}

impl ApplyPatchTool {
    async fn apply(&self, patch: &str, dry_run: bool) -> Result<String, String> {
        let parsed = parse_patch(patch)?;
        let root = canonical_root(&self.root).await?;
        let path = resolve_existing(&root, &parsed.path).await?;
        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|error| format!("failed to inspect `{}`: {error}", parsed.path))?;
        if !metadata.is_file() {
            return Err(format!("`{}` is not a regular file", parsed.path));
        }
        if metadata.len() > MAX_EDIT_FILE_BYTES {
            return Err(format!(
                "file `{}` is too large to patch ({} bytes, limit {MAX_EDIT_FILE_BYTES} bytes)",
                parsed.path,
                metadata.len()
            ));
        }
        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|error| format!("failed to read `{}`: {error}", parsed.path))?;
        let content = String::from_utf8(bytes)
            .map_err(|_| format!("file `{}` is not valid UTF-8", parsed.path))?;
        let updated = parsed.apply(&content)?;
        let summary = format!(
            "{} hunk(s), +{} -{}",
            parsed.hunks.len(),
            parsed.lines_added,
            parsed.lines_removed
        );
        if dry_run {
            return Ok(format!(
                "dry run: patch for `{}` is valid ({summary})",
                parsed.path
            ));
        }
        atomic_replace_existing(&self.root, &parsed.path, updated.as_bytes()).await?;
        Ok(format!("applied patch to `{}` ({summary})", parsed.path))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineKind {
    Context,
    Remove,
    Add,
}

#[derive(Debug)]
struct PatchLine {
    kind: LineKind,
    text: String,
}

#[derive(Debug)]
struct Hunk {
    old_start: usize,
    old_count: usize,
    lines: Vec<PatchLine>,
}

#[derive(Debug)]
struct ParsedPatch {
    path: String,
    hunks: Vec<Hunk>,
    lines_added: usize,
    lines_removed: usize,
}

impl ParsedPatch {
    /// Applies the already-validated hunks to `original`.
    fn apply(&self, original: &str) -> Result<String, String> {
        let ends_with_newline = original.ends_with('\n');
        let original_lines: Vec<&str> = if original.is_empty() {
            Vec::new()
        } else {
            let mut lines: Vec<&str> = original.split('\n').collect();
            if ends_with_newline {
                lines.pop();
            }
            lines
        };

        let mut output: Vec<String> = Vec::new();
        let mut cursor = 0_usize;
        for hunk in &self.hunks {
            let index = if hunk.old_count == 0 {
                hunk.old_start
            } else {
                hunk.old_start.saturating_sub(1)
            };
            if index < cursor {
                return Err(format!(
                    "patch hunks for `{}` overlap or are out of order",
                    self.path
                ));
            }
            if index > original_lines.len() {
                return Err(format!(
                    "patch hunk starts beyond the end of `{}` (line {})",
                    self.path, hunk.old_start
                ));
            }
            output.extend(
                original_lines[cursor..index]
                    .iter()
                    .map(|line| (*line).to_owned()),
            );
            let mut position = index;
            for line in &hunk.lines {
                match line.kind {
                    LineKind::Context => {
                        let actual = original_lines.get(position).ok_or_else(|| {
                            format!(
                                "patch context for `{}` extends beyond the end of the file",
                                self.path
                            )
                        })?;
                        if *actual != line.text {
                            return Err(format!(
                                "patch context mismatch in `{}` at line {}: expected `{}`, found `{}`",
                                self.path,
                                position + 1,
                                line.text,
                                actual
                            ));
                        }
                        output.push(line.text.clone());
                        position += 1;
                    }
                    LineKind::Remove => {
                        let actual = original_lines.get(position).ok_or_else(|| {
                            format!(
                                "patch removal for `{}` extends beyond the end of the file",
                                self.path
                            )
                        })?;
                        if *actual != line.text {
                            return Err(format!(
                                "patch removal mismatch in `{}` at line {}: expected `{}`, found `{}`",
                                self.path,
                                position + 1,
                                line.text,
                                actual
                            ));
                        }
                        position += 1;
                    }
                    LineKind::Add => output.push(line.text.clone()),
                }
            }
            cursor = position;
        }
        output.extend(
            original_lines[cursor..]
                .iter()
                .map(|line| (*line).to_owned()),
        );

        let mut updated = output.join("\n");
        if ends_with_newline {
            updated.push('\n');
        }
        Ok(updated)
    }
}

/// Parses and validates a minimal single-file unified diff.
fn parse_patch(input: &str) -> Result<ParsedPatch, String> {
    if input.trim().is_empty() {
        return Err("apply_patch requires a non-empty unified diff".to_owned());
    }
    if input.len() > MAX_PATCH_BYTES {
        return Err(format!(
            "apply_patch input is too large ({} bytes, limit {MAX_PATCH_BYTES} bytes)",
            input.len()
        ));
    }
    let mut lines: Vec<&str> = input.split('\n').collect();
    if lines.last() == Some(&"") {
        lines.pop();
    }
    if lines.iter().any(|line| line.contains('\r')) {
        return Err(
            "apply_patch supports LF line endings only; CRLF patches are rejected".to_owned(),
        );
    }

    let mut index = 0_usize;
    while index < lines.len() {
        let line = lines[index];
        if line.is_empty() || line.starts_with("diff --git ") || line.starts_with("index ") {
            index += 1;
            continue;
        }
        break;
    }

    let old_raw = lines
        .get(index)
        .and_then(|line| line.strip_prefix("--- "))
        .ok_or_else(|| "apply_patch requires a `--- path` header".to_owned())?;
    index += 1;
    let new_raw = lines
        .get(index)
        .and_then(|line| line.strip_prefix("+++ "))
        .ok_or_else(|| "apply_patch requires a `+++ path` header".to_owned())?;
    index += 1;

    let old_path = normalize_path(old_raw)?;
    let new_path = normalize_path(new_raw)?;
    let path = if let (Some(old), Some(new)) =
        (old_path.strip_prefix("a/"), new_path.strip_prefix("b/"))
    {
        if old != new {
            return Err(
                "apply_patch does not support renames; the `---` and `+++` paths must match"
                    .to_owned(),
            );
        }
        new.to_owned()
    } else if old_path == new_path {
        old_path
    } else {
        return Err(
            "apply_patch requires matching relative paths in the `---` and `+++` headers"
                .to_owned(),
        );
    };

    let mut hunks = Vec::new();
    let mut lines_added = 0_usize;
    let mut lines_removed = 0_usize;
    while index < lines.len() {
        let line = lines[index];
        if line.is_empty() {
            index += 1;
            continue;
        }
        if line.starts_with("--- ") {
            return Err("apply_patch supports exactly one file per patch".to_owned());
        }
        let (old_start, old_count, new_count) = parse_hunk_header(line, &path)?;
        index += 1;

        let mut body = Vec::new();
        let mut old_seen = 0_usize;
        let mut new_seen = 0_usize;
        while old_seen < old_count || new_seen < new_count {
            let body_line = lines.get(index).ok_or_else(|| {
                format!(
                    "patch hunk for `{path}` is truncated: fewer lines than its header declares"
                )
            })?;
            if body_line.starts_with("@@ -") {
                return Err(format!(
                    "patch hunk for `{path}` ended before its declared line counts"
                ));
            }
            let (kind, text) = match body_line.as_bytes().first() {
                Some(b' ') => (LineKind::Context, &body_line[1..]),
                Some(b'-') => (LineKind::Remove, &body_line[1..]),
                Some(b'+') => (LineKind::Add, &body_line[1..]),
                Some(b'\\') => {
                    return Err(
                        "apply_patch does not support `\\ No newline at end of file` markers"
                            .to_owned(),
                    );
                }
                _ => {
                    return Err(format!(
                        "apply_patch encountered an invalid hunk line for `{path}`: `{body_line}`"
                    ));
                }
            };
            match kind {
                LineKind::Context => {
                    if old_seen >= old_count || new_seen >= new_count {
                        return Err(format!(
                            "patch hunk for `{path}` has more context lines than its header declares"
                        ));
                    }
                    old_seen += 1;
                    new_seen += 1;
                }
                LineKind::Remove => {
                    if old_seen >= old_count {
                        return Err(format!(
                            "patch hunk for `{path}` has more removed lines than its header declares"
                        ));
                    }
                    old_seen += 1;
                    lines_removed += 1;
                }
                LineKind::Add => {
                    if new_seen >= new_count {
                        return Err(format!(
                            "patch hunk for `{path}` has more added lines than its header declares"
                        ));
                    }
                    new_seen += 1;
                    lines_added += 1;
                }
            }
            body.push(PatchLine {
                kind,
                text: text.to_owned(),
            });
            index += 1;
        }
        if old_seen != old_count || new_seen != new_count {
            return Err(format!(
                "patch hunk for `{path}` does not match its declared line counts"
            ));
        }
        hunks.push(Hunk {
            old_start,
            old_count,
            lines: body,
        });
    }

    if hunks.is_empty() {
        return Err(format!("patch for `{path}` contains no hunks"));
    }

    Ok(ParsedPatch {
        path,
        hunks,
        lines_added,
        lines_removed,
    })
}

/// Parses `@@ -start[,count] +start[,count] @@`.
fn parse_hunk_header(line: &str, path: &str) -> Result<(usize, usize, usize), String> {
    let rest = line
        .strip_prefix("@@ -")
        .ok_or_else(|| format!("invalid hunk header for `{path}`: `{line}`"))?;
    let (old_part, rest) = rest
        .split_once(" +")
        .ok_or_else(|| format!("invalid hunk header for `{path}`: `{line}`"))?;
    let (new_part, _) = rest
        .split_once(" @@")
        .ok_or_else(|| format!("invalid hunk header for `{path}`: `{line}`"))?;
    let (old_start, old_count) = parse_range(old_part, "old", path, line)?;
    let (new_start, new_count) = parse_range(new_part, "new", path, line)?;
    if old_start == 0 && old_count > 0 {
        return Err(format!(
            "invalid hunk header for `{path}`: old start must be at least 1 when it has lines"
        ));
    }
    if new_start == 0 && new_count > 0 {
        return Err(format!(
            "invalid hunk header for `{path}`: new start must be at least 1 when it has lines"
        ));
    }
    Ok((old_start, old_count, new_count))
}

/// Parses one `start[,count]` range.
fn parse_range(part: &str, label: &str, path: &str, line: &str) -> Result<(usize, usize), String> {
    let (start, count) = match part.split_once(',') {
        Some((start, count)) => (start, count),
        None => (part, "1"),
    };
    let start = start
        .parse::<usize>()
        .map_err(|_| format!("invalid {label} start in hunk header for `{path}`: `{line}`"))?;
    let count = count
        .parse::<usize>()
        .map_err(|_| format!("invalid {label} count in hunk header for `{path}`: `{line}`"))?;
    Ok((start, count))
}

/// Normalizes one header path, rejecting escapes and unsupported forms.
fn normalize_path(raw: &str) -> Result<String, String> {
    let path = raw.split('\t').next().unwrap_or("").trim();
    if path.is_empty() {
        return Err("apply_patch header path must not be empty".to_owned());
    }
    if path == "/dev/null" {
        return Err(
            "apply_patch does not support file creation or deletion (`/dev/null`)".to_owned(),
        );
    }
    if std::path::Path::new(path).is_absolute() {
        return Err(format!(
            "apply_patch path `{path}` must be relative to the workspace root"
        ));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(format!(
                "apply_patch path `{path}` contains an unsupported component"
            ));
        }
    }
    Ok(path.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    fn workspace_file(label: &str, content: &str) -> (TempDir, ApplyPatchTool) {
        let temp = TempDir::new(label);
        std::fs::write(temp.path().join("file.txt"), content).expect("test file must be written");
        let tool = ApplyPatchTool::new(temp.path());
        (temp, tool)
    }

    #[tokio::test]
    async fn a_matching_patch_replaces_only_the_declared_lines() {
        let (temp, tool) = workspace_file("apply", "alpha\nbeta\ngamma\n");
        let patch =
            "--- a/file.txt\n+++ b/file.txt\n@@ -1,3 +1,3 @@\n alpha\n-beta\n+BETA\n gamma\n";

        let result = tool.execute(json!({"patch": patch})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(
            std::fs::read_to_string(temp.path().join("file.txt"))
                .expect("patched file must remain readable"),
            "alpha\nBETA\ngamma\n"
        );
        assert!(
            result.content.contains("1 hunk") && result.content.contains("+1 -1"),
            "the result must report the hunk and line counts: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn a_context_mismatch_leaves_the_file_untouched() {
        let (temp, tool) = workspace_file("mismatch", "alpha\nbeta\ngamma\n");
        let patch =
            "--- a/file.txt\n+++ b/file.txt\n@@ -1,3 +1,3 @@\n alpha\n-WRONG\n+BETA\n gamma\n";

        let result = tool.execute(json!({"patch": patch})).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("mismatch"),
            "the failure must identify the mismatch: {}",
            result.content
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("file.txt"))
                .expect("file must remain readable"),
            "alpha\nbeta\ngamma\n"
        );
    }

    #[tokio::test]
    async fn dry_run_validates_without_writing() {
        let (temp, tool) = workspace_file("dry-run", "alpha\n");
        let patch = "--- a/file.txt\n+++ b/file.txt\n@@ -1 +1 @@\n-alpha\n+beta\n";

        let result = tool.execute(json!({"patch": patch, "dry_run": true})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(
            result.content.contains("dry run"),
            "the result must identify a dry run: {}",
            result.content
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("file.txt"))
                .expect("file must remain readable"),
            "alpha\n"
        );
    }

    #[tokio::test]
    async fn an_insertion_hunk_adds_lines_at_the_declared_position() {
        let (temp, tool) = workspace_file("insert", "alpha\ncharlie\n");
        let patch = "--- a/file.txt\n+++ b/file.txt\n@@ -1,0 +2,1 @@\n+bravo\n";

        let result = tool.execute(json!({"patch": patch})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(
            std::fs::read_to_string(temp.path().join("file.txt"))
                .expect("patched file must remain readable"),
            "alpha\nbravo\ncharlie\n"
        );
    }

    #[tokio::test]
    async fn a_file_without_a_trailing_newline_keeps_that_state() {
        let (temp, tool) = workspace_file("no-newline", "alpha\nbeta");
        let patch = "--- a/file.txt\n+++ b/file.txt\n@@ -1,2 +1,2 @@\n alpha\n-beta\n+BETA\n";

        let result = tool.execute(json!({"patch": patch})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(
            std::fs::read_to_string(temp.path().join("file.txt"))
                .expect("patched file must remain readable"),
            "alpha\nBETA"
        );
    }

    #[tokio::test]
    async fn a_parent_escape_is_rejected_before_any_write() {
        let temp = TempDir::new("escape");
        std::fs::write(temp.path().join("file.txt"), "inside\n").expect("file must exist");
        let patch = "--- a/../outside.txt\n+++ b/../outside.txt\n@@ -1 +1 @@\n-inside\n+changed\n";

        let result = ApplyPatchTool::new(temp.path())
            .execute(json!({"patch": patch}))
            .await;

        assert!(result.is_error);
        assert!(
            result.content.contains("unsupported component"),
            "the failure must reject the escape: {}",
            result.content
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("file.txt"))
                .expect("file must remain readable"),
            "inside\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_escape_is_rejected() {
        use std::os::unix::fs::symlink;

        let workspace = TempDir::new("patch-escape");
        let outside = TempDir::new("patch-escape-outside");
        std::fs::write(outside.path().join("secret.txt"), "outside\n").expect("file must exist");
        symlink(outside.path(), workspace.path().join("link")).expect("symlink must be created");
        let patch =
            "--- a/link/secret.txt\n+++ b/link/secret.txt\n@@ -1 +1 @@\n-outside\n+changed\n";

        let result = ApplyPatchTool::new(workspace.path())
            .execute(json!({"patch": patch}))
            .await;

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

    #[test]
    fn multiple_files_are_rejected() {
        let patch = "--- a/file.txt\n+++ b/file.txt\n@@ -1 +1 @@\n-a\n+b\n--- a/other.txt\n+++ b/other.txt\n@@ -1 +1 @@\n-c\n+d\n";

        let error = summarize_patch(patch).expect_err("multiple files must be rejected");

        assert!(
            error.contains("exactly one file"),
            "the error must explain the limit: {error}"
        );
    }

    #[test]
    fn the_summary_reports_the_target_and_line_counts() {
        let patch =
            "--- a/src/file.txt\n+++ b/src/file.txt\n@@ -1,2 +1,3 @@\n one\n-two\n+TWO\n+three\n";

        let summary = summarize_patch(patch).expect("the patch must parse");

        assert_eq!(summary.path, "src/file.txt");
        assert_eq!(summary.hunks, 1);
        assert_eq!(summary.lines_added, 2);
        assert_eq!(summary.lines_removed, 1);
    }
}
