//! Bounded, redacted summaries for one-time approval requests.
//!
//! Approval prompts must show enough for a human to decide without copying the
//! full tool arguments into the protocol or terminal. Known tools get a
//! purpose-built summary: `exec` reports its program, arguments, and `cwd`;
//! `write_file` reports a bounded line diff instead of the file body. Unknown
//! tools keep their argument shape with secret-looking fields redacted and
//! oversized values truncated. Environment variables are never read here.

use std::path::Path;

use jessica_core::types::ToolCall;
use serde_json::{Map, Value, json};

/// Maximum characters retained from one string value.
const MAX_STRING_CHARS: usize = 256;
/// Maximum array elements retained from one argument value.
const MAX_ARRAY_ITEMS: usize = 16;
/// Maximum object depth retained from one argument value.
const MAX_DEPTH: usize = 3;
/// Maximum bytes read from an existing write target when summarizing a diff.
const MAX_DIFF_BYTES: u64 = 256 * 1024;

/// Builds the redacted argument summary carried by an approval request.
///
/// `root` is used only to compute a bounded `write_file` diff against an
/// existing target. Reads outside the workspace root are skipped. The result
/// contains no environment variables and no secret-looking field values.
pub(crate) async fn approval_request_input(root: &Path, call: &ToolCall) -> Value {
    match call.name.as_str() {
        "exec" => {
            let program = bounded_string(call.arguments.get("program"));
            let args = call
                .arguments
                .get("args")
                .map(|value| summarize(value, 0))
                .unwrap_or_else(|| Value::Array(Vec::new()));
            let cwd = bounded_string(call.arguments.get("cwd"));
            json!({
                "program": program,
                "args": args,
                "cwd": cwd,
            })
        }
        "bash" => json!({
            "command": bounded_string(call.arguments.get("command")),
        }),
        "delegate_task" => json!({
            "task": bounded_string(call.arguments.get("task")),
        }),
        "write_file" => write_file_summary(root, call).await,
        "apply_patch" => apply_patch_summary(call),
        "edit_file" => edit_file_summary(call),
        _ => summarize(&call.arguments, 0),
    }
}

/// Builds a patch summary with only the target, hunk counts, and dry-run flag.
///
/// A malformed patch is reported generically: parser diagnostics can quote
/// hunk lines, which may contain secrets and must not enter approval output.
fn apply_patch_summary(call: &ToolCall) -> Value {
    let Some(patch) = call.arguments.get("patch").and_then(Value::as_str) else {
        return json!({"valid": false, "reason": "patch argument is missing or not a string"});
    };
    match jessica_tools::summarize_patch(patch) {
        Ok(summary) => json!({
            "path": truncate(&summary.path),
            "hunks": summary.hunks,
            "lines_added": summary.lines_added,
            "lines_removed": summary.lines_removed,
            "dry_run": call
                .arguments
                .get("dry_run")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }),
        Err(_) => json!({"valid": false, "reason": "patch could not be parsed"}),
    }
}

/// Builds an edit summary with sizes and counts instead of file bodies.
fn edit_file_summary(call: &ToolCall) -> Value {
    let mut summary = Map::new();
    summary.insert(
        "path".to_owned(),
        bounded_string(call.arguments.get("path")),
    );
    for (field, output) in [("old", "old"), ("new", "new")] {
        if let Some(text) = call.arguments.get(field).and_then(Value::as_str) {
            summary.insert(
                format!("{output}_bytes"),
                Value::Number(serde_json::Number::from(text.len())),
            );
            summary.insert(
                format!("{output}_lines"),
                Value::Number(serde_json::Number::from(text.lines().count())),
            );
        }
    }
    summary.insert(
        "expected_occurrences".to_owned(),
        call.arguments
            .get("expected_occurrences")
            .cloned()
            .unwrap_or(Value::Null),
    );
    Value::Object(summary)
}

/// Builds the write-file summary with a bounded diff instead of the content.
async fn write_file_summary(root: &Path, call: &ToolCall) -> Value {
    let path = call.arguments.get("path").and_then(Value::as_str);
    let content = call.arguments.get("content").and_then(Value::as_str);
    let mut summary = Map::new();
    if let Some(path) = path {
        summary.insert("path".to_owned(), Value::String(truncate(path)));
    }
    if let Some(content) = content {
        summary.insert(
            "content_bytes".to_owned(),
            Value::Number(serde_json::Number::from(content.len())),
        );
        summary.insert(
            "content_lines".to_owned(),
            Value::Number(serde_json::Number::from(content.lines().count())),
        );
    }
    summary.insert(
        "diff".to_owned(),
        write_file_diff(root, path, content).await,
    );
    Value::Object(summary)
}

/// Summarizes the line-level change against an existing target.
///
/// The diff is intentionally bounded and approximate: it reports common
/// prefix/suffix lines as unchanged and counts the changed lines. It never
/// carries old or new file content.
async fn write_file_diff(root: &Path, path: Option<&str>, content: Option<&str>) -> Value {
    let (Some(path), Some(content)) = (path, content) else {
        return json!({"available": false});
    };
    let Some(old) = read_existing_workspace_file(root, path).await else {
        return json!({
            "available": true,
            "existing": false,
            "lines_added": content.lines().count(),
            "lines_removed": 0,
            "truncated": false,
        });
    };
    let (old, truncated) = old;
    let (added, removed) = line_diff(&old, content);
    json!({
        "available": true,
        "existing": true,
        "lines_added": added,
        "lines_removed": removed,
        "truncated": truncated,
    })
}

/// Reads an existing file only when its canonical path stays inside `root`.
async fn read_existing_workspace_file(root: &Path, path: &str) -> Option<(String, bool)> {
    let root = tokio::fs::canonicalize(root).await.ok()?;
    let requested = Path::new(path);
    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        root.join(requested)
    };
    let canonical = tokio::fs::canonicalize(candidate).await.ok()?;
    if !canonical.starts_with(&root) {
        return None;
    }
    let metadata = tokio::fs::metadata(&canonical).await.ok()?;
    if !metadata.is_file() || metadata.len() > MAX_DIFF_BYTES {
        return Some((String::new(), true));
    }
    let bytes = tokio::fs::read(&canonical).await.ok()?;
    match String::from_utf8(bytes) {
        Ok(text) => Some((text, false)),
        Err(_) => Some((String::new(), true)),
    }
}

/// Returns changed-line counts using common prefix and suffix lines.
fn line_diff(old: &str, new: &str) -> (usize, usize) {
    let old: Vec<&str> = old.lines().collect();
    let new: Vec<&str> = new.lines().collect();
    let mut prefix = 0;
    while prefix < old.len() && prefix < new.len() && old[prefix] == new[prefix] {
        prefix += 1;
    }
    let mut suffix = 0;
    while suffix < old.len().saturating_sub(prefix)
        && suffix < new.len().saturating_sub(prefix)
        && old[old.len() - 1 - suffix] == new[new.len() - 1 - suffix]
    {
        suffix += 1;
    }
    (
        new.len().saturating_sub(prefix + suffix),
        old.len().saturating_sub(prefix + suffix),
    )
}

/// Recursively redacts and bounds one JSON value.
fn summarize(value: &Value, depth: usize) -> Value {
    if depth >= MAX_DEPTH {
        return Value::String("[truncated]".to_owned());
    }
    match value {
        Value::Object(object) => {
            let mut keys: Vec<&String> = object.keys().collect();
            keys.sort_unstable();
            let mut summary = Map::new();
            for key in keys.iter().take(MAX_ARRAY_ITEMS) {
                let value = if is_secret_key(key) {
                    Value::String("[redacted]".to_owned())
                } else {
                    summarize(&object[*key], depth + 1)
                };
                summary.insert((*key).clone(), value);
            }
            if keys.len() > MAX_ARRAY_ITEMS {
                summary.insert("_truncated".to_owned(), Value::Bool(true));
            }
            Value::Object(summary)
        }
        Value::Array(array) => {
            let mut summary: Vec<Value> = array
                .iter()
                .take(MAX_ARRAY_ITEMS)
                .map(|value| summarize(value, depth + 1))
                .collect();
            if array.len() > MAX_ARRAY_ITEMS {
                summary.push(Value::String("[truncated]".to_owned()));
            }
            Value::Array(summary)
        }
        Value::String(text) => Value::String(truncate(text)),
        other => other.clone(),
    }
}

/// Returns whether a field name looks secret-bearing.
fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "secret",
        "token",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "credential",
        "authorization",
        "private_key",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

/// Returns a bounded string representation of an optional JSON string.
fn bounded_string(value: Option<&Value>) -> Value {
    match value.and_then(Value::as_str) {
        Some(text) => Value::String(truncate(text)),
        None => Value::Null,
    }
}

/// Truncates one string to [`MAX_STRING_CHARS`] characters.
fn truncate(text: &str) -> String {
    if text.chars().count() <= MAX_STRING_CHARS {
        return text.to_owned();
    }
    let mut truncated: String = text.chars().take(MAX_STRING_CHARS).collect();
    truncated.push_str("...");
    truncated
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "jessica-approval-{label}-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temp dir must be created");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[tokio::test]
    async fn exec_summary_contains_only_program_arguments_and_cwd() {
        let temp = TempDir::new("exec");
        let call = ToolCall::new(
            "call",
            "exec",
            json!({
                "program": "git",
                "args": ["push", "--force", "origin"],
                "cwd": "repo",
                "timeout_ms": 1000,
                "env": {"API_TOKEN": "must-not-appear"}
            }),
        );

        let summary = approval_request_input(&temp.path, &call).await;

        assert_eq!(summary["program"], "git");
        assert_eq!(summary["args"], json!(["push", "--force", "origin"]));
        assert_eq!(summary["cwd"], "repo");
        let rendered = summary.to_string();
        assert!(!rendered.contains("must-not-appear"));
        assert!(!rendered.contains("timeout_ms"));
    }

    #[tokio::test]
    async fn write_file_summary_reports_a_diff_not_the_content() {
        let temp = TempDir::new("write");
        std::fs::write(temp.path.join("note.txt"), "alpha\nbeta\n").expect("file must exist");
        let call = ToolCall::new(
            "call",
            "write_file",
            json!({"path": "note.txt", "content": "alpha\nBETA\nsecret-body\n"}),
        );

        let summary = approval_request_input(&temp.path, &call).await;

        assert_eq!(summary["path"], "note.txt");
        assert_eq!(summary["diff"]["existing"], true);
        assert_eq!(summary["diff"]["lines_added"], 2);
        assert_eq!(summary["diff"]["lines_removed"], 1);
        let rendered = summary.to_string();
        assert!(!rendered.contains("secret-body"));
        assert!(!rendered.contains("beta"));
    }

    #[tokio::test]
    async fn write_file_diff_never_reads_outside_the_workspace() {
        let temp = TempDir::new("write-root");
        let outside = TempDir::new("write-outside");
        let secret = "outside-secret-body";
        std::fs::write(outside.path.join("secret.txt"), secret).expect("file must exist");
        let call = ToolCall::new(
            "call",
            "write_file",
            json!({
                "path": outside.path.join("secret.txt").to_string_lossy(),
                "content": "replacement"
            }),
        );

        let summary = approval_request_input(&temp.path, &call).await;

        assert_eq!(summary["diff"]["existing"], false);
        assert!(!summary.to_string().contains(secret));
    }

    #[tokio::test]
    async fn unknown_tool_summaries_redact_secret_fields_and_truncate_values() {
        let temp = TempDir::new("generic");
        let call = ToolCall::new(
            "call",
            "mcp__server__upload",
            json!({
                "api_key": "secret-key",
                "nested": {"password": "hunter2", "safe": "visible"},
                "payload": "x".repeat(MAX_STRING_CHARS + 100),
            }),
        );

        let summary = approval_request_input(&temp.path, &call).await;

        assert_eq!(summary["api_key"], "[redacted]");
        assert_eq!(summary["nested"]["password"], "[redacted]");
        assert_eq!(summary["nested"]["safe"], "visible");
        let payload = summary["payload"]
            .as_str()
            .expect("payload must be a string");
        assert_eq!(payload.chars().count(), MAX_STRING_CHARS + 3);
        assert!(!summary.to_string().contains("secret-key"));
        assert!(!summary.to_string().contains("hunter2"));
    }

    #[tokio::test]
    async fn apply_patch_summary_reports_counts_without_patch_content() {
        let temp = TempDir::new("apply-patch");
        let call = ToolCall::new(
            "call",
            "apply_patch",
            json!({
                "dry_run": true,
                "patch": "--- a/notes.txt\n+++ b/notes.txt\n@@ -1,2 +1,2 @@\n-secret-body\n+replacement\n second\n"
            }),
        );

        let summary = approval_request_input(&temp.path, &call).await;

        assert_eq!(summary["path"], "notes.txt");
        assert_eq!(summary["hunks"], 1);
        assert_eq!(summary["lines_added"], 1);
        assert_eq!(summary["lines_removed"], 1);
        assert_eq!(summary["dry_run"], true);
        let rendered = summary.to_string();
        assert!(!rendered.contains("secret-body"));
        assert!(!rendered.contains("replacement"));
    }

    #[tokio::test]
    async fn edit_file_summary_reports_sizes_without_replacement_text() {
        let temp = TempDir::new("edit-file");
        let call = ToolCall::new(
            "call",
            "edit_file",
            json!({
                "path": "notes.txt",
                "old": "secret-body",
                "new": "replacement-body",
                "expected_occurrences": 2
            }),
        );

        let summary = approval_request_input(&temp.path, &call).await;

        assert_eq!(summary["path"], "notes.txt");
        assert_eq!(summary["old_bytes"], 11);
        assert_eq!(summary["new_bytes"], 16);
        assert_eq!(summary["expected_occurrences"], 2);
        let rendered = summary.to_string();
        assert!(!rendered.contains("secret-body"));
        assert!(!rendered.contains("replacement-body"));
    }

    #[test]
    fn line_diff_counts_changed_lines_between_common_prefix_and_suffix() {
        assert_eq!(line_diff("a\nb\nc\n", "a\nB\nc\n"), (1, 1));
        assert_eq!(line_diff("", "a\nb\n"), (2, 0));
        assert_eq!(line_diff("a\nb\n", ""), (0, 2));
        assert_eq!(line_diff("a\nb\n", "a\nb\n"), (0, 0));
    }
}
