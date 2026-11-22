//! Workspace-confined directory listing.
//!
//! `list_dir` is intentionally non-recursive. Entries are sorted by file name,
//! ignored directory names are skipped, and the result is capped so a huge
//! directory cannot flood the model context. Symlinks are reported but never
//! followed, and the requested directory itself is canonicalized and confined
//! to the workspace.

use std::collections::BTreeSet;
use std::path::PathBuf;

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::workspace::{canonical_root, resolve_existing};
use crate::{failure, success};

/// Default directory names skipped by `list_dir`.
pub const DEFAULT_LIST_DIR_IGNORES: &[&str] = &[".git", "target"];

/// Default maximum number of entries rendered by `list_dir`.
pub const DEFAULT_LIST_DIR_ENTRIES: usize = 500;

/// Policy for one [`ListDirTool`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListDirOptions {
    /// Entry names skipped without inspection. Matching is by file name at the
    /// listed directory level only.
    pub ignore_dirs: Vec<String>,
    /// Maximum number of entries rendered.
    pub max_entries: usize,
}

impl Default for ListDirOptions {
    fn default() -> Self {
        Self {
            ignore_dirs: DEFAULT_LIST_DIR_IGNORES
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
            max_entries: DEFAULT_LIST_DIR_ENTRIES,
        }
    }
}

/// Lists one directory inside a workspace.
pub struct ListDirTool {
    root: PathBuf,
    options: ListDirOptions,
}

impl ListDirTool {
    /// Creates a listing tool rooted at `root`.
    pub fn new(root: impl Into<PathBuf>, options: ListDirOptions) -> Self {
        Self {
            root: root.into(),
            options,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListDirArgs {
    path: Option<String>,
}

#[async_trait]
impl Tool for ListDirTool {
    fn name(&self) -> &str {
        "list_dir"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "list_dir",
                "description": format!(
                    "List one directory inside the workspace. Entries are sorted by name, symlinks are not followed, ignored names are skipped, and at most {} entries are returned.",
                    self.options.max_entries
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Directory relative to the workspace root; defaults to the root."
                        }
                    }
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let args: ListDirArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return failure(format!("invalid list_dir arguments: {error}")),
        };
        match self.list(args.path.as_deref().unwrap_or(".")).await {
            Ok(rendered) => success(rendered),
            Err(message) => failure(message),
        }
    }
}

impl ListDirTool {
    async fn list(&self, requested: &str) -> Result<String, String> {
        let root = canonical_root(&self.root).await?;
        let directory = resolve_existing(&root, requested).await?;
        let metadata = tokio::fs::metadata(&directory)
            .await
            .map_err(|error| format!("failed to inspect `{requested}`: {error}"))?;
        if !metadata.is_dir() {
            return Err(format!("`{requested}` is not a directory"));
        }

        let ignored: BTreeSet<&str> = self
            .options
            .ignore_dirs
            .iter()
            .map(String::as_str)
            .collect();
        let mut entries = Vec::new();
        let mut ignored_count = 0_usize;
        let mut read_dir = tokio::fs::read_dir(&directory)
            .await
            .map_err(|error| format!("failed to list `{requested}`: {error}"))?;
        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|error| format!("failed to list `{requested}`: {error}"))?
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            if ignored.contains(name.as_str()) {
                ignored_count += 1;
                continue;
            }
            let path = entry.path();
            let metadata = tokio::fs::symlink_metadata(&path)
                .await
                .map_err(|error| format!("failed to inspect `{}`: {error}", path.display()))?;
            entries.push((name, path, metadata));
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));

        let total = entries.len();
        let shown: Vec<_> = entries.into_iter().take(self.options.max_entries).collect();
        let mut rendered = format!("directory: {requested}\nentries: {}/{total}\n", shown.len());
        for (name, path, metadata) in &shown {
            if metadata.file_type().is_symlink() {
                let target = match tokio::fs::read_link(path).await {
                    Ok(target) => target.display().to_string(),
                    Err(error) => format!("<unreadable: {error}>"),
                };
                rendered.push_str(&format!("symlink\t{name}\t-> {target}\n"));
            } else if metadata.is_dir() {
                rendered.push_str(&format!("dir\t{name}\n"));
            } else if metadata.is_file() {
                rendered.push_str(&format!("file\t{}\t{name}\n", metadata.len()));
            } else {
                rendered.push_str(&format!("other\t{name}\n"));
            }
        }
        if ignored_count > 0 {
            rendered.push_str(&format!(
                "[ignored {ignored_count} entries matched tools.list_dir.ignore_dirs]\n"
            ));
        }
        if total > shown.len() {
            rendered.push_str(&format!(
                "[truncated: showing {} of {total} entries; raise tools.list_dir.max_entries for more]\n",
                shown.len()
            ));
        }
        Ok(rendered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;
    use serde_json::json;

    #[tokio::test]
    async fn entries_are_sorted_and_summarize_type_and_size() {
        let temp = TempDir::new("list-sorted");
        std::fs::create_dir(temp.path().join("alpha_dir")).expect("directory must be created");
        std::fs::write(temp.path().join("beta.txt"), b"hello").expect("file must be written");
        let tool = ListDirTool::new(temp.path(), ListDirOptions::default());

        let result = tool.execute(json!({})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        let lines: Vec<&str> = result.content.lines().collect();
        assert_eq!(lines[0], "directory: .");
        assert_eq!(lines[1], "entries: 2/2");
        assert_eq!(lines[2], "dir\talpha_dir");
        assert_eq!(lines[3], "file\t5\tbeta.txt");
    }

    #[tokio::test]
    async fn the_entry_cap_truncates_after_sorting_and_says_so() {
        let temp = TempDir::new("list-cap");
        for name in ["a.txt", "b.txt", "c.txt", "d.txt"] {
            std::fs::write(temp.path().join(name), b"x").expect("file must be written");
        }
        let tool = ListDirTool::new(
            temp.path(),
            ListDirOptions {
                max_entries: 2,
                ..ListDirOptions::default()
            },
        );

        let result = tool.execute(json!({})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(result.content.contains("entries: 2/4"));
        assert!(result.content.contains("file\t1\ta.txt"));
        assert!(result.content.contains("file\t1\tb.txt"));
        assert!(!result.content.contains("c.txt"));
        assert!(
            result.content.contains("truncated: showing 2 of 4 entries"),
            "the result must disclose the cap: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn configured_ignored_directories_are_skipped() {
        let temp = TempDir::new("list-ignore");
        std::fs::create_dir(temp.path().join(".git")).expect(".git must be created");
        std::fs::create_dir(temp.path().join("target")).expect("target must be created");
        std::fs::create_dir(temp.path().join("src")).expect("src must be created");
        let tool = ListDirTool::new(temp.path(), ListDirOptions::default());

        let result = tool.execute(json!({})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(result.content.contains("entries: 1/1"));
        assert!(result.content.contains("dir\tsrc"));
        assert!(!result.content.contains(".git"));
        assert!(!result.content.contains("target"));
        assert!(
            result.content.contains("ignored 2 entries"),
            "ignored entries must be counted: {}",
            result.content
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_are_reported_but_never_followed() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new("list-symlink");
        let outside = TempDir::new("list-symlink-target");
        std::fs::write(outside.path().join("secret.txt"), "secret")
            .expect("outside file must be written");
        symlink(outside.path(), temp.path().join("escape")).expect("symlink must be created");
        let tool = ListDirTool::new(temp.path(), ListDirOptions::default());

        let listing = tool.execute(json!({})).await;
        assert!(!listing.is_error, "unexpected failure: {}", listing.content);
        assert!(
            listing.content.contains("symlink\tescape\t-> "),
            "the symlink must be reported, not followed: {}",
            listing.content
        );
        assert!(
            !listing.content.contains("secret.txt"),
            "listing must not reveal outside contents: {}",
            listing.content
        );

        let escaped = tool.execute(json!({"path": "escape"})).await;
        assert!(escaped.is_error, "following the symlink must fail");
        assert!(
            escaped.content.contains("outside the workspace"),
            "the escape must be named: {}",
            escaped.content
        );
        assert!(
            !escaped.content.contains("secret.txt"),
            "the failure must not leak outside contents: {}",
            escaped.content
        );
    }

    #[tokio::test]
    async fn a_file_path_is_rejected_as_not_a_directory() {
        let temp = TempDir::new("list-file");
        std::fs::write(temp.path().join("file.txt"), b"x").expect("file must be written");
        let tool = ListDirTool::new(temp.path(), ListDirOptions::default());

        let result = tool.execute(json!({"path": "file.txt"})).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("is not a directory"),
            "the error must explain the type mismatch: {}",
            result.content
        );
    }
}
