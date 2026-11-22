//! Literal recursive workspace search.
//!
//! `grep` performs a literal substring search, never a regular expression, so
//! characters such as `.`, `*`, `(`, and `[` have no special meaning. The walk
//! is deterministic (entries sorted by name, depth-first), never follows
//! symlinks, skips configured directory names, and ignores binary or oversized
//! files. File count, per-file size, match count, and rendered output are all
//! bounded independently.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::workspace::{canonical_root, resolve_existing};
use crate::{failure, success};

/// Default directory names skipped by `grep`.
pub const DEFAULT_GREP_IGNORES: &[&str] = &[".git", "target"];

/// Default maximum number of files inspected by one `grep` call.
pub const DEFAULT_GREP_FILES: usize = 2_000;

/// Default maximum size of one file inspected by `grep`.
pub const DEFAULT_GREP_FILE_BYTES: usize = 1024 * 1024;

/// Default maximum number of matching lines returned by `grep`.
pub const DEFAULT_GREP_MATCHES: usize = 200;

/// Default maximum rendered match bytes returned by `grep`.
pub const DEFAULT_GREP_OUTPUT_BYTES: usize = 64 * 1024;

/// Maximum characters rendered from one matching line.
const MAX_MATCH_LINE_CHARS: usize = 400;

/// Policy for one [`GrepTool`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepOptions {
    /// Directory names skipped during the walk.
    pub ignore_dirs: Vec<String>,
    /// Maximum number of regular files read.
    pub max_files: usize,
    /// Maximum file size in bytes; larger files are skipped.
    pub max_file_bytes: usize,
    /// Maximum number of matching lines returned.
    pub max_matches: usize,
    /// Maximum bytes of rendered `file:line:text` output.
    pub max_output_bytes: usize,
}

impl Default for GrepOptions {
    fn default() -> Self {
        Self {
            ignore_dirs: DEFAULT_GREP_IGNORES
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
            max_files: DEFAULT_GREP_FILES,
            max_file_bytes: DEFAULT_GREP_FILE_BYTES,
            max_matches: DEFAULT_GREP_MATCHES,
            max_output_bytes: DEFAULT_GREP_OUTPUT_BYTES,
        }
    }
}

/// Searches a workspace for a literal string.
pub struct GrepTool {
    root: PathBuf,
    options: GrepOptions,
}

impl GrepTool {
    /// Creates a search tool rooted at `root`.
    pub fn new(root: impl Into<PathBuf>, options: GrepOptions) -> Self {
        Self {
            root: root.into(),
            options,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrepArgs {
    pattern: String,
    path: Option<String>,
    case_sensitive: Option<bool>,
}

#[async_trait]
impl Tool for GrepTool {
    fn name(&self) -> &str {
        "grep"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "grep",
                "description": format!(
                    "Search workspace files for a literal string. Regular expression syntax is not supported: every character matches itself. Skips binary files, files larger than {} bytes, and configured directories. Returns at most {} matches or {} bytes.",
                    self.options.max_file_bytes,
                    self.options.max_matches,
                    self.options.max_output_bytes
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "pattern": {
                            "type": "string",
                            "description": "Literal text to find."
                        },
                        "path": {
                            "type": "string",
                            "description": "File or directory relative to the workspace root; defaults to the root."
                        },
                        "case_sensitive": {
                            "type": "boolean",
                            "description": "Whether matching is case-sensitive. Defaults to true."
                        }
                    },
                    "required": ["pattern"]
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let args: GrepArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return failure(format!("invalid grep arguments: {error}")),
        };
        if args.pattern.is_empty() {
            return failure("grep pattern must not be empty");
        }
        match self
            .search(
                &args.pattern,
                args.path.as_deref().unwrap_or("."),
                args.case_sensitive.unwrap_or(true),
            )
            .await
        {
            Ok(rendered) => success(rendered),
            Err(message) => failure(message),
        }
    }
}

/// Why a file produced no matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkipReason {
    Binary,
    Oversized,
    Unreadable,
}

/// One file's matches or the reason it was skipped.
enum FileOutcome {
    Matches(Vec<(u64, String)>),
    Skipped(SkipReason),
}

impl GrepTool {
    async fn search(
        &self,
        pattern: &str,
        requested: &str,
        case_sensitive: bool,
    ) -> Result<String, String> {
        let root = canonical_root(&self.root).await?;
        let start = resolve_existing(&root, requested).await?;
        let metadata = tokio::fs::metadata(&start)
            .await
            .map_err(|error| format!("failed to inspect `{requested}`: {error}"))?;

        let needle = if case_sensitive {
            pattern.to_owned()
        } else {
            pattern.to_lowercase()
        };
        let ignored: BTreeSet<&str> = self
            .options
            .ignore_dirs
            .iter()
            .map(String::as_str)
            .collect();

        let mut files_scanned = 0_usize;
        let mut matches: Vec<String> = Vec::new();
        let mut output_bytes = 0_usize;
        let mut binary_files = 0_usize;
        let mut oversized_files = 0_usize;
        let mut unreadable_files = 0_usize;
        let mut ignored_directories = 0_usize;
        let mut symlinks = 0_usize;
        let mut stop = None;

        if metadata.is_file() {
            files_scanned = 1;
            match self.scan_file(&start, &needle, case_sensitive).await {
                FileOutcome::Matches(file_matches) => {
                    let relative = relative_path(&root, &start);
                    if let Some(reason) = push_matches(
                        &mut matches,
                        &mut output_bytes,
                        &relative,
                        file_matches,
                        self.options.max_matches,
                        self.options.max_output_bytes,
                    ) {
                        stop = Some(reason);
                    }
                }
                FileOutcome::Skipped(reason) => match reason {
                    SkipReason::Binary => binary_files += 1,
                    SkipReason::Oversized => oversized_files += 1,
                    SkipReason::Unreadable => unreadable_files += 1,
                },
            }
        } else if metadata.is_dir() {
            let mut stack = vec![start];
            'walk: while let Some(directory) = stack.pop() {
                let mut entries = Vec::new();
                let mut read_dir = tokio::fs::read_dir(&directory)
                    .await
                    .map_err(|error| format!("failed to search `{requested}`: {error}"))?;
                while let Some(entry) = read_dir
                    .next_entry()
                    .await
                    .map_err(|error| format!("failed to search `{requested}`: {error}"))?
                {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    let path = entry.path();
                    let metadata = match tokio::fs::symlink_metadata(&path).await {
                        Ok(metadata) => metadata,
                        Err(_) => {
                            unreadable_files += 1;
                            continue;
                        }
                    };
                    if metadata.file_type().is_symlink() {
                        symlinks += 1;
                        continue;
                    }
                    if metadata.is_dir() {
                        if ignored.contains(name.as_str()) {
                            ignored_directories += 1;
                        } else {
                            entries.push((name, path, true));
                        }
                    } else if metadata.is_file() {
                        entries.push((name, path, false));
                    }
                }
                entries.sort_by(|left, right| left.0.cmp(&right.0));

                let mut subdirectories = Vec::new();
                for (_, path, is_directory) in entries {
                    if is_directory {
                        subdirectories.push(path);
                        continue;
                    }
                    if files_scanned >= self.options.max_files {
                        stop = Some(format!("max_files={}", self.options.max_files));
                        break 'walk;
                    }
                    files_scanned += 1;
                    match self.scan_file(&path, &needle, case_sensitive).await {
                        FileOutcome::Matches(file_matches) => {
                            let relative = relative_path(&root, &path);
                            if let Some(reason) = push_matches(
                                &mut matches,
                                &mut output_bytes,
                                &relative,
                                file_matches,
                                self.options.max_matches,
                                self.options.max_output_bytes,
                            ) {
                                stop = Some(reason);
                                break 'walk;
                            }
                        }
                        FileOutcome::Skipped(SkipReason::Binary) => binary_files += 1,
                        FileOutcome::Skipped(SkipReason::Oversized) => oversized_files += 1,
                        FileOutcome::Skipped(SkipReason::Unreadable) => unreadable_files += 1,
                    }
                }
                for subdirectory in subdirectories.into_iter().rev() {
                    stack.push(subdirectory);
                }
            }
        } else {
            return Err(format!("`{requested}` is not a regular file or directory"));
        }

        Ok(render_summary(
            &matches,
            files_scanned,
            stop.as_deref(),
            &[
                ("binary files", binary_files),
                ("oversized files", oversized_files),
                ("unreadable files", unreadable_files),
                ("ignored directories", ignored_directories),
                ("symlinks", symlinks),
            ],
        ))
    }

    /// Reads one file and returns its literal matches or a skip reason.
    async fn scan_file(&self, path: &Path, needle: &str, case_sensitive: bool) -> FileOutcome {
        let metadata = match tokio::fs::symlink_metadata(path).await {
            Ok(metadata) => metadata,
            Err(_) => return FileOutcome::Skipped(SkipReason::Unreadable),
        };
        if metadata.len() > self.options.max_file_bytes as u64 {
            return FileOutcome::Skipped(SkipReason::Oversized);
        }
        let bytes = match tokio::fs::read(path).await {
            Ok(bytes) => bytes,
            Err(_) => return FileOutcome::Skipped(SkipReason::Unreadable),
        };
        if bytes.contains(&0) {
            return FileOutcome::Skipped(SkipReason::Binary);
        }
        if bytes.len() > self.options.max_file_bytes {
            return FileOutcome::Skipped(SkipReason::Oversized);
        }
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(_) => return FileOutcome::Skipped(SkipReason::Binary),
        };

        let mut file_matches = Vec::new();
        for (index, line) in text.lines().enumerate() {
            let matched = if case_sensitive {
                line.contains(needle)
            } else {
                line.to_lowercase().contains(needle)
            };
            if matched {
                file_matches.push((
                    u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                    truncate_line(line),
                ));
            }
        }
        FileOutcome::Matches(file_matches)
    }
}

/// Appends rendered matches, returning a stop reason when a limit is reached.
fn push_matches(
    output: &mut Vec<String>,
    output_bytes: &mut usize,
    relative: &str,
    file_matches: Vec<(u64, String)>,
    max_matches: usize,
    max_output_bytes: usize,
) -> Option<String> {
    for (line_number, line) in file_matches {
        if output.len() >= max_matches {
            return Some(format!("max_matches={max_matches}"));
        }
        let rendered = format!("{relative}:{line_number}:{line}\n");
        if output_bytes.saturating_add(rendered.len()) > max_output_bytes {
            return Some(format!("max_output_bytes={max_output_bytes}"));
        }
        *output_bytes += rendered.len();
        output.push(rendered);
    }
    None
}

/// Renders the final match list and skip summary.
fn render_summary(
    matches: &[String],
    files_scanned: usize,
    stop: Option<&str>,
    skips: &[(&str, usize)],
) -> String {
    let mut rendered = String::new();
    if matches.is_empty() {
        rendered.push_str("no matches\n");
    } else {
        for line in matches {
            rendered.push_str(line);
        }
    }
    rendered.push_str(&format!(
        "[{} matches in {files_scanned} files",
        matches.len()
    ));
    if let Some(reason) = stop {
        rendered.push_str(&format!("; truncated at {reason}"));
    }
    rendered.push_str("]\n");

    let skipped = skips
        .iter()
        .filter(|(_, count)| *count > 0)
        .map(|(label, count)| format!("{count} {label}"))
        .collect::<Vec<_>>();
    if !skipped.is_empty() {
        rendered.push_str(&format!("[skipped: {}]\n", skipped.join(", ")));
    }
    rendered
}

/// Returns one matching line, bounded per line.
fn truncate_line(line: &str) -> String {
    if line.chars().count() <= MAX_MATCH_LINE_CHARS {
        return line.to_owned();
    }
    let mut truncated: String = line.chars().take(MAX_MATCH_LINE_CHARS).collect();
    truncated.push_str("...");
    truncated
}

/// Returns a workspace-relative path with `/` separators.
fn relative_path(root: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(root).unwrap_or(path);
    relative.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;
    use serde_json::json;

    #[tokio::test]
    async fn regex_metacharacters_match_literally() {
        let temp = TempDir::new("grep-literal");
        std::fs::write(temp.path().join("special.txt"), b"a.b(c)[d]\naxbxcxd\n")
            .expect("fixture must be written");
        let tool = GrepTool::new(temp.path(), GrepOptions::default());

        let result = tool.execute(json!({"pattern": "a.b(c)[d]"})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(result.content.contains("special.txt:1:a.b(c)[d]"));
        assert!(
            !result.content.contains("axbxcxd"),
            "regex metacharacters must not act as wildcards: {}",
            result.content
        );
        assert!(result.content.contains("[1 matches in 1 files]"));
    }

    #[tokio::test]
    async fn case_sensitivity_is_opt_in() {
        let temp = TempDir::new("grep-case");
        std::fs::write(temp.path().join("case.txt"), b"Hello World\n").expect("fixture");
        let tool = GrepTool::new(temp.path(), GrepOptions::default());

        let sensitive = tool.execute(json!({"pattern": "hello"})).await;
        assert!(!sensitive.is_error, "{}", sensitive.content);
        assert!(sensitive.content.contains("no matches"));

        let insensitive = tool
            .execute(json!({"pattern": "hello", "case_sensitive": false}))
            .await;
        assert!(!insensitive.is_error, "{}", insensitive.content);
        assert!(insensitive.content.contains("case.txt:1:Hello World"));
    }

    #[tokio::test]
    async fn binary_and_oversized_files_are_skipped_and_counted() {
        let temp = TempDir::new("grep-skip");
        std::fs::write(temp.path().join("binary.bin"), b"needle\0binary").expect("fixture");
        std::fs::write(temp.path().join("large.txt"), b"needle in a large file").expect("fixture");
        std::fs::write(temp.path().join("ok.txt"), b"needle\n").expect("fixture");
        let tool = GrepTool::new(
            temp.path(),
            GrepOptions {
                max_file_bytes: 16,
                ..GrepOptions::default()
            },
        );

        let result = tool.execute(json!({"pattern": "needle"})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(
            result
                .content
                .contains("skipped: 1 binary files, 1 oversized files"),
            "both skips must be counted: {}",
            result.content
        );
        assert!(
            !result.content.contains("binary.bin:1") && !result.content.contains("large.txt:1"),
            "skipped files must not produce matches: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn ignored_directories_are_not_searched() {
        let temp = TempDir::new("grep-ignore");
        std::fs::create_dir(temp.path().join(".git")).expect(".git");
        std::fs::create_dir(temp.path().join("target")).expect("target");
        std::fs::create_dir(temp.path().join("src")).expect("src");
        std::fs::write(temp.path().join(".git/config.txt"), b"needle").expect("fixture");
        std::fs::write(temp.path().join("target/generated.txt"), b"needle").expect("fixture");
        std::fs::write(temp.path().join("src/main.txt"), b"needle").expect("fixture");
        let tool = GrepTool::new(temp.path(), GrepOptions::default());

        let result = tool.execute(json!({"pattern": "needle"})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(result.content.contains("src/main.txt:1:needle"));
        assert!(
            !result.content.contains(".git/") && !result.content.contains("target/"),
            "ignored directories must not be searched: {}",
            result.content
        );
        assert!(
            result.content.contains("ignored directories"),
            "the skip summary must mention ignored directories: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn the_match_cap_truncates_and_reports_why() {
        let temp = TempDir::new("grep-cap");
        std::fs::write(temp.path().join("many.txt"), b"needle\nneedle\nneedle\n").expect("fixture");
        let tool = GrepTool::new(
            temp.path(),
            GrepOptions {
                max_matches: 1,
                ..GrepOptions::default()
            },
        );

        let result = tool.execute(json!({"pattern": "needle"})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(
            result
                .content
                .lines()
                .filter(|line| line.starts_with("many.txt:"))
                .count(),
            1,
            "the match cap must retain exactly one match: {}",
            result.content
        );
        assert!(
            result.content.contains("truncated at max_matches=1"),
            "the cap must be reported: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn the_output_byte_cap_stops_before_exceeding_the_budget() {
        let temp = TempDir::new("grep-bytes");
        let content = (0..20)
            .map(|index| format!("needle-{index:02}\n"))
            .collect::<String>();
        std::fs::write(temp.path().join("many.txt"), content).expect("fixture");
        let tool = GrepTool::new(
            temp.path(),
            GrepOptions {
                max_output_bytes: 40,
                ..GrepOptions::default()
            },
        );

        let result = tool.execute(json!({"pattern": "needle"})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(
            result.content.contains("truncated at max_output_bytes=40"),
            "the output cap must be reported: {}",
            result.content
        );
        let rendered_matches: Vec<&str> = result
            .content
            .lines()
            .filter(|line| line.starts_with("many.txt:"))
            .collect();
        assert!(
            !rendered_matches.is_empty() && rendered_matches.len() < 20,
            "the result must contain some but not all matches: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn a_path_outside_the_workspace_is_rejected() {
        let temp = TempDir::new("grep-escape");
        let outside = TempDir::new("grep-escape-target");
        std::fs::write(outside.path().join("secret.txt"), "needle").expect("fixture");
        let tool = GrepTool::new(temp.path(), GrepOptions::default());

        let result = tool
            .execute(json!({"pattern": "needle", "path": outside.path().to_string_lossy()}))
            .await;

        assert!(result.is_error);
        assert!(result.content.contains("outside the workspace"));
    }

    #[tokio::test]
    async fn an_empty_pattern_is_rejected() {
        let temp = TempDir::new("grep-empty");
        let tool = GrepTool::new(temp.path(), GrepOptions::default());

        let result = tool.execute(json!({"pattern": ""})).await;

        assert!(result.is_error);
        assert!(result.content.contains("must not be empty"));
    }
}
