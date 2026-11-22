//! `bash` tool with a wall-clock timeout and bounded output.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};

use crate::{failure, success};

/// Default wall-clock timeout for one command.
pub const DEFAULT_BASH_TIMEOUT: Duration = Duration::from_secs(10);
/// Default cap for each captured stream.
const DEFAULT_OUTPUT_LIMIT: usize = 64 * 1024;

/// Environment variable names passed to bash by default.
///
/// This list mirrors the harness supervisor defaults. The crates cannot share
/// the constant because `jessica-tools` may not depend on `jessica-harness`;
/// both sides intentionally keep the same minimal set.
const DEFAULT_ENV_ALLOWLIST: &[&str] = &["PATH", "HOME", "LANG", "TERM", "TMPDIR"];

/// Environment-variable prefixes whose values are never inherited.
const SENSITIVE_ENV_PREFIXES: &[&str] = &[
    "OPENAI_",
    "ANTHROPIC_",
    "JESSICA_",
    "AWS_",
    "AZURE_",
    "GOOGLE_",
    "GCP_",
    "GITHUB_",
    "GH_",
    "GITLAB_",
    "SLACK_",
    "SSH_",
    "LD_",
    "DYLD_",
];

/// Environment-variable name fragments that mark a value as credential-bearing.
const SENSITIVE_ENV_MARKERS: &[&str] = &[
    "KEY",
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "CREDENTIAL",
    "AUTH",
    "COOKIE",
    "SESSION",
];

/// Exact environment-variable names that can change command execution.
const SENSITIVE_ENV_NAMES: &[&str] = &["BASH_ENV", "ENV", "PROMPT_COMMAND"];

/// Runs bash commands with a hard timeout and bounded stdout/stderr capture.
///
/// The tool has no enable gate of its own: the host decides whether to expose
/// it. The CLI hides it unless `tools.bash.enabled` is explicitly true.
///
/// Commands start with an empty environment containing only the built-in
/// allowlist plus explicitly configured extensions; credential-bearing names
/// are always filtered. A command that exits non-zero is reported as a failed
/// [`ToolResult`] whose content still contains the captured output. Output
/// beyond the per-stream limit is discarded and marked with a truncation
/// notice; the rendered result can additionally be capped at a total byte
/// limit so it never trips the harness result-size guard.
pub struct BashTool {
    timeout: Duration,
    output_limit: usize,
    content_limit: Option<usize>,
    env_allowlist: Vec<String>,
    current_dir: Option<PathBuf>,
}

impl BashTool {
    /// Creates a tool with a 10-second timeout, a 64 KiB per-stream limit, and
    /// a 64 KiB rendered-content limit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a tool with explicit per-stream limits.
    ///
    /// `output_limit` applies separately to stdout and stderr. A zero limit
    /// keeps no output while still draining the pipes. The rendered result is
    /// not additionally capped; use [`BashTool::with_content_limit`] for that.
    pub fn with_limits(timeout: Duration, output_limit: usize) -> Self {
        Self {
            timeout,
            output_limit,
            content_limit: None,
            env_allowlist: Vec::new(),
            current_dir: None,
        }
    }

    /// Caps the rendered result at `limit` bytes after truncation notices are
    /// added.
    ///
    /// The cut lands on a UTF-8 boundary and the retained result records the
    /// original byte count. This is the limit that keeps a bash result within
    /// `budget.max_result_bytes`.
    pub fn with_content_limit(mut self, limit: usize) -> Self {
        self.content_limit = Some(limit);
        self
    }

    /// Adds environment variable names on top of the built-in defaults.
    ///
    /// Credential-bearing names are always filtered, even when listed here.
    pub fn with_env_allowlist<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.env_allowlist.extend(names.into_iter().map(Into::into));
        self
    }

    /// Sets the directory in which commands are spawned.
    ///
    /// This is additive: a tool built by [`BashTool::new`] keeps using the
    /// process current directory when no directory is set. A path that does not
    /// exist is reported as a spawn failure when the command runs, not when the
    /// tool is built.
    pub fn with_current_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(dir.into());
        self
    }
}

impl Default for BashTool {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_BASH_TIMEOUT,
            output_limit: DEFAULT_OUTPUT_LIMIT,
            content_limit: Some(DEFAULT_OUTPUT_LIMIT),
            env_allowlist: Vec::new(),
            current_dir: None,
        }
    }
}

#[derive(Deserialize)]
struct BashArgs {
    command: String,
}

#[async_trait]
impl Tool for BashTool {
    fn name(&self) -> &str {
        "bash"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "bash",
                "description": format!(
                    "Run a bash command in the current directory with a cleared environment. The command is killed after {} seconds; stdout and stderr are each capped at {} bytes{}.",
                    self.timeout.as_secs_f64(),
                    self.output_limit,
                    self.content_limit.map_or_else(String::new, |limit| format!(
                        " and the rendered result is capped at {limit} bytes"
                    ))
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "Bash command to execute."
                        }
                    },
                    "required": ["command"]
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let args: BashArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return failure(format!("invalid bash arguments: {error}")),
        };

        let mut command = Command::new("bash");
        command
            .arg("-c")
            .arg(&args.command)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .env_clear();
        apply_environment(&mut command, &self.env_allowlist);
        if let Some(dir) = &self.current_dir {
            command.current_dir(dir);
        }

        let child = match command.spawn() {
            Ok(child) => child,
            Err(error) => return failure(format!("failed to spawn bash: {error}")),
        };

        match tokio::time::timeout(self.timeout, run_command(child, self.output_limit)).await {
            Err(_) => failure(format!(
                "command timed out after {} seconds",
                self.timeout.as_secs_f64()
            )),
            Ok(Err(error)) => failure(format!("failed to run command: {error}")),
            Ok(Ok(output)) => {
                let rendered = render(&output);
                let rendered = match self.content_limit {
                    Some(limit) => {
                        let source_total = output.stdout_total.saturating_add(output.stderr_total);
                        cap_content(&rendered, limit, source_total)
                    }
                    None => rendered,
                };
                if output.status.success() {
                    success(rendered)
                } else {
                    failure(rendered)
                }
            }
        }
    }
}

struct CommandOutput {
    stdout: Vec<u8>,
    stdout_truncated: bool,
    stdout_total: u64,
    stderr: Vec<u8>,
    stderr_truncated: bool,
    stderr_total: u64,
    status: std::process::ExitStatus,
}

/// Reads both pipes concurrently, then waits for the command to exit.
///
/// The whole function is wrapped in a timeout by the caller; dropping it kills
/// the child because `kill_on_drop` is set.
async fn run_command(mut child: Child, output_limit: usize) -> std::io::Result<CommandOutput> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("stdout pipe was not captured"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| std::io::Error::other("stderr pipe was not captured"))?;

    let (stdout, stderr) = tokio::join!(
        read_capped(stdout, output_limit),
        read_capped(stderr, output_limit),
    );
    let (stdout, stdout_truncated, stdout_total) = stdout?;
    let (stderr, stderr_truncated, stderr_total) = stderr?;
    let status = child.wait().await?;

    Ok(CommandOutput {
        stdout,
        stdout_truncated,
        stdout_total,
        stderr,
        stderr_truncated,
        stderr_total,
        status,
    })
}

/// Reads `reader` to EOF, keeping at most `limit` bytes.
///
/// Returns the retained bytes, whether bytes were discarded, and the total
/// number of bytes produced by the stream.
async fn read_capped<R>(mut reader: R, limit: usize) -> std::io::Result<(Vec<u8>, bool, u64)>
where
    R: AsyncRead + Unpin,
{
    let mut captured = Vec::new();
    let mut buffer = [0u8; 8192];
    let mut truncated = false;
    let mut total = 0_u64;

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if captured.len() < limit {
            let remaining = limit - captured.len();
            let take = read.min(remaining);
            captured.extend_from_slice(&buffer[..take]);
            if take < read {
                truncated = true;
            }
        } else {
            truncated = true;
        }
    }

    Ok((captured, truncated, total))
}

fn render(output: &CommandOutput) -> String {
    let mut rendered = String::new();

    match output.status.code() {
        Some(code) => rendered.push_str(&format!("exit code: {code}\n")),
        None => rendered.push_str("exit code: terminated by signal\n"),
    }

    rendered.push_str("stdout:\n");
    rendered.push_str(&String::from_utf8_lossy(&output.stdout));
    if output.stdout_truncated {
        rendered.push_str(&format!(
            "\n[stdout truncated at {} bytes: {} bytes total]",
            output.stdout.len(),
            output.stdout_total
        ));
    }

    rendered.push_str("\nstderr:\n");
    rendered.push_str(&String::from_utf8_lossy(&output.stderr));
    if output.stderr_truncated {
        rendered.push_str(&format!(
            "\n[stderr truncated at {} bytes: {} bytes total]",
            output.stderr.len(),
            output.stderr_total
        ));
    }

    rendered
}

/// Applies the built-in allowlist plus extensions to a cleared environment.
fn apply_environment(command: &mut Command, extensions: &[String]) {
    for name in effective_env_allowlist(extensions) {
        if !is_valid_env_name(&name) || is_sensitive_env_name(&name) {
            continue;
        }
        if let Some(value) = std::env::var_os(&name) {
            command.env(name, value);
        }
    }
}

/// Returns the effective allowlist with duplicates removed.
fn effective_env_allowlist(extensions: &[String]) -> Vec<String> {
    let mut names: Vec<String> = DEFAULT_ENV_ALLOWLIST
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    for name in extensions {
        if !names.iter().any(|existing| existing == name) {
            names.push(name.clone());
        }
    }
    names
}

/// Returns whether `name` is a syntactically usable environment variable name.
fn is_valid_env_name(name: &str) -> bool {
    !name.is_empty()
        && !name.chars().any(|character| {
            character.is_whitespace() || matches!(character, '=' | '\0' | '*' | '?')
        })
}

/// Returns whether `name` must never be inherited by bash.
fn is_sensitive_env_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    SENSITIVE_ENV_NAMES.contains(&upper.as_str())
        || SENSITIVE_ENV_PREFIXES
            .iter()
            .any(|prefix| upper.starts_with(prefix))
        || SENSITIVE_ENV_MARKERS
            .iter()
            .any(|marker| upper.contains(marker))
}

/// Caps a rendered result at `max_bytes` on a UTF-8 boundary.
///
/// `source_total` is the number of bytes the command produced before
/// per-stream truncation, so the notice still reports the true output size when
/// the per-stream notice itself is cut off. When the limit is too small for the
/// notice, only as much of the notice as fits is returned.
fn cap_content(rendered: &str, max_bytes: usize, source_total: u64) -> String {
    if rendered.len() <= max_bytes {
        return rendered.to_owned();
    }

    let rendered_total = rendered.len();
    let mut kept = max_bytes;
    loop {
        let notice = truncation_notice(kept, rendered_total, source_total);
        if kept.saturating_add(notice.len()) <= max_bytes {
            let mut capped = String::with_capacity(kept + notice.len());
            capped.push_str(&rendered[..kept]);
            capped.push_str(&notice);
            return capped;
        }
        let next = floor_char_boundary(rendered, max_bytes.saturating_sub(notice.len()));
        if next == kept {
            let mut capped = String::with_capacity(max_bytes);
            capped.push_str(&rendered[..kept]);
            capped.push_str(&notice);
            let end = floor_char_boundary(&capped, max_bytes);
            return capped[..end].to_owned();
        }
        kept = next;
    }
}

/// Builds the truncation notice for a capped rendered result.
fn truncation_notice(kept: usize, rendered_total: usize, source_total: u64) -> String {
    if source_total == rendered_total as u64 {
        format!("\n[output truncated: kept {kept} of {rendered_total} bytes]")
    } else {
        format!(
            "\n[output truncated: kept {kept} of {rendered_total} rendered bytes; command produced {source_total} bytes]"
        )
    }
}

/// Returns the largest index at or below `index` that is a UTF-8 boundary.
fn floor_char_boundary(text: &str, index: usize) -> usize {
    let mut index = index.min(text.len());
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Temporary directory removed when the test ends.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        /// Creates a unique directory and stores its canonical path.
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "jessica-bash-{label}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("the clock must be after the epoch")
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).expect("temporary directory must be creatable");
            let path = std::fs::canonicalize(&path).expect("temporary directory must resolve");
            Self { path }
        }

        /// Returns the canonical path.
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[tokio::test]
    async fn an_explicit_current_directory_is_used_by_spawned_commands() {
        let temp = TempDir::new("explicit-cwd");
        std::fs::write(temp.path().join("marker.txt"), "inside")
            .expect("marker file must be writable");
        let tool =
            BashTool::with_limits(Duration::from_secs(10), 4096).with_current_dir(temp.path());

        let result = tool
            .execute(json!({"command": "pwd -P && test -f marker.txt && echo marker-present"}))
            .await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(
            result.content.contains("marker-present"),
            "the command must observe the configured directory: {}",
            result.content
        );
        assert!(
            result
                .content
                .contains(temp.path().to_str().expect("temp path must be UTF-8")),
            "pwd must report the configured directory: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn an_unset_current_directory_keeps_inheriting_the_process_directory() {
        let expected =
            std::env::current_dir().expect("the process current directory must be readable");
        let expected = std::fs::canonicalize(expected).expect("current directory must resolve");
        let tool = BashTool::new();

        let result = tool.execute(json!({"command": "pwd -P"})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(
            result
                .content
                .contains(expected.to_str().expect("current path must be UTF-8")),
            "an unset directory must keep the original behavior: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn a_missing_current_directory_is_reported_as_a_spawn_failure() {
        let missing = std::env::temp_dir().join(format!(
            "jessica-bash-missing-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the clock must be after the epoch")
                .as_nanos()
        ));
        let tool = BashTool::new().with_current_dir(&missing);

        let result = tool.execute(json!({"command": "true"})).await;

        assert!(result.is_error, "a missing cwd must fail the tool call");
        assert!(
            result.content.contains("failed to spawn bash"),
            "the failure must name the spawn step: {}",
            result.content
        );
    }
}
