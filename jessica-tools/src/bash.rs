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
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default cap for each captured stream.
const DEFAULT_OUTPUT_LIMIT: usize = 64 * 1024;

/// Runs bash commands with a hard timeout and bounded stdout/stderr capture.
///
/// A command that exits non-zero is reported as a failed [`ToolResult`] whose
/// content still contains the captured output. Output beyond the per-stream
/// limit is discarded and marked with a truncation notice.
pub struct BashTool {
    timeout: Duration,
    output_limit: usize,
    current_dir: Option<PathBuf>,
}

impl BashTool {
    /// Creates a tool with a 10-second timeout and 64 KiB per-stream limit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a tool with explicit limits.
    ///
    /// `output_limit` applies separately to stdout and stderr. A zero limit
    /// keeps no output while still draining the pipes.
    pub fn with_limits(timeout: Duration, output_limit: usize) -> Self {
        Self {
            timeout,
            output_limit,
            current_dir: None,
        }
    }

    /// Sets the directory in which commands are spawned.
    ///
    /// This is additive: a tool built by [`BashTool::new`] keeps inheriting the
    /// process current directory. A path that does not exist is reported as a
    /// spawn failure when the command runs, not when the tool is built.
    pub fn with_current_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(dir.into());
        self
    }
}

impl Default for BashTool {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            output_limit: DEFAULT_OUTPUT_LIMIT,
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
                    "Run a bash command in the current directory. The command is killed after {} seconds; stdout and stderr are each capped at {} bytes.",
                    self.timeout.as_secs_f64(),
                    self.output_limit
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
            .kill_on_drop(true);
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
    stderr: Vec<u8>,
    stderr_truncated: bool,
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
    let (stdout, stdout_truncated) = stdout?;
    let (stderr, stderr_truncated) = stderr?;
    let status = child.wait().await?;

    Ok(CommandOutput {
        stdout,
        stdout_truncated,
        stderr,
        stderr_truncated,
        status,
    })
}

/// Reads `reader` to EOF, keeping at most `limit` bytes.
async fn read_capped<R>(mut reader: R, limit: usize) -> std::io::Result<(Vec<u8>, bool)>
where
    R: AsyncRead + Unpin,
{
    let mut captured = Vec::new();
    let mut buffer = [0u8; 8192];
    let mut truncated = false;

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
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

    Ok((captured, truncated))
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
            "\n[stdout truncated at {} bytes]",
            output.stdout.len()
        ));
    }

    rendered.push_str("\nstderr:\n");
    rendered.push_str(&String::from_utf8_lossy(&output.stderr));
    if output.stderr_truncated {
        rendered.push_str(&format!(
            "\n[stderr truncated at {} bytes]",
            output.stderr.len()
        ));
    }

    rendered
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
