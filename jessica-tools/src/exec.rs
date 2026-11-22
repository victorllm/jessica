//! Direct program execution without a shell.
//!
//! `exec` differs from `bash` in two deliberate ways: it never invokes a shell,
//! and a non-zero exit status is a completed tool call (`is_error = false`)
//! whose JSON result carries `exit_code`. Spawn failures, timeouts, and
//! supervision failures are the only error results. Arguments stay an array so
//! a later policy phase can match program and argument prefixes without parsing
//! shell text.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::command::{CommandError, CommandOutput, CommandRequest, CommandRunner};
use crate::workspace::{canonical_root, resolve_existing};
use crate::{failure, success};

/// Runs one program directly, without a shell.
///
/// The request carries a program and an argument array. The configured runner
/// owns environment isolation and the wall-clock deadline; this tool resolves
/// and confines `cwd`, validates the optional per-call timeout, and renders a
/// structured JSON result. Process-group cleanup and rlimits remain a later
/// task.
pub struct ExecTool {
    root: PathBuf,
    runner: Arc<dyn CommandRunner>,
    timeout: Duration,
    output_limit: usize,
    env_allowlist: Vec<String>,
}

impl ExecTool {
    /// Creates an exec tool rooted at `root`.
    ///
    /// `timeout` is the maximum per-call deadline; `output_limit` caps each
    /// captured stream. A request may choose a shorter timeout but cannot
    /// exceed this maximum.
    pub fn new(
        root: impl Into<PathBuf>,
        runner: Arc<dyn CommandRunner>,
        timeout: Duration,
        output_limit: usize,
    ) -> Self {
        Self {
            root: root.into(),
            runner,
            timeout,
            output_limit,
            env_allowlist: Vec::new(),
        }
    }

    /// Adds environment variable names passed to the runner.
    ///
    /// The runner applies its own default allowlist and credential filtering.
    pub fn with_env_allowlist<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.env_allowlist.extend(names.into_iter().map(Into::into));
        self
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecArgs {
    program: String,
    #[serde(default)]
    args: Vec<String>,
    cwd: Option<String>,
    timeout_ms: Option<u64>,
}

#[async_trait]
impl Tool for ExecTool {
    fn name(&self) -> &str {
        "exec"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "exec",
                "description": format!(
                    "Run a program directly without a shell. Program and arguments are separate fields, so shell metacharacters are literal. Non-zero exits are reported as a completed result with exit_code; the call times out after at most {} seconds.",
                    self.timeout.as_secs_f64()
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "program": {
                            "type": "string",
                            "description": "Executable name or path, with no shell parsing."
                        },
                        "args": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Arguments passed in order without shell interpretation."
                        },
                        "cwd": {
                            "type": "string",
                            "description": "Working directory relative to the workspace root; defaults to the root."
                        },
                        "timeout_ms": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Optional shorter deadline in milliseconds; values above the configured maximum are clamped."
                        }
                    },
                    "required": ["program"]
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let args: ExecArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return failure(format!("invalid exec arguments: {error}")),
        };
        if args.program.trim().is_empty() {
            return failure("exec program must not be empty");
        }
        let timeout = match args.timeout_ms {
            Some(0) => return failure("exec timeout_ms must be at least 1"),
            Some(milliseconds) => Duration::from_millis(milliseconds).min(self.timeout),
            None => self.timeout,
        };

        let cwd = match &args.cwd {
            Some(requested) => match canonical_root(&self.root).await {
                Ok(root) => match resolve_existing(&root, requested).await {
                    Ok(path) => path,
                    Err(message) => return failure(message),
                },
                Err(message) => return failure(message),
            },
            None => match canonical_root(&self.root).await {
                Ok(root) => root,
                Err(message) => return failure(message),
            },
        };
        match tokio::fs::metadata(&cwd).await {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return failure(format!("exec cwd `{}` is not a directory", cwd.display())),
            Err(error) => {
                return failure(format!(
                    "failed to inspect exec cwd `{}`: {error}",
                    cwd.display()
                ));
            }
        }

        let started = Instant::now();
        let request = CommandRequest {
            program: args.program,
            args: args.args,
            cwd,
            env_allowlist: self.env_allowlist.clone(),
            timeout,
            output_limit: self.output_limit,
        };
        let result = self.runner.run(request).await;
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

        match result {
            Ok(output) => success(render_output(&output, duration_ms, timeout)),
            Err(error) => failure(render_error(&error, duration_ms)),
        }
    }
}

/// Renders a completed command as structured JSON.
fn render_output(output: &CommandOutput, duration_ms: u64, timeout: Duration) -> String {
    let exit_code = output.status.code();
    let status = if exit_code.is_some() {
        "exited"
    } else {
        "signaled"
    };
    json!({
        "status": status,
        "tool": "exec",
        "exit_code": exit_code,
        "duration_ms": duration_ms,
        "timeout_ms": u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        "stdout": String::from_utf8_lossy(&output.stdout.bytes),
        "stdout_bytes": output.stdout.total_bytes,
        "stdout_truncated": output.stdout.truncated,
        "stderr": String::from_utf8_lossy(&output.stderr.bytes),
        "stderr_bytes": output.stderr.total_bytes,
        "stderr_truncated": output.stderr.truncated,
    })
    .to_string()
}

/// Renders a supervision failure as structured JSON.
fn render_error(error: &CommandError, duration_ms: u64) -> String {
    let status = match error {
        CommandError::Spawn { .. } => "spawn_error",
        CommandError::Timeout { .. } => "timeout",
        CommandError::Read { .. } | CommandError::Wait { .. } => "io_error",
    };
    json!({
        "status": status,
        "tool": "exec",
        "duration_ms": duration_ms,
        "error": error.to_string(),
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::Value;

    use super::*;
    use crate::command::CommandOutput;
    use crate::test_support::{RecordingRunner, TempDir, exit_status, stream};

    fn completed(code: i32, stdout: &[u8], stderr: &[u8]) -> CommandOutput {
        CommandOutput {
            status: exit_status(code),
            stdout: stream(stdout),
            stderr: stream(stderr),
        }
    }

    fn tool_with(runner: Arc<dyn CommandRunner>, timeout: Duration) -> ExecTool {
        ExecTool::new(PathBuf::from("."), runner, timeout, 4096)
    }

    fn recording() -> (Arc<RecordingRunner>, ExecTool) {
        let concrete = Arc::new(RecordingRunner::repeating(completed(0, b"", b"")));
        let runner: Arc<dyn CommandRunner> = concrete.clone();
        (concrete, tool_with(runner, Duration::from_secs(2)))
    }

    #[tokio::test]
    async fn a_nonzero_exit_is_a_completed_result_with_exit_code() {
        let runner: Arc<dyn CommandRunner> = Arc::new(RecordingRunner::repeating(completed(
            3,
            b"partial output",
            b"failure detail",
        )));
        let tool = tool_with(runner, Duration::from_secs(5));

        let result = tool.execute(json!({"program": "false"})).await;

        assert!(
            !result.is_error,
            "a non-zero exit is not a tool error: {}",
            result.content
        );
        let value: Value = serde_json::from_str(&result.content).expect("result must be JSON");
        assert_eq!(value["status"], "exited");
        assert_eq!(value["tool"], "exec");
        assert_eq!(value["exit_code"], 3);
        assert_eq!(value["stdout"], "partial output");
        assert_eq!(value["stderr"], "failure detail");
        assert_eq!(value["stdout_bytes"], 14);
        assert_eq!(value["stderr_bytes"], 14);
        assert_eq!(value["stdout_truncated"], false);
    }

    #[tokio::test]
    async fn program_and_arguments_are_forwarded_as_an_array_without_a_shell() {
        let runner = Arc::new(RecordingRunner::repeating(completed(0, b"", b"")));
        let temp = TempDir::new("exec-args");
        let tool = ExecTool::new(
            temp.path(),
            Arc::clone(&runner) as Arc<dyn CommandRunner>,
            Duration::from_secs(5),
            4096,
        );

        let result = tool
            .execute(json!({
                "program": "printf",
                "args": ["%s", "a b; $(echo injected)", "*"],
                "cwd": "."
            }))
            .await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        let requests = runner.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, "printf");
        assert_eq!(
            requests[0].args,
            ["%s", "a b; $(echo injected)", "*"],
            "argument boundaries and shell metacharacters must be preserved"
        );
        assert_eq!(
            requests[0].cwd,
            temp.path(),
            "cwd must resolve to the workspace root"
        );
    }

    #[tokio::test]
    async fn a_requested_timeout_is_clamped_to_the_configured_maximum() {
        let (runner, tool) = recording();

        let result = tool
            .execute(json!({"program": "sleep", "timeout_ms": 60_000}))
            .await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        let requests = runner.requests();
        assert_eq!(requests[0].timeout, Duration::from_secs(2));
        let value: Value = serde_json::from_str(&result.content).expect("result must be JSON");
        assert_eq!(value["timeout_ms"], 2_000);
    }

    #[tokio::test]
    async fn a_zero_timeout_is_rejected_before_the_runner_is_called() {
        let (runner, tool) = recording();

        let result = tool
            .execute(json!({"program": "true", "timeout_ms": 0}))
            .await;

        assert!(result.is_error);
        assert!(result.content.contains("timeout_ms must be at least 1"));
        assert!(runner.requests().is_empty());
    }

    #[tokio::test]
    async fn a_spawn_failure_is_an_error_result() {
        let tool = ExecTool::new(
            PathBuf::from("."),
            Arc::new(FailingRunner("spawn")),
            Duration::from_secs(2),
            4096,
        );

        let result = tool.execute(json!({"program": "missing-program"})).await;

        assert!(result.is_error, "a spawn failure must be a tool error");
        let value: Value = serde_json::from_str(&result.content).expect("result must be JSON");
        assert_eq!(value["status"], "spawn_error");
        assert_eq!(value["tool"], "exec");
        assert!(
            value["error"]
                .as_str()
                .is_some_and(|error| error.contains("missing-program"))
        );
    }

    #[tokio::test]
    async fn a_timeout_is_an_error_result() {
        let tool = ExecTool::new(
            PathBuf::from("."),
            Arc::new(FailingRunner("timeout")),
            Duration::from_secs(2),
            4096,
        );

        let result = tool.execute(json!({"program": "sleep"})).await;

        assert!(result.is_error, "a timeout must be a tool error");
        let value: Value = serde_json::from_str(&result.content).expect("result must be JSON");
        assert_eq!(value["status"], "timeout");
        assert!(
            value["error"]
                .as_str()
                .is_some_and(|error| error.contains("timed out"))
        );
    }

    #[tokio::test]
    async fn cwd_escape_is_rejected_before_spawning() {
        let temp = TempDir::new("exec-cwd");
        let outside = TempDir::new("exec-cwd-outside");
        let runner = Arc::new(RecordingRunner::repeating(completed(0, b"", b"")));
        let tool = ExecTool::new(
            temp.path(),
            Arc::clone(&runner) as Arc<dyn CommandRunner>,
            Duration::from_secs(2),
            4096,
        );

        let result = tool
            .execute(json!({"program": "true", "cwd": outside.path().to_string_lossy()}))
            .await;

        assert!(result.is_error);
        assert!(result.content.contains("outside the workspace"));
        assert!(
            runner.requests().is_empty(),
            "a rejected cwd must not reach the runner"
        );
    }

    #[tokio::test]
    async fn a_missing_cwd_is_rejected_before_spawning() {
        let temp = TempDir::new("exec-missing-cwd");
        let runner = Arc::new(RecordingRunner::repeating(completed(0, b"", b"")));
        let tool = ExecTool::new(
            temp.path(),
            Arc::clone(&runner) as Arc<dyn CommandRunner>,
            Duration::from_secs(2),
            4096,
        );

        let result = tool
            .execute(json!({"program": "true", "cwd": "missing"}))
            .await;

        assert!(result.is_error);
        assert!(result.content.contains("failed to resolve"));
        assert!(runner.requests().is_empty());
    }

    #[tokio::test]
    async fn empty_programs_and_unknown_fields_are_rejected() {
        let (runner, tool) = recording();

        let empty = tool.execute(json!({"program": "   "})).await;
        assert!(empty.is_error);
        assert!(empty.content.contains("must not be empty"));

        let unknown = tool
            .execute(json!({"program": "true", "shell": "echo hi"}))
            .await;
        assert!(unknown.is_error);
        assert!(unknown.content.contains("invalid exec arguments"));
        assert!(runner.requests().is_empty());
    }

    /// A runner that returns one requested supervision failure.
    struct FailingRunner(&'static str);

    #[async_trait]
    impl CommandRunner for FailingRunner {
        async fn run(&self, request: CommandRequest) -> Result<CommandOutput, CommandError> {
            match self.0 {
                "spawn" => Err(CommandError::Spawn {
                    program: request.program,
                    source: io::Error::other("not found"),
                }),
                _ => Err(CommandError::Timeout {
                    program: request.program,
                    timeout_ms: 2_000,
                }),
            }
        }
    }
}
