//! Read-only Git inspection tools.
//!
//! The tools invoke `git` directly with a fixed, read-only subcommand and a
//! fixed argument array. There is no shell and no user-controlled Git argument,
//! so a model cannot request a mutating subcommand or inject options. The
//! working directory is always the canonical workspace root, which must be
//! inside the configured workspace. Command execution, environment isolation,
//! deadline enforcement, and output capture are delegated to the configured
//! [`CommandRunner`].

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::command::{CommandOutput, CommandRequest, CommandRunner, CommandStream};
use crate::failure;
use crate::workspace::canonical_root;

/// Fixed read-only Git operations exposed by [`GitTool`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitOperation {
    /// `git status --porcelain=v1 --untracked-files=all`
    Status,
    /// `git diff --no-ext-diff --no-color --`
    Diff,
}

impl GitOperation {
    /// Returns the model-visible tool name.
    pub fn tool_name(self) -> &'static str {
        match self {
            Self::Status => "git_status",
            Self::Diff => "git_diff",
        }
    }

    /// Returns the fixed argument array passed to Git.
    fn arguments(self) -> &'static [&'static str] {
        match self {
            Self::Status => &[
                "--no-pager",
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ],
            Self::Diff => &["--no-pager", "diff", "--no-ext-diff", "--no-color", "--"],
        }
    }
}

/// Runs one fixed, read-only Git command in the workspace root.
///
/// The tool accepts no arguments. A non-zero exit, a missing `git` binary, or
/// a timeout is reported as a failed tool result; a workspace that is not a Git
/// repository receives a stable hint instead of a panic. Output is bounded by
/// the runner's per-stream limit and the host result manager.
pub struct GitTool {
    root: PathBuf,
    operation: GitOperation,
    runner: Arc<dyn CommandRunner>,
    timeout: Duration,
    output_limit: usize,
    env_allowlist: Vec<String>,
}

impl GitTool {
    /// Creates a Git inspection tool rooted at `root`.
    pub fn new(
        root: impl Into<PathBuf>,
        operation: GitOperation,
        runner: Arc<dyn CommandRunner>,
        timeout: Duration,
        output_limit: usize,
    ) -> Self {
        Self {
            root: root.into(),
            operation,
            runner,
            timeout,
            output_limit,
            env_allowlist: Vec::new(),
        }
    }

    /// Adds environment variable names passed to the runner.
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
struct GitArgs {}

#[async_trait]
impl Tool for GitTool {
    fn name(&self) -> &str {
        self.operation.tool_name()
    }

    fn schema(&self) -> ToolSchema {
        let (name, description) = match self.operation {
            GitOperation::Status => (
                "git_status",
                "Show `git status --porcelain=v1` for the workspace. Takes no arguments; the subcommand is fixed and read-only.",
            ),
            GitOperation::Diff => (
                "git_diff",
                "Show unstaged `git diff` output for the workspace. Takes no arguments; the subcommand is fixed and read-only.",
            ),
        };
        json!({
            "type": "function",
            "function": {
                "name": name,
                "description": description,
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        if let Err(error) = serde_json::from_value::<GitArgs>(args) {
            return failure(format!("invalid {} arguments: {error}", self.name()));
        }
        let root = match canonical_root(&self.root).await {
            Ok(root) => root,
            Err(message) => return failure(message),
        };
        let request = CommandRequest {
            program: "git".to_owned(),
            args: self
                .operation
                .arguments()
                .iter()
                .map(|argument| (*argument).to_owned())
                .collect(),
            cwd: root.clone(),
            env_allowlist: self.env_allowlist.clone(),
            timeout: self.timeout,
            output_limit: self.output_limit,
        };
        match self.runner.run(request).await {
            Ok(output) => render(self.operation, &root, &output),
            Err(error) => failure(error.to_string()),
        }
    }
}

/// Renders a completed Git command.
fn render(operation: GitOperation, root: &std::path::Path, output: &CommandOutput) -> ToolResult {
    let stdout = String::from_utf8_lossy(&output.stdout.bytes);
    let stderr = String::from_utf8_lossy(&output.stderr.bytes);

    if !output.status.success() {
        if stderr.to_ascii_lowercase().contains("not a git repository") {
            return failure(format!(
                "not a git repository: {}; run this tool from a git worktree or initialize one with `git init`",
                root.display()
            ));
        }
        return failure(format!(
            "{} failed with exit code {}:\n{}{}",
            operation.tool_name(),
            output
                .status
                .code()
                .map_or_else(|| "signal".to_owned(), |code| code.to_string()),
            stream_section("stderr", &output.stderr),
            stream_section("stdout", &output.stdout),
        ));
    }

    let text = match operation {
        GitOperation::Status if stdout.trim().is_empty() => "working tree clean".to_owned(),
        GitOperation::Diff if stdout.trim().is_empty() => "no diff".to_owned(),
        _ => stdout.into_owned(),
    };
    let mut rendered = text;
    if output.stdout.truncated {
        rendered.push_str(&truncation_notice("stdout", &output.stdout));
    }
    if output.stderr.truncated {
        rendered.push_str(&truncation_notice("stderr", &output.stderr));
    }
    ToolResult::success(String::new(), rendered)
}

/// Renders one stream for a failure message.
fn stream_section(name: &str, stream: &CommandStream) -> String {
    let text = String::from_utf8_lossy(&stream.bytes);
    let mut section = format!("{name}:\n{text}");
    if stream.truncated {
        section.push_str(&truncation_notice(name, stream));
    }
    if !section.ends_with('\n') {
        section.push('\n');
    }
    section
}

/// Builds a truncation notice for one captured stream.
fn truncation_notice(name: &str, stream: &CommandStream) -> String {
    format!(
        "\n[{name} truncated at {} bytes: {} bytes total]\n",
        stream.bytes.len(),
        stream.total_bytes
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::command::{CommandError, CommandOutput, CommandStream};
    use crate::test_support::{RecordingRunner, TempDir, exit_status, stream};

    fn completed(code: i32, stdout: &[u8], stderr: &[u8]) -> CommandOutput {
        CommandOutput {
            status: exit_status(code),
            stdout: stream(stdout),
            stderr: stream(stderr),
        }
    }

    fn tool_with(
        root: &std::path::Path,
        operation: GitOperation,
        output: CommandOutput,
    ) -> (Arc<RecordingRunner>, GitTool) {
        let runner = Arc::new(RecordingRunner::repeating(output));
        let tool = GitTool::new(
            root,
            operation,
            Arc::clone(&runner) as Arc<dyn CommandRunner>,
            Duration::from_secs(5),
            4096,
        );
        (runner, tool)
    }

    #[tokio::test]
    async fn status_uses_fixed_arguments_and_the_workspace_root() {
        let temp = TempDir::new("git-status");
        let (runner, tool) = tool_with(
            temp.path(),
            GitOperation::Status,
            completed(0, b"?? new.txt\n", b""),
        );

        let result = tool.execute(json!({})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(result.content, "?? new.txt\n");
        let requests = runner.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, "git");
        assert_eq!(
            requests[0].args,
            [
                "--no-pager",
                "status",
                "--porcelain=v1",
                "--untracked-files=all"
            ]
        );
        assert_eq!(requests[0].cwd, temp.path());
    }

    #[tokio::test]
    async fn diff_uses_read_only_flags_and_reports_an_empty_diff() {
        let temp = TempDir::new("git-diff");
        let (runner, tool) = tool_with(temp.path(), GitOperation::Diff, completed(0, b"", b""));

        let result = tool.execute(json!({})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(result.content, "no diff");
        assert_eq!(
            runner.requests()[0].args,
            ["--no-pager", "diff", "--no-ext-diff", "--no-color", "--"]
        );
    }

    #[tokio::test]
    async fn a_clean_status_is_explicit() {
        let temp = TempDir::new("git-clean");
        let (_runner, tool) = tool_with(temp.path(), GitOperation::Status, completed(0, b"", b""));

        let result = tool.execute(json!({})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert_eq!(result.content, "working tree clean");
    }

    #[tokio::test]
    async fn a_missing_repository_returns_a_stable_hint() {
        let temp = TempDir::new("git-not-repo");
        let (_runner, tool) = tool_with(
            temp.path(),
            GitOperation::Status,
            completed(
                128,
                b"",
                b"fatal: not a git repository (or any of the parent directories): .git\n",
            ),
        );

        let result = tool.execute(json!({})).await;

        assert!(result.is_error, "not a repository must be an error result");
        assert!(
            result.content.contains("not a git repository") && result.content.contains("git init"),
            "the hint must explain how to proceed: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn another_nonzero_exit_reports_the_exit_code_and_streams() {
        let temp = TempDir::new("git-failure");
        let (_runner, tool) = tool_with(
            temp.path(),
            GitOperation::Diff,
            completed(2, b"partial", b"fatal detail"),
        );

        let result = tool.execute(json!({})).await;

        assert!(result.is_error);
        assert!(result.content.contains("git_diff failed with exit code 2"));
        assert!(result.content.contains("stderr:\nfatal detail"));
        assert!(result.content.contains("stdout:\npartial"));
    }

    #[tokio::test]
    async fn user_arguments_are_rejected_without_running_git() {
        let temp = TempDir::new("git-injection");
        let (runner, tool) = tool_with(temp.path(), GitOperation::Status, completed(0, b"", b""));

        let result = tool.execute(json!({"args": ["push", "--force"]})).await;

        assert!(result.is_error);
        assert!(result.content.contains("invalid git_status arguments"));
        assert!(
            runner.requests().is_empty(),
            "user-controlled git arguments must never reach the runner"
        );
    }

    #[tokio::test]
    async fn a_runner_failure_is_a_tool_error() {
        let temp = TempDir::new("git-runner-error");
        let runner: Arc<dyn CommandRunner> = Arc::new(FailingRunner);
        let tool = GitTool::new(
            temp.path(),
            GitOperation::Status,
            runner,
            Duration::from_secs(5),
            4096,
        );

        let result = tool.execute(json!({})).await;

        assert!(result.is_error);
        assert!(result.content.contains("could not start `git`"));
    }

    #[tokio::test]
    async fn truncated_output_is_disclosed() {
        let temp = TempDir::new("git-truncated");
        let output = CommandOutput {
            status: exit_status(0),
            stdout: CommandStream {
                bytes: b"?? a\n".to_vec(),
                truncated: true,
                total_bytes: 999,
            },
            stderr: stream(b""),
        };
        let (_runner, tool) = tool_with(temp.path(), GitOperation::Status, output);

        let result = tool.execute(json!({})).await;

        assert!(!result.is_error, "unexpected failure: {}", result.content);
        assert!(
            result
                .content
                .contains("stdout truncated at 5 bytes: 999 bytes total"),
            "the truncation must be visible: {}",
            result.content
        );
    }

    /// A runner whose spawn always fails.
    struct FailingRunner;

    #[async_trait]
    impl CommandRunner for FailingRunner {
        async fn run(&self, request: CommandRequest) -> Result<CommandOutput, CommandError> {
            Err(CommandError::Spawn {
                program: request.program,
                source: std::io::Error::other("git is not installed"),
            })
        }
    }
}
