//! Command-backed acceptance check used by the example CLI configuration.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::acceptance::{AcceptanceCheck, AcceptanceContext, AcceptanceResult};
use jessica_harness::{
    CapturedStream, CommandSpec, DEFAULT_COMMAND_TIMEOUT, DEFAULT_OUTPUT_LIMIT, SupervisedOutput,
    SupervisorError, cap_rendered_output_with_total, run_supervised,
};

use crate::config::ResolvedSupervisor;

/// Runs one external command in a fixed working directory.
///
/// The command runs directly, without a shell; callers that need shell syntax
/// can invoke `sh -c`. It starts with an empty environment containing only the
/// supervisor allowlist, and the complete spawn/read/wait sequence is bounded
/// by a deadline. A zero exit status passes. Non-zero status, a signal, a
/// timeout, a spawn failure, or a missing executable fails with diagnostics
/// that are fed back to the model on retry. Captured output is capped with an
/// explicit truncation notice.
#[derive(Debug, Clone)]
pub(crate) struct CommandAcceptanceCheck {
    name: String,
    program: String,
    args: Vec<String>,
    current_dir: PathBuf,
    env_allowlist: Vec<String>,
    timeout: Duration,
    output_limit: usize,
}

impl CommandAcceptanceCheck {
    /// Creates a check that runs `program` with `args` in `current_dir`.
    pub(crate) fn new(
        name: impl Into<String>,
        program: impl Into<String>,
        args: Vec<String>,
        current_dir: PathBuf,
    ) -> Self {
        Self {
            name: name.into(),
            program: program.into(),
            args,
            current_dir,
            env_allowlist: Vec::new(),
            timeout: DEFAULT_COMMAND_TIMEOUT,
            output_limit: DEFAULT_OUTPUT_LIMIT,
        }
    }

    /// Applies the resolved supervisor limits to this check.
    pub(crate) fn with_supervisor(mut self, supervisor: &ResolvedSupervisor) -> Self {
        self.env_allowlist = supervisor.env_allowlist.clone();
        self.timeout = supervisor.acceptance_timeout;
        self.output_limit = supervisor.max_output_bytes;
        self
    }
}

#[async_trait]
impl AcceptanceCheck for CommandAcceptanceCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self, _context: &AcceptanceContext<'_>) -> AcceptanceResult {
        let spec = CommandSpec {
            program: self.program.clone(),
            args: self.args.clone(),
            current_dir: Some(self.current_dir.clone()),
            env_allowlist: self.env_allowlist.clone(),
            output_limit: self.output_limit,
        };

        match run_supervised(&spec, self.timeout).await {
            Ok(output) if output.status.success() => {
                AcceptanceResult::pass(format!("command `{}` exited successfully", self.program))
            }
            Ok(output) => {
                let source_total = output
                    .stdout
                    .total_bytes
                    .saturating_add(output.stderr.total_bytes);
                let rendered = cap_rendered_output_with_total(
                    &render_output(&output),
                    self.output_limit,
                    source_total,
                );
                AcceptanceResult::fail(format!(
                    "acceptance command `{}` failed with {}.{rendered}",
                    self.program, output.status
                ))
            }
            Err(SupervisorError::Timeout { timeout_ms, .. }) => AcceptanceResult::fail(format!(
                "acceptance command `{}` timed out after {timeout_ms} ms and was terminated",
                self.program
            )),
            Err(error) => AcceptanceResult::fail(format!(
                "could not run acceptance command `{}` in `{}`: {error}",
                self.program,
                self.current_dir.display()
            )),
        }
    }
}

/// Renders captured output, preferring stderr because failures usually explain
/// themselves there.
fn render_output(output: &SupervisedOutput) -> String {
    let stderr = render_stream(&output.stderr);
    if !stderr.is_empty() {
        return format!("\nstderr: {stderr}");
    }

    let stdout = render_stream(&output.stdout);
    if stdout.is_empty() {
        String::new()
    } else {
        format!("\nstdout: {stdout}")
    }
}

/// Renders one stream, appending its truncation state.
fn render_stream(stream: &CapturedStream) -> String {
    if stream.bytes.is_empty() && !stream.truncated {
        return String::new();
    }
    let mut rendered = String::from_utf8_lossy(&stream.bytes).into_owned();
    if stream.truncated {
        rendered.push_str(&format!(
            "\n…(stream truncated at {} bytes: {} bytes total)",
            stream.bytes.len(),
            stream.total_bytes
        ));
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use jessica_core::types::Message;
    use std::time::Instant;

    fn check(command: &str) -> CommandAcceptanceCheck {
        CommandAcceptanceCheck::new(
            "test-check",
            "sh",
            vec!["-c".to_owned(), command.to_owned()],
            std::env::current_dir().expect("test cwd must exist"),
        )
    }

    async fn run(check: &CommandAcceptanceCheck) -> AcceptanceResult {
        check
            .check(&AcceptanceContext {
                messages: &[Message::user("fix it")],
                last_output: "done",
            })
            .await
    }

    #[tokio::test]
    async fn zero_exit_status_passes() {
        let result = run(&check("printf hello")).await;
        assert!(result.passed);
        assert!(result.feedback.contains("exited successfully"));
        assert_eq!(check("true").name(), "test-check");
    }

    #[tokio::test]
    async fn non_zero_exit_status_fails_with_stderr_feedback() {
        let result = run(&check("printf boom >&2; exit 7")).await;
        assert!(!result.passed);
        assert!(result.feedback.contains("failed with"));
        assert!(result.feedback.contains("boom"));
        assert!(result.feedback.contains('7'));
    }

    #[tokio::test]
    async fn missing_program_fails_without_panicking() {
        let check = CommandAcceptanceCheck::new(
            "missing",
            "/definitely/not/a/jessica-check",
            Vec::new(),
            std::env::current_dir().expect("test cwd must exist"),
        );
        let result = run(&check).await;
        assert!(!result.passed);
        assert!(result.feedback.contains("could not start"));
        assert!(result.feedback.contains("/definitely/not/a/jessica-check"));
    }

    #[tokio::test]
    async fn a_hung_acceptance_command_times_out_and_fails_the_check() {
        let mut check = check("exec sleep 30");
        check.timeout = Duration::from_millis(100);
        let started = Instant::now();

        let result = run(&check).await;

        assert!(!result.passed);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the check must return instead of hanging the turn"
        );
        assert!(
            result.feedback.contains("timed out"),
            "the feedback must name the timeout: {}",
            result.feedback
        );
    }

    #[tokio::test]
    async fn acceptance_feedback_is_truncated_at_the_configured_limit() {
        let mut check = check(
            "i=0; while [ $i -lt 20 ]; do printf 'abcdefghijklmnopqrstuvwxyz' >&2; i=$((i+1)); done; exit 9",
        );
        check.output_limit = 64;

        let result = run(&check).await;

        assert!(!result.passed);
        assert!(
            result.feedback.contains("truncated"),
            "truncation must be observable: {}",
            result.feedback
        );
        assert!(
            result.feedback.contains("of "),
            "the notice must record the original size: {}",
            result.feedback
        );
        assert!(
            result.feedback.contains('9'),
            "the exit code must survive truncation: {}",
            result.feedback
        );
    }
}
