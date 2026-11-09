//! Command-backed acceptance check used by the example CLI configuration.

use std::path::PathBuf;
use std::process::Stdio;

use async_trait::async_trait;
use jessica_core::acceptance::{AcceptanceCheck, AcceptanceContext, AcceptanceResult};
use tokio::process::Command;

/// Maximum captured bytes per output stream in acceptance feedback.
const OUTPUT_LIMIT: usize = 8 * 1024;

/// Runs one external command in a fixed working directory.
///
/// The command runs directly, without a shell; callers that need shell syntax
/// can invoke `sh -c`. A zero exit status passes. Non-zero status, a signal, a
/// spawn failure, or a missing executable fails with diagnostics that are fed
/// back to the model on retry.
#[derive(Debug, Clone)]
pub(crate) struct CommandAcceptanceCheck {
    name: String,
    program: String,
    args: Vec<String>,
    current_dir: PathBuf,
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
        }
    }
}

#[async_trait]
impl AcceptanceCheck for CommandAcceptanceCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self, _context: &AcceptanceContext<'_>) -> AcceptanceResult {
        let output = Command::new(&self.program)
            .args(&self.args)
            .current_dir(&self.current_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .output()
            .await;

        match output {
            Ok(output) if output.status.success() => {
                AcceptanceResult::pass(format!("command `{}` exited successfully", self.program))
            }
            Ok(output) => {
                let mut feedback = format!(
                    "acceptance command `{}` failed with {}.{}",
                    self.program,
                    output.status,
                    render_output(&output.stdout, &output.stderr)
                );
                if feedback.chars().count() > OUTPUT_LIMIT * 2 {
                    let mut truncated: String = feedback.chars().take(OUTPUT_LIMIT * 2).collect();
                    truncated.push('…');
                    feedback = truncated;
                }
                AcceptanceResult::fail(feedback)
            }
            Err(error) => AcceptanceResult::fail(format!(
                "could not start acceptance command `{}` in `{}`: {error}",
                self.program,
                self.current_dir.display()
            )),
        }
    }
}

/// Renders captured output, preferring stderr because failures usually explain
/// themselves there.
fn render_output(stdout: &[u8], stderr: &[u8]) -> String {
    let stderr = capped(stderr);
    if !stderr.is_empty() {
        return format!("\nstderr: {stderr}");
    }

    let stdout = capped(stdout);
    if stdout.is_empty() {
        String::new()
    } else {
        format!("\nstdout: {stdout}")
    }
}

fn capped(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= OUTPUT_LIMIT {
        text.into_owned()
    } else {
        let mut truncated = String::from_utf8_lossy(&bytes[..OUTPUT_LIMIT]).into_owned();
        truncated.push_str("\n…(output truncated)");
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jessica_core::types::Message;

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
}
