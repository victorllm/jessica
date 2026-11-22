//! Adapter from `jessica-tools` command requests to the execution supervisor.
//!
//! `jessica-tools` cannot depend on `jessica-harness`, so command-backed native
//! tools accept a [`CommandRunner`]. This adapter is the production
//! implementation: it maps a tool request onto
//! [`jessica_harness::supervisor::run_supervised`], which clears the
//! environment, filters credential-bearing variables, enforces one deadline for
//! spawn/read/wait, caps both streams, and kills the child on drop. No shell is
//! involved.

use async_trait::async_trait;
use jessica_harness::{CommandSpec, SupervisorError, run_supervised};
use jessica_tools::{CommandError, CommandOutput, CommandRequest, CommandRunner, CommandStream};

/// Runs native-tool command requests through the Phase 9.2 supervisor.
pub(crate) struct HarnessCommandRunner;

#[async_trait]
impl CommandRunner for HarnessCommandRunner {
    async fn run(&self, request: CommandRequest) -> Result<CommandOutput, CommandError> {
        let spec = CommandSpec::new(request.program)
            .with_args(request.args)
            .with_current_dir(request.cwd)
            .with_env_allowlist(request.env_allowlist)
            .with_output_limit(request.output_limit);
        match run_supervised(&spec, request.timeout).await {
            Ok(output) => Ok(CommandOutput {
                status: output.status,
                stdout: convert_stream(output.stdout),
                stderr: convert_stream(output.stderr),
            }),
            Err(error) => Err(convert_error(error)),
        }
    }
}

/// Converts one captured supervisor stream.
fn convert_stream(stream: jessica_harness::CapturedStream) -> CommandStream {
    CommandStream {
        bytes: stream.bytes,
        truncated: stream.truncated,
        total_bytes: stream.total_bytes,
    }
}

/// Converts a supervisor failure without losing its structured variant.
fn convert_error(error: SupervisorError) -> CommandError {
    match error {
        SupervisorError::Spawn { program, source } => CommandError::Spawn { program, source },
        SupervisorError::Timeout {
            program,
            timeout_ms,
        } => CommandError::Timeout {
            program,
            timeout_ms,
        },
        SupervisorError::Read { program, source } => CommandError::Read { program, source },
        SupervisorError::Wait { program, source } => CommandError::Wait { program, source },
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use super::*;

    fn request(program: &str, args: &[&str], timeout: Duration) -> CommandRequest {
        CommandRequest {
            program: program.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            cwd: std::env::current_dir().expect("current directory must be readable"),
            env_allowlist: Vec::new(),
            timeout,
            output_limit: 4096,
        }
    }

    #[tokio::test]
    async fn a_nonzero_exit_is_returned_as_a_completed_output() {
        let output = HarnessCommandRunner
            .run(request(
                "sh",
                &["-c", "printf out; printf err >&2; exit 7"],
                Duration::from_secs(5),
            ))
            .await
            .expect("a non-zero exit is not a supervision error");

        assert_eq!(output.status.code(), Some(7));
        assert_eq!(output.stdout.bytes, b"out");
        assert_eq!(output.stderr.bytes, b"err");
        assert!(!output.stdout.truncated);
    }

    #[tokio::test]
    async fn literal_arguments_are_passed_without_shell_parsing() {
        let output = HarnessCommandRunner
            .run(request(
                "printf",
                &["%s", "a b; $(echo injected) *"],
                Duration::from_secs(5),
            ))
            .await
            .expect("printf must run");

        assert_eq!(output.stdout.bytes, b"a b; $(echo injected) *");
    }

    #[tokio::test]
    async fn a_timeout_is_structured_and_names_the_program() {
        let error = HarnessCommandRunner
            .run(request("sleep", &["5"], Duration::from_millis(50)))
            .await
            .expect_err("sleep must time out");

        assert!(
            matches!(&error, CommandError::Timeout { program, timeout_ms }
                if program == "sleep" && *timeout_ms == 50),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn a_missing_program_is_a_spawn_error() {
        let missing = format!("jessica-missing-{}", std::process::id());
        let error = HarnessCommandRunner
            .run(request(&missing, &[], Duration::from_secs(5)))
            .await
            .expect_err("a missing program must fail");

        assert!(
            matches!(&error, CommandError::Spawn { program, .. } if program == &missing),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn a_missing_working_directory_is_a_spawn_error() {
        let mut request = request("true", &[], Duration::from_secs(5));
        request.cwd = PathBuf::from(format!(
            "/definitely-missing-jessica-{}",
            std::process::id()
        ));

        let error = HarnessCommandRunner
            .run(request)
            .await
            .expect_err("a missing cwd must fail");

        assert!(
            matches!(error, CommandError::Spawn { .. }),
            "unexpected error: {error}"
        );
    }
}
