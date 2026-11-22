//! Command execution contract shared by native command-backed tools.
//!
//! `jessica-tools` may not depend on `jessica-harness`, so tools accept a
//! [`CommandRunner`] instead of calling the supervisor directly. The CLI
//! supplies an adapter over `jessica_harness::supervisor::run_supervised`, which
//! provides the execution-supervisor semantics: cleared environment, allowlist
//! filtering, a whole-lifecycle deadline, bounded output capture, and
//! `kill_on_drop`. Tests may substitute a recording runner to observe the exact
//! program, argument array, working directory, and environment allowlist.

use std::path::PathBuf;
use std::process::ExitStatus;
use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;

/// One command request, with no shell interpretation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRequest {
    /// Program to execute directly.
    pub program: String,
    /// Arguments passed as an array, preserving boundaries and literal special
    /// characters.
    pub args: Vec<String>,
    /// Working directory, already resolved and confined by the caller.
    pub cwd: PathBuf,
    /// Additional environment names on top of the runner's built-in defaults.
    pub env_allowlist: Vec<String>,
    /// Whole-lifecycle deadline for spawn, output capture, and exit.
    pub timeout: Duration,
    /// Maximum captured bytes per output stream.
    pub output_limit: usize,
}

/// One captured output stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandStream {
    /// Retained bytes, at most the request's output limit.
    pub bytes: Vec<u8>,
    /// Whether bytes were discarded.
    pub truncated: bool,
    /// Total bytes produced, including discarded bytes.
    pub total_bytes: u64,
}

/// A command that ran to completion.
///
/// A non-zero exit status is a completed run, not a runner error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    /// Exit status of the command.
    pub status: ExitStatus,
    /// Captured standard output.
    pub stdout: CommandStream,
    /// Captured standard error.
    pub stderr: CommandStream,
}

/// A failure to start, supervise, or collect a command.
#[derive(Debug, Error)]
pub enum CommandError {
    /// The program could not be started.
    #[error("could not start `{program}`: {source}")]
    Spawn {
        /// Program that failed to start.
        program: String,
        /// Operating-system error.
        #[source]
        source: std::io::Error,
    },
    /// The command exceeded its deadline and was killed.
    #[error("command `{program}` timed out after {timeout_ms} ms")]
    Timeout {
        /// Program that timed out.
        program: String,
        /// Deadline that expired.
        timeout_ms: u64,
    },
    /// Reading a captured stream failed.
    #[error("could not read output from `{program}`: {source}")]
    Read {
        /// Program whose output failed.
        program: String,
        /// Operating-system error.
        #[source]
        source: std::io::Error,
    },
    /// Waiting for the command to exit failed.
    #[error("could not wait for `{program}`: {source}")]
    Wait {
        /// Program whose wait failed.
        program: String,
        /// Operating-system error.
        #[source]
        source: std::io::Error,
    },
}

/// Executes a [`CommandRequest`] under host supervision.
///
/// Implementations must not invoke a shell, must start from a cleared
/// environment, and must return a non-zero exit status as a successful
/// [`CommandOutput`].
#[async_trait]
pub trait CommandRunner: Send + Sync {
    /// Runs one command request.
    async fn run(&self, request: CommandRequest) -> Result<CommandOutput, CommandError>;
}
