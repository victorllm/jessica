//! Bounded, environment-isolated command execution for host-side checks.
//!
//! Every supervised command starts with an empty environment and receives only
//! explicitly allowlisted variables. Credential-bearing names are filtered even
//! when a caller adds them, so provider keys and Jessica credentials never reach
//! a child process through inheritance.
//!
//! The whole lifecycle — spawn, pipe reads, and exit wait — runs under one
//! deadline. Dropping the future kills the direct child because `kill_on_drop`
//! is always set. Process-group cleanup and rlimits require platform-specific
//! dependencies and are intentionally not part of this module.

use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

/// Environment variable names passed to supervised commands by default.
///
/// These variables are needed by ordinary command-line programs and do not
/// carry credentials. Configuration may add names but cannot remove these.
pub const DEFAULT_ENV_ALLOWLIST: &[&str] = &["PATH", "HOME", "LANG", "TERM", "TMPDIR"];

/// Default wall-clock deadline for one supervised command.
pub const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// Default capture limit for each output stream, in bytes.
pub const DEFAULT_OUTPUT_LIMIT: usize = 64 * 1024;

/// Environment-variable prefixes whose values are never inherited.
///
/// The prefixes cover provider credentials, Jessica's own configuration, cloud
/// credentials, source-control tokens, SSH agent sockets, and dynamic-linker
/// injection variables.
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

/// Returns whether `name` is a syntactically usable environment variable name.
///
/// A usable name is non-empty, contains no whitespace, and contains none of
/// `=`, NUL, `*`, or `?`. The wildcard characters are rejected so an allowlist
/// cannot accidentally request a whole prefix.
pub fn is_valid_env_name(name: &str) -> bool {
    !name.is_empty()
        && !name.chars().any(|character| {
            character.is_whitespace() || matches!(character, '=' | '\0' | '*' | '?')
        })
}

/// Returns whether `name` never may be inherited by a supervised command.
///
/// Credential detection is intentionally conservative: a false positive only
/// withholds a variable, while a false negative could leak a secret. Callers
/// may not override this with configuration.
pub fn is_sensitive_env_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    SENSITIVE_ENV_NAMES.contains(&upper.as_str())
        || SENSITIVE_ENV_PREFIXES
            .iter()
            .any(|prefix| upper.starts_with(prefix))
        || SENSITIVE_ENV_MARKERS
            .iter()
            .any(|marker| upper.contains(marker))
}

/// Returns the effective allowlist: built-in defaults followed by configured
/// extensions, with duplicates removed.
///
/// Order is deterministic: defaults keep their declaration order and extensions
/// keep configuration order. Sensitive names are not filtered here so callers
/// can validate and reject them; [`run_supervised`] filters them again as a
/// defense-in-depth measure.
pub fn effective_env_allowlist(extensions: &[String]) -> Vec<String> {
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

/// One command to run under supervision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    /// Program to execute directly, without a shell.
    pub program: String,
    /// Program arguments in order.
    pub args: Vec<String>,
    /// Working directory. When absent, the process current directory is used.
    pub current_dir: Option<PathBuf>,
    /// Additional environment names on top of [`DEFAULT_ENV_ALLOWLIST`].
    pub env_allowlist: Vec<String>,
    /// Maximum captured bytes per output stream.
    pub output_limit: usize,
}

impl CommandSpec {
    /// Creates a command with the default output limit and no environment
    /// extensions.
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            current_dir: None,
            env_allowlist: Vec::new(),
            output_limit: DEFAULT_OUTPUT_LIMIT,
        }
    }

    /// Appends program arguments in iteration order.
    pub fn with_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Sets the working directory.
    pub fn with_current_dir(mut self, directory: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(directory.into());
        self
    }

    /// Adds environment names on top of the built-in defaults.
    pub fn with_env_allowlist<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.env_allowlist.extend(names.into_iter().map(Into::into));
        self
    }

    /// Sets the per-stream capture limit.
    pub fn with_output_limit(mut self, limit: usize) -> Self {
        self.output_limit = limit;
        self
    }
}

/// One captured output stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedStream {
    /// Retained bytes, at most the configured limit.
    pub bytes: Vec<u8>,
    /// Whether any bytes were discarded.
    pub truncated: bool,
    /// Total bytes the stream produced, including discarded bytes.
    pub total_bytes: u64,
}

/// A supervised command that ran to completion.
///
/// A non-zero [`status`](Self::status) means the command executed but failed;
/// it is not a supervisor error.
#[derive(Debug, Clone)]
pub struct SupervisedOutput {
    /// Exit status of the command.
    pub status: ExitStatus,
    /// Captured standard output.
    pub stdout: CapturedStream,
    /// Captured standard error.
    pub stderr: CapturedStream,
}

/// A failure to start, supervise, or collect a command.
///
/// Spawn failures, timeouts, pipe-read failures, and wait failures are distinct
/// from a non-zero exit status, which is returned as a successful
/// [`SupervisedOutput`].
#[derive(Debug, Error)]
pub enum SupervisorError {
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

/// Runs `spec` under `timeout` with a cleared environment.
///
/// The deadline covers spawning, reading both pipes, and waiting for exit. On
/// timeout the child is dropped with `kill_on_drop` set, which terminates the
/// direct child. Arguments and environment values are never included in error
/// messages.
pub async fn run_supervised(
    spec: &CommandSpec,
    timeout: Duration,
) -> Result<SupervisedOutput, SupervisorError> {
    match tokio::time::timeout(timeout, run(spec)).await {
        Ok(result) => result,
        Err(_) => Err(SupervisorError::Timeout {
            program: spec.program.clone(),
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        }),
    }
}

/// Spawns the command, drains both pipes, and waits for exit.
async fn run(spec: &CommandSpec) -> Result<SupervisedOutput, SupervisorError> {
    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env_clear();
    apply_environment(&mut command, &spec.env_allowlist);
    if let Some(directory) = &spec.current_dir {
        command.current_dir(directory);
    }

    let mut child = command.spawn().map_err(|source| SupervisorError::Spawn {
        program: spec.program.clone(),
        source,
    })?;
    let stdout = child.stdout.take().ok_or_else(|| SupervisorError::Read {
        program: spec.program.clone(),
        source: std::io::Error::other("stdout pipe was not captured"),
    })?;
    let stderr = child.stderr.take().ok_or_else(|| SupervisorError::Read {
        program: spec.program.clone(),
        source: std::io::Error::other("stderr pipe was not captured"),
    })?;

    let (stdout, stderr) = tokio::join!(
        read_capped(stdout, spec.output_limit),
        read_capped(stderr, spec.output_limit),
    );
    let stdout = stdout.map_err(|source| SupervisorError::Read {
        program: spec.program.clone(),
        source,
    })?;
    let stderr = stderr.map_err(|source| SupervisorError::Read {
        program: spec.program.clone(),
        source,
    })?;
    let status = child.wait().await.map_err(|source| SupervisorError::Wait {
        program: spec.program.clone(),
        source,
    })?;

    Ok(SupervisedOutput {
        status,
        stdout,
        stderr,
    })
}

/// Applies the effective allowlist to a command whose environment is empty.
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

/// Reads `reader` to EOF, retaining at most `limit` bytes.
async fn read_capped<R>(mut reader: R, limit: usize) -> std::io::Result<CapturedStream>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut truncated = false;
    let mut total_bytes = 0_u64;

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        total_bytes = total_bytes.saturating_add(read as u64);
        if bytes.len() < limit {
            let remaining = limit - bytes.len();
            let take = read.min(remaining);
            bytes.extend_from_slice(&buffer[..take]);
            if take < read {
                truncated = true;
            }
        } else {
            truncated = true;
        }
    }

    Ok(CapturedStream {
        bytes,
        truncated,
        total_bytes,
    })
}

/// Caps `rendered` at `max_bytes`, appending a truncation notice that records
/// the retained and rendered byte counts.
///
/// The cut always lands on a UTF-8 boundary. When `max_bytes` is too small for
/// the notice itself, only as much of the notice as fits is returned.
pub fn cap_rendered_output(rendered: &str, max_bytes: usize) -> String {
    cap_rendered_output_with_total(rendered, max_bytes, rendered.len() as u64)
}

/// Caps `rendered` at `max_bytes` and records how many bytes the command
/// itself produced.
///
/// Use this when stream-level truncation may already have discarded bytes:
/// `source_total` is the sum of both streams' original sizes, so the notice
/// still reports the true command output size even when the per-stream
/// truncation notice is cut off.
pub fn cap_rendered_output_with_total(
    rendered: &str,
    max_bytes: usize,
    source_total: u64,
) -> String {
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

/// Builds the truncation notice used by [`cap_rendered_output_with_total`].
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
