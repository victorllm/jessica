//! Shared helpers for native-tool behavior tests.

use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::command::{CommandError, CommandOutput, CommandRequest, CommandRunner, CommandStream};

/// Temporary directory removed when the test ends.
pub(crate) struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Creates a unique directory and stores its canonical path.
    pub(crate) fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-tools-{label}-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the clock must be after the epoch")
                .as_nanos(),
            unique
        ));
        std::fs::create_dir_all(&path).expect("temporary directory must be creatable");
        let path = std::fs::canonicalize(&path).expect("temporary directory must resolve");
        Self { path }
    }

    /// Returns the canonical path.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Returns a successful exit status for `code`.
pub(crate) fn exit_status(code: i32) -> ExitStatus {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(code << 8)
    }
    #[cfg(not(unix))]
    {
        std::process::Command::new(if code == 0 { "true" } else { "false" })
            .status()
            .expect("test helper command must run")
    }
}

/// Builds one captured stream.
pub(crate) fn stream(bytes: &[u8]) -> CommandStream {
    CommandStream {
        bytes: bytes.to_vec(),
        truncated: false,
        total_bytes: bytes.len() as u64,
    }
}

/// A runner that records every request and repeats one canned output.
pub(crate) struct RecordingRunner {
    responses: Mutex<Vec<CommandOutput>>,
    requests: Mutex<Vec<CommandRequest>>,
}

impl RecordingRunner {
    /// Builds a runner that returns `output` for every call.
    pub(crate) fn repeating(output: CommandOutput) -> Self {
        Self {
            responses: Mutex::new(vec![output]),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// Returns every request observed so far.
    pub(crate) fn requests(&self) -> Vec<CommandRequest> {
        self.requests
            .lock()
            .expect("request lock must not be poisoned")
            .clone()
    }
}

#[async_trait]
impl CommandRunner for RecordingRunner {
    async fn run(&self, request: CommandRequest) -> Result<CommandOutput, CommandError> {
        self.requests
            .lock()
            .expect("request lock must not be poisoned")
            .push(request);
        let responses = self
            .responses
            .lock()
            .expect("response lock must not be poisoned");
        let output = responses
            .first()
            .cloned()
            .expect("the runner must have a canned output");
        Ok(output)
    }
}
