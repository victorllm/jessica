//! End-to-end behavior tests for the ACP v1 stdio front end.
//!
//! Every test spawns the real `jessica` binary and speaks newline-delimited
//! JSON-RPC over its stdin/stdout. This exercises the actual transport rather
//! than an in-process mock, so stdout purity, EOF, and signals are observed
//! exactly as an ACP editor would observe them.

use std::process::ExitStatus;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::time::timeout;

/// Upper bound for any single protocol round trip or process exit.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// One running ACP agent process under test.
struct AcpProcess {
    /// Child process; stdin is taken when the test closes the stream.
    child: Child,
    /// Line-oriented protocol output.
    stdout: BufReader<ChildStdout>,
    /// Diagnostics, read only by tests that assert on them.
    stderr: Option<ChildStderr>,
}

impl AcpProcess {
    /// Starts `jessica acp` with optional extra arguments.
    async fn spawn(extra_args: &[&str]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_jessica-cli"));
        command
            .arg("acp")
            .args(extra_args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().expect("the jessica binary must start");
        let stdout = BufReader::new(child.stdout.take().expect("stdout is piped"));
        let stderr = child.stderr.take();
        Self {
            child,
            stdout,
            stderr,
        }
    }

    /// Sends one JSON-RPC message as a single newline-terminated line.
    async fn send(&mut self, message: Value) {
        let stdin = self.child.stdin.as_mut().expect("stdin is still open");
        let mut line = serde_json::to_vec(&message).expect("test message must serialize");
        line.push(b'\n');
        stdin
            .write_all(&line)
            .await
            .expect("writing to ACP stdin must succeed");
        stdin
            .flush()
            .await
            .expect("flushing ACP stdin must succeed");
    }

    /// Reads one non-empty protocol line, failing on EOF or timeout.
    async fn recv_line(&mut self) -> String {
        loop {
            let mut line = String::new();
            let read = self.stdout.read_line(&mut line);
            let count = match timeout(TEST_TIMEOUT, read).await {
                Ok(Ok(count)) => count,
                Ok(Err(error)) => panic!("failed to read ACP stdout: {error}"),
                Err(_) => panic!("timed out waiting for an ACP response"),
            };
            assert!(count > 0, "ACP stdout closed before a response arrived");
            let trimmed = line.trim_end_matches(['\r', '\n']);
            if !trimmed.is_empty() {
                return trimmed.to_owned();
            }
        }
    }

    /// Reads one line and parses it as a JSON-RPC message.
    async fn recv(&mut self) -> Value {
        let line = self.recv_line().await;
        parse_json_rpc(&line)
    }

    /// Closes stdin to signal clean EOF.
    fn close_stdin(&mut self) {
        drop(self.child.stdin.take());
    }

    /// Waits for the process to exit within [`TEST_TIMEOUT`].
    ///
    /// This closes stdin, which is a clean EOF shutdown. Tests that assert on
    /// the signal exit path must use [`Self::wait_keeping_stdin`] instead.
    async fn wait(&mut self) -> ExitStatus {
        match timeout(TEST_TIMEOUT, self.child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => panic!("failed to wait for the ACP process: {error}"),
            Err(_) => panic!("ACP process did not exit within {TEST_TIMEOUT:?}"),
        }
    }

    /// Waits for the process to exit without closing stdin.
    ///
    /// `Child::wait` drops the stdin handle before waiting, which itself
    /// triggers the EOF shutdown and can race the signal under test. Polling
    /// `try_wait` keeps the pipe open, so only the signal can end the process.
    async fn wait_keeping_stdin(&mut self) -> ExitStatus {
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return status,
                Ok(None) => {}
                Err(error) => panic!("failed to poll the ACP process: {error}"),
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "ACP process did not exit within {TEST_TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Reads everything still buffered on stdout after the process exited.
    async fn remaining_stdout(&mut self) -> String {
        let mut rest = String::new();
        timeout(TEST_TIMEOUT, self.stdout.read_to_string(&mut rest))
            .await
            .expect("reading remaining stdout must not time out")
            .expect("remaining stdout must be readable");
        rest
    }

    /// Reads all stderr after the process exited.
    async fn remaining_stderr(&mut self) -> String {
        let Some(mut stderr) = self.stderr.take() else {
            return String::new();
        };
        let mut output = String::new();
        timeout(TEST_TIMEOUT, stderr.read_to_string(&mut output))
            .await
            .expect("reading stderr must not time out")
            .expect("stderr must be readable");
        output
    }
}

/// Parses a protocol line and asserts it is valid JSON-RPC 2.0.
fn parse_json_rpc(line: &str) -> Value {
    let value: Value =
        serde_json::from_str(line).unwrap_or_else(|error| panic!("not JSON: {error}: {line}"));
    assert_eq!(
        value["jsonrpc"], "2.0",
        "every stdout line must be JSON-RPC 2.0: {line}"
    );
    assert!(
        value.get("id").is_some(),
        "responses must carry a request id: {line}"
    );
    value
}

/// Builds an `initialize` request with the given protocol version.
fn initialize(id: u64, protocol_version: u16) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": protocol_version,
            "clientCapabilities": {},
        },
    })
}

#[tokio::test]
async fn acp_initialize_reports_v1_and_explicit_empty_capabilities() {
    let mut acp = AcpProcess::spawn(&[]).await;

    acp.send(initialize(1, 1)).await;
    let response = acp.recv().await;

    assert_eq!(response["id"], 1);
    let result = response
        .get("result")
        .unwrap_or_else(|| panic!("initialize must succeed: {response}"));
    assert_eq!(result["protocolVersion"], 1);
    assert_eq!(
        result["agentInfo"]["name"], "jessica",
        "clients must see the agent identity"
    );
    assert_eq!(
        result["agentInfo"]["version"],
        env!("CARGO_PKG_VERSION"),
        "agentInfo.version must come from the crate version"
    );
    assert_eq!(result["agentCapabilities"]["loadSession"], false);
    assert_eq!(
        result["agentCapabilities"]["promptCapabilities"],
        json!({"image": false, "audio": false, "embeddedContext": false})
    );
    assert_eq!(
        result["agentCapabilities"]["mcpCapabilities"],
        json!({"http": false, "sse": false})
    );
    assert_eq!(
        result["agentCapabilities"]["sessionCapabilities"],
        json!({}),
        "no optional session lifecycle method is advertised"
    );
    assert_eq!(result["authMethods"], json!([]));

    acp.close_stdin();
    let status = acp.wait().await;
    assert_eq!(status.code(), Some(0), "stdin EOF must be a clean exit");
}

#[tokio::test]
async fn acp_initialize_with_unsupported_version_still_reports_v1() {
    let mut acp = AcpProcess::spawn(&[]).await;

    for (id, requested) in [(1, 2), (2, 999)] {
        acp.send(initialize(id, requested)).await;
        let response = acp.recv().await;

        let result = response
            .get("result")
            .unwrap_or_else(|| panic!("initialize must return a result: {response}"));
        assert_eq!(
            result["protocolVersion"], 1,
            "the agent must return its latest supported version instead of panicking or disconnecting"
        );
    }

    acp.close_stdin();
    let status = acp.wait().await;
    assert_eq!(status.code(), Some(0));
}

#[tokio::test]
async fn acp_unknown_method_returns_method_not_found_and_keeps_running() {
    let mut acp = AcpProcess::spawn(&[]).await;

    // ACP v1 requires initialization before any other request, so complete the
    // handshake first; the unimplemented method is then the only reason for
    // the error code under test.
    acp.send(initialize(1, 1)).await;
    assert!(acp.recv().await.get("result").is_some());

    for (id, method) in [
        ("unknown-1", "session/new"),
        ("unknown-2", "session/prompt"),
    ] {
        acp.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": {},
        }))
        .await;
        let response = acp.recv().await;

        assert_eq!(response["id"], id, "string request ids round-trip");
        assert_eq!(
            response["error"]["code"], -32601,
            "an unimplemented method must be reported as Method not found: {response}"
        );
    }

    acp.close_stdin();
    let status = acp.wait().await;
    assert_eq!(
        status.code(),
        Some(0),
        "a method-not-found error must not crash the process"
    );
}

#[tokio::test]
async fn acp_stdout_contains_only_json_rpc_lines() {
    let mut acp = AcpProcess::spawn(&[]).await;

    acp.send(initialize(1, 1)).await;
    let first = acp.recv_line().await;
    parse_json_rpc(&first);

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "jessica/not-implemented",
        "params": {},
    }))
    .await;
    let second = acp.recv_line().await;
    parse_json_rpc(&second);

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));

    let rest = acp.remaining_stdout().await;
    for line in rest.lines().filter(|line| !line.trim().is_empty()) {
        parse_json_rpc(line);
    }
    assert!(
        rest.trim().is_empty(),
        "ACP stdout must carry only the two protocol responses, got extra output: {rest:?}"
    );
}

#[tokio::test]
async fn acp_stdin_eof_exits_cleanly_within_timeout() {
    let mut acp = AcpProcess::spawn(&[]).await;

    acp.close_stdin();
    let status = acp.wait().await;

    assert_eq!(status.code(), Some(0));
    assert!(
        acp.remaining_stdout().await.trim().is_empty(),
        "EOF without requests must not produce stdout output"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn acp_sigterm_exits_with_143_and_leaves_stdout_pure() {
    let mut acp = AcpProcess::spawn(&[]).await;

    // A completed initialize proves the signal handlers are installed before
    // the test sends SIGTERM; otherwise the default disposition could race.
    acp.send(initialize(1, 1)).await;
    parse_json_rpc(&acp.recv_line().await);

    let pid = acp.child.id().expect("the child must still be running");
    let status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("the kill utility must be available");
    assert!(status.success(), "SIGTERM delivery must succeed");

    // Keep stdin open: closing it would be a clean EOF shutdown and could win
    // the race against the signal, which is not what this test observes.
    let exit = acp.wait_keeping_stdin().await;
    let stderr = acp.remaining_stderr().await;
    assert_eq!(
        exit.code(),
        Some(143),
        "SIGTERM must use the existing graceful-shutdown exit code; stderr: {stderr:?}"
    );

    let rest = acp.remaining_stdout().await;
    for line in rest.lines().filter(|line| !line.trim().is_empty()) {
        parse_json_rpc(line);
    }

    assert!(
        stderr.contains("SIGTERM"),
        "the shutdown diagnostic belongs on stderr: {stderr:?}"
    );
}

#[tokio::test]
async fn acp_config_flag_is_honored_before_serving() {
    let missing = std::env::temp_dir().join(format!(
        "jessica-acp-missing-{}-{}.toml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock must be after the epoch")
            .as_nanos()
    ));
    let mut acp = AcpProcess::spawn(&["--config", missing.to_str().expect("utf-8 path")]).await;

    acp.close_stdin();
    let status = acp.wait().await;

    assert_eq!(
        status.code(),
        Some(1),
        "an unreadable configuration must fail the process"
    );
    assert!(
        acp.remaining_stdout().await.is_empty(),
        "configuration failures must not write to ACP stdout"
    );
    assert!(
        acp.remaining_stderr().await.contains("configuration"),
        "the configuration failure must be reported on stderr"
    );
}
