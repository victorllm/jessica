//! End-to-end behavior tests for the ACP v1 stdio front end.
//!
//! Every test spawns the real `jessica` binary and speaks newline-delimited
//! JSON-RPC over its stdin/stdout. This exercises the actual transport rather
//! than an in-process mock, so stdout purity, EOF, and signals are observed
//! exactly as an ACP editor would observe them.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::time::timeout;

/// Upper bound for any single protocol round trip or process exit.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Python interpreter used to run the deterministic MCP fixture.
const PYTHON: &str = "/usr/bin/python3";

/// Absolute path to the MCP stdio fixture.
const MCP_STUB: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/mcp_stub.py");

/// Requires the local interpreter and fixture before starting an MCP test.
fn assert_mcp_fixture_available() {
    assert!(
        Path::new(PYTHON).is_file(),
        "MCP acceptance requires the Python interpreter at {PYTHON}"
    );
    assert!(
        Path::new(MCP_STUB).is_file(),
        "MCP acceptance requires the fixture at {MCP_STUB}"
    );
}

/// How the test client answers `session/request_permission`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PermissionMode {
    /// Approve every sensitive tool exactly once.
    AllowOnce,
    /// Reject every sensitive tool exactly once.
    RejectOnce,
    /// Answer with the protocol-level `cancelled` outcome.
    Cancelled,
    /// Leave requests to the test so it can assert on them.
    Manual,
}

/// One answer the client may send to a permission request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PermissionAnswer {
    /// Select the offered `allow_once` option.
    AllowOnce,
    /// Select the offered `reject_once` option.
    RejectOnce,
    /// Cancel the whole prompt turn.
    Cancelled,
}

impl PermissionAnswer {
    /// Builds the `result.outcome` payload for one answer.
    fn outcome(self) -> Value {
        match self {
            Self::AllowOnce => json!({"outcome": "selected", "optionId": "allow_once"}),
            Self::RejectOnce => json!({"outcome": "selected", "optionId": "reject_once"}),
            Self::Cancelled => json!({"outcome": "cancelled"}),
        }
    }
}

/// One running ACP agent process under test.
struct AcpProcess {
    /// Child process; stdin is taken when the test closes the stream.
    child: Child,
    /// Line-oriented protocol output; `None` after a test closes the pipe.
    stdout: Option<BufReader<ChildStdout>>,
    /// Diagnostics, read only by tests that assert on them.
    stderr: Option<ChildStderr>,
    /// Automatic answer for permission requests.
    permission_mode: PermissionMode,
    /// Every permission request observed, in arrival order.
    permission_requests: Vec<Value>,
}

impl AcpProcess {
    /// Starts `jessica acp` with optional extra arguments.
    async fn spawn(extra_args: &[&str]) -> Self {
        Self::spawn_in(None, &[], extra_args).await
    }

    /// Starts `jessica acp` with a working directory and environment overrides.
    ///
    /// The working directory becomes the ACP startup root, so tests that need
    /// a scratch workspace also create sessions inside it.
    async fn spawn_in(
        current_dir: Option<&Path>,
        environment: &[(&str, String)],
        extra_args: &[&str],
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_jessica"));
        command
            .arg("acp")
            .args(extra_args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        if let Some(directory) = current_dir {
            command.current_dir(directory);
        }
        for (key, value) in environment {
            command.env(key, value);
        }

        let mut child = command.spawn().expect("the jessica binary must start");
        let stdout = BufReader::new(child.stdout.take().expect("stdout is piped"));
        let stderr = child.stderr.take();
        Self {
            child,
            stdout: Some(stdout),
            stderr,
            permission_mode: PermissionMode::AllowOnce,
            permission_requests: Vec::new(),
        }
    }

    /// Changes how the client answers incoming permission requests.
    fn set_permission_mode(&mut self, mode: PermissionMode) {
        self.permission_mode = mode;
    }

    /// Returns every permission request observed so far.
    fn permission_requests(&self) -> &[Value] {
        &self.permission_requests
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

    /// Reads one non-empty protocol line without interpreting it.
    async fn read_protocol_line(&mut self) -> String {
        let stdout = self
            .stdout
            .as_mut()
            .expect("the test must not read protocol output after closing stdout");
        loop {
            let mut line = String::new();
            let read = stdout.read_line(&mut line);
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

    /// Reads one protocol line, answering permission requests automatically.
    ///
    /// Permission requests are client-bound JSON-RPC requests rather than
    /// session updates, so they may appear between any two updates. Tests that
    /// assert on them switch to [`PermissionMode::Manual`] and read the
    /// request itself.
    async fn recv_line(&mut self) -> String {
        loop {
            let line = self.read_protocol_line().await;
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                return line;
            };
            if message.get("method").and_then(Value::as_str) != Some("session/request_permission") {
                return line;
            }
            self.permission_requests.push(message.clone());
            let answer = match self.permission_mode {
                PermissionMode::Manual => return line,
                PermissionMode::AllowOnce => PermissionAnswer::AllowOnce,
                PermissionMode::RejectOnce => PermissionAnswer::RejectOnce,
                PermissionMode::Cancelled => PermissionAnswer::Cancelled,
            };
            self.respond_permission(&message, answer).await;
        }
    }

    /// Sends one answer for a permission request, echoing its JSON-RPC id.
    async fn respond_permission(&mut self, request: &Value, answer: PermissionAnswer) {
        let id = request
            .get("id")
            .cloned()
            .expect("permission requests must carry an id");
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {"outcome": answer.outcome()},
        }))
        .await;
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

    /// Drops the stdout read end so the next protocol write fails.
    fn close_stdout(&mut self) {
        drop(self.stdout.take());
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
        let Some(mut stdout) = self.stdout.take() else {
            return String::new();
        };
        let mut rest = String::new();
        timeout(TEST_TIMEOUT, stdout.read_to_string(&mut rest))
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

/// Parses a protocol line that may be a notification instead of a response.
fn parse_message(line: &str) -> Value {
    let value: Value =
        serde_json::from_str(line).unwrap_or_else(|error| panic!("not JSON: {error}: {line}"));
    assert_eq!(
        value["jsonrpc"], "2.0",
        "every stdout line must be JSON-RPC 2.0: {line}"
    );
    value
}

/// A scratch workspace removed when the test finishes.
struct TempDir {
    /// Canonical directory path.
    path: PathBuf,
}

impl TempDir {
    /// Creates a unique, canonicalized temporary directory.
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-acp-e2e-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temporary workspace must be created");
        let path = std::fs::canonicalize(path).expect("temporary workspace must canonicalize");
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Mock OpenAI-compatible server that replays scripted SSE responses.
struct MockProvider {
    /// Bound server address.
    address: SocketAddr,
    /// Request bodies observed in order.
    requests: Arc<Mutex<Vec<String>>>,
    /// Signals the server thread to stop.
    stop: Arc<AtomicBool>,
    /// Server thread handle.
    handle: Option<thread::JoinHandle<()>>,
}

impl MockProvider {
    /// Starts a server that answers the next requests with `responses`.
    fn start(responses: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("mock provider must bind");
        listener
            .set_nonblocking(true)
            .expect("mock provider must become non-blocking");
        let address = listener.local_addr().expect("mock provider address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let recorded = Arc::clone(&requests);
        let flag = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(25);
            let mut served = 0;
            while served < responses.len() && !flag.load(Ordering::Relaxed) {
                assert!(
                    Instant::now() < deadline,
                    "mock provider timed out waiting for request {}",
                    served + 1
                );
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .expect("accepted stream must become blocking");
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .expect("accepted stream must have a read timeout");
                        let Some(body) = read_http_request(&mut stream) else {
                            continue;
                        };
                        recorded
                            .lock()
                            .expect("request log must not be poisoned")
                            .push(body);
                        stream
                            .write_all(responses[served].as_bytes())
                            .expect("mock response must be written");
                        stream.flush().expect("mock response must be flushed");
                        served += 1;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("mock provider accept failed: {error}"),
                }
            }
        });
        Self {
            address,
            requests,
            stop,
            handle: Some(handle),
        }
    }

    /// Returns the provider base URL for `OPENAI_BASE_URL`.
    fn url(&self) -> String {
        format!("http://{}/v1", self.address)
    }

    /// Returns every request body observed so far.
    fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("request log must not be poisoned")
            .clone()
    }

    /// Stops the server thread and returns the observed request bodies.
    fn finish(mut self) -> Vec<String> {
        self.stop_and_join();
        self.requests()
    }

    /// Signals the server and joins its thread.
    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("mock provider thread must finish");
        }
    }
}

impl Drop for MockProvider {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

/// Reads one HTTP request and returns its body.
fn read_http_request(stream: &mut TcpStream) -> Option<String> {
    let mut data = Vec::new();
    let mut buffer = [0_u8; 4096];

    let header_end = loop {
        if let Some(position) = data.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        data.extend_from_slice(&buffer[..read]);
    };

    let headers = String::from_utf8_lossy(&data[..header_end]).to_ascii_lowercase();
    let content_length = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map(|value| value.trim().parse::<usize>().unwrap_or(0))
        .unwrap_or(0);

    while data.len() < header_end + content_length {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            break;
        }
        data.extend_from_slice(&buffer[..read]);
    }

    let body_end = (header_end + content_length).min(data.len());
    Some(String::from_utf8_lossy(&data[header_end..body_end]).into_owned())
}

/// Wraps SSE payloads in one complete HTTP response.
fn sse(payloads: &[String]) -> String {
    let mut body = String::new();
    for payload in payloads {
        body.push_str("data: ");
        body.push_str(payload);
        body.push_str("\n\n");
    }
    body.push_str("data: [DONE]\n\n");

    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Returns one provider failure response with no body.
fn internal_server_error() -> String {
    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        .to_owned()
}

/// Returns one SSE response carrying text.
fn text_response(content: &str) -> String {
    sse(&[content_chunk(content), finish_chunk("stop")])
}

/// Returns one SSE response requesting tools.
fn tool_calls_response(calls: &[(&str, &str, Value)]) -> String {
    let mut payloads = Vec::with_capacity(calls.len() + 1);
    for (index, (id, name, arguments)) in calls.iter().enumerate() {
        payloads.push(tool_call_chunk(index, id, name, &arguments.to_string()));
    }
    payloads.push(finish_chunk("tool_calls"));
    sse(&payloads)
}

/// Returns one content delta chunk.
fn content_chunk(content: &str) -> String {
    json!({
        "choices": [{
            "index": 0,
            "delta": {"content": content},
            "finish_reason": null
        }]
    })
    .to_string()
}

/// Returns one token usage chunk.
fn usage_chunk(prompt_tokens: u64, completion_tokens: u64) -> String {
    json!({
        "choices": [],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
        }
    })
    .to_string()
}

/// Returns one tool call delta chunk.
fn tool_call_chunk(index: usize, id: &str, name: &str, arguments: &str) -> String {
    json!({
        "choices": [{
            "index": 0,
            "delta": {
                "tool_calls": [{
                    "index": index,
                    "id": id,
                    "function": {"name": name, "arguments": arguments}
                }]
            },
            "finish_reason": null
        }]
    })
    .to_string()
}

/// Returns one finish chunk.
fn finish_chunk(reason: &str) -> String {
    json!({
        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]
    })
    .to_string()
}

/// Builds an `initialize` request with the given protocol version.
fn initialize(id: u64, protocol_version: u16) -> Value {
    initialize_with_capabilities(id, protocol_version, json!({}))
}

/// Builds an `initialize` request with explicit client capabilities.
fn initialize_with_capabilities(id: u64, protocol_version: u16, capabilities: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": protocol_version,
            "clientCapabilities": capabilities,
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
        json!({"close": {}}),
        "only close is available without persistence"
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

/// Completes the mandatory ACP handshake.
async fn handshake(acp: &mut AcpProcess) {
    handshake_with_capabilities(acp, json!({})).await;
}

/// Completes the handshake while advertising explicit client capabilities.
async fn handshake_with_capabilities(acp: &mut AcpProcess, capabilities: Value) {
    acp.send(initialize_with_capabilities(1, 1, capabilities))
        .await;
    let response = acp.recv().await;
    assert!(
        response.get("result").is_some(),
        "initialize must succeed: {response}"
    );
}

/// Completes the handshake while advertising live notice support.
async fn handshake_with_notices(acp: &mut AcpProcess) {
    handshake_with_capabilities(acp, json!({"session": {"notices": {}}})).await;
}

/// Creates a session and returns its protocol id.
async fn create_session(acp: &mut AcpProcess, id: u64, cwd: &Path) -> String {
    acp.send(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/new",
        "params": {
            "cwd": cwd.to_str().expect("utf-8 cwd"),
            "mcpServers": [],
        },
    }))
    .await;
    let response = acp.recv().await;
    response["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("session/new must return a session id: {response}"))
        .to_owned()
}

/// Sends one request that has no preceding notifications and reads its response.
async fn request_response(acp: &mut AcpProcess, id: u64, method: &str, params: Value) -> Value {
    acp.send(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    }))
    .await;
    acp.recv().await
}

/// Builds one ACP stdio MCP server declaration for the fixture.
fn mcp_stdio_server(environment: &[(&str, String)]) -> Value {
    let env: Vec<Value> = environment
        .iter()
        .map(|(name, value)| json!({"name": name, "value": value}))
        .collect();
    json!({
        "name": "stub",
        "command": PYTHON,
        "args": ["-I", MCP_STUB],
        "env": env,
    })
}

/// Returns the fixture's environment for one test workspace and mode.
fn mcp_stub_env(
    workspace: &TempDir,
    mode: &str,
    secret: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut environment = vec![
        ("MCP_STUB_MODE", mode.to_owned()),
        ("MCP_STUB_PID_FILE", mcp_pid_path(workspace)),
        ("MCP_STUB_CALL_LOG", mcp_call_log_path(workspace)),
    ];
    if let Some(secret) = secret {
        environment.push(("MCP_STUB_SECRET", secret.to_owned()));
        environment.push(("MCP_STUB_REQUIRE_SECRET", secret.to_owned()));
    }
    environment
}

/// Returns the fixture pid-file path for one workspace.
fn mcp_pid_path(workspace: &TempDir) -> String {
    workspace
        .path
        .join("mcp.pid")
        .to_string_lossy()
        .into_owned()
}

/// Returns the fixture tool-call-log path for one workspace.
fn mcp_call_log_path(workspace: &TempDir) -> String {
    workspace
        .path
        .join("mcp-calls.log")
        .to_string_lossy()
        .into_owned()
}

/// Writes a harness configuration with a fixed MCP tool allowlist.
fn write_mcp_config(workspace: &TempDir, allowed_tools: &[&str], tool_timeout_ms: u64) -> PathBuf {
    let path = workspace.path.join("harness.toml");
    let allowed_tools =
        serde_json::to_string(allowed_tools).expect("allowlist must serialize as JSON");
    std::fs::write(
        &path,
        format!(
            "[budget]\nmax_rounds = 4\nmax_tokens = 100000\n\
             tool_timeout_ms = {tool_timeout_ms}\n\n\
             [policy]\nallowed_tools = {allowed_tools}\n"
        ),
    )
    .expect("harness configuration must be written");
    path
}

/// Creates a session declaring exactly one stdio MCP server.
async fn create_mcp_session(acp: &mut AcpProcess, id: u64, cwd: &Path, server: Value) -> Value {
    acp.send(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/new",
        "params": {
            "cwd": cwd.to_str().expect("utf-8 cwd"),
            "mcpServers": [server],
        },
    }))
    .await;
    acp.recv().await
}

/// Waits for the fixture to publish its pid.
async fn wait_for_mcp_pid(path: &Path) -> u32 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(text) = tokio::fs::read_to_string(path).await
            && let Ok(pid) = text.trim().parse::<u32>()
        {
            return pid;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "MCP stub did not write its pid file at {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Waits until the operating system no longer reports the process.
///
/// A killed but unreaped child remains visible as a zombie in `/proc`, so this
/// also verifies that Jessica reaped the MCP child rather than only signalling
/// it.
async fn wait_for_process_gone(pid: u32) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if !Path::new(&format!("/proc/{pid}")).exists() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "MCP child {pid} was not reaped"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Returns the fixture's recorded tool invocations.
fn mcp_call_log(workspace: &TempDir) -> String {
    std::fs::read_to_string(mcp_call_log_path(workspace)).unwrap_or_default()
}

/// Sends `session/load` and collects every replay update before its response.
async fn load_session_until_response(
    acp: &mut AcpProcess,
    id: u64,
    session_id: &str,
    cwd: &Path,
) -> (Vec<Value>, Value) {
    acp.send(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/load",
        "params": {
            "sessionId": session_id,
            "cwd": cwd.to_str().expect("utf-8 cwd"),
            "mcpServers": [],
        },
    }))
    .await;
    collect_until_response(acp, id, session_id).await
}

/// Configuration enabling both the event store and the ACP session index.
const PERSISTENCE_CONFIG: &str = "\
[budget]
max_rounds = 4
max_tokens = 100000

[policy]
allowed_tools = [\"read_file\"]

[events]
path = \"events.db\"

[acp]
state_dir = \"acp-state\"
";

/// Sends one prompt and collects every update that precedes its response.
async fn prompt_until_response(
    acp: &mut AcpProcess,
    id: u64,
    session_id: &str,
    prompt: Value,
) -> (Vec<Value>, Value) {
    acp.send(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/prompt",
        "params": {"sessionId": session_id, "prompt": prompt},
    }))
    .await;
    collect_until_response(acp, id, session_id).await
}

/// Collects one session's updates until its prompt response arrives.
async fn collect_until_response(
    acp: &mut AcpProcess,
    id: u64,
    session_id: &str,
) -> (Vec<Value>, Value) {
    let mut updates = Vec::new();
    loop {
        let line = acp.recv_line().await;
        let message = parse_message(&line);
        if message.get("id").is_some() {
            assert_eq!(message["id"], id, "the response must match the prompt id");
            return (updates, message);
        }
        if is_protocol_level_notification(&message) {
            continue;
        }
        assert_eq!(
            message["method"], "session/update",
            "only session updates may precede a prompt response: {message}"
        );
        assert_eq!(
            message["params"]["sessionId"], session_id,
            "every update must carry the prompt's session id: {message}"
        );
        updates.push(message["params"]["update"].clone());
    }
}

/// Returns whether `message` is a protocol-level `$/` notification.
fn is_protocol_level_notification(message: &Value) -> bool {
    message
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method.starts_with("$/"))
}

/// Waits for the first update of `kind` on one session.
async fn collect_update_of_kind(acp: &mut AcpProcess, session_id: &str, kind: &str) -> Vec<Value> {
    let mut updates = Vec::new();
    loop {
        let line = acp.recv_line().await;
        let message = parse_message(&line);
        assert!(
            message.get("id").is_none(),
            "a prompt response arrived before the expected {kind} update: {message}"
        );
        assert_eq!(message["method"], "session/update");
        assert_eq!(
            message["params"]["sessionId"], session_id,
            "the expected update must belong to the active session: {message}"
        );
        let update = message["params"]["update"].clone();
        let found = update_kind(&update) == kind;
        updates.push(update);
        if found {
            return updates;
        }
    }
}

/// Reads messages until the next client-bound permission request arrives.
///
/// Only meaningful in [`PermissionMode::Manual`]; the default modes consume
/// requests inside [`AcpProcess::recv_line`].
async fn collect_until_permission(acp: &mut AcpProcess) -> (Vec<Value>, Value) {
    let mut messages = Vec::new();
    loop {
        let line = acp.recv_line().await;
        let message = parse_message(&line);
        if message.get("method").and_then(Value::as_str) == Some("session/request_permission") {
            return (messages, message);
        }
        messages.push(message);
    }
}

/// Reads session updates until one tool call reports `status`.
async fn collect_until_tool_status(
    acp: &mut AcpProcess,
    session_id: &str,
    call_id: &str,
    status: &str,
) -> Vec<Value> {
    let mut updates = Vec::new();
    loop {
        let line = acp.recv_line().await;
        let message = parse_message(&line);
        assert!(
            message.get("id").is_none(),
            "a response arrived before {call_id} reached {status}: {message}"
        );
        if is_protocol_level_notification(&message) {
            continue;
        }
        assert_eq!(message["method"], "session/update");
        assert_eq!(message["params"]["sessionId"], session_id);
        let update = message["params"]["update"].clone();
        let found = matches!(update_kind(&update), "tool_call" | "tool_call_update")
            && update["toolCallId"] == call_id
            && update["status"] == status;
        updates.push(update);
        if found {
            return updates;
        }
    }
}

/// Sends the `session/cancel` notification for one session.
async fn send_cancel(acp: &mut AcpProcess, session_id: &str) {
    acp.send(json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": session_id},
    }))
    .await;
}

/// Returns the `sessionUpdate` tag of one update object.
fn update_kind(update: &Value) -> &str {
    update["sessionUpdate"]
        .as_str()
        .unwrap_or_else(|| panic!("updates must carry sessionUpdate: {update}"))
}

/// Atomically replaces a configuration file so the watcher never sees a
/// transient empty document.
fn write_config_atomic(path: &Path, body: &str) {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, body).expect("temporary config must be written");
    std::fs::rename(&temporary, path).expect("config must be replaced atomically");
}

/// Returns the system prompt sent in one provider request.
fn request_system_prompt(body: &str) -> String {
    let request: Value = serde_json::from_str(body).expect("provider request must be JSON");
    request["messages"][0]["content"]
        .as_str()
        .expect("the first provider message must be the system prompt")
        .to_owned()
}

#[tokio::test]
async fn acp_unknown_method_returns_method_not_found_and_keeps_running() {
    let mut acp = AcpProcess::spawn(&[]).await;

    // ACP v1 requires initialization before any other request, so complete the
    // handshake first; the unimplemented method is then the only reason for
    // the error code under test.
    handshake(&mut acp).await;

    for (id, method) in [
        ("unknown-1", "session/unknown"),
        ("unknown-2", "jessica/not-implemented"),
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
        assert_eq!(
            response["error"]["message"], "Method not found",
            "the method-not-found message must stay stable: {response}"
        );
        assert_eq!(
            response["error"]["data"], method,
            "the SDK may identify the missing method, but must not echo request parameters: {response}"
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
async fn acp_session_new_validates_cwd_and_rejects_unsupported_fields() {
    let workspace = TempDir::new("cwd");
    let outside = TempDir::new("outside");
    let file = workspace.path.join("note.txt");
    std::fs::write(&file, "not a directory").expect("fixture file must be written");

    let mut acp = AcpProcess::spawn_in(Some(&workspace.path), &[], &[]).await;
    handshake(&mut acp).await;

    let cases = [
        ("relative", json!("relative/path"), None),
        ("missing", json!(workspace.path.join("missing")), None),
        ("file", json!(file), None),
        ("outside", json!(outside.path), None),
    ];

    for (index, (label, cwd, mcp_servers)) in cases.into_iter().enumerate() {
        let id = (index + 2) as u64;
        acp.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/new",
            "params": {
                "cwd": cwd,
                "mcpServers": mcp_servers.unwrap_or_else(|| json!([])),
            },
        }))
        .await;
        let response = acp.recv().await;

        assert_eq!(
            response["error"]["code"], -32602,
            "{label} must be rejected as invalid params: {response}"
        );
        assert!(
            response.get("result").is_none(),
            "{label} must not create a session: {response}"
        );
    }

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_session_limit_accepts_the_last_slot_and_rejects_the_next() {
    let workspace = TempDir::new("session-limit");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, "[acp]\nmax_sessions = 1\n")
        .expect("ACP configuration must be written");

    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;

    let first = create_session(&mut acp, 2, &workspace.path).await;
    assert!(!first.is_empty());

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "session/new",
        "params": {"cwd": workspace.path, "mcpServers": []},
    }))
    .await;
    let rejected = acp.recv().await;

    assert_eq!(
        rejected["error"]["code"], -32602,
        "the session limit is an invalid-params boundary: {rejected}"
    );
    assert_eq!(
        rejected["error"]["message"], "session limit reached",
        "the limit error must be stable and actionable: {rejected}"
    );
    assert_eq!(rejected["error"]["data"]["reason"], "session_limit_reached");
    assert_eq!(rejected["error"]["data"]["maxSessions"], 1);

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_prompt_validation_rejects_bad_content_without_a_model_call() {
    let workspace = TempDir::new("content");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, "[acp]\nmax_prompt_bytes = 8\n")
        .expect("ACP configuration must be written");
    let provider = MockProvider::start(vec![text_response("accepted")]);

    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;

    let prompts = [
        (
            "image",
            json!([{"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"}]),
            "unsupported prompt content block: image",
        ),
        (
            "embedded resource",
            json!([{"type": "resource", "resource": {"text": "hi", "uri": "file:///x"}}]),
            "unsupported prompt content block: resource",
        ),
        (
            "empty",
            json!([{"type": "text", "text": "   "}]),
            "prompt must not be empty",
        ),
        (
            "oversized",
            json!([{"type": "text", "text": "123456789"}]),
            "prompt exceeds the maximum of 8 bytes",
        ),
    ];

    for (index, (label, prompt, expected_message)) in prompts.into_iter().enumerate() {
        let id = (index + 3) as u64;
        acp.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/prompt",
            "params": {"sessionId": session, "prompt": prompt},
        }))
        .await;
        let response = acp.recv().await;

        assert_eq!(
            response["error"]["code"], -32602,
            "{label} prompt must be rejected: {response}"
        );
        assert_eq!(
            response["error"]["message"], expected_message,
            "{label} must return a stable, actionable message: {response}"
        );
    }

    let (_, accepted) = prompt_until_response(
        &mut acp,
        7,
        &session,
        json!([{"type": "text", "text": "12345678"}]),
    )
    .await;
    assert_eq!(
        accepted["result"]["stopReason"], "end_turn",
        "a prompt exactly at the byte limit must be accepted: {accepted}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    let requests = provider.finish();
    assert_eq!(
        requests.len(),
        1,
        "only the prompt exactly at the limit may reach the provider"
    );
    assert!(requests[0].contains("12345678"));
}

#[tokio::test]
async fn acp_streams_updates_before_the_response_and_replays_history() {
    let workspace = TempDir::new("stream");
    let note = workspace.path.join("NOTE.txt");
    std::fs::write(&note, "note contents").expect("fixture note must be written");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 4\nmax_tokens = 100000\n\
         [policy]\nallowed_tools = [\"read_file\"]\n",
    )
    .expect("harness configuration must be written");

    let provider = MockProvider::start(vec![
        text_response("first answer"),
        tool_calls_response(&[("call_read", "read_file", json!({"path": "NOTE.txt"}))]),
        text_response("second answer"),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;

    let (first_updates, first_response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "first question"}]),
    )
    .await;
    assert_eq!(first_response["result"]["stopReason"], "end_turn");
    assert!(
        !first_updates.is_empty(),
        "a text turn must emit at least one chunk"
    );
    let mut message_id = None;
    let mut first_text = String::new();
    for update in &first_updates {
        assert_eq!(update_kind(update), "agent_message_chunk");
        let id = update["messageId"]
            .as_str()
            .expect("chunks must carry a message id");
        match &message_id {
            Some(expected) => assert_eq!(id, expected, "one prompt has one message id"),
            None => message_id = Some(id.to_owned()),
        }
        first_text.push_str(update["content"]["text"].as_str().expect("text chunk"));
    }
    assert_eq!(
        first_text, "first answer",
        "every chunk must arrive before the prompt response"
    );

    let (second_updates, second_response) = prompt_until_response(
        &mut acp,
        4,
        &session,
        json!([{"type": "text", "text": "please read NOTE.txt"}]),
    )
    .await;
    assert_eq!(second_response["result"]["stopReason"], "end_turn");
    let kinds: Vec<&str> = second_updates.iter().map(update_kind).collect();
    assert_eq!(
        kinds,
        vec!["tool_call", "tool_call_update", "agent_message_chunk"],
        "tool lifecycle and text must be ordered and complete: {second_updates:?}"
    );

    let started = &second_updates[0];
    assert_eq!(started["toolCallId"], "call_read");
    assert_eq!(started["status"], "in_progress");
    assert_eq!(started["kind"], "read");
    assert_eq!(started["title"], "read_file: NOTE.txt");
    assert_eq!(started["rawInput"], json!({"path": "NOTE.txt"}));
    assert_eq!(
        started["locations"][0]["path"],
        json!(note.to_str().expect("utf-8 note path"))
    );

    let finished = &second_updates[1];
    assert_eq!(finished["toolCallId"], "call_read");
    assert_eq!(finished["status"], "completed");
    assert_eq!(finished["rawOutput"], "1\tnote contents\n");
    assert_eq!(finished["content"][0]["type"], "content");
    assert_eq!(finished["content"][0]["content"]["type"], "text");
    assert_eq!(
        finished["content"][0]["content"]["text"],
        "1\tnote contents\n"
    );
    assert_eq!(second_updates[2]["content"]["text"], "second answer");
    assert!(
        acp.permission_requests().is_empty(),
        "read_file is policy-only and must never ask for permission: {:?}",
        acp.permission_requests()
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));

    let requests = provider.finish();
    assert_eq!(
        requests.len(),
        3,
        "two prompts plus one tool continuation must reach the provider"
    );
    let second_request: Value =
        serde_json::from_str(&requests[1]).expect("provider request must be JSON");
    let contents: Vec<&str> = second_request["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter_map(|message| message["content"].as_str())
        .collect();
    assert_eq!(
        &contents[contents.len() - 3..],
        ["first question", "first answer", "please read NOTE.txt"],
        "the second prompt must receive bootstrap then history then the new user message"
    );
    assert!(
        requests[2].contains("note contents"),
        "the tool result must be replayed to the provider"
    );
}

#[tokio::test]
async fn acp_initialize_advertises_indexed_lifecycle_only_after_opening_persistence() {
    let workspace = TempDir::new("load-capability");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, PERSISTENCE_CONFIG).expect("harness configuration must be written");
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;

    acp.send(initialize(1, 1)).await;
    let response = acp.recv().await;

    assert_eq!(
        response["result"]["agentCapabilities"]["loadSession"], true,
        "a successfully opened index and event store must advertise session/load: {response}"
    );
    assert_eq!(
        response["result"]["agentCapabilities"]["sessionCapabilities"],
        json!({"list": {}, "delete": {}, "resume": {}, "close": {}}),
        "persistence must advertise every supported indexed lifecycle method: {response}"
    );
    assert!(
        workspace.path.join("acp-state").is_dir(),
        "startup must create the configured ACP state directory"
    );
    assert!(
        workspace.path.join("events.db").is_file(),
        "startup must open the configured event store before advertising load"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_session_load_after_restart_replays_history_and_continues_context() {
    let workspace = TempDir::new("load-restart");
    let note = workspace.path.join("NOTE.txt");
    std::fs::write(&note, "note contents").expect("fixture note must be written");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, PERSISTENCE_CONFIG).expect("harness configuration must be written");

    let first_provider = MockProvider::start(vec![
        tool_calls_response(&[("call_read", "read_file", json!({"path": "NOTE.txt"}))]),
        text_response("read result answer"),
    ]);
    let mut first = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", first_provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut first).await;
    let session = create_session(&mut first, 2, &workspace.path).await;
    let (updates, response) = prompt_until_response(
        &mut first,
        3,
        &session,
        json!([{"type": "text", "text": "please read NOTE.txt"}]),
    )
    .await;
    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "the turn providing persisted history must complete: {response}"
    );
    assert_eq!(
        updates.iter().map(update_kind).collect::<Vec<_>>(),
        vec!["tool_call", "tool_call_update", "agent_message_chunk"],
        "the fixture must persist a tool turn and its answer: {updates:?}"
    );
    first.close_stdin();
    assert_eq!(first.wait().await.code(), Some(0));
    assert_eq!(
        first_provider.finish().len(),
        2,
        "the first process must persist both model rounds"
    );

    let second_provider = MockProvider::start(vec![text_response("second answer")]);
    let mut second = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", second_provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut second).await;

    let (replayed, load_response) =
        load_session_until_response(&mut second, 2, &session, &workspace.path).await;
    assert!(
        load_response.get("result").is_some(),
        "loading a persisted session must succeed: {load_response}"
    );
    assert_eq!(
        replayed.iter().map(update_kind).collect::<Vec<_>>(),
        vec![
            "user_message_chunk",
            "tool_call",
            "tool_call_update",
            "agent_message_chunk"
        ],
        "the complete non-system conversation must be replayed before the load response: {replayed:?}"
    );
    assert_eq!(replayed[0]["content"]["text"], "please read NOTE.txt");
    assert_eq!(replayed[1]["toolCallId"], "call_read");
    assert_eq!(replayed[1]["status"], "in_progress");
    assert_eq!(replayed[2]["toolCallId"], "call_read");
    assert_eq!(replayed[2]["status"], "completed");
    assert_eq!(replayed[2]["rawOutput"], "1\tnote contents\n");
    assert_eq!(replayed[3]["content"]["text"], "read result answer");
    assert_ne!(
        replayed[0]["messageId"], replayed[3]["messageId"],
        "each replayed message must have a distinct message id"
    );

    let (_, prompt_response) = prompt_until_response(
        &mut second,
        3,
        &session,
        json!([{"type": "text", "text": "what was in the note?"}]),
    )
    .await;
    assert_eq!(
        prompt_response["result"]["stopReason"], "end_turn",
        "a loaded session must accept another prompt: {prompt_response}"
    );

    second.close_stdin();
    assert_eq!(second.wait().await.code(), Some(0));
    let requests = second_provider.finish();
    assert_eq!(
        requests.len(),
        1,
        "loading must replay persisted history without calling the model"
    );
    let request: Value = serde_json::from_str(&requests[0]).expect("provider request must be JSON");
    let messages = request["messages"]
        .as_array()
        .expect("the provider request must carry messages");
    assert_eq!(
        messages
            .last()
            .and_then(|message| message["content"].as_str()),
        Some("what was in the note?"),
        "the new user message must be appended after restored history"
    );
    assert!(
        messages.iter().any(
            |message| message["role"] == "user" && message["content"] == "please read NOTE.txt"
        ),
        "the restored user turn must reach the provider: {messages:?}"
    );
    assert!(
        messages.iter().any(|message| message["role"] == "assistant"
            && message["tool_calls"][0]["id"] == "call_read"),
        "the restored assistant tool call must reach the provider: {messages:?}"
    );
    assert!(
        messages
            .iter()
            .any(|message| message["role"] == "tool" && message["content"] == "1\tnote contents\n"),
        "the restored tool result must reach the provider: {messages:?}"
    );
    assert!(
        messages
            .iter()
            .any(|message| message["role"] == "assistant"
                && message["content"] == "read result answer"),
        "the restored assistant answer must reach the provider: {messages:?}"
    );
}

#[tokio::test]
async fn acp_session_load_handles_empty_state_and_stable_errors() {
    let workspace = TempDir::new("load-errors");
    let subdirectory = workspace.path.join("sub");
    std::fs::create_dir_all(&subdirectory).expect("subdirectory must exist");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, PERSISTENCE_CONFIG).expect("harness configuration must be written");

    let mut first = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut first).await;
    let session = create_session(&mut first, 2, &workspace.path).await;
    first.close_stdin();
    assert_eq!(first.wait().await.code(), Some(0));

    let mut second = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut second).await;

    second
        .send(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/load",
            "params": {
                "sessionId": "acp-does-not-exist",
                "cwd": workspace.path,
                "mcpServers": [],
            },
        }))
        .await;
    let unknown = second.recv().await;
    assert_eq!(
        unknown["error"]["code"], -32002,
        "an unknown session must use the reserved not-found code: {unknown}"
    );

    second
        .send(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/load",
            "params": {
                "sessionId": session,
                "cwd": subdirectory,
                "mcpServers": [],
            },
        }))
        .await;
    let wrong_cwd = second.recv().await;
    assert_eq!(
        wrong_cwd["error"]["code"], -32602,
        "a different cwd must be rejected as invalid params: {wrong_cwd}"
    );
    assert!(
        wrong_cwd["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("cwd does not match")),
        "the error must identify the cwd mismatch: {wrong_cwd}"
    );

    second
        .send(json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "session/load",
            "params": {
                "sessionId": session,
                "cwd": workspace.path,
                "mcpServers": [{
                    "name": "server",
                    "command": "/bin/true",
                    "args": [],
                    "env": []
                }],
            },
        }))
        .await;
    let unsupported = second.recv().await;
    assert_eq!(
        unsupported["error"]["code"], -32602,
        "load must reject MCP servers because only session/new starts them: {unsupported}"
    );

    let (replayed, empty) =
        load_session_until_response(&mut second, 5, &session, &workspace.path).await;
    assert!(
        empty.get("result").is_some(),
        "a session with no committed turns must still load: {empty}"
    );
    assert!(
        replayed.is_empty(),
        "an empty session must not replay synthetic updates: {replayed:?}"
    );

    second
        .send(json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "session/load",
            "params": {
                "sessionId": session,
                "cwd": workspace.path,
                "mcpServers": [],
            },
        }))
        .await;
    let duplicate = second.recv().await;
    assert_eq!(
        duplicate["error"]["code"], -32602,
        "loading the same live session twice must be rejected: {duplicate}"
    );

    second.close_stdin();
    assert_eq!(second.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_session_load_without_persistence_is_method_not_found() {
    let mut acp = AcpProcess::spawn(&[]).await;
    handshake(&mut acp).await;
    let cwd = std::env::current_dir().expect("the test process must have a working directory");

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "session/load",
        "params": {
            "sessionId": "acp-unrecoverable",
            "cwd": cwd,
            "mcpServers": [],
        },
    }))
    .await;
    let response = acp.recv().await;

    assert_eq!(
        response["error"]["code"], -32601,
        "session/load must be absent when no persistence was configured: {response}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_session_list_exposes_only_metadata_and_skips_corrupt_indexes() {
    let workspace = TempDir::new("list");
    let subdirectory = workspace.path.join("sub");
    std::fs::create_dir_all(&subdirectory).expect("subdirectory must exist");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, PERSISTENCE_CONFIG).expect("harness configuration must be written");

    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let first = create_session(&mut acp, 2, &workspace.path).await;
    let second = create_session(&mut acp, 3, &subdirectory).await;

    // A damaged index must be skipped while every healthy session remains
    // listable. The filename is valid hex so only content validation can
    // reject it.
    std::fs::write(
        workspace.path.join("acp-state").join("deadbeef.json"),
        b"{not-json",
    )
    .expect("corrupt index fixture must be written");

    let response = request_response(&mut acp, 4, "session/list", json!({})).await;
    let sessions = response["result"]["sessions"]
        .as_array()
        .unwrap_or_else(|| panic!("session/list must return an array: {response}"));
    assert_eq!(
        sessions.len(),
        2,
        "the corrupt index must be skipped and both healthy indexes listed: {response}"
    );
    let mut listed_ids = std::collections::BTreeSet::new();
    for session in sessions {
        let object = session
            .as_object()
            .unwrap_or_else(|| panic!("each session must be an object: {session}"));
        assert_eq!(
            object
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["cwd", "sessionId", "updatedAt"]),
            "list must expose id, cwd, and update time only: {session}"
        );
        assert!(
            session.get("title").is_none(),
            "Jessica stores no session titles and must not fabricate one: {session}"
        );
        let updated_at = session["updatedAt"]
            .as_str()
            .unwrap_or_else(|| panic!("updatedAt must be a string: {session}"));
        assert_eq!(
            updated_at.len(),
            24,
            "updatedAt must use a fixed UTC RFC 3339 shape: {updated_at}"
        );
        assert!(
            updated_at.ends_with('Z') && updated_at.as_bytes()[10] == b'T',
            "updatedAt must be UTC with a T separator: {updated_at}"
        );
        listed_ids.insert(
            session["sessionId"]
                .as_str()
                .expect("sessionId must be a string")
                .to_owned(),
        );
    }
    assert_eq!(
        listed_ids,
        std::collections::BTreeSet::from([first.clone(), second.clone()]),
        "list must return exactly the healthy sessions"
    );

    let filtered =
        request_response(&mut acp, 5, "session/list", json!({"cwd": subdirectory})).await;
    let filtered_sessions = filtered["result"]["sessions"]
        .as_array()
        .unwrap_or_else(|| panic!("filtered list must return an array: {filtered}"));
    assert_eq!(
        filtered_sessions,
        &vec![json!({
            "sessionId": second,
            "cwd": subdirectory,
            "updatedAt": filtered_sessions[0]["updatedAt"],
        })],
        "the cwd filter must select exactly the canonical matching session: {filtered}"
    );

    for (id, params) in [
        (6, json!({"cursor": "not-a-jessica-cursor"})),
        (7, json!({"cwd": "relative/path"})),
        (8, json!({"cwd": std::env::temp_dir()})),
    ] {
        let error = request_response(&mut acp, id, "session/list", params).await;
        assert_eq!(
            error["error"]["code"], -32602,
            "invalid cursors and cwds must be rejected: {error}"
        );
    }

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_session_close_releases_live_state_and_keeps_the_index_for_reload() {
    let workspace = TempDir::new("close");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, PERSISTENCE_CONFIG).expect("harness configuration must be written");

    let mut first = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut first).await;
    let session = create_session(&mut first, 2, &workspace.path).await;

    let closed = request_response(
        &mut first,
        3,
        "session/close",
        json!({"sessionId": session}),
    )
    .await;
    assert!(
        closed.get("result").is_some(),
        "closing a live session must succeed: {closed}"
    );

    let prompt_after_close = request_response(
        &mut first,
        4,
        "session/prompt",
        json!({
            "sessionId": session,
            "prompt": [{"type": "text", "text": "must not run"}],
        }),
    )
    .await;
    assert_eq!(
        prompt_after_close["error"]["code"], -32002,
        "close must remove the live session immediately: {prompt_after_close}"
    );

    let repeated = request_response(
        &mut first,
        5,
        "session/close",
        json!({"sessionId": session}),
    )
    .await;
    assert!(
        repeated.get("result").is_some(),
        "closing an indexed but idle session is idempotent: {repeated}"
    );
    let unknown = request_response(
        &mut first,
        6,
        "session/close",
        json!({"sessionId": "acp-does-not-exist"}),
    )
    .await;
    assert_eq!(
        unknown["error"]["code"], -32002,
        "closing an unknown session must be session-not-found: {unknown}"
    );

    let listed = request_response(&mut first, 7, "session/list", json!({})).await;
    assert_eq!(
        listed["result"]["sessions"]
            .as_array()
            .expect("list must return an array")
            .len(),
        1,
        "close must retain the index so the session remains listable: {listed}"
    );
    let (replayed, reloaded) =
        load_session_until_response(&mut first, 8, &session, &workspace.path).await;
    assert!(
        reloaded.get("result").is_some(),
        "the retained index must load after close: {reloaded}"
    );
    assert!(
        replayed.is_empty(),
        "a closed session with no turns must not synthesize replay: {replayed:?}"
    );

    first.close_stdin();
    assert_eq!(first.wait().await.code(), Some(0));

    let mut second = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut second).await;
    let (_, restart_load) =
        load_session_until_response(&mut second, 2, &session, &workspace.path).await;
    assert!(
        restart_load.get("result").is_some(),
        "close must not delete the index across restarts: {restart_load}"
    );
    second.close_stdin();
    assert_eq!(second.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_session_close_cancels_active_work_and_acknowledges_after_it_stops() {
    let workspace = TempDir::new("close-active");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\"]\n\n\
         [tools.bash]\nenabled = true\n\n\
         [events]\npath = \"events.db\"\n\n\
         [acp]\nstate_dir = \"acp-state\"\n",
    )
    .expect("harness configuration must be written");
    let provider = MockProvider::start(vec![tool_calls_response(&[(
        "call_sleep",
        "bash",
        json!({"command": "sleep 30"}),
    )])]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "session/prompt",
        "params": {
            "sessionId": session,
            "prompt": [{"type": "text", "text": "start and then close"}],
        },
    }))
    .await;
    collect_update_of_kind(&mut acp, &session, "tool_call").await;

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "session/close",
        "params": {"sessionId": session},
    }))
    .await;

    let mut prompt_response = None;
    let mut close_response = None;
    while prompt_response.is_none() || close_response.is_none() {
        let line = acp.recv_line().await;
        let message = parse_message(&line);
        if is_protocol_level_notification(&message) {
            continue;
        }
        match message.get("id").and_then(Value::as_u64) {
            Some(3) => prompt_response = Some(message),
            Some(4) => close_response = Some(message),
            Some(other) => panic!("unexpected response id {other}: {message}"),
            None => {
                assert_eq!(message["method"], "session/update");
                assert_eq!(
                    message["params"]["sessionId"], session,
                    "the closed session must not emit updates for another session"
                );
            }
        }
    }
    let prompt_response = prompt_response.expect("the prompt must receive a response");
    assert_eq!(
        prompt_response["result"]["stopReason"], "cancelled",
        "close must cancel the active prompt: {prompt_response}"
    );
    let close_response = close_response.expect("close must receive a response");
    assert!(
        close_response.get("result").is_some(),
        "close must succeed after the active prompt stops: {close_response}"
    );

    let listed = request_response(&mut acp, 5, "session/list", json!({})).await;
    assert_eq!(
        listed["result"]["sessions"].as_array().map(Vec::len),
        Some(1),
        "close must retain the indexed session: {listed}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    assert_eq!(
        provider.finish().len(),
        1,
        "the interrupted tool must not start a second model round after close"
    );
}

#[tokio::test]
async fn acp_session_resume_restores_context_without_replaying_history() {
    let workspace = TempDir::new("resume");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, PERSISTENCE_CONFIG).expect("harness configuration must be written");

    let first_provider = MockProvider::start(vec![text_response("first answer")]);
    let mut first = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", first_provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut first).await;
    let session = create_session(&mut first, 2, &workspace.path).await;
    let (_, prompt_response) = prompt_until_response(
        &mut first,
        3,
        &session,
        json!([{"type": "text", "text": "remember this"}]),
    )
    .await;
    assert_eq!(prompt_response["result"]["stopReason"], "end_turn");
    first.close_stdin();
    assert_eq!(first.wait().await.code(), Some(0));
    assert_eq!(first_provider.finish().len(), 1);

    let second_provider = MockProvider::start(vec![text_response("second answer")]);
    let mut second = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", second_provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut second).await;

    let resumed = request_response(
        &mut second,
        2,
        "session/resume",
        json!({
            "sessionId": session,
            "cwd": workspace.path,
            "mcpServers": [],
        }),
    )
    .await;
    assert!(
        resumed.get("result").is_some(),
        "resume must succeed without replay notifications: {resumed}"
    );
    assert!(
        second_provider.requests().is_empty(),
        "resume must not call the model"
    );

    let live_resume = request_response(
        &mut second,
        3,
        "session/resume",
        json!({
            "sessionId": session,
            "cwd": workspace.path,
            "mcpServers": [],
        }),
    )
    .await;
    assert!(
        live_resume.get("result").is_some(),
        "resuming the same live session with the same cwd must be idempotent: {live_resume}"
    );

    let wrong_cwd = request_response(
        &mut second,
        4,
        "session/resume",
        json!({
            "sessionId": session,
            "cwd": std::env::temp_dir(),
            "mcpServers": [],
        }),
    )
    .await;
    assert_eq!(
        wrong_cwd["error"]["code"], -32602,
        "resume must reject a cwd outside the startup root: {wrong_cwd}"
    );

    let unknown = request_response(
        &mut second,
        5,
        "session/resume",
        json!({
            "sessionId": "acp-does-not-exist",
            "cwd": workspace.path,
            "mcpServers": [],
        }),
    )
    .await;
    assert_eq!(
        unknown["error"]["code"], -32002,
        "resume of an unknown session must be session-not-found: {unknown}"
    );

    let unsupported = request_response(
        &mut second,
        6,
        "session/resume",
        json!({
            "sessionId": session,
            "cwd": workspace.path,
            "mcpServers": [{
                "name": "server",
                "command": "/bin/true",
                "args": [],
                "env": []
            }],
        }),
    )
    .await;
    assert_eq!(
        unsupported["error"]["code"], -32602,
        "resume must reject MCP servers because only session/new starts them: {unsupported}"
    );

    let (_, continued) = prompt_until_response(
        &mut second,
        7,
        &session,
        json!([{"type": "text", "text": "what did I ask you to remember?"}]),
    )
    .await;
    assert_eq!(
        continued["result"]["stopReason"], "end_turn",
        "a resumed session must accept a prompt: {continued}"
    );

    second.close_stdin();
    assert_eq!(second.wait().await.code(), Some(0));
    let requests = second_provider.finish();
    assert_eq!(
        requests.len(),
        1,
        "resume must restore context without spending a model request"
    );
    let request: Value = serde_json::from_str(&requests[0]).expect("provider request must be JSON");
    let contents: Vec<&str> = request["messages"]
        .as_array()
        .expect("provider messages must be an array")
        .iter()
        .filter_map(|message| message["content"].as_str())
        .collect();
    assert!(
        contents.contains(&"remember this") && contents.contains(&"first answer"),
        "resume must restore earlier turns into the next prompt: {contents:?}"
    );
    assert_eq!(
        contents.last().copied(),
        Some("what did I ask you to remember?"),
        "the new prompt must be appended after restored history"
    );
}

#[tokio::test]
async fn acp_session_delete_is_logical_and_removes_only_the_index() {
    let workspace = TempDir::new("delete");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, PERSISTENCE_CONFIG).expect("harness configuration must be written");
    let provider = MockProvider::start(vec![text_response("persisted answer")]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;
    let (_, prompt_response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "keep the event log"}]),
    )
    .await;
    assert_eq!(prompt_response["result"]["stopReason"], "end_turn");

    let state_dir = workspace.path.join("acp-state");
    let events_db = workspace.path.join("events.db");
    assert!(
        std::fs::read_dir(&state_dir)
            .expect("state directory must be readable")
            .next()
            .is_some(),
        "the session index must exist before deletion"
    );
    assert!(
        events_db.is_file(),
        "the event store must exist before deletion"
    );

    let deleted =
        request_response(&mut acp, 4, "session/delete", json!({"sessionId": session})).await;
    assert!(
        deleted.get("result").is_some(),
        "deleting a live indexed session must succeed: {deleted}"
    );
    assert_eq!(
        std::fs::read_dir(&state_dir)
            .expect("state directory must be readable")
            .count(),
        0,
        "delete must remove exactly the ACP index"
    );
    assert!(
        events_db.is_file(),
        "delete must not physically remove the append-only event store"
    );

    let listed = request_response(&mut acp, 5, "session/list", json!({})).await;
    assert_eq!(
        listed["result"]["sessions"],
        json!([]),
        "a deleted session must disappear from list: {listed}"
    );
    let load = request_response(
        &mut acp,
        6,
        "session/load",
        json!({
            "sessionId": session,
            "cwd": workspace.path,
            "mcpServers": [],
        }),
    )
    .await;
    assert_eq!(
        load["error"]["code"], -32002,
        "a deleted session must no longer load: {load}"
    );
    let prompt_after_delete = request_response(
        &mut acp,
        7,
        "session/prompt",
        json!({
            "sessionId": session,
            "prompt": [{"type": "text", "text": "must not run"}],
        }),
    )
    .await;
    assert_eq!(
        prompt_after_delete["error"]["code"], -32002,
        "delete must release the live session: {prompt_after_delete}"
    );
    let repeated =
        request_response(&mut acp, 8, "session/delete", json!({"sessionId": session})).await;
    assert_eq!(
        repeated["error"]["code"], -32002,
        "deleting an unknown session must be session-not-found: {repeated}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    assert_eq!(
        provider.finish().len(),
        1,
        "deleting a session must not trigger model work"
    );
}

#[tokio::test]
async fn acp_session_delete_cancels_active_work_without_reindexing() {
    let workspace = TempDir::new("delete-active");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\"]\n\n\
         [tools.bash]\nenabled = true\n\n\
         [events]\npath = \"events.db\"\n\n\
         [acp]\nstate_dir = \"acp-state\"\n",
    )
    .expect("harness configuration must be written");
    let provider = MockProvider::start(vec![tool_calls_response(&[(
        "call_sleep",
        "bash",
        json!({"command": "sleep 30"}),
    )])]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "session/prompt",
        "params": {
            "sessionId": session,
            "prompt": [{"type": "text", "text": "start and then delete"}],
        },
    }))
    .await;
    collect_update_of_kind(&mut acp, &session, "tool_call").await;

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "session/delete",
        "params": {"sessionId": session},
    }))
    .await;

    let mut prompt_response = None;
    let mut delete_response = None;
    while prompt_response.is_none() || delete_response.is_none() {
        let line = acp.recv_line().await;
        let message = parse_message(&line);
        if is_protocol_level_notification(&message) {
            continue;
        }
        match message.get("id").and_then(Value::as_u64) {
            Some(3) => prompt_response = Some(message),
            Some(4) => delete_response = Some(message),
            Some(other) => panic!("unexpected response id {other}: {message}"),
            None => assert_eq!(message["method"], "session/update"),
        }
    }
    assert_eq!(
        prompt_response.expect("the prompt must respond")["result"]["stopReason"],
        "cancelled",
        "delete must cancel the active prompt"
    );
    assert!(
        delete_response
            .expect("delete must respond")
            .get("result")
            .is_some(),
        "delete must succeed after cancelling the active prompt"
    );

    assert_eq!(
        std::fs::read_dir(workspace.path.join("acp-state"))
            .expect("state directory must be readable")
            .count(),
        0,
        "a late prompt commit must not recreate the deleted index"
    );
    assert!(
        workspace.path.join("events.db").is_file(),
        "delete must retain the event store"
    );
    let listed = request_response(&mut acp, 5, "session/list", json!({})).await;
    assert_eq!(
        listed["result"]["sessions"],
        json!([]),
        "the deleted session must not return to list: {listed}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    assert_eq!(provider.finish().len(), 1);
}

#[tokio::test]
async fn acp_session_list_paginates_without_duplicates_or_loss() {
    let workspace = TempDir::new("list-pages");
    let config = workspace.path.join("harness.toml");
    let config_body = format!("{PERSISTENCE_CONFIG}max_sessions = 105\n");
    std::fs::write(&config, config_body).expect("harness configuration must be written");
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;

    let mut created = std::collections::BTreeSet::new();
    for sequence in 0..101_u64 {
        let session = create_session(&mut acp, 2 + sequence, &workspace.path).await;
        created.insert(session);
    }

    let first_page = request_response(&mut acp, 200, "session/list", json!({})).await;
    let first_sessions = first_page["result"]["sessions"]
        .as_array()
        .unwrap_or_else(|| panic!("the first page must be an array: {first_page}"));
    assert_eq!(
        first_sessions.len(),
        100,
        "one page must contain the fixed maximum of 100 sessions: {first_page}"
    );
    let cursor = first_page["result"]["nextCursor"]
        .as_str()
        .unwrap_or_else(|| panic!("a full first page must provide a cursor: {first_page}"));

    let second_page =
        request_response(&mut acp, 201, "session/list", json!({"cursor": cursor})).await;
    let second_sessions = second_page["result"]["sessions"]
        .as_array()
        .unwrap_or_else(|| panic!("the second page must be an array: {second_page}"));
    assert_eq!(
        second_sessions.len(),
        1,
        "the second page must contain the remaining session: {second_page}"
    );
    assert!(
        second_page["result"].get("nextCursor").is_none(),
        "the final page must omit the cursor: {second_page}"
    );

    let mut paged = std::collections::BTreeSet::new();
    for session in first_sessions.iter().chain(second_sessions) {
        let id = session["sessionId"]
            .as_str()
            .expect("listed sessions must carry an id")
            .to_owned();
        assert!(
            paged.insert(id),
            "pagination must never return a duplicate session"
        );
    }
    assert_eq!(
        paged, created,
        "concatenating both pages must return every created session exactly once"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_indexed_lifecycle_without_persistence_is_method_not_found_but_close_works() {
    let mut acp = AcpProcess::spawn(&[]).await;
    handshake(&mut acp).await;
    let cwd = std::env::current_dir().expect("the test process must have a working directory");

    for (id, method, params) in [
        (2, "session/list", json!({})),
        (
            3,
            "session/resume",
            json!({
                "sessionId": "acp-missing",
                "cwd": cwd,
                "mcpServers": [],
            }),
        ),
        (4, "session/delete", json!({"sessionId": "acp-missing"})),
    ] {
        let response = request_response(&mut acp, id, method, params).await;
        assert_eq!(
            response["error"]["code"], -32601,
            "{method} must be method-not-found without persistence: {response}"
        );
    }

    let session = create_session(&mut acp, 5, &cwd).await;
    let closed =
        request_response(&mut acp, 6, "session/close", json!({"sessionId": session})).await;
    assert!(
        closed.get("result").is_some(),
        "close must work without persistence because it only frees live state: {closed}"
    );
    let closed_twice =
        request_response(&mut acp, 7, "session/close", json!({"sessionId": session})).await;
    assert_eq!(
        closed_twice["error"]["code"], -32002,
        "close does not retain a memory-only session: {closed_twice}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_cancel_is_scoped_closes_in_flight_tools_and_keeps_the_session_usable() {
    let workspace = TempDir::new("cancel");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\"]\n\n\
         [tools.bash]\nenabled = true\n",
    )
    .expect("harness configuration must be written");
    let provider = MockProvider::start(vec![
        tool_calls_response(&[("call_sleep", "bash", json!({"command": "sleep 30"}))]),
        tool_calls_response(&[(
            "call_wait",
            "bash",
            json!({"command": "while [ ! -f go ]; do sleep 0.05; done"}),
        )]),
        text_response("other session answer"),
        text_response("after cancellation"),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let cancelled_session = create_session(&mut acp, 2, &workspace.path).await;
    let other_session = create_session(&mut acp, 3, &workspace.path).await;

    // Cancelling an unknown session and a session with no active prompt is an
    // ignored notification, not an error or a global cancellation.
    send_cancel(&mut acp, "acp-unknown").await;
    send_cancel(&mut acp, &cancelled_session).await;

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "session/prompt",
        "params": {
            "sessionId": cancelled_session,
            "prompt": [{"type": "text", "text": "first question"}],
        },
    }))
    .await;
    let mut started_updates =
        collect_update_of_kind(&mut acp, &cancelled_session, "tool_call").await;
    assert!(
        started_updates.iter().any(|update| {
            update_kind(update) == "tool_call"
                && update["toolCallId"] == "call_sleep"
                && update.get("status").is_none()
        }),
        "ACP omits the default pending status, so the call must be pending before permission: {started_updates:?}"
    );
    started_updates.extend(
        collect_until_tool_status(&mut acp, &cancelled_session, "call_sleep", "in_progress").await,
    );
    assert!(
        started_updates.iter().any(|update| {
            update_kind(update) == "tool_call_update"
                && update["toolCallId"] == "call_sleep"
                && update["status"] == "in_progress"
        }),
        "the approved bash tool must be observably in progress: {started_updates:?}"
    );

    // The same session cannot start a second prompt while the first is active.
    let (_, busy_response) = prompt_until_response(
        &mut acp,
        5,
        &cancelled_session,
        json!([{"type": "text", "text": "must be busy"}]),
    )
    .await;
    assert_eq!(
        busy_response["error"]["code"], -32602,
        "a second active prompt must be rejected: {busy_response}"
    );
    assert_eq!(
        busy_response["error"]["data"]["reason"], "session_busy",
        "the busy error must be actionable: {busy_response}"
    );

    // A different session starts and stays in a tool while the first session
    // is still running. Cancelling the first must not stop the second.
    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 6,
        "method": "session/prompt",
        "params": {
            "sessionId": other_session,
            "prompt": [{"type": "text", "text": "finish independently"}],
        },
    }))
    .await;
    collect_update_of_kind(&mut acp, &other_session, "tool_call").await;
    collect_until_tool_status(&mut acp, &other_session, "call_wait", "in_progress").await;

    send_cancel(&mut acp, &cancelled_session).await;
    let (closing_updates, cancelled_response) =
        collect_until_response(&mut acp, 4, &cancelled_session).await;
    assert_eq!(
        cancelled_response["result"]["stopReason"], "cancelled",
        "the cancelled prompt must not report end_turn: {cancelled_response}"
    );

    // The second session was blocked in bash until this file appeared. It
    // finishes only after the other session's cancellation has been answered.
    std::fs::write(workspace.path.join("go"), "release").expect("the release file must be written");
    let (_, other_response) = collect_until_response(&mut acp, 6, &other_session).await;
    assert_eq!(
        other_response["result"]["stopReason"], "end_turn",
        "the untouched session must finish normally: {other_response}"
    );

    let mut final_status = std::collections::BTreeMap::new();
    for update in started_updates.iter().chain(closing_updates.iter()) {
        if matches!(update_kind(update), "tool_call" | "tool_call_update") {
            let id = update["toolCallId"]
                .as_str()
                .expect("tool updates must carry an id");
            // ACP omits the default `pending` status on the announcing update.
            let status = update["status"].as_str().unwrap_or("pending");
            final_status.insert(id.to_owned(), status.to_owned());
        }
    }
    assert_eq!(
        final_status.get("call_sleep").map(String::as_str),
        Some("failed"),
        "an interrupted tool must receive a failed update: {closing_updates:?}"
    );
    assert!(
        closing_updates.iter().any(|update| {
            update_kind(update) == "tool_call_update"
                && update["status"] == "failed"
                && update["content"][0]["content"]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("cancelled"))
        }),
        "the failed update must explain the cancellation: {closing_updates:?}"
    );
    assert!(
        final_status.values().all(|status| status != "in_progress"),
        "no tool call may remain in progress after the response: {final_status:?}"
    );

    // The cancelled session stays usable and its partial turn is carried into
    // the next provider request with a synthesized tool result.
    let (_, follow_up) = prompt_until_response(
        &mut acp,
        7,
        &cancelled_session,
        json!([{"type": "text", "text": "continue after cancel"}]),
    )
    .await;
    assert_eq!(
        follow_up["result"]["stopReason"], "end_turn",
        "a cancelled prompt must not poison its session: {follow_up}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    let requests = provider.finish();
    assert_eq!(
        requests.len(),
        4,
        "two interrupted tools plus two completed continuations must reach the provider"
    );
    assert!(
        requests[3].contains("first question")
            && requests[3].contains("tool execution was interrupted")
            && requests[3].contains("continue after cancel"),
        "cancelled history must keep partial output and a synthesized tool result: {}",
        requests[3]
    );
}

/// Starts an ACP process with one allowed sensitive tool and a scratch workspace.
async fn spawn_permission_harness(
    workspace: &TempDir,
    allowed_tools: &str,
    provider: &MockProvider,
) -> AcpProcess {
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        format!(
            "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
             [policy]\nallowed_tools = [{allowed_tools}]\n\n\
             [tools.bash]\nenabled = true\n"
        ),
    )
    .expect("harness configuration must be written");
    AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await
}

#[tokio::test]
async fn acp_allow_once_asks_before_running_a_sensitive_tool() {
    let workspace = TempDir::new("permission-allow");
    let provider = MockProvider::start(vec![
        tool_calls_response(&[(
            "call_approved",
            "bash",
            json!({"command": "printf approved > approved.txt"}),
        )]),
        text_response("approved command finished"),
    ]);
    let mut acp = spawn_permission_harness(&workspace, "\"bash\"", &provider).await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;
    acp.set_permission_mode(PermissionMode::Manual);

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "session/prompt",
        "params": {
            "sessionId": session,
            "prompt": [{"type": "text", "text": "run the approved command"}],
        },
    }))
    .await;

    let (before, request) = collect_until_permission(&mut acp).await;
    let announced = before
        .iter()
        .find(|message| {
            message["method"] == "session/update"
                && update_kind(&message["params"]["update"]) == "tool_call"
                && message["params"]["update"]["toolCallId"] == "call_approved"
        })
        .unwrap_or_else(|| panic!("the tool_call update must precede the request: {before:?}"));
    let announced = &announced["params"]["update"];
    assert!(
        announced.get("status").is_none(),
        "ACP omits the default pending status, so a sensitive tool must not claim execution: {announced}"
    );
    assert_eq!(announced["title"], "bash: printf approved > approved.txt");
    assert_eq!(
        announced["rawInput"]["command"], "printf approved > approved.txt",
        "the client must see what it is asked to approve: {announced}"
    );
    assert!(
        before.iter().all(|message| {
            let update = &message["params"]["update"];
            !(matches!(update_kind(update), "tool_call" | "tool_call_update")
                && update["toolCallId"] == "call_approved"
                && update["status"] == "in_progress")
        }),
        "no execution update may precede the permission request: {before:?}"
    );
    assert!(
        !workspace.path.join("approved.txt").exists(),
        "the tool must not run before the client answers"
    );

    assert_eq!(request["method"], "session/request_permission");
    assert_eq!(request["params"]["sessionId"], session);
    assert!(
        request.get("id").is_some(),
        "the permission request must be a request with a JSON-RPC id: {request}"
    );
    let requested = &request["params"]["toolCall"];
    assert_eq!(requested["toolCallId"], "call_approved");
    assert_eq!(requested["status"], "pending");
    assert_eq!(requested["title"], "bash: printf approved > approved.txt");
    let options: Vec<(&str, &str)> = request["params"]["options"]
        .as_array()
        .expect("permission options must be an array")
        .iter()
        .map(|option| {
            (
                option["optionId"]
                    .as_str()
                    .expect("options must carry an id"),
                option["kind"].as_str().expect("options must carry a kind"),
            )
        })
        .collect();
    assert_eq!(
        options,
        vec![("allow_once", "allow_once"), ("reject_once", "reject_once")],
        "only one-shot choices may be offered: {request}"
    );
    assert_eq!(
        acp.permission_requests().len(),
        1,
        "one sensitive tool call must produce one permission request"
    );

    acp.respond_permission(&request, PermissionAnswer::AllowOnce)
        .await;
    let (updates, response) = collect_until_response(&mut acp, 3, &session).await;
    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "an approved tool must let the turn finish: {response}"
    );
    let statuses: Vec<&str> = updates
        .iter()
        .filter(|update| {
            matches!(update_kind(update), "tool_call" | "tool_call_update")
                && update["toolCallId"] == "call_approved"
        })
        .filter_map(|update| update["status"].as_str())
        .collect();
    assert_eq!(
        statuses,
        vec!["in_progress", "completed"],
        "approval must open execution and record completion: {updates:?}"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.path.join("approved.txt"))
            .expect("the approved command must write its file"),
        "approved"
    );
    assert_eq!(acp.permission_requests().len(), 1);
    assert_eq!(
        provider.finish().len(),
        2,
        "one tool round plus one final answer must reach the provider"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_reject_once_never_runs_the_tool_and_reports_the_denial() {
    let workspace = TempDir::new("permission-reject");
    let provider = MockProvider::start(vec![
        tool_calls_response(&[(
            "call_denied",
            "write_file",
            json!({"path": "denied.txt", "content": "must not exist"}),
        )]),
        text_response("adapted after rejection"),
    ]);
    let mut acp = spawn_permission_harness(&workspace, "\"write_file\"", &provider).await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;
    acp.set_permission_mode(PermissionMode::RejectOnce);

    let (updates, response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "write the denied file"}]),
    )
    .await;

    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "the model may adapt after a rejection: {response}"
    );
    assert_eq!(
        acp.permission_requests().len(),
        1,
        "the sensitive call must be offered exactly once"
    );
    assert_eq!(
        acp.permission_requests()[0]["params"]["toolCall"]["toolCallId"],
        "call_denied"
    );
    let started = updates
        .iter()
        .find(|update| update_kind(update) == "tool_call" && update["toolCallId"] == "call_denied")
        .expect("the rejected call must be announced");
    assert!(
        started.get("status").is_none(),
        "ACP omits the default pending status: {started}"
    );
    let statuses: Vec<&str> = updates
        .iter()
        .filter(|update| {
            matches!(update_kind(update), "tool_call" | "tool_call_update")
                && update["toolCallId"] == "call_denied"
        })
        .filter_map(|update| update["status"].as_str())
        .collect();
    assert_eq!(
        statuses,
        vec!["failed"],
        "a rejected call must never enter in_progress: {updates:?}"
    );
    let failed = updates
        .iter()
        .find(|update| {
            update_kind(update) == "tool_call_update" && update["toolCallId"] == "call_denied"
        })
        .expect("the rejected call must finish");
    assert!(
        failed["content"][0]["content"]["text"]
            .as_str()
            .is_some_and(|text| text.contains("denied by the user")),
        "the model must see why the call did not run: {failed}"
    );
    assert!(
        !workspace.path.join("denied.txt").exists(),
        "a rejected write must not touch the workspace"
    );
    let requests = provider.finish();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1].contains("denied by the user"),
        "the denial must be returned to the model"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_cancelled_permission_outcome_ends_the_turn_without_running_the_tool() {
    let workspace = TempDir::new("permission-cancelled");
    let provider = MockProvider::start(vec![
        tool_calls_response(&[(
            "call_cancelled",
            "bash",
            json!({"command": "printf ran > ran.txt"}),
        )]),
        text_response("after cancelled permission"),
    ]);
    let mut acp = spawn_permission_harness(&workspace, "\"bash\"", &provider).await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;
    acp.set_permission_mode(PermissionMode::Cancelled);

    let (updates, response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "ask and cancel"}]),
    )
    .await;

    assert_eq!(
        response["result"]["stopReason"], "cancelled",
        "a cancelled permission outcome must end the turn cancelled: {response}"
    );
    assert!(
        updates.iter().all(|update| {
            !(matches!(update_kind(update), "tool_call" | "tool_call_update")
                && update["toolCallId"] == "call_cancelled"
                && update["status"] == "in_progress")
        }),
        "a cancelled permission must not open execution: {updates:?}"
    );
    assert!(
        !workspace.path.join("ran.txt").exists(),
        "the cancelled tool must not run"
    );
    assert_eq!(acp.permission_requests().len(), 1);
    assert_eq!(
        acp.permission_requests()[0]["params"]["toolCall"]["toolCallId"],
        "call_cancelled"
    );

    let (_, follow_up) = prompt_until_response(
        &mut acp,
        4,
        &session,
        json!([{"type": "text", "text": "continue after cancellation"}]),
    )
    .await;
    assert_eq!(
        follow_up["result"]["stopReason"], "end_turn",
        "a cancelled permission must not poison the session: {follow_up}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    assert_eq!(provider.finish().len(), 2);
}

#[tokio::test]
async fn acp_cancel_while_a_permission_is_pending_never_runs_the_tool() {
    let workspace = TempDir::new("permission-pending-cancel");
    let provider = MockProvider::start(vec![
        tool_calls_response(&[(
            "call_pending",
            "bash",
            json!({"command": "printf ran > pending.txt"}),
        )]),
        text_response("after pending cancellation"),
    ]);
    let mut acp = spawn_permission_harness(&workspace, "\"bash\"", &provider).await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;
    acp.set_permission_mode(PermissionMode::Manual);

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "session/prompt",
        "params": {
            "sessionId": session,
            "prompt": [{"type": "text", "text": "ask and wait"}],
        },
    }))
    .await;
    let (_, request) = collect_until_permission(&mut acp).await;
    assert_eq!(request["params"]["toolCall"]["toolCallId"], "call_pending");

    send_cancel(&mut acp, &session).await;
    let (updates, response) = collect_until_response(&mut acp, 3, &session).await;
    assert_eq!(
        response["result"]["stopReason"], "cancelled",
        "cancelling a pending permission must cancel the turn: {response}"
    );
    assert!(
        updates.iter().all(|update| {
            !(matches!(update_kind(update), "tool_call" | "tool_call_update")
                && update["toolCallId"] == "call_pending"
                && update["status"] == "in_progress")
        }),
        "a cancelled pending request must never open execution: {updates:?}"
    );
    assert!(
        !workspace.path.join("pending.txt").exists(),
        "the unapproved tool must not run after cancellation"
    );
    assert_eq!(acp.permission_requests().len(), 1);

    let (_, follow_up) = prompt_until_response(
        &mut acp,
        4,
        &session,
        json!([{"type": "text", "text": "continue after cancel"}]),
    )
    .await;
    assert_eq!(
        follow_up["result"]["stopReason"], "end_turn",
        "the session must stay usable: {follow_up}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    assert_eq!(provider.finish().len(), 2);
}

#[tokio::test]
async fn acp_delegation_asks_only_for_the_parent_call() {
    let workspace = TempDir::new("permission-delegation");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\", \"delegate_task\"]\n\n\
         [tools.bash]\nenabled = true\n\n\
         [delegation]\nenabled = true\n",
    )
    .expect("harness configuration must be written");
    let provider = MockProvider::start(vec![
        tool_calls_response(&[(
            "call_delegate",
            "delegate_task",
            json!({"task": "write the child marker", "tools": ["bash"]}),
        )]),
        tool_calls_response(&[(
            "child_marker",
            "bash",
            json!({"command": "printf child > child.txt"}),
        )]),
        text_response("child wrote the marker"),
        text_response("parent done"),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;

    let (updates, response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "delegate the marker"}]),
    )
    .await;

    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "the delegated turn must complete: {response}"
    );
    assert_eq!(
        acp.permission_requests().len(),
        1,
        "only the top-level delegate_task call may ask; child tools run under it"
    );
    let request = &acp.permission_requests()[0];
    assert_eq!(request["params"]["toolCall"]["toolCallId"], "call_delegate");
    assert_eq!(
        request["params"]["toolCall"]["rawInput"]["task"],
        "write the child marker"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.path.join("child.txt"))
            .expect("the child bash tool must have run without a second request"),
        "child"
    );
    let completed = updates
        .iter()
        .find(|update| {
            update_kind(update) == "tool_call_update"
                && update["toolCallId"] == "call_delegate"
                && update["status"] == "completed"
        })
        .unwrap_or_else(|| panic!("the parent call must complete: {updates:?}"));
    assert!(
        completed["rawOutput"]
            .as_str()
            .is_some_and(|output| output.contains("child wrote the marker")),
        "the delegated answer must reach the parent: {completed}"
    );
    assert_eq!(
        provider.finish().len(),
        4,
        "parent, child tool round, child answer, parent answer"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[cfg(unix)]
#[tokio::test]
async fn acp_sigterm_cancels_two_active_sessions_and_exits_within_the_bound() {
    let workspace = TempDir::new("sigterm-active");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\"]\n\n\
         [tools.bash]\nenabled = true\n\n\
         [skills]\nenabled = true\npath = \"skills.db\"\ndrain_timeout_ms = 500\n",
    )
    .expect("harness configuration must be written");
    let provider = MockProvider::start(vec![
        tool_calls_response(&[("call_a", "bash", json!({"command": "sleep 30"}))]),
        tool_calls_response(&[("call_b", "bash", json!({"command": "sleep 30"}))]),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let first = create_session(&mut acp, 2, &workspace.path).await;
    let second = create_session(&mut acp, 3, &workspace.path).await;

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "session/prompt",
        "params": {"sessionId": first, "prompt": [{"type": "text", "text": "first"}]},
    }))
    .await;
    collect_update_of_kind(&mut acp, &first, "tool_call").await;
    collect_until_tool_status(&mut acp, &first, "call_a", "in_progress").await;
    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "session/prompt",
        "params": {"sessionId": second, "prompt": [{"type": "text", "text": "second"}]},
    }))
    .await;
    collect_update_of_kind(&mut acp, &second, "tool_call").await;

    let pid = acp.child.id().expect("the child must still be running");
    let status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("the kill utility must be available");
    assert!(status.success(), "SIGTERM delivery must succeed");

    let started = Instant::now();
    let exit = acp.wait_keeping_stdin().await;
    assert!(
        started.elapsed() < TEST_TIMEOUT,
        "two active sessions must drain within the bounded timeout"
    );
    let stderr = acp.remaining_stderr().await;
    assert_eq!(
        exit.code(),
        Some(143),
        "the first signal must keep the SIGTERM exit code; stderr: {stderr:?}"
    );
    assert!(
        !stderr.contains("panicked"),
        "shutdown must not panic: {stderr:?}"
    );

    let rest = acp.remaining_stdout().await;
    let responses: Vec<Value> = rest
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(parse_message)
        .filter(|message| message.get("id").is_some())
        .collect();
    for id in [4, 5] {
        let response = responses
            .iter()
            .find(|message| message["id"] == id)
            .unwrap_or_else(|| panic!("session {id} must receive a prompt response: {rest:?}"));
        assert_eq!(
            response["result"]["stopReason"], "cancelled",
            "shutdown must cancel every active prompt: {response}"
        );
    }
    assert!(
        workspace.path.join("skills.db").is_file(),
        "the configured skill worker must be assembled and drained before exit"
    );
    assert_eq!(provider.finish().len(), 2);
}

#[tokio::test]
async fn acp_stdin_eof_cancels_two_active_sessions_and_exits_within_the_bound() {
    let workspace = TempDir::new("eof-active");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\"]\n\n\
         [tools.bash]\nenabled = true\n",
    )
    .expect("harness configuration must be written");
    let provider = MockProvider::start(vec![
        tool_calls_response(&[("call_a", "bash", json!({"command": "sleep 30"}))]),
        tool_calls_response(&[("call_b", "bash", json!({"command": "sleep 30"}))]),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let first = create_session(&mut acp, 2, &workspace.path).await;
    let second = create_session(&mut acp, 3, &workspace.path).await;

    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "session/prompt",
        "params": {"sessionId": first, "prompt": [{"type": "text", "text": "first"}]},
    }))
    .await;
    collect_update_of_kind(&mut acp, &first, "tool_call").await;
    collect_until_tool_status(&mut acp, &first, "call_a", "in_progress").await;
    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "session/prompt",
        "params": {"sessionId": second, "prompt": [{"type": "text", "text": "second"}]},
    }))
    .await;
    collect_update_of_kind(&mut acp, &second, "tool_call").await;

    acp.close_stdin();
    let started = Instant::now();
    let status = acp.wait().await;
    assert!(
        started.elapsed() < TEST_TIMEOUT,
        "stdin EOF must cancel active prompts and exit within the bound"
    );
    assert_eq!(
        status.code(),
        Some(0),
        "a client disconnect is a clean ACP termination"
    );
    assert_eq!(
        provider.finish().len(),
        2,
        "disconnecting must not let interrupted tools start another provider call"
    );
}

#[tokio::test]
async fn acp_stdout_failure_cancels_work_without_panicking_or_hanging() {
    let workspace = TempDir::new("stdout-failure");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\"]\n\n\
         [tools.bash]\nenabled = true\n",
    )
    .expect("harness configuration must be written");
    let provider = MockProvider::start(vec![tool_calls_response(&[(
        "call_sleep",
        "bash",
        json!({"command": "sleep 30"}),
    )])]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;

    // Drop the read end before the prompt so the first session update writes
    // into a closed pipe. The client is gone; tools must not keep running.
    acp.close_stdout();
    acp.send(json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "session/prompt",
        "params": {"sessionId": session, "prompt": [{"type": "text", "text": "start"}]},
    }))
    .await;

    let exit = acp.wait_keeping_stdin().await;
    let stderr = acp.remaining_stderr().await;
    assert!(
        exit.code().is_some(),
        "a broken stdout must end the process instead of retrying forever"
    );
    assert!(
        !stderr.contains("panicked"),
        "a protocol write failure must not panic: {stderr:?}"
    );
    assert_eq!(
        provider.finish().len(),
        1,
        "the interrupted tool must not reach the provider again after the client disconnects"
    );
}

#[tokio::test]
async fn acp_maps_token_budget_exhaustion_to_max_tokens_and_keeps_history() {
    let workspace = TempDir::new("token-budget");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_tokens = 1\nmax_rounds = 4\n\n[policy]\nallowed_tools = []\n",
    )
    .expect("budget configuration must be written");
    let provider = MockProvider::start(vec![
        sse(&[
            content_chunk("partial answer"),
            usage_chunk(10, 10),
            finish_chunk("stop"),
        ]),
        text_response("after budget"),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;

    let (updates, response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "spend tokens"}]),
    )
    .await;
    assert_eq!(
        response["result"]["stopReason"], "max_tokens",
        "the token boundary must map to max_tokens: {response}"
    );
    assert!(
        updates
            .iter()
            .any(|update| update_kind(update) == "agent_message_chunk"
                && update["content"]["text"] == "partial answer"),
        "text produced before the boundary must still stream: {updates:?}"
    );
    assert!(
        updates.iter().all(|update| update_kind(update) != "notice"),
        "a client that did not advertise session.notices must receive zero notices: {updates:?}"
    );

    let (_updates, response) = prompt_until_response(
        &mut acp,
        4,
        &session,
        json!([{"type": "text", "text": "continue"}]),
    )
    .await;
    assert_eq!(response["result"]["stopReason"], "end_turn");

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    let requests = provider.finish();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1].contains("partial answer"),
        "a governed stop must still commit the partial turn"
    );
}

#[tokio::test]
async fn acp_sends_a_schema_valid_notice_only_when_the_client_advertises_it() {
    let workspace = TempDir::new("notices");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_tokens = 1\nmax_rounds = 4\n\n[policy]\nallowed_tools = []\n",
    )
    .expect("budget configuration must be written");
    let provider = MockProvider::start(vec![sse(&[
        content_chunk("partial answer"),
        usage_chunk(10, 10),
        finish_chunk("stop"),
    ])]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake_with_notices(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;

    let (updates, response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "spend tokens"}]),
    )
    .await;

    assert_eq!(
        response["result"]["stopReason"], "max_tokens",
        "the stop reason remains the primary budget signal: {response}"
    );
    let notices: Vec<&Value> = updates
        .iter()
        .filter(|update| update_kind(update) == "notice")
        .collect();
    assert_eq!(
        notices.len(),
        1,
        "a client advertising notices must receive one content-free budget notice: {updates:?}"
    );
    let notice = notices[0];
    assert_eq!(notice["severity"], "warning");
    assert_eq!(notice["title"], "Token budget exhausted");
    assert!(
        notice["description"]
            .as_str()
            .is_some_and(|description| !description.is_empty()),
        "the notice must remain schema-valid with a usable description: {notice}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    assert_eq!(provider.finish().len(), 1);
}

#[tokio::test]
async fn acp_error_paths_are_stable_and_keep_sensitive_content_out_of_stderr() {
    const PROMPT_SECRET: &str = "PROMPT_SECRET_7f3c";
    const TOOL_SECRET: &str = "TOOL_SECRET_91ab";

    let workspace = TempDir::new("privacy");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 4\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\"]\n\n\
         [tools.bash]\nenabled = true\n",
    )
    .expect("harness configuration must be written");
    let provider = MockProvider::start(vec![
        tool_calls_response(&[(
            "call_secret",
            "bash",
            json!({"command": format!("printf '%s' '{TOOL_SECRET}'")}),
        )]),
        internal_server_error(),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;

    let (_, missing) = prompt_until_response(
        &mut acp,
        2,
        "acp-missing",
        json!([{"type": "text", "text": format!("{PROMPT_SECRET} missing session")}]),
    )
    .await;
    assert_eq!(
        missing["error"]["code"], -32002,
        "an unknown session must be resource-not-found: {missing}"
    );
    assert_eq!(missing["error"]["message"], "Resource not found");
    assert_eq!(missing["error"]["data"]["sessionId"], "acp-missing");
    assert!(
        !missing.to_string().contains(PROMPT_SECRET),
        "the unknown-session error must not echo prompt content: {missing}"
    );

    let session = create_session(&mut acp, 3, &workspace.path).await;
    let (_updates, failed) = prompt_until_response(
        &mut acp,
        4,
        &session,
        json!([{"type": "text", "text": format!("{PROMPT_SECRET} run the tool")}]),
    )
    .await;
    assert_eq!(
        failed["error"]["code"], -32603,
        "an internal provider failure must use the internal-error code: {failed}"
    );
    assert_eq!(
        failed["error"]["message"], "prompt failed",
        "provider details must not leak through the error: {failed}"
    );
    assert!(
        !failed.to_string().contains(PROMPT_SECRET) && !failed.to_string().contains(TOOL_SECRET),
        "the prompt error must not echo prompt or tool content: {failed}"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    let stderr = acp.remaining_stderr().await;
    assert!(
        !stderr.contains(PROMPT_SECRET),
        "stderr must never contain the prompt: {stderr:?}"
    );
    assert!(
        !stderr.contains(TOOL_SECRET),
        "stderr must never contain tool arguments or results: {stderr:?}"
    );
    assert!(
        !stderr.contains("panicked"),
        "error handling must not panic: {stderr:?}"
    );

    let rest = acp.remaining_stdout().await;
    for line in rest.lines().filter(|line| !line.trim().is_empty()) {
        parse_message(line);
    }

    let requests = provider.finish();
    assert_eq!(
        requests.len(),
        2,
        "the failing tool turn must make one tool-continuation provider call"
    );
    assert!(
        requests[0].contains(PROMPT_SECRET),
        "the fixture must actually send the sensitive prompt to the provider"
    );
    assert!(
        requests[1].contains(TOOL_SECRET),
        "the fixture must actually send the sensitive tool call to the provider"
    );
}

#[tokio::test]
async fn acp_hot_reload_applies_reloadable_sections_and_rejects_acp_limits() {
    let workspace = TempDir::new("hot-reload");
    let config = workspace.path.join("harness.toml");
    let base = "\
[budget]
max_rounds = 4
max_tokens = 100000

[policy]
allowed_tools = [\"read_file\"]

[acp]
max_prompt_bytes = 8
";
    write_config_atomic(&config, base);
    let provider = MockProvider::start(vec![
        text_response("first answer"),
        text_response("second answer"),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;
    let session = create_session(&mut acp, 2, &workspace.path).await;

    let (_, first) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "12345678"}]),
    )
    .await;
    assert_eq!(first["result"]["stopReason"], "end_turn");

    // A policy change is reloadable and must affect the next prompt's tools.
    write_config_atomic(
        &config,
        "\
[budget]
max_rounds = 4
max_tokens = 100000

[policy]
allowed_tools = [\"bash\"]

[tools.bash]
enabled = true

[acp]
max_prompt_bytes = 8
",
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let (_, second) = prompt_until_response(
        &mut acp,
        4,
        &session,
        json!([{"type": "text", "text": "next"}]),
    )
    .await;
    assert_eq!(
        second["result"]["stopReason"], "end_turn",
        "the prompt after a reload must run normally: {second}"
    );

    // ACP limits are startup-only. The watcher must reject the whole document
    // and keep the startup value even though the policy change alone would be
    // reloadable.
    write_config_atomic(
        &config,
        "\
[budget]
max_rounds = 4
max_tokens = 100000

[policy]
allowed_tools = [\"bash\"]

[tools.bash]
enabled = true

[acp]
max_prompt_bytes = 64
",
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let (_, rejected) = prompt_until_response(
        &mut acp,
        5,
        &session,
        json!([{"type": "text", "text": "123456789"}]),
    )
    .await;
    assert_eq!(
        rejected["error"]["code"], -32602,
        "an ACP limit reload must not change the startup prompt bound: {rejected}"
    );
    assert_eq!(
        rejected["error"]["message"],
        "prompt exceeds the maximum of 8 bytes"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    let stderr = acp.remaining_stderr().await;
    assert!(
        stderr.contains("[config] reloaded"),
        "a reloadable change must be reported on stderr: {stderr:?}"
    );
    assert!(
        stderr.contains("[config] reload rejected: `acp`"),
        "an ACP limit change must be rejected by name: {stderr:?}"
    );

    let requests = provider.finish();
    assert_eq!(requests.len(), 2);
    assert!(
        request_system_prompt(&requests[0]).contains("allowlist: read_file"),
        "the first prompt must use the startup policy"
    );
    assert!(
        request_system_prompt(&requests[1]).contains("allowlist: bash"),
        "the next prompt must use the reloaded policy"
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

#[tokio::test]
async fn acp_mcp_stdio_discovers_and_invokes_namespaced_tool() {
    assert_mcp_fixture_available();
    let workspace = TempDir::new("mcp-invoke");
    let secret = "mcp-secret-must-not-leak";
    let config = write_mcp_config(&workspace, &["mcp__stub__echo"], 10_000);
    let provider = MockProvider::start(vec![
        tool_calls_response(&[(
            "call_mcp_echo",
            "mcp__stub__echo",
            json!({"text": "hello from mcp"}),
        )]),
        text_response("mcp turn complete"),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;

    let response = create_mcp_session(
        &mut acp,
        2,
        &workspace.path,
        mcp_stdio_server(&mcp_stub_env(&workspace, "normal", Some(secret))),
    )
    .await;
    let session = response["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("stdio MCP session must be created: {response}"))
        .to_owned();
    let pid = wait_for_mcp_pid(&workspace.path.join("mcp.pid")).await;
    assert!(
        Path::new(&format!("/proc/{pid}")).exists(),
        "the MCP child must be running after session/new"
    );

    let (updates, prompt_response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "use the MCP echo tool"}]),
    )
    .await;
    assert_eq!(
        prompt_response["result"]["stopReason"], "end_turn",
        "the MCP-backed turn must complete: {prompt_response}"
    );
    let started = updates
        .iter()
        .find(|update| update_kind(update) == "tool_call" && update["name"] == "mcp__stub__echo")
        .unwrap_or_else(|| panic!("the namespaced MCP tool must start: {updates:?}"));
    assert!(
        started.get("kind").is_none(),
        "ACP omits the default `other` tool kind: {started}"
    );
    assert_eq!(started["rawInput"], json!({"text": "hello from mcp"}));
    let finished = updates
        .iter()
        .find(|update| {
            update_kind(update) == "tool_call_update" && update["toolCallId"] == "call_mcp_echo"
        })
        .unwrap_or_else(|| panic!("the MCP tool must finish: {updates:?}"));
    assert_eq!(finished["status"], "completed");
    assert_eq!(finished["rawOutput"], "hello from mcp");
    assert_eq!(
        mcp_call_log(&workspace).trim(),
        "echo",
        "exactly one real MCP tools/call must have occurred"
    );

    let requests = provider.finish();
    assert_eq!(requests.len(), 2, "one tool round plus one final answer");
    assert!(
        requests[0].contains("mcp__stub__echo"),
        "the provider must receive the namespaced MCP tool schema"
    );
    assert!(
        requests[1].contains("hello from mcp"),
        "the MCP result must be returned to the model"
    );
    assert!(
        !requests.iter().any(|request| request.contains(secret)),
        "MCP environment values must never reach the provider"
    );
    assert!(
        !format!("{updates:?}{prompt_response}").contains(secret),
        "MCP environment values must never reach protocol updates"
    );

    let close = request_response(&mut acp, 4, "session/close", json!({"sessionId": session})).await;
    assert!(
        close.get("result").is_some(),
        "closing the MCP session must succeed: {close}"
    );
    wait_for_process_gone(pid).await;

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    let stderr = acp.remaining_stderr().await;
    assert!(
        !stderr.contains(secret),
        "the drained MCP stderr must not be logged: {stderr}"
    );
}

#[tokio::test]
async fn acp_mcp_tool_hidden_by_policy_is_unknown_to_the_model() {
    assert_mcp_fixture_available();
    let workspace = TempDir::new("mcp-denied");
    let config = write_mcp_config(&workspace, &["read_file"], 10_000);
    let provider = MockProvider::start(vec![
        tool_calls_response(&[(
            "call_denied",
            "mcp__stub__echo",
            json!({"text": "must not reach stub"}),
        )]),
        text_response("adapted after an unknown tool"),
    ]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;

    let response = create_mcp_session(
        &mut acp,
        2,
        &workspace.path,
        mcp_stdio_server(&mcp_stub_env(&workspace, "normal", None)),
    )
    .await;
    let session = response["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("stdio MCP session must be created: {response}"))
        .to_owned();
    let pid = wait_for_mcp_pid(&workspace.path.join("mcp.pid")).await;

    let (updates, prompt_response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "try the MCP tool"}]),
    )
    .await;
    assert_eq!(
        prompt_response["result"]["stopReason"], "end_turn",
        "the model may adapt after an unknown tool: {prompt_response}"
    );
    let failed = updates
        .iter()
        .find(|update| {
            update_kind(update) == "tool_call_update" && update["toolCallId"] == "call_denied"
        })
        .unwrap_or_else(|| panic!("the hidden MCP tool call must finish: {updates:?}"));
    assert_eq!(failed["status"], "failed");
    assert!(
        failed["content"][0]["content"]["text"]
            .as_str()
            .is_some_and(|text| text.contains("unknown tool `mcp__stub__echo`")),
        "a hidden tool must be unknown to the model: {failed}"
    );
    assert_eq!(
        mcp_call_log(&workspace),
        "",
        "a policy-hidden MCP tool must never reach the child process"
    );

    let requests = provider.finish();
    assert_eq!(requests.len(), 2);
    assert!(
        !requests[0].contains("mcp__stub__echo"),
        "the provider schema must not advertise a policy-hidden tool"
    );
    assert!(
        requests[1].contains("unknown tool `mcp__stub__echo`"),
        "the unknown-tool error must be returned to the model"
    );

    let close = request_response(&mut acp, 4, "session/close", json!({"sessionId": session})).await;
    assert!(close.get("result").is_some(), "close must succeed: {close}");
    wait_for_process_gone(pid).await;
    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_mcp_tool_timeout_ends_turn_and_close_reaps_child() {
    assert_mcp_fixture_available();
    let workspace = TempDir::new("mcp-timeout");
    let config = write_mcp_config(&workspace, &["mcp__stub__slow"], 250);
    let provider = MockProvider::start(vec![tool_calls_response(&[(
        "call_slow",
        "mcp__stub__slow",
        json!({"seconds": 30}),
    )])]);
    let mut acp = AcpProcess::spawn_in(
        Some(&workspace.path),
        &[
            ("OPENAI_API_KEY", "test-key".to_owned()),
            ("OPENAI_BASE_URL", provider.url()),
            ("OPENAI_MODEL", "test-model".to_owned()),
        ],
        &["--config", config.to_str().expect("utf-8 config path")],
    )
    .await;
    handshake(&mut acp).await;

    let response = create_mcp_session(
        &mut acp,
        2,
        &workspace.path,
        mcp_stdio_server(&mcp_stub_env(&workspace, "normal", None)),
    )
    .await;
    let session = response["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("stdio MCP session must be created: {response}"))
        .to_owned();
    let pid = wait_for_mcp_pid(&workspace.path.join("mcp.pid")).await;

    let started = Instant::now();
    let (updates, prompt_response) = prompt_until_response(
        &mut acp,
        3,
        &session,
        json!([{"type": "text", "text": "call the slow MCP tool"}]),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the configured tool timeout must stop the MCP call promptly"
    );
    assert_eq!(
        prompt_response["result"]["stopReason"], "max_turn_requests",
        "a tool timeout is a governed turn boundary: {prompt_response}"
    );
    let statuses: Vec<&str> = updates
        .iter()
        .filter(|update| matches!(update_kind(update), "tool_call" | "tool_call_update"))
        .filter_map(|update| update["status"].as_str())
        .collect();
    assert_eq!(
        statuses,
        vec!["in_progress", "failed"],
        "the timed-out MCP call must end failed and never remain in progress: {updates:?}"
    );
    assert_eq!(
        mcp_call_log(&workspace).trim(),
        "slow",
        "the timeout must occur after the call reached the MCP server"
    );

    assert_eq!(provider.finish().len(), 1);
    let close = request_response(&mut acp, 4, "session/close", json!({"sessionId": session})).await;
    assert!(close.get("result").is_some(), "close must succeed: {close}");
    wait_for_process_gone(pid).await;
    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_mcp_startup_failures_return_internal_error_and_reap_child() {
    assert_mcp_fixture_available();
    let workspace = TempDir::new("mcp-startup-failure");
    let mut acp = AcpProcess::spawn_in(Some(&workspace.path), &[], &[]).await;
    handshake(&mut acp).await;

    for (index, mode) in ["init-error", "list-error", "malformed"]
        .into_iter()
        .enumerate()
    {
        let pid_file = workspace.path.join("mcp.pid");
        let _ = std::fs::remove_file(&pid_file);
        let response = create_mcp_session(
            &mut acp,
            2 + index as u64,
            &workspace.path,
            mcp_stdio_server(&mcp_stub_env(&workspace, mode, None)),
        )
        .await;
        assert_eq!(
            response["error"]["code"], -32603,
            "{mode} must fail session creation without leaking protocol details: {response}"
        );
        assert_eq!(
            response["error"]["data"]["reason"], "mcp_server_startup_failed",
            "the error data must carry the stable MCP startup reason: {response}"
        );
        let pid = wait_for_mcp_pid(&pid_file).await;
        wait_for_process_gone(pid).await;
    }

    let _ = std::fs::remove_file(workspace.path.join("mcp.pid"));
    let response = create_mcp_session(
        &mut acp,
        10,
        &workspace.path,
        mcp_stdio_server(&mcp_stub_env(&workspace, "normal", None)),
    )
    .await;
    let session = response["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("the agent must still create sessions: {response}"))
        .to_owned();
    let pid = wait_for_mcp_pid(&workspace.path.join("mcp.pid")).await;
    let close =
        request_response(&mut acp, 11, "session/close", json!({"sessionId": session})).await;
    assert!(close.get("result").is_some(), "close must succeed: {close}");
    wait_for_process_gone(pid).await;

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}

#[tokio::test]
async fn acp_mcp_children_are_reaped_on_shutdown_and_env_is_not_logged() {
    assert_mcp_fixture_available();
    let workspace = TempDir::new("mcp-shutdown");
    let secret = "shutdown-secret-must-not-leak";
    let mut acp = AcpProcess::spawn_in(Some(&workspace.path), &[], &[]).await;
    handshake(&mut acp).await;

    let response = create_mcp_session(
        &mut acp,
        2,
        &workspace.path,
        mcp_stdio_server(&mcp_stub_env(&workspace, "normal", Some(secret))),
    )
    .await;
    assert!(
        response["result"]["sessionId"].is_string(),
        "the session must start with the expected secret: {response}"
    );
    let pid = wait_for_mcp_pid(&workspace.path.join("mcp.pid")).await;
    assert!(
        Path::new(&format!("/proc/{pid}")).exists(),
        "the MCP child must be alive before shutdown"
    );
    assert!(
        !response.to_string().contains(secret),
        "environment values must not appear in session/new output"
    );

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    wait_for_process_gone(pid).await;
    let stderr = acp.remaining_stderr().await;
    assert!(
        !stderr.contains(secret),
        "MCP child stderr must be drained without being logged: {stderr}"
    );
}

#[tokio::test]
async fn acp_session_new_rejects_http_mcp_and_keeps_running() {
    let workspace = TempDir::new("mcp-http");
    let mut acp = AcpProcess::spawn_in(Some(&workspace.path), &[], &[]).await;
    handshake(&mut acp).await;

    let response = create_mcp_session(
        &mut acp,
        2,
        &workspace.path,
        json!({
            "name": "remote",
            "url": "https://example.invalid/mcp",
            "headers": [],
        }),
    )
    .await;
    assert_eq!(
        response["error"]["code"], -32602,
        "HTTP MCP must be rejected because only stdio is supported: {response}"
    );
    assert!(
        response.get("result").is_none(),
        "HTTP MCP must not create a session: {response}"
    );

    // The failed request must not poison later session creation.
    let created = create_session(&mut acp, 3, &workspace.path).await;
    let close = request_response(&mut acp, 4, "session/close", json!({"sessionId": created})).await;
    assert!(close.get("result").is_some(), "close must succeed: {close}");

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
}
