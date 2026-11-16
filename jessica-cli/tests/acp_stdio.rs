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

/// One running ACP agent process under test.
struct AcpProcess {
    /// Child process; stdin is taken when the test closes the stream.
    child: Child,
    /// Line-oriented protocol output; `None` after a test closes the pipe.
    stdout: Option<BufReader<ChildStdout>>,
    /// Diagnostics, read only by tests that assert on them.
    stderr: Option<ChildStderr>,
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
        let mut command = Command::new(env!("CARGO_BIN_EXE_jessica-cli"));
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
        (
            "mcp",
            json!(workspace.path),
            Some(json!([{"name": "server", "command": "/bin/true", "args": [], "env": []}])),
        ),
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
    assert_eq!(finished["rawOutput"], "note contents");
    assert_eq!(finished["content"][0]["type"], "content");
    assert_eq!(finished["content"][0]["content"]["type"], "text");
    assert_eq!(finished["content"][0]["content"]["text"], "note contents");
    assert_eq!(second_updates[2]["content"]["text"], "second answer");

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
async fn acp_initialize_advertises_load_only_after_opening_persistence() {
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
    assert_eq!(replayed[2]["rawOutput"], "note contents");
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
            .any(|message| message["role"] == "tool" && message["content"] == "note contents"),
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
        "load must reject MCP servers just like session/new: {unsupported}"
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
async fn acp_cancel_is_scoped_closes_in_flight_tools_and_keeps_the_session_usable() {
    let workspace = TempDir::new("cancel");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\"]\n",
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
    let started_updates = collect_update_of_kind(&mut acp, &cancelled_session, "tool_call").await;
    assert!(
        started_updates.iter().any(|update| {
            update_kind(update) == "tool_call"
                && update["toolCallId"] == "call_sleep"
                && update["status"] == "in_progress"
        }),
        "the bash tool must be observably in progress: {started_updates:?}"
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
            let status = update["status"]
                .as_str()
                .expect("tool updates must carry a status");
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

#[cfg(unix)]
#[tokio::test]
async fn acp_sigterm_cancels_two_active_sessions_and_exits_within_the_bound() {
    let workspace = TempDir::new("sigterm-active");
    let config = workspace.path.join("harness.toml");
    std::fs::write(
        &config,
        "[budget]\nmax_rounds = 8\nmax_tokens = 100000\n\n\
         [policy]\nallowed_tools = [\"bash\"]\n\n\
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
         [policy]\nallowed_tools = [\"bash\"]\n",
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
         [policy]\nallowed_tools = [\"bash\"]\n",
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
         [policy]\nallowed_tools = [\"bash\"]\n",
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
