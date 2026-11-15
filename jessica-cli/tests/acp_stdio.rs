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
    /// Line-oriented protocol output.
    stdout: BufReader<ChildStdout>,
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

/// Completes the mandatory ACP handshake.
async fn handshake(acp: &mut AcpProcess) {
    acp.send(initialize(1, 1)).await;
    let response = acp.recv().await;
    assert!(
        response.get("result").is_some(),
        "initialize must succeed: {response}"
    );
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

/// Returns the `sessionUpdate` tag of one update object.
fn update_kind(update: &Value) -> &str {
    update["sessionUpdate"]
        .as_str()
        .unwrap_or_else(|| panic!("updates must carry sessionUpdate: {update}"))
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
async fn acp_prompt_validation_rejects_bad_content_without_a_model_call() {
    let workspace = TempDir::new("content");
    let config = workspace.path.join("harness.toml");
    std::fs::write(&config, "[acp]\nmax_prompt_bytes = 8\n")
        .expect("ACP configuration must be written");
    let provider = MockProvider::start(Vec::new());

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
        ),
        (
            "embedded resource",
            json!([{"type": "resource", "resource": {"text": "hi", "uri": "file:///x"}}]),
        ),
        ("empty", json!([{"type": "text", "text": "   "}])),
        ("oversized", json!([{"type": "text", "text": "123456789"}])),
    ];

    for (index, (label, prompt)) in prompts.into_iter().enumerate() {
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
    }

    acp.close_stdin();
    assert_eq!(acp.wait().await.code(), Some(0));
    assert!(
        provider.finish().is_empty(),
        "rejected prompts must never reach the provider"
    );
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
