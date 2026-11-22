//! End-to-end acceptance for the execution supervisor.
//!
//! Starts a scripted SSE server, runs the real CLI against a `bash` tool call,
//! and asserts that:
//!
//! - the child process sees only the supervisor environment allowlist;
//! - provider credentials and Jessica-prefixed host variables are absent;
//! - output beyond `tools.supervisor.max_output_bytes` is truncated and the
//!   truncation is fed back to the model without a `BudgetExceeded` event.
//!
//! No network access is involved; the provider is a local listener.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Overall budget for the CLI child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock server to observe two requests.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);

/// Temporary workspace removed recursively.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-supervisor-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temporary workspace should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[test]
fn bash_receives_only_the_supervisor_environment_and_truncated_output() {
    let temp = TempDir::new("env");
    let config = temp.path().join("harness.toml");
    std::fs::write(
        &config,
        "[profile]\ntrace = \"off\"\n\
         [policy]\nallowed_tools = [\"bash\"]\n\
         [tools.supervisor]\n\
         env_allowlist = [\"E2E_MARKER\"]\n\
         command_timeout_ms = 2000\n\
         max_output_bytes = 512\n",
    )
    .expect("configuration should be written");

    let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
    let address = listener.local_addr().expect("mock server address");
    let server = thread::spawn(move || serve(listener));

    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(temp.path())
        .env("OPENAI_API_KEY", "test-key-that-must-not-leak")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .env("E2E_MARKER", "supervisor-e2e")
        .env("JESSICA_E2E_SECRET", "jessica-secret-that-must-not-leak")
        .args(["--config", "harness.toml", "inspect bash environment"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should start");

    let status = wait_with_timeout(&mut child);
    let output = child
        .wait_with_output()
        .expect("CLI output should be collected");
    let requests = server.join().expect("mock server should finish");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        status.success(),
        "CLI failed with {status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        requests.len(),
        2,
        "expected two model requests, got {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        requests.len()
    );

    let result_request = &requests[1];
    assert!(
        !result_request.contains("test-key-that-must-not-leak"),
        "the provider key value must never reach bash"
    );
    assert!(
        !result_request.contains("jessica-secret-that-must-not-leak"),
        "Jessica host credentials must never reach bash"
    );

    let parsed: serde_json::Value =
        serde_json::from_str(result_request).expect("the second provider request must be JSON");
    let tool_content = parsed["messages"]
        .as_array()
        .expect("the request must carry messages")
        .iter()
        .find(|message| message["role"] == "tool")
        .and_then(|message| message["content"].as_str())
        .expect("the bash result must be included in the second request");
    assert!(
        tool_content.contains("E2E_MARKER=supervisor-e2e"),
        "the configured allowlist extension must reach bash: {tool_content}"
    );
    assert!(
        tool_content.contains("PATH="),
        "the default PATH entry must reach bash: {tool_content}"
    );
    assert!(
        !tool_content.contains("OPENAI_API_KEY="),
        "the provider key must never reach bash: {tool_content}"
    );
    assert!(
        !tool_content.contains("JESSICA_E2E_SECRET="),
        "Jessica host credentials must never reach bash: {tool_content}"
    );
    assert!(
        tool_content.contains("truncated"),
        "the truncated bash result must be fed back to the model: {tool_content}"
    );
    assert!(
        tool_content.contains("command produced"),
        "the truncation notice must record the original output size: {tool_content}"
    );
    assert!(
        !stdout.contains("BudgetExceeded"),
        "a capped result must not trip the result-size guard:\n{stdout}"
    );
    assert!(
        stdout.contains("supervisor environment inspected"),
        "the turn must complete normally:\n{stdout}"
    );
}

/// Waits for the child, killing it when the overall budget expires.
fn wait_with_timeout(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + CHILD_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().expect("CLI status should be readable") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("CLI exceeded {CHILD_TIMEOUT:?}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// Serves two scripted chat-completion responses and returns request bodies.
fn serve(listener: TcpListener) -> Vec<String> {
    listener
        .set_nonblocking(true)
        .expect("listener should become non-blocking");
    let deadline = Instant::now() + SERVER_TIMEOUT;
    let mut bodies = Vec::new();

    while bodies.len() < 2 && Instant::now() < deadline {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .expect("connection should become blocking");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("read timeout should be set");
                if let Some(body) = read_request(&mut stream) {
                    let response = response_for(bodies.len());
                    stream
                        .write_all(response.as_bytes())
                        .expect("response should be written");
                    stream.flush().expect("response should be flushed");
                    bodies.push(body);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("mock server accept failed: {error}"),
        }
    }

    bodies
}

/// Reads one HTTP request and returns its body.
fn read_request(stream: &mut TcpStream) -> Option<String> {
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

/// Returns the SSE response for request `index`.
fn response_for(index: usize) -> String {
    let payloads = match index {
        0 => {
            let command = "env | grep -E '^(E2E_MARKER|OPENAI_API_KEY|JESSICA_E2E_SECRET|PATH)=' | sort; \
                           i=0; while [ $i -lt 200 ]; do printf 'abcdefghij'; i=$((i+1)); done";
            vec![
                tool_call_chunk(
                    "call_bash",
                    "bash",
                    &serde_json::json!({ "command": command }).to_string(),
                ),
                finish_chunk("tool_calls"),
            ]
        }
        _ => vec![
            content_chunk("supervisor environment inspected"),
            finish_chunk("stop"),
        ],
    };
    sse(&payloads)
}

/// Builds an SSE body from JSON payloads.
fn sse(payloads: &[String]) -> String {
    let mut body = String::new();
    for payload in payloads {
        body.push_str("data: ");
        body.push_str(payload);
        body.push_str("\n\n");
    }
    body.push_str("data: [DONE]\n\n");

    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn tool_call_chunk(id: &str, name: &str, arguments: &str) -> String {
    serde_json::json!({
        "choices": [{
            "index": 0,
            "delta": {
                "tool_calls": [{
                    "index": 0,
                    "id": id,
                    "function": {"name": name, "arguments": arguments}
                }]
            },
            "finish_reason": null
        }]
    })
    .to_string()
}

fn finish_chunk(reason: &str) -> String {
    serde_json::json!({
        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]
    })
    .to_string()
}

fn content_chunk(content: &str) -> String {
    serde_json::json!({
        "choices": [{
            "index": 0,
            "delta": {"content": content},
            "finish_reason": null
        }]
    })
    .to_string()
}
