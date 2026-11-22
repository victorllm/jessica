//! End-to-end acceptance for the Task 9.5 native tools.
//!
//! Runs the real CLI against a scripted SSE server. The model calls `exec`
//! three times (literal arguments, environment cleanup, and a non-zero exit),
//! then `read_file` with a line offset. Every result is inspected from the
//! next provider request, so the test observes exactly what the model saw.
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
/// Overall budget for the mock server to observe every request.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);
/// Number of provider requests scripted by this test.
const REQUESTS: usize = 5;

/// Temporary workspace removed recursively.
struct TempDir {
    /// Workspace path.
    path: PathBuf,
}

impl TempDir {
    /// Creates a unique temporary workspace.
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-native-tools-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temporary workspace should be created");
        Self { path }
    }

    /// Returns the workspace path.
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
fn exec_and_read_file_are_governed_and_reported_through_the_real_agent_loop() {
    let temp = TempDir::new("exec-read");
    std::fs::write(temp.path().join("notes.txt"), "alpha\nbeta\ngamma\ndelta\n")
        .expect("workspace file should be written");
    std::fs::write(
        temp.path().join("harness.toml"),
        "[profile]\ntrace = \"off\"\n\
         [policy]\nallowed_tools = [\"exec\", \"read_file\"]\n\
         [tools.exec]\ntimeout_ms = 5000\nmax_args_bytes = 65536\n",
    )
    .expect("configuration should be written");

    let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
    let address = listener.local_addr().expect("mock server address");
    let server = thread::spawn(move || serve(listener));

    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(temp.path())
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .args([
            "--config",
            "harness.toml",
            "run the native tool acceptance turns",
        ])
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
        REQUESTS,
        "every scripted turn must complete\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("native tools complete"),
        "the turn must complete normally:\n{stdout}"
    );

    let literal = exec_result(&requests[1]);
    assert_eq!(literal["status"], "exited");
    assert_eq!(literal["exit_code"], 0);
    assert_eq!(
        literal["stdout"], "a b; $(echo injected) *",
        "arguments must be literal and never shell-expanded: {literal}"
    );
    assert_eq!(literal["stdout_truncated"], false);

    let environment = exec_result(&requests[2]);
    let environment_stdout = environment["stdout"]
        .as_str()
        .expect("env output must be text");
    assert!(
        environment_stdout
            .lines()
            .any(|line| line.starts_with("PATH=")),
        "the supervisor defaults must provide PATH: {environment_stdout}"
    );
    assert!(
        !environment_stdout.contains("OPENAI_API_KEY") && !environment_stdout.contains("test-key"),
        "provider credentials must not reach exec:\n{environment_stdout}"
    );

    let content = tool_content(&requests[3]);
    assert!(
        !content.starts_with("tool error:"),
        "a non-zero exit is a completed call, not a tool error: {content}"
    );
    let failed = exec_result(&requests[3]);
    assert_eq!(failed["status"], "exited");
    assert_eq!(
        failed["exit_code"], 1,
        "the model must receive the real exit code: {failed}"
    );

    let read = tool_content(&requests[4]);
    assert!(
        read.starts_with("2\tbeta\n3\tgamma\n"),
        "read_file must return one-based line numbers for the requested range: {read:?}"
    );
    assert!(
        !read.contains("alpha") && !read.contains("delta"),
        "lines outside the requested range must be absent: {read:?}"
    );
    assert!(
        read.contains("call read_file with offset=4"),
        "the footer must carry the next line offset: {read:?}"
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

/// Serves the scripted chat-completion responses and returns request bodies.
fn serve(listener: TcpListener) -> Vec<String> {
    listener
        .set_nonblocking(true)
        .expect("listener should become non-blocking");
    let deadline = Instant::now() + SERVER_TIMEOUT;
    let mut bodies = Vec::new();

    while bodies.len() < REQUESTS && Instant::now() < deadline {
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

/// Returns the scripted response for request `index`.
fn response_for(index: usize) -> String {
    let payloads = match index {
        0 => vec![
            tool_call_chunk(
                "call_literal",
                "exec",
                &serde_json::json!({
                    "program": "printf",
                    "args": ["%s", "a b; $(echo injected) *"],
                })
                .to_string(),
            ),
            finish_chunk("tool_calls"),
        ],
        1 => vec![
            tool_call_chunk(
                "call_env",
                "exec",
                &serde_json::json!({"program": "env", "args": []}).to_string(),
            ),
            finish_chunk("tool_calls"),
        ],
        2 => vec![
            tool_call_chunk(
                "call_false",
                "exec",
                &serde_json::json!({"program": "false", "args": []}).to_string(),
            ),
            finish_chunk("tool_calls"),
        ],
        3 => vec![
            tool_call_chunk(
                "call_read",
                "read_file",
                &serde_json::json!({"path": "notes.txt", "offset": 2, "limit": 2}).to_string(),
            ),
            finish_chunk("tool_calls"),
        ],
        _ => vec![content_chunk("native tools complete"), finish_chunk("stop")],
    };
    sse(&payloads)
}

/// Returns the last tool message content in one provider request.
fn tool_content(request: &str) -> String {
    let parsed: serde_json::Value =
        serde_json::from_str(request).expect("provider request must be JSON");
    parsed["messages"]
        .as_array()
        .expect("provider request must carry messages")
        .iter()
        .filter(|message| message["role"] == "tool")
        .filter_map(|message| message["content"].as_str())
        .next_back()
        .expect("the provider request must carry a tool result")
        .to_owned()
}

/// Parses the JSON document produced by `exec`.
fn exec_result(request: &str) -> serde_json::Value {
    let content = tool_content(request);
    let parsed: serde_json::Value =
        serde_json::from_str(&content).expect("exec must return a JSON document");
    assert_eq!(parsed["tool"], "exec", "unexpected exec result: {content}");
    parsed
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

/// Builds a tool-call chunk.
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

/// Builds a finish chunk.
fn finish_chunk(reason: &str) -> String {
    serde_json::json!({
        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]
    })
    .to_string()
}

/// Builds a text-content chunk.
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
