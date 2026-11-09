//! End-to-end acceptance for wasm tools.
//!
//! Starts a scripted SSE server, runs the real CLI binary against the example
//! wasm echo plugin, and asserts that:
//!
//! - the wasm tool schema is registered and visible to the provider;
//! - a native tool outside the allowlist is denied by policy;
//! - the wasm tool executes and its JSON result reaches the next model request.
//!
//! No API key or network access is involved; the server is a local listener.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Overall budget for the child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock server to observe three requests.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("jessica-cli must live inside the workspace")
        .to_path_buf()
}

#[test]
fn cli_calls_a_wasm_tool_and_policy_denies_an_unlisted_native_tool() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
    let address = listener.local_addr().expect("mock server address");
    let server = thread::spawn(move || serve(listener));

    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica-cli"))
        .current_dir(workspace_root())
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .args([
            "--config",
            "examples/wasm_tool.toml",
            "run the wasm echo tool",
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
        3,
        "expected three model requests, got {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        requests.len()
    );

    assert!(
        requests[0].contains("\"echo_json\""),
        "wasm tool schema was not offered to the provider: {}",
        requests[0]
    );
    assert!(
        stdout.contains("[policy] denied read_file"),
        "missing policy denial in stdout:\n{stdout}"
    );
    assert!(
        requests[1].contains("denied by policy"),
        "denial result was not fed back to the model: {}",
        requests[1]
    );
    assert!(
        stdout.contains("[tool] echo_json"),
        "wasm tool call was not rendered:\n{stdout}"
    );
    assert!(
        stdout.contains("[tool:ok] {\"text\":\"hello\"}"),
        "wasm tool result was not successful:\n{stdout}"
    );
    assert!(
        requests[2].contains("call_echo") && requests[2].contains("hello"),
        "wasm result was not fed back to the model: {}",
        requests[2]
    );
    assert!(
        stdout.contains("wasm tool completed"),
        "final assistant text missing:\n{stdout}"
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

/// Serves three scripted chat-completion responses and returns request bodies.
fn serve(listener: TcpListener) -> Vec<String> {
    listener
        .set_nonblocking(true)
        .expect("listener should become non-blocking");
    let deadline = Instant::now() + SERVER_TIMEOUT;
    let mut bodies = Vec::new();

    while bodies.len() < 3 && Instant::now() < deadline {
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
        0 => vec![
            tool_call_chunk("call_denied", "read_file", "{\"path\":\"Cargo.toml\"}"),
            finish_chunk("tool_calls"),
        ],
        1 => vec![
            tool_call_chunk("call_echo", "echo_json", "{\"text\":\"hello\"}"),
            finish_chunk("tool_calls"),
        ],
        _ => vec![content_chunk("wasm tool completed"), finish_chunk("stop")],
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
