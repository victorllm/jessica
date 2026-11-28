//! End-to-end acceptance for the engine-owned catalog profile.
//!
//! The CLI keeps its TOML profile and descriptor reporting, but the engine
//! applies the profile to the turn catalog. A `readonly` turn must therefore
//! offer the model only read-only native tools even though the allowlist names
//! `write_file` and `exec`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Overall budget for the CLI child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock provider to observe its one request.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);

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
            "jessica-catalog-profile-{label}-{}-{unique}",
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
fn a_readonly_turn_never_offers_write_or_exec_tools_to_the_model() {
    let temp = TempDir::new("readonly");
    std::fs::write(temp.path().join("notes.txt"), "hello\n").expect("workspace file");
    std::fs::write(
        temp.path().join("harness.toml"),
        "[profile]\ntrace = \"off\"\n\
         [policy]\nallowed_tools = [\"read_file\", \"write_file\", \"exec\", \"bash\"]\n\
         [tools]\nprofile = \"readonly\"\n",
    )
    .expect("configuration should be written");

    let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
    let address = listener.local_addr().expect("mock server address");
    let server = thread::spawn(move || serve_once(listener));

    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(temp.path())
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .args([
            "--config",
            "harness.toml",
            "run the readonly acceptance turn",
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
    let request = server.join().expect("mock server should finish");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        status.success(),
        "CLI failed with {status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let request = request.expect("the provider must receive one request");
    let parsed: serde_json::Value =
        serde_json::from_str(&request).expect("provider request must be JSON");
    let mut offered: Vec<&str> = parsed["tools"]
        .as_array()
        .expect("the provider request must carry tools")
        .iter()
        .filter_map(|tool| tool["function"]["name"].as_str())
        .collect();
    offered.sort_unstable();

    assert_eq!(
        offered,
        ["read_file"],
        "a readonly turn must expose only read-only native tools"
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

/// Serves one text completion and returns the observed request body.
fn serve_once(listener: TcpListener) -> Option<String> {
    listener
        .set_nonblocking(true)
        .expect("listener should become non-blocking");
    let deadline = Instant::now() + SERVER_TIMEOUT;

    while Instant::now() < deadline {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .expect("connection should become blocking");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("read timeout should be set");
                let body = read_request(&mut stream)?;
                let response = sse(&[
                    serde_json::json!({
                        "choices": [{
                            "index": 0,
                            "delta": {"content": "readonly complete"},
                            "finish_reason": null
                        }]
                    })
                    .to_string(),
                    serde_json::json!({
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
                    })
                    .to_string(),
                ]);
                stream
                    .write_all(response.as_bytes())
                    .expect("response should be written");
                stream.flush().expect("response should be flushed");
                return Some(body);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("mock server accept failed: {error}"),
        }
    }
    None
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
