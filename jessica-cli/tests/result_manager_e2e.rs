//! End-to-end acceptance for the result manager and artifact pagination.
//!
//! Runs the real CLI against a scripted SSE server. The first model turn reads
//! a 100 KiB file; the CLI must feed back a bounded preview with a digest
//! instead of the full content. The second turn reads a page from that artifact
//! through `read_artifact`; the page must contain the requested bytes and
//! pagination cursor without creating another artifact.
//!
//! No network access is involved; the provider is a local listener.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Overall budget for the CLI child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock server to observe three requests.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);
/// Size of the file read by the first tool call.
const FILE_BYTES: usize = 100 * 1024;
/// Reader offset used by the pagination turn.
const PAGE_OFFSET: usize = 32 * 1024;
/// Reader page length.
const PAGE_LIMIT: usize = 4096;

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
            "jessica-result-manager-{label}-{}-{unique}",
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
fn a_large_read_file_is_managed_and_paginated_through_read_artifact() {
    let temp = TempDir::new("read-page");
    let pattern = "0123456789abcdef";
    let content = pattern.repeat(FILE_BYTES / pattern.len());
    assert_eq!(content.len(), FILE_BYTES);
    std::fs::write(temp.path().join("big.txt"), &content).expect("large file should be written");

    std::fs::write(
        temp.path().join("harness.toml"),
        "[profile]\ntrace = \"off\"\n\
         [policy]\nallowed_tools = [\"read_file\", \"read_artifact\"]\n\
         [tools]\n\
         inline_threshold_bytes = 32768\n\
         max_inline_bytes = 65536\n\
         max_artifact_bytes = 16777216\n\
         artifact_dir = \".jessica/artifacts\"\n\
         artifact_retention_days = 7\n\
         artifact_max_bytes = 268435456\n",
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
            "read and paginate the large file",
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
        "expected a tool turn, a pagination turn, and a final answer\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        !stdout.contains("BudgetExceeded"),
        "a managed result must not trip the result-size guard:\n{stdout}"
    );
    assert!(
        stdout.contains("artifact pagination complete"),
        "the turn must complete normally:\n{stdout}"
    );

    let preview = tool_content(&requests[1]);
    assert!(
        preview.starts_with("[result artifact]"),
        "the model must receive a managed preview: {preview}"
    );
    assert!(
        preview.contains("original_bytes: 102400"),
        "the preview must report the original size: {preview}"
    );
    assert!(
        preview.contains("truncated: false"),
        "the artifact stores the full result: {preview}"
    );
    assert!(
        preview.len() <= 64 * 1024,
        "the preview must respect max_inline_bytes: {}",
        preview.len()
    );
    assert!(
        !preview.contains(&content),
        "the full file content must not enter the model context"
    );
    let digest = digest_line(&preview);

    let page = tool_content(&requests[2]);
    let expected = &content[PAGE_OFFSET..PAGE_OFFSET + PAGE_LIMIT];
    assert!(
        page.contains(&format!("digest: {digest}")),
        "the page must identify its artifact: {page}"
    );
    assert!(
        page.contains(&format!("offset: {PAGE_OFFSET}")),
        "the page must report the requested offset: {page}"
    );
    assert!(
        page.contains(&format!("bytes: {PAGE_LIMIT}")),
        "the page must contain exactly one page: {page}"
    );
    assert!(
        page.contains(&format!("next_offset: {}", PAGE_OFFSET + PAGE_LIMIT)),
        "the page must return the next cursor: {page}"
    );
    assert!(
        page.contains(expected),
        "the page must contain the bytes at the requested offset"
    );
    assert!(
        page.len() < PAGE_LIMIT + 1024,
        "the page envelope must stay bounded: {}",
        page.len()
    );

    let artifacts = artifact_files(temp.path());
    assert_eq!(
        artifacts.len(),
        1,
        "read_artifact must not create another artifact: {artifacts:?}"
    );
    assert_eq!(
        std::fs::read(&artifacts[0]).expect("artifact must read"),
        content.as_bytes()
    );
    assert_eq!(
        std::fs::metadata(&artifacts[0])
            .expect("artifact metadata must read")
            .permissions()
            .mode()
            & 0o777,
        0o600,
        "artifact files must be private"
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
                    let response = response_for(bodies.len(), &body);
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
fn response_for(index: usize, request: &str) -> String {
    let payloads = match index {
        0 => vec![
            tool_call_chunk(
                "call_read",
                "read_file",
                &serde_json::json!({ "path": "big.txt" }).to_string(),
            ),
            finish_chunk("tool_calls"),
        ],
        1 => {
            let content = tool_content(request);
            let digest = digest_line(&content);
            vec![
                tool_call_chunk(
                    "call_page",
                    "read_artifact",
                    &serde_json::json!({
                        "digest": digest,
                        "offset": PAGE_OFFSET,
                        "limit": PAGE_LIMIT,
                    })
                    .to_string(),
                ),
                finish_chunk("tool_calls"),
            ]
        }
        _ => vec![
            content_chunk("artifact pagination complete"),
            finish_chunk("stop"),
        ],
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

/// Extracts the digest header from a managed result.
fn digest_line(content: &str) -> String {
    content
        .lines()
        .find_map(|line| line.strip_prefix("digest: "))
        .expect("managed content must carry a digest")
        .to_owned()
}

/// Returns every non-sidecar artifact file under the workspace.
fn artifact_files(root: &Path) -> Vec<PathBuf> {
    let artifacts = root.join(".jessica/artifacts");
    let mut files = Vec::new();
    let Ok(shards) = std::fs::read_dir(&artifacts) else {
        return files;
    };
    for shard in shards.flatten() {
        let Ok(entries) = std::fs::read_dir(shard.path()) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.len() == 64
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    files
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
