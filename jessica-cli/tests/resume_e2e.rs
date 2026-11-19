//! End-to-end acceptance for `--resume`.
//!
//! The test writes a session whose event log stops after one complete tool
//! batch, then runs the real CLI binary with `--resume`. The recorded batch must
//! be loaded from SQLite rather than re-executed: the mock provider receives
//! exactly one request, that request contains the recorded assistant tool call
//! and tool result, and the resumed run appends its own events to the same
//! session.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use jessica_core::trace::{TracePayload, TraceTurnMode};
use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId, ToolCall, ToolResult};
use jessica_memory::{EventStore, Timestamp};
use serde_json::json;

/// Overall budget for the child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock server to observe one request.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("jessica-cli must live inside the workspace")
        .to_path_buf()
}

/// Temporary directory removed when the test finishes.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-cli-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temporary directory should be created");
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[test]
fn resume_continues_from_the_recorded_checkpoint_without_replaying_tools() {
    let temp = TempDir::new("resume");
    let database = temp.path.join("events.sqlite");
    let session = SessionId::new("cli-resume-session");
    seed_interrupted_session(&database, &session);

    let config = temp.path.join("harness.toml");
    std::fs::write(
        &config,
        format!(
            "[budget]\nmax_rounds = 4\n\n[events]\npath = \"{}\"\n",
            database.display()
        ),
    )
    .expect("config should be written");

    let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
    let address = listener.local_addr().expect("mock server address");
    let server = thread::spawn(move || serve_once(listener));

    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace_root())
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .arg("--config")
        .arg(&config)
        .arg("--resume")
        .arg(session.as_str())
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
        1,
        "the resumed run must make exactly one model request, got {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        requests.len()
    );

    let request = &requests[0];
    assert!(
        request.contains("finish the resume test"),
        "the recorded user message is missing from the resumed request: {request}"
    );
    assert!(
        request.contains("call_1"),
        "the recorded tool call is missing from the resumed request: {request}"
    );
    assert!(
        request.contains("recorded tool output"),
        "the recorded tool result is missing from the resumed request: {request}"
    );
    assert!(
        stdout.contains("resumed done"),
        "the resumed turn's output is missing:\n{stdout}"
    );

    let store = EventStore::open(&database).expect("event store should reopen");
    let events = store.events(&session).expect("events should load");
    assert_eq!(
        events.iter().map(|event| event.seq()).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7],
        "the resumed run must append to the same session stream"
    );
    // The version-1 prefix already completed round one, so the resumed model
    // call continues at round two and records the full version-2 vocabulary.
    assert_eq!(events[3].event(), &AgentEvent::RoundStarted { round: 2 });
    assert_eq!(
        events[4].event(),
        &AgentEvent::ModelDelta {
            text: "resumed done".to_owned(),
        }
    );
    assert_eq!(
        events[5].event(),
        &AgentEvent::RoundFinished {
            round: 2,
            usage: None,
        }
    );
    assert_eq!(
        events[6].event(),
        &AgentEvent::SessionEnded {
            session_id: session.clone(),
            reason: SessionEndReason::Completed,
        }
    );

    let summaries = store.trace_turns(10).expect("turn summaries should load");
    assert_eq!(summaries.len(), 1, "the resume must record one turn");
    assert!(summaries[0].closed(), "the resumed turn must close");
    let trace = store
        .trace_turn(summaries[0].turn_id())
        .expect("turn trace should load")
        .expect("the summarized turn must exist");
    let traced = trace.events();
    assert_eq!(
        traced.len(),
        6,
        "turn start, four agent events, and turn end: {traced:?}"
    );
    assert!(
        matches!(
            &traced[0].event().payload,
            TracePayload::TurnStarted {
                mode: TraceTurnMode::Resumed,
                messages,
            } if messages.len() == 4
        ),
        "a resumed trace must start with the recovered conversation"
    );
    assert_eq!(
        traced[0].event().session_id.as_ref(),
        Some(&session),
        "the resumed turn must name the session it continues"
    );
    for entry in &traced[1..5] {
        assert_eq!(entry.event().attempt, Some(1));
        assert_eq!(entry.event().session_id.as_ref(), Some(&session));
        assert!(
            matches!(&entry.event().payload, TracePayload::Agent { .. }),
            "the recovered batch must not be replayed into the trace"
        );
    }
    assert!(matches!(
        &traced[5].event().payload,
        TracePayload::TurnEnded {
            reason: SessionEndReason::Completed
        }
    ));
    let turn_id = summaries[0].turn_id().to_string();
    drop(store);

    let verified = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace_root())
        .arg("--config")
        .arg(&config)
        .arg("trace")
        .arg("verify")
        .arg("--turn")
        .arg(&turn_id)
        .arg("--replay")
        .output()
        .expect("trace verification should run");
    let stdout = String::from_utf8_lossy(&verified.stdout);
    let stderr = String::from_utf8_lossy(&verified.stderr);
    assert!(
        verified.status.success(),
        "the real resumed trace must replay\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("replay: ok attempts=1 events=4"),
        "the resumed trace must replay all four recorded events:\n{stdout}"
    );
    assert!(
        stderr.is_empty(),
        "successful replay must not write to stderr:\n{stderr}"
    );
}

/// Writes a session that stopped after its first complete tool batch.
fn seed_interrupted_session(database: &Path, session: &SessionId) {
    let mut store = EventStore::open(database).expect("event store should open");
    store
        .record_session(
            session,
            &[
                Message::system("resume test system prompt"),
                Message::user("finish the resume test"),
            ],
            Timestamp::from_millis(1),
        )
        .expect("session metadata should be recorded");

    let call = ToolCall::new("call_1", "read_file", json!({"path": "Cargo.toml"}));
    for (index, event) in [
        AgentEvent::SessionStarted {
            session_id: session.clone(),
            messages: Vec::new(),
        },
        AgentEvent::ToolCallStarted { call },
        AgentEvent::ToolCallFinished {
            result: ToolResult::success("call_1", "recorded tool output"),
        },
    ]
    .into_iter()
    .enumerate()
    {
        store
            .append(session, &event, Timestamp::from_millis(2 + index as i64))
            .expect("seeded event should append");
    }
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

/// Serves one scripted text response and returns the request bodies observed.
fn serve_once(listener: TcpListener) -> Vec<String> {
    listener
        .set_nonblocking(true)
        .expect("listener should become non-blocking");
    let deadline = Instant::now() + SERVER_TIMEOUT;
    let mut bodies = Vec::new();

    while bodies.is_empty() && Instant::now() < deadline {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .expect("connection should become blocking");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("read timeout should be set");
                if let Some(body) = read_request(&mut stream) {
                    stream
                        .write_all(resume_response().as_bytes())
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

/// Returns the single SSE response used for the resumed turn.
fn resume_response() -> String {
    let payloads = [content_chunk("resumed done"), finish_chunk("stop")];
    let mut body = String::new();
    for payload in payloads {
        body.push_str("data: ");
        body.push_str(&payload);
        body.push_str("\n\n");
    }
    body.push_str("data: [DONE]\n\n");

    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn finish_chunk(reason: &str) -> String {
    json!({
        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]
    })
    .to_string()
}

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
