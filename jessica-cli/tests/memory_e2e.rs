//! End-to-end acceptance for layered memory in the real CLI.
//!
//! The tests drive the compiled binary against a scripted SSE server and a
//! temporary workspace. The first scenario runs twice: the first run must write
//! a completed turn, and the second run must inject that turn between the
//! system prompt and the user message. The remaining scenarios pin the
//! determinism boundaries: `--resume` must not recall or inject, `replay` must
//! not touch the memory database, disabled memory must do nothing, and an open
//! failure must degrade without changing the exit code.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId, ToolCall, ToolResult};
use jessica_memory::{
    EventStore, MemoryLog, NewMemoryEntry, SqliteMemoryStore, Timestamp, WorkspaceScope,
};
use serde_json::{Value, json};

/// Overall budget for one child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock server to observe one request.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);

/// Title rendered by `MemoryGuide`.
const MEMORY_TITLE: &str = "[记忆参考";

/// Temporary directory removed when the test finishes.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-cli-memory-e2e-{label}-{}-{unique}",
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
fn completed_turns_are_written_and_injected_into_a_new_session() {
    let temp = TempDir::new("two-runs");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[budget]\nmax_rounds = 4\n\n\
         [memory]\n\
         enabled = true\n\
         path = \".jessica/memory.db\"\n\
         recall_limit = 5\n\
         max_inject_bytes = 4096\n\
         embedder = \"deterministic\"\n\
         write_policy = \"completed\"\n",
    );

    let (first, first_requests) = run_turn(
        &temp.path,
        &config,
        "Remember this fact: the launch code is ORANGE-7.",
        "The launch code is ORANGE-7.",
    );
    assert_success(&first);
    let first_stdout = String::from_utf8_lossy(&first.stdout);
    assert!(
        first_stdout.contains("The launch code is ORANGE-7."),
        "the scripted first answer is missing:\n{first_stdout}"
    );
    assert_eq!(first_requests.len(), 1);
    assert!(
        !first_requests[0].contains(MEMORY_TITLE),
        "the first session has no prior memory to inject: {}",
        first_requests[0]
    );

    let database = temp.path.join(".jessica/memory.db");
    assert!(
        database.is_file(),
        "the completed turn must create the database"
    );
    let scope = workspace_scope(&database, &temp.path);
    let log = MemoryLog::open(&database).expect("memory database should reopen");
    let records = log
        .search_keyword(&scope, "ORANGE", 10)
        .expect("keyword search should succeed");
    assert_eq!(
        records.len(),
        1,
        "exactly one completed turn must be stored"
    );
    let record = &records[0];
    assert_eq!(record.metadata()["kind"], "turn");
    assert_eq!(record.metadata()["truncated"], false);
    assert_eq!(record.metadata()["tool_call_count"], 0);
    assert_eq!(record.metadata()["turn_index"], 0);
    assert!(record.content().contains("ORANGE-7"));
    assert!(record.content().contains("Remember this fact"));
    drop(log);

    let (second, second_requests) = run_turn(
        &temp.path,
        &config,
        "What is the launch code?",
        "It is ORANGE-7.",
    );
    assert_success(&second);
    let second_stdout = String::from_utf8_lossy(&second.stdout);
    assert!(
        second_stdout.contains("It is ORANGE-7."),
        "the second answer must come from the scripted model:\n{second_stdout}"
    );
    assert_eq!(second_requests.len(), 1);

    let request = &second_requests[0];
    assert!(
        request.contains(MEMORY_TITLE),
        "the second request must carry recalled memory: {request}"
    );
    assert!(
        request.contains("ORANGE-7"),
        "the recalled fact must reach the provider: {request}"
    );

    let messages = request_messages(request);
    assert_eq!(
        messages.len(),
        3,
        "expected system prompt, memory message, and user message: {request}"
    );
    assert_eq!(messages[0]["role"], "system");
    assert!(
        messages[0]["content"]
            .as_str()
            .expect("system content")
            .contains("You are Jessica")
    );
    assert_eq!(messages[1]["role"], "system");
    let memory_content = messages[1]["content"].as_str().expect("memory content");
    assert!(memory_content.contains(MEMORY_TITLE));
    assert!(memory_content.contains("ORANGE-7"));
    assert_eq!(messages[2]["role"], "user");
    assert_eq!(messages[2]["content"], "What is the launch code?");
}

#[test]
fn resume_never_recalls_or_injects_but_still_records_the_completed_turn() {
    let temp = TempDir::new("resume");
    let events = temp.path.join("events.sqlite");
    let memory = temp.path.join("memory.db");
    let session = SessionId::new("cli-resume-memory-session");
    seed_interrupted_session(&events, &session);
    seed_short_term_memory(
        &memory,
        &temp.path,
        &session,
        "Remember: the launch code is ORANGE-7.",
    );

    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[budget]\nmax_rounds = 4\n\n\
         [events]\npath = \"events.sqlite\"\n\n\
         [memory]\nenabled = true\npath = \"memory.db\"\n",
    );

    let (output, requests) = run_cli(
        &temp.path,
        &config,
        &["--resume", session.as_str()],
        "resumed done",
    );
    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("resumed done"),
        "the resumed answer is missing:\n{stdout}"
    );
    assert_eq!(
        requests.len(),
        1,
        "resume must make exactly one model request"
    );
    assert!(
        !requests[0].contains(MEMORY_TITLE),
        "resume must never re-inject memory: {}",
        requests[0]
    );
    assert!(
        !requests[0].contains("ORANGE-7"),
        "the pre-seeded memory must not reach the resumed request: {}",
        requests[0]
    );

    let scope = workspace_scope(&memory, &temp.path);
    let log = MemoryLog::open(&memory).expect("memory database should reopen");
    let records = log
        .search_keyword(&scope, "resumed", 10)
        .expect("keyword search should succeed");
    assert_eq!(
        records.len(),
        1,
        "the resumed Completed turn must be written exactly once"
    );
    assert_eq!(records[0].metadata()["kind"], "turn");
    assert!(records[0].content().contains("finish the resume test"));
    assert!(records[0].content().contains("resumed done"));
}

#[test]
fn replay_never_opens_or_creates_the_memory_database() {
    let temp = TempDir::new("replay");
    let events = temp.path.join("events.sqlite");
    let session = SessionId::new("cli-replay-memory-session");
    seed_completed_session(&events, &session);

    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[events]\npath = \"events.sqlite\"\n\n\
         [memory]\nenabled = true\npath = \".jessica/memory.db\"\n",
    );

    let output = run_replay(&temp.path, &config, &session, "out.jsonl");
    assert_success(&output);
    assert!(String::from_utf8_lossy(&output.stdout).contains("wrote"));

    assert!(
        !temp.path.join(".jessica").exists(),
        "replay must not create the memory directory or database"
    );
    assert!(!temp.path.join(".jessica/memory.db").exists());
}

#[test]
fn disabled_memory_does_not_inject_or_create_a_database() {
    let temp = TempDir::new("disabled");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[profile]\ntrace = \"off\"\n\
         [memory]\nenabled = false\npath = \".jessica/memory.db\"\n",
    );

    let (output, requests) = run_turn(&temp.path, &config, "hello", "plain answer");
    assert_success(&output);
    assert_eq!(requests.len(), 1);
    assert!(
        !requests[0].contains(MEMORY_TITLE),
        "disabled memory must not inject context: {}",
        requests[0]
    );
    let messages = request_messages(&requests[0]);
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["role"], "system");
    assert_eq!(messages[1]["role"], "user");
    assert!(
        !temp.path.join(".jessica").exists(),
        "disabled memory must not create the database directory"
    );
}

#[test]
fn memory_open_failure_degrades_without_changing_the_exit_code() {
    let temp = TempDir::new("open-failure");
    let blocker = temp.path.join(".jessica");
    std::fs::write(&blocker, b"not a directory").expect("blocking file should be written");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[profile]\ntrace = \"off\"\n\
         [memory]\nenabled = true\npath = \".jessica/memory.db\"\n",
    );

    let (output, requests) = run_turn(&temp.path, &config, "hello", "still works");
    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("still works"), "stdout:\n{stdout}");
    assert!(
        stderr.contains("[warn] memory disabled"),
        "an open failure must be warned about:\n{stderr}"
    );
    assert_eq!(requests.len(), 1);
    assert!(!requests[0].contains(MEMORY_TITLE));
    assert!(blocker.is_file(), "the invalid path must be left untouched");
}

/// Writes a TOML configuration file.
fn write_config(path: &Path, body: &str) {
    std::fs::write(path, body).expect("configuration should be written");
}

/// Returns the fixed workspace scope used by the CLI for `workspace`.
fn workspace_scope(database: &Path, workspace: &Path) -> WorkspaceScope {
    let canonical = std::fs::canonicalize(workspace).expect("workspace should canonicalize");
    let store =
        SqliteMemoryStore::open(database, canonical, None).expect("memory store should open");
    store.workspace_scope().clone()
}

/// Seeds one short-term memory entry for `session`.
fn seed_short_term_memory(database: &Path, workspace: &Path, session: &SessionId, content: &str) {
    let scope = workspace_scope(database, workspace);
    let log = MemoryLog::open(database).expect("memory log should open");
    let entry = NewMemoryEntry::new(session.clone(), scope, content, Timestamp::from_millis(1))
        .expect("memory entry should construct");
    log.append(entry).expect("memory entry should append");
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

/// Writes a completed session with one tool round and one final text round.
fn seed_completed_session(database: &Path, session: &SessionId) {
    let mut store = EventStore::open(database).expect("event store should open");
    store
        .record_session(
            session,
            &[
                Message::system("replay test system prompt"),
                Message::user("replay this session"),
            ],
            Timestamp::from_millis(1),
        )
        .expect("session metadata should be recorded");

    let call = ToolCall::new("call_1", "read_file", json!({"path": "Cargo.toml"}));
    let events = [
        AgentEvent::SessionStarted {
            session_id: session.clone(),
            messages: Vec::new(),
        },
        AgentEvent::ModelDelta {
            text: "reading the manifest".to_owned(),
        },
        AgentEvent::ToolCallStarted { call },
        AgentEvent::ToolCallFinished {
            result: ToolResult::success("call_1", "recorded tool output"),
        },
        AgentEvent::ModelDelta {
            text: "done".to_owned(),
        },
        AgentEvent::SessionEnded {
            session_id: session.clone(),
            reason: SessionEndReason::Completed,
        },
    ];
    for (index, event) in events.iter().enumerate() {
        store
            .append(session, event, Timestamp::from_millis(2 + index as i64))
            .expect("seeded event should append");
    }
}

/// Runs one prompt turn against a scripted text response.
fn run_turn(
    workspace: &Path,
    config: &Path,
    prompt: &str,
    response: &str,
) -> (Output, Vec<String>) {
    run_cli(workspace, config, &[prompt], response)
}

/// Runs the CLI with `extra_args` against a scripted text response.
fn run_cli(
    workspace: &Path,
    config: &Path,
    extra_args: &[&str],
    response: &str,
) -> (Output, Vec<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
    let address = listener.local_addr().expect("mock server address");
    let body = text_response(response);
    let server = thread::spawn(move || serve_once(listener, body));

    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .env_remove("JESSICA_PROVIDER")
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .arg("--config")
        .arg(config)
        .args(extra_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should start");

    let status = wait_with_timeout(&mut child);
    let mut output = child
        .wait_with_output()
        .expect("CLI output should be collected");
    output.status = status;
    let requests = server.join().expect("mock server should finish");
    (output, requests)
}

/// Runs the real CLI in replay mode without any provider configuration.
fn run_replay(workspace: &Path, config: &Path, session: &SessionId, output_path: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("OPENAI_MODEL")
        .env_remove("JESSICA_PROVIDER")
        .arg("--config")
        .arg(config)
        .arg("replay")
        .arg("--session")
        .arg(session.as_str())
        .arg("--out")
        .arg(output_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should start");

    let status = wait_with_timeout(&mut child);
    let mut output = child
        .wait_with_output()
        .expect("CLI output should be collected");
    output.status = status;
    output
}

/// Asserts that a child exited successfully, printing its output on failure.
fn assert_success(output: &Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "CLI failed with {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
}

/// Parses the `messages` array from an OpenAI request body.
fn request_messages(body: &str) -> Vec<Value> {
    let parsed: Value = serde_json::from_str(body).expect("request body should be JSON");
    parsed["messages"]
        .as_array()
        .expect("request body should contain messages")
        .clone()
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

/// Serves one scripted response and returns the request body observed.
fn serve_once(listener: TcpListener, response: String) -> Vec<String> {
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

/// Returns one SSE response carrying `content` and a stop finish reason.
fn text_response(content: &str) -> String {
    let payloads = [content_chunk(content), finish_chunk("stop")];
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
