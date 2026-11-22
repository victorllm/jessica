//! End-to-end acceptance for delegated child agents.
//!
//! Every scenario drives the compiled CLI against a scripted SSE server and a
//! temporary workspace. The tests pin the observable parent/child boundary:
//! child requests contain only the fixed system prompt and the delegated task,
//! child tools are narrowed to the requested native subset, child events never
//! reach the parent legacy event stream but the full child trajectory is
//! recorded as nested trace evidence, child failures do not change the parent
//! exit code, child token usage does change the parent budget, replay and resume
//! never execute a child agent, and a session interrupted inside `delegate_task`
//! is rejected rather than replayed.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use jessica_core::trace::TracePayload;
use jessica_core::types::{
    AgentEvent, BudgetKind, Message, SessionEndReason, SessionId, ToolCall, ToolResult,
};
use jessica_memory::{EventStore, Timestamp};
use jessica_runtime::{
    CHILD_AGENT_SYSTEM_PROMPT, DELEGATE_TASK_TOOL_NAME, ParentCapabilities, child_session_hash,
    narrow_tools,
};
use serde_json::{Value, json};

/// Overall budget for one CLI child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock server to observe requests.
const SERVER_TIMEOUT: Duration = Duration::from_secs(30);
/// Native tools the child factory and parent capabilities know about.
const NATIVE_CHILD_TOOLS: &[&str] = &["bash", "read_file", "write_file"];
/// Parent allowlist used by most scenarios.
const ALL_TOOLS: &[&str] = &["bash", "read_file", "write_file", "delegate_task"];

/// Temporary directory removed when the test finishes.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-cli-delegation-e2e-{label}-{}-{unique}",
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

/// Delegation settings used to render one TOML configuration.
struct Settings<'a> {
    /// Parent allowlist.
    policy: &'a [&'a str],
    /// Parent token budget.
    max_tokens: u64,
    /// Parent per-tool timeout.
    tool_timeout_ms: u64,
    /// Child model-round budget.
    max_rounds: u32,
    /// Child wall-clock limit.
    timeout_ms: u64,
    /// Maximum serialized delegation result size.
    max_result_bytes: usize,
    /// Maximum child tool calls.
    max_child_tool_calls: u32,
    /// Optional child `bash` deadline.
    bash_timeout_ms: Option<u64>,
    /// Optional result size below which a child result stays inline.
    inline_threshold_bytes: Option<usize>,
    /// Optional hard cap on a managed child result.
    max_inline_bytes: Option<usize>,
    /// Optional maximum bytes stored for one child artifact.
    max_artifact_bytes: Option<usize>,
}

impl Default for Settings<'_> {
    fn default() -> Self {
        Self {
            policy: ALL_TOOLS,
            max_tokens: 1_000_000,
            tool_timeout_ms: 10_000,
            max_rounds: 4,
            timeout_ms: 8_000,
            max_result_bytes: 32_768,
            max_child_tool_calls: 20,
            bash_timeout_ms: None,
            inline_threshold_bytes: None,
            max_inline_bytes: None,
            max_artifact_bytes: None,
        }
    }
}

/// Renders a configuration with delegation enabled.
fn enabled_config(settings: &Settings<'_>) -> String {
    let allowed = settings
        .policy
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let mut tools = String::new();
    if settings.inline_threshold_bytes.is_some()
        || settings.max_inline_bytes.is_some()
        || settings.max_artifact_bytes.is_some()
    {
        tools.push_str("[tools]\n");
        if let Some(bytes) = settings.inline_threshold_bytes {
            tools.push_str(&format!("inline_threshold_bytes = {bytes}\n"));
        }
        if let Some(bytes) = settings.max_inline_bytes {
            tools.push_str(&format!("max_inline_bytes = {bytes}\n"));
        }
        if let Some(bytes) = settings.max_artifact_bytes {
            tools.push_str(&format!("max_artifact_bytes = {bytes}\n"));
        }
        tools.push('\n');
    }
    let bash_timeout = settings
        .bash_timeout_ms
        .map_or_else(String::new, |milliseconds| {
            format!("timeout_ms = {milliseconds}\n")
        });
    format!(
        "[events]\npath = \"events.sqlite\"\n\n\
         [budget]\nmax_tokens = {}\ntool_timeout_ms = {}\nmax_rounds = 16\n\n\
         [policy]\nallowed_tools = [{allowed}]\n\n\
         [delegation]\nenabled = true\nmax_rounds = {}\ntimeout_ms = {}\n\
         max_task_bytes = 8192\nmax_result_bytes = {}\nmax_child_tool_calls = {}\n\n\
         {tools}[tools.bash]\nenabled = true\n{bash_timeout}",
        settings.max_tokens,
        settings.tool_timeout_ms,
        settings.max_rounds,
        settings.timeout_ms,
        settings.max_result_bytes,
        settings.max_child_tool_calls,
    )
}

/// Renders a configuration with delegation disabled.
fn disabled_config() -> String {
    "[events]\npath = \"events.sqlite\"\n\n\
     [policy]\nallowed_tools = [\"bash\", \"read_file\", \"write_file\"]\n\n\
     [tools.bash]\nenabled = true\n"
        .to_owned()
}

fn write_config(path: &Path, body: &str) {
    std::fs::write(path, body).expect("configuration should be written");
}

/// One scripted server action.
enum Plan {
    /// Answer immediately with a complete SSE response.
    Sse(String),
    /// Wait before answering; used to trigger the delegation timeout.
    Hang {
        /// Delay before writing the response.
        delay: Duration,
        /// Response written after the delay.
        response: String,
    },
}

/// Scripted HTTP server that records every request body.
struct ScriptedServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<Vec<String>>,
}

impl ScriptedServer {
    fn start(plans: Vec<Plan>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
        let address = listener.local_addr().expect("mock server address");
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = thread::spawn(move || serve(listener, flag, plans));
        Self {
            address,
            stop,
            handle,
        }
    }

    fn address(&self) -> SocketAddr {
        self.address
    }

    fn finish(self) -> Vec<String> {
        self.stop.store(true, Ordering::SeqCst);
        self.handle.join().expect("mock server should finish")
    }
}

/// Accepts requests until stopped, answering from the plan queue.
fn serve(listener: TcpListener, stop: Arc<AtomicBool>, plans: Vec<Plan>) -> Vec<String> {
    listener
        .set_nonblocking(true)
        .expect("listener should become non-blocking");
    let deadline = Instant::now() + SERVER_TIMEOUT;
    let mut plans: VecDeque<Plan> = plans.into();
    let mut bodies = Vec::new();

    while !stop.load(Ordering::SeqCst) && Instant::now() < deadline {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .expect("connection should become blocking");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("read timeout should be set");
                if let Some(body) = read_request(&mut stream) {
                    bodies.push(body);
                    let response = match plans.pop_front() {
                        Some(Plan::Sse(response)) => response,
                        Some(Plan::Hang { delay, response }) => {
                            thread::sleep(delay);
                            response
                        }
                        None => "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\
                             Connection: close\r\n\r\n"
                            .to_owned(),
                    };
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
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

/// One completed CLI turn plus all provider requests it made.
struct TurnRun {
    output: Output,
    requests: Vec<String>,
    pid: u32,
}

impl TurnRun {
    /// Session id allocated by the CLI for a fresh one-shot turn.
    fn session(&self, counter: u64) -> SessionId {
        SessionId::new(format!("cli-{}-{counter}", self.pid))
    }

    /// Provider requests addressed to a delegated child.
    fn child_requests(&self) -> Vec<&str> {
        self.requests
            .iter()
            .map(String::as_str)
            .filter(|body| body.contains(CHILD_AGENT_SYSTEM_PROMPT))
            .collect()
    }

    /// Provider requests addressed to the parent agent.
    fn parent_requests(&self) -> Vec<&str> {
        self.requests
            .iter()
            .map(String::as_str)
            .filter(|body| !body.contains(CHILD_AGENT_SYSTEM_PROMPT))
            .collect()
    }
}

/// Runs one CLI turn against a scripted server.
fn run_turn(workspace: &Path, config: &Path, prompt: &str, plans: Vec<Plan>) -> TurnRun {
    let server = ScriptedServer::start(plans);
    let address = server.address();
    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .env_remove("JESSICA_PROVIDER")
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .arg("--config")
        .arg(config)
        .arg(prompt)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should start");

    let pid = child.id();
    let status = wait_with_timeout(&mut child);
    let mut output = child
        .wait_with_output()
        .expect("CLI output should be collected");
    output.status = status;
    let requests = server.finish();
    TurnRun {
        output,
        requests,
        pid,
    }
}

/// Runs `--resume` against a server that must observe no requests.
fn run_resume(workspace: &Path, config: &Path, session: &SessionId) -> (Output, Vec<String>) {
    let server = ScriptedServer::start(Vec::new());
    let address = server.address();
    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .env_remove("JESSICA_PROVIDER")
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .arg("--config")
        .arg(config)
        .arg("--resume")
        .arg(session.as_str())
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
    let requests = server.finish();
    (output, requests)
}

/// Runs replay without any provider configuration.
fn run_replay(workspace: &Path, config: &Path, session: &SessionId, output_path: &Path) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .env_remove("JESSICA_PROVIDER")
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("OPENAI_MODEL")
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

/// Runs one `jessica trace` subcommand without provider configuration.
fn run_trace_command(workspace: &Path, config: &Path, arguments: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .env_remove("JESSICA_PROVIDER")
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("OPENAI_MODEL")
        .arg("--config")
        .arg(config)
        .arg("trace")
        .args(arguments)
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

/// Returns the tool names advertised in a request body.
fn request_tool_names(body: &str) -> Vec<String> {
    let parsed: Value = serde_json::from_str(body).expect("request body should be JSON");
    parsed["tools"]
        .as_array()
        .map(|tools| {
            tools
                .iter()
                .filter_map(|tool| tool["function"]["name"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn assert_tool_names(body: &str, expected: &[&str]) {
    let actual = request_tool_names(body);
    let actual: Vec<&str> = actual.iter().map(String::as_str).collect();
    assert_eq!(actual, expected);
}

/// Asserts the initial child request contains exactly the fixed prompt and task.
fn assert_child_request(body: &str, expected_task: &str, expected_tools: &[&str]) {
    let messages = request_messages(body);
    assert_eq!(
        messages.len(),
        2,
        "a child starts with exactly two messages: {body}"
    );
    assert_eq!(messages[0]["role"], "system");
    assert_eq!(messages[0]["content"], CHILD_AGENT_SYSTEM_PROMPT);
    assert_eq!(messages[1]["role"], "user");
    assert_eq!(messages[1]["content"], expected_task);
    assert_tool_names(body, expected_tools);
}

/// Parses every tool message content as the delegation result JSON.
fn tool_result_jsons(body: &str) -> Vec<Value> {
    request_messages(body)
        .iter()
        .filter(|message| message["role"] == "tool")
        .map(|message| {
            let content = message["content"]
                .as_str()
                .expect("tool message content should be a string");
            let content = content.strip_prefix("tool error: ").unwrap_or(content);
            serde_json::from_str(content).expect("delegation result should be JSON")
        })
        .collect()
}

/// Reads all persisted events for one session.
fn read_events(database: &Path, session: &SessionId) -> Vec<AgentEvent> {
    let store = EventStore::open(database).expect("event store should open");
    store
        .events(session)
        .expect("events should be readable")
        .into_iter()
        .map(|stored| stored.event().clone())
        .collect()
}

/// Asserts that no metadata or events were persisted for `session`.
fn assert_session_absent(database: &Path, session: &SessionId) {
    let store = EventStore::open(database).expect("event store should open");
    assert!(
        store
            .session_messages(session)
            .expect("session lookup should succeed")
            .is_none(),
        "child metadata must not be persisted"
    );
    assert!(
        store
            .events(session)
            .expect("events should be readable")
            .is_empty(),
        "child events must not be persisted"
    );
}

/// Computes the trace-only child session for a task and tool request.
fn child_trace(task: &str, tools: &[&str], allowed: &[&str]) -> (String, SessionId) {
    let parent = ParentCapabilities::new(
        allowed.iter().map(|name| (*name).to_owned()),
        NATIVE_CHILD_TOOLS.iter().map(|name| (*name).to_owned()),
    );
    let requested: Vec<String> = tools.iter().map(|name| (*name).to_owned()).collect();
    let narrowed = narrow_tools(&parent, &requested).expect("child tools should narrow");
    let hash = child_session_hash(task, &narrowed);
    (hash.clone(), SessionId::new(hash))
}

/// Returns delegate starts and their matching results in event order.
fn delegate_events(events: &[AgentEvent]) -> (Vec<ToolCall>, Vec<ToolResult>) {
    let calls: Vec<ToolCall> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolCallStarted { call } if call.name == DELEGATE_TASK_TOOL_NAME => {
                Some(call.clone())
            }
            _ => None,
        })
        .collect();
    let ids: Vec<&str> = calls.iter().map(|call| call.id.as_str()).collect();
    let results = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolCallFinished { result }
                if ids.contains(&result.tool_call_id.as_str()) =>
            {
                Some(result.clone())
            }
            _ => None,
        })
        .collect();
    (calls, results)
}

#[test]
fn normal_delegation_keeps_child_events_out_of_the_parent_store() {
    let temp = TempDir::new("normal");
    std::fs::write(temp.path.join("NOTE.txt"), "ORANGE-NOTE").expect("note should be written");
    let config = temp.path.join("harness.toml");
    write_config(&config, &enabled_config(&Settings::default()));

    let run = run_turn(
        &temp.path,
        &config,
        "Please delegate. PARENT-SECRET-MARKER",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "read the note", "tools": ["read_file"]}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "child_call",
                "read_file",
                json!({"path": "NOTE.txt"}),
            )])),
            Plan::Sse(text_response("child answer: ORANGE-NOTE")),
            Plan::Sse(text_response("parent done")),
        ],
    );

    assert_success(&run.output);
    assert!(
        String::from_utf8_lossy(&run.output.stdout).contains("parent done"),
        "the parent answer must be printed"
    );
    assert_eq!(
        run.requests.len(),
        4,
        "parent, child tool, child answer, parent"
    );

    let children = run.child_requests();
    assert_eq!(children.len(), 2);
    assert_child_request(children[0], "read the note", &["read_file"]);
    assert!(
        !children[0].contains("PARENT-SECRET-MARKER"),
        "parent history must not leak into the child: {}",
        children[0]
    );
    let child_follow_up = request_messages(children[1]);
    assert_eq!(child_follow_up.len(), 4);
    assert_eq!(child_follow_up[3]["role"], "tool");
    assert_eq!(child_follow_up[3]["content"], "1\tORANGE-NOTE\n");

    let parent = run.parent_requests();
    assert_eq!(parent.len(), 2);
    assert_tool_names(
        parent[0],
        &["bash", "delegate_task", "read_file", "write_file"],
    );
    let results = tool_result_jsons(parent[1]);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["status"], "ok");
    assert_eq!(results[0]["answer"], "child answer: ORANGE-NOTE");
    assert_eq!(results[0]["rounds"], 2);
    assert_eq!(results[0]["tool_calls"], 1);

    let (hash, child_session) = child_trace("read the note", &["read_file"], ALL_TOOLS);
    assert_eq!(results[0]["child_session_hash"], hash);

    let database = temp.path.join("events.sqlite");
    let events = read_events(&database, &run.session(0));
    let (calls, delegate_results) = delegate_events(&events);
    assert_eq!(calls.len(), 1, "exactly one delegate call was recorded");
    assert_eq!(delegate_results.len(), 1);
    assert_eq!(calls[0].arguments["task"], "read the note");
    assert_eq!(delegate_results[0].tool_call_id, calls[0].id);
    assert!(!delegate_results[0].is_error);
    assert!(
        !events.iter().any(
            |event| matches!(event, AgentEvent::ToolCallStarted { call } if call.name == "read_file")
        ),
        "child tool events must not enter the parent stream"
    );
    assert_session_absent(&database, &child_session);
}

#[test]
fn delegated_child_trajectory_is_verifiable_trace_evidence() {
    let temp = TempDir::new("trace-evidence");
    std::fs::write(temp.path.join("NOTE.txt"), "ORANGE-NOTE").expect("note should be written");
    let config = temp.path.join("harness.toml");
    write_config(&config, &enabled_config(&Settings::default()));

    let run = run_turn(
        &temp.path,
        &config,
        "Please delegate.",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "read the note", "tools": ["read_file"]}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "child_call",
                "read_file",
                json!({"path": "NOTE.txt"}),
            )])),
            Plan::Sse(text_response("child answer: ORANGE-NOTE")),
            Plan::Sse(text_response("parent done")),
        ],
    );
    assert_success(&run.output);

    let database = temp.path.join("events.sqlite");
    let store = EventStore::open(&database).expect("event store should open");
    let summaries = store.trace_turns(10).expect("trace turns should list");
    assert_eq!(summaries.len(), 1, "the turn must produce one trace");
    let turn = summaries[0].turn_id().clone();
    let trace = store
        .trace_turn(&turn)
        .expect("trace should read")
        .expect("trace should exist");
    assert!(trace.closed(), "the delegation turn must close");
    let entries = trace.events();

    let (_, child_session) = child_trace("read the note", &["read_file"], ALL_TOOLS);
    let child_indices: Vec<usize> = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| matches!(&entry.event().payload, TracePayload::ChildAgent { .. }))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(
        child_indices.len(),
        9,
        "the whole child transcript is evidence"
    );
    for index in &child_indices {
        let envelope = entries[*index].event();
        assert_eq!(envelope.attempt, Some(1));
        assert_eq!(envelope.session_id.as_ref(), Some(&child_session));
        match &envelope.payload {
            TracePayload::ChildAgent { call_id, .. } => {
                assert_eq!(call_id, "call_delegate");
            }
            other => panic!("expected child evidence at entry {index}, found {other:?}"),
        }
    }

    let child_events: Vec<&AgentEvent> = child_indices
        .iter()
        .map(|index| match &entries[*index].event().payload {
            TracePayload::ChildAgent { event, .. } => event,
            other => panic!("expected child evidence at entry {index}, found {other:?}"),
        })
        .collect();
    match &child_events[0] {
        AgentEvent::SessionStarted { messages, .. } => {
            assert_eq!(
                messages,
                &vec![
                    Message::system(CHILD_AGENT_SYSTEM_PROMPT),
                    Message::user("read the note"),
                ],
                "child evidence records the isolated bootstrap"
            );
        }
        other => panic!("expected the child session start, found {other:?}"),
    }
    assert!(
        child_events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolCallStarted { call } if call.id == "child_call" && call.name == "read_file"
        )),
        "the child read_file call must be visible in evidence: {child_events:?}"
    );
    assert!(matches!(
        child_events.last(),
        Some(AgentEvent::SessionEnded {
            reason: SessionEndReason::Completed,
            ..
        })
    ));

    let child_start = entries
        .iter()
        .position(|entry| {
            matches!(
                &entry.event().payload,
                TracePayload::Agent {
                    event: AgentEvent::ChildSessionStarted { child_session_id, call_id }
                } if child_session_id == &child_session && call_id == "call_delegate"
            )
        })
        .expect("the parent must bracket the child start");
    let child_end = entries
        .iter()
        .position(|entry| {
            matches!(
                &entry.event().payload,
                TracePayload::Agent {
                    event: AgentEvent::ChildSessionEnded { child_session_id, call_id, .. }
                } if child_session_id == &child_session && call_id == "call_delegate"
            )
        })
        .expect("the parent must bracket the child end");
    assert!(
        child_start < child_indices[0],
        "child evidence must follow the parent start"
    );
    assert!(
        *child_indices.last().unwrap() < child_end,
        "child evidence must precede the parent end"
    );
    match &entries[child_end].event().payload {
        TracePayload::Agent {
            event:
                AgentEvent::ChildSessionEnded {
                    reason,
                    rounds,
                    tool_calls,
                    tool_errors,
                    ..
                },
        } => {
            assert_eq!(*reason, SessionEndReason::Completed);
            assert_eq!((*rounds, *tool_calls, *tool_errors), (2, 1, 0));
        }
        other => panic!("expected the parent child end, found {other:?}"),
    }
    drop(store);

    assert_session_absent(&database, &child_session);
    let turn = turn.to_string();
    let verified = run_trace_command(&temp.path, &config, &["verify", "--turn", turn.as_str()]);
    assert!(
        verified.status.success(),
        "child evidence must pass verification\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&verified.stdout),
        String::from_utf8_lossy(&verified.stderr)
    );
    let replayed = run_trace_command(
        &temp.path,
        &config,
        &["verify", "--turn", turn.as_str(), "--replay"],
    );
    assert!(
        replayed.status.success(),
        "replay must ignore child evidence without diverging\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&replayed.stdout),
        String::from_utf8_lossy(&replayed.stderr)
    );

    let bundle = temp.path.join("child-evidence.json");
    let bundle_path = bundle.to_str().expect("bundle path must be UTF-8");
    let exported = run_trace_command(
        &temp.path,
        &config,
        &["export", "--turn", turn.as_str(), "--out", bundle_path],
    );
    assert!(
        exported.status.success(),
        "child evidence must export\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&exported.stdout),
        String::from_utf8_lossy(&exported.stderr)
    );
    let verified_bundle = run_trace_command(
        &temp.path,
        &config,
        &["verify-bundle", "--bundle", bundle_path, "--json"],
    );
    assert!(
        verified_bundle.status.success(),
        "the exported bundle must verify\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&verified_bundle.stdout),
        String::from_utf8_lossy(&verified_bundle.stderr)
    );
    let report: Value =
        serde_json::from_slice(&verified_bundle.stdout).expect("bundle report must be JSON");
    assert_eq!(report["summary"]["turns"], 1);
    assert_eq!(report["summary"]["ok"], 1);

    let bundle_json: Value =
        serde_json::from_slice(&std::fs::read(&bundle).expect("bundle should be readable"))
            .expect("bundle must be JSON");
    let bundle_entries = bundle_json["turns"][0]["entries"]
        .as_array()
        .expect("bundle entries");
    let child_payloads: Vec<Value> = bundle_entries
        .iter()
        .filter_map(|entry| {
            let encoded: Value = serde_json::from_str(entry["payload_json"].as_str()?).ok()?;
            (encoded["payload"]["kind"] == "child_agent").then_some(encoded)
        })
        .collect();
    assert_eq!(
        child_payloads.len(),
        9,
        "the bundle carries every child event"
    );
    for payload in child_payloads {
        assert_eq!(payload["payload"]["call_id"], "call_delegate");
        assert_eq!(payload["session_id"], json!(child_session.as_str()));
        assert_eq!(payload["attempt"], 1);
    }

    let mut tampered = bundle_json.clone();
    let entries = tampered["turns"][0]["entries"]
        .as_array_mut()
        .expect("bundle entries");
    let child_entry = entries
        .iter_mut()
        .find(|entry| {
            entry["payload_json"].as_str().is_some_and(|raw| {
                raw.contains("child answer")
                    && serde_json::from_str::<Value>(raw)
                        .is_ok_and(|encoded| encoded["payload"]["kind"] == "child_agent")
            })
        })
        .expect("the bundle must carry the child model output as evidence");
    let original = child_entry["payload_json"]
        .as_str()
        .expect("payload must be a string")
        .to_owned();
    child_entry["payload_json"] =
        Value::String(original.replace("child answer", "tampered child answer"));
    let tampered_bundle = temp.path.join("tampered-child-evidence.json");
    std::fs::write(
        &tampered_bundle,
        serde_json::to_vec_pretty(&tampered).expect("bundle must serialize"),
    )
    .expect("tampered bundle should be written");
    let rejected = run_trace_command(
        &temp.path,
        &config,
        &[
            "verify-bundle",
            "--bundle",
            tampered_bundle
                .to_str()
                .expect("tampered bundle path must be UTF-8"),
            "--json",
        ],
    );
    assert!(
        !rejected.status.success(),
        "editing a child event must invalidate the evidence\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&rejected.stdout),
        String::from_utf8_lossy(&rejected.stderr)
    );
    let report: Value =
        serde_json::from_slice(&rejected.stdout).expect("failure report must be JSON");
    let kinds: Vec<&str> = report["turns"][0]["problems"]
        .as_array()
        .expect("turn problems")
        .iter()
        .filter_map(|problem| problem["kind"].as_str())
        .collect();
    assert!(
        kinds.contains(&"digest_mismatch"),
        "tampered child evidence must break the hash chain: {kinds:?}"
    );
}

#[test]
fn child_round_exhaustion_is_reported_while_the_parent_completes() {
    let temp = TempDir::new("rounds");
    std::fs::write(temp.path.join("Cargo.toml"), "test manifest").expect("manifest should exist");
    let config = temp.path.join("harness.toml");
    let settings = Settings {
        max_rounds: 2,
        ..Settings::default()
    };
    write_config(&config, &enabled_config(&settings));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate a long job",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "keep reading", "tools": ["read_file"]}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "read_one",
                "read_file",
                json!({"path": "Cargo.toml"}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "read_two",
                "read_file",
                json!({"path": "Cargo.toml"}),
            )])),
            Plan::Sse(text_response("parent survives")),
        ],
    );

    assert_success(&run.output);
    assert_eq!(run.child_requests().len(), 2);
    let parent = run.parent_requests();
    assert_eq!(parent.len(), 2);
    let results = tool_result_jsons(parent[1]);
    assert_eq!(results[0]["status"], "error");
    let error = results[0]["error"].as_str().expect("error string");
    assert!(
        error.contains("2 round limit"),
        "round exhaustion must be reported: {error}"
    );

    let events = read_events(&temp.path.join("events.sqlite"), &run.session(0));
    let (calls, delegate_results) = delegate_events(&events);
    assert_eq!(calls.len(), 1);
    assert_eq!(delegate_results.len(), 1);
    assert!(
        delegate_results[0].is_error,
        "the parent sees a failed tool"
    );
    assert!(
        String::from_utf8_lossy(&run.output.stdout).contains("parent survives"),
        "the parent turn must still complete"
    );
}

#[test]
fn child_tool_call_limit_skips_the_n_plus_one_tool() {
    let temp = TempDir::new("tool-limit");
    let config = temp.path.join("harness.toml");
    let settings = Settings {
        max_child_tool_calls: 1,
        ..Settings::default()
    };
    write_config(&config, &enabled_config(&settings));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate two writes",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "write two files", "tools": ["write_file"]}),
            )])),
            Plan::Sse(tool_calls_response(&[
                (
                    "write_one",
                    "write_file",
                    json!({"path": "first.txt", "content": "first"}),
                ),
                (
                    "write_two",
                    "write_file",
                    json!({"path": "second.txt", "content": "second"}),
                ),
            ])),
            Plan::Sse(text_response("parent continues")),
        ],
    );

    assert_success(&run.output);
    assert_eq!(run.child_requests().len(), 1);
    assert_eq!(
        std::fs::read_to_string(temp.path.join("first.txt")).expect("the first write should run"),
        "first"
    );
    assert!(
        !temp.path.join("second.txt").exists(),
        "the N+1 tool must not execute"
    );

    let results = tool_result_jsons(run.parent_requests()[1]);
    assert_eq!(results[0]["status"], "error");
    let error = results[0]["error"].as_str().expect("error string");
    assert!(
        error.contains("tool call limit of 1"),
        "the limit must be named: {error}"
    );

    let events = read_events(&temp.path.join("events.sqlite"), &run.session(0));
    assert!(
        !events.iter().any(
            |event| matches!(event, AgentEvent::ToolCallStarted { call } if call.name == "write_file")
        ),
        "child write events must not enter the parent stream"
    );
}

#[test]
fn child_tool_failure_makes_the_whole_delegation_an_error() {
    let temp = TempDir::new("tool-failure");
    let config = temp.path.join("harness.toml");
    write_config(&config, &enabled_config(&Settings::default()));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate a missing read",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "read missing", "tools": ["read_file"]}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "child_read",
                "read_file",
                json!({"path": "missing.txt"}),
            )])),
            Plan::Sse(text_response("the child still answers")),
            Plan::Sse(text_response("parent handled it")),
        ],
    );

    assert_success(&run.output);
    assert_eq!(run.child_requests().len(), 2);
    let results = tool_result_jsons(run.parent_requests()[1]);
    assert_eq!(results[0]["status"], "error");
    let error = results[0]["error"].as_str().expect("error string");
    assert!(
        error.contains("child tool failure"),
        "a child tool error must dominate the answer: {error}"
    );

    let events = read_events(&temp.path.join("events.sqlite"), &run.session(0));
    let (_, delegate_results) = delegate_events(&events);
    assert_eq!(delegate_results.len(), 1);
    assert!(delegate_results[0].is_error);
}

#[test]
fn child_timeout_is_bounded_by_the_delegation_timeout() {
    let temp = TempDir::new("timeout");
    let config = temp.path.join("harness.toml");
    let settings = Settings {
        timeout_ms: 5_001,
        ..Settings::default()
    };
    write_config(&config, &enabled_config(&settings));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate a hanging job",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "hang forever", "tools": ["read_file"]}),
            )])),
            Plan::Hang {
                delay: Duration::from_millis(5_400),
                response: text_response("too late"),
            },
            Plan::Sse(text_response("parent after timeout")),
        ],
    );

    assert_success(&run.output);
    assert_eq!(run.child_requests().len(), 1);
    let parent = run.parent_requests();
    assert_eq!(parent.len(), 2);
    let results = tool_result_jsons(parent[1]);
    assert_eq!(results[0]["status"], "error");
    let error = results[0]["error"].as_str().expect("error string");
    assert!(
        error.contains("timed out after 5001ms"),
        "the child timeout must be reported: {error}"
    );
    assert!(
        String::from_utf8_lossy(&run.output.stdout).contains("parent after timeout"),
        "the parent must complete after the child timeout"
    );
}

#[test]
fn child_bash_does_not_inherit_provider_credentials() {
    let temp = TempDir::new("child-env");
    let config = temp.path.join("harness.toml");
    write_config(&config, &enabled_config(&Settings::default()));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate an environment check",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "check the environment", "tools": ["bash"]}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "child_env",
                "bash",
                json!({
                    "command": "printf '%s' \"${OPENAI_API_KEY:-UNSET}\" > child-env.txt"
                }),
            )])),
            Plan::Sse(text_response("the child checked its environment")),
            Plan::Sse(text_response("parent done")),
        ],
    );

    assert_success(&run.output);
    assert_eq!(
        std::fs::read_to_string(temp.path.join("child-env.txt"))
            .expect("the child command must write its observation"),
        "UNSET",
        "the supervisor must clear the provider credential from child bash"
    );
    let children = run.child_requests();
    assert_eq!(children.len(), 2);
    assert!(
        !children[1].contains("test-key"),
        "the provider key must never enter the child transcript"
    );

    let results = tool_result_jsons(run.parent_requests()[1]);
    assert_eq!(results[0]["status"], "ok");
}

#[test]
fn child_bash_is_killed_by_the_supervisor_deadline() {
    let temp = TempDir::new("child-bash-timeout");
    let config = temp.path.join("harness.toml");
    let settings = Settings {
        bash_timeout_ms: Some(250),
        ..Settings::default()
    };
    write_config(&config, &enabled_config(&settings));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate a hanging child command",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "run a slow child command", "tools": ["bash"]}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "child_slow",
                "bash",
                json!({"command": "sleep 2; printf finished > finished.txt"}),
            )])),
            Plan::Sse(text_response("the child saw the timeout")),
            Plan::Sse(text_response("parent done")),
        ],
    );

    assert_success(&run.output);
    let children = run.child_requests();
    assert_eq!(children.len(), 2);
    assert!(
        children[1].contains("timed out after 0.25 seconds"),
        "child bash must use the configured supervisor deadline: {}",
        children[1]
    );
    assert!(
        !temp.path.join("finished.txt").exists(),
        "the supervisor must terminate the child command before it writes"
    );

    let results = tool_result_jsons(run.parent_requests()[1]);
    assert_eq!(results[0]["status"], "error");
    assert!(
        results[0]["error"]
            .as_str()
            .is_some_and(|error| error.contains("child tool failure")),
        "the parent must observe the child tool failure: {}",
        results[0]
    );
}

#[test]
fn oversized_child_tool_results_become_workspace_artifacts() {
    let temp = TempDir::new("child-artifact");
    let config = temp.path.join("harness.toml");
    let settings = Settings {
        inline_threshold_bytes: Some(1024),
        max_inline_bytes: Some(2048),
        max_artifact_bytes: Some(8192),
        ..Settings::default()
    };
    write_config(&config, &enabled_config(&settings));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate a verbose child command",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "produce a large child result", "tools": ["bash"]}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "child_large",
                "bash",
                json!({"command": "printf 'y%.0s' {1..2000}"}),
            )])),
            Plan::Sse(text_response("the child read the artifact")),
            Plan::Sse(text_response("parent done")),
        ],
    );

    assert_success(&run.output);
    let children = run.child_requests();
    assert_eq!(children.len(), 2);
    let child_follow_up = request_messages(children[1]);
    let tool_content = child_follow_up[3]["content"]
        .as_str()
        .expect("the child tool result must be text");
    assert!(
        tool_content.contains("[result artifact]") && tool_content.contains("digest: sha256:"),
        "the child must receive the artifact envelope instead of the payload: {tool_content}"
    );
    assert!(
        !tool_content.contains(&"y".repeat(1_500)) && tool_content.len() <= 2048,
        "the oversized child output must enter the transcript only as a bounded preview"
    );

    let artifacts = artifact_files(&temp.path);
    let data = artifacts
        .iter()
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.len() == 64)
                && std::fs::read(path)
                    .is_ok_and(|bytes| bytes.iter().filter(|byte| **byte == b'y').count() >= 2_000)
        })
        .expect("the child output artifact must exist");
    let stored = std::fs::read(data).expect("artifact must be readable");
    assert!(
        stored.iter().filter(|byte| **byte == b'y').count() >= 2_000,
        "the artifact must retain the child output, got {} bytes",
        stored.len()
    );
    assert!(
        data.with_extension("json").is_file(),
        "the child artifact must have a sidecar: {artifacts:?}"
    );

    let results = tool_result_jsons(run.parent_requests()[1]);
    assert_eq!(results[0]["status"], "ok");
    assert_eq!(results[0]["answer"], "the child read the artifact");
}

/// Returns every regular file under the default artifact directory.
fn artifact_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(shards) = std::fs::read_dir(root.join(".jessica/artifacts")) else {
        return files;
    };
    for shard in shards.flatten() {
        let Ok(entries) = std::fs::read_dir(shard.path()) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry.path().is_file() {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    files
}

#[test]
fn child_usage_exhausts_the_parent_token_budget() {
    let temp = TempDir::new("budget");
    let config = temp.path.join("harness.toml");
    let settings = Settings {
        max_tokens: 10,
        ..Settings::default()
    };
    write_config(&config, &enabled_config(&settings));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate a costly job",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "costly task", "tools": []}),
            )])),
            Plan::Sse(text_response_with_usage("expensive child answer", 8, 8)),
        ],
    );

    assert_eq!(
        run.output.status.code(),
        Some(1),
        "the parent budget must fail the turn"
    );
    assert_eq!(run.requests.len(), 2, "the parent must not call again");

    let database = temp.path.join("events.sqlite");
    let events = read_events(&database, &run.session(0));
    assert!(
        events.iter().any(|event| matches!(
            event,
            AgentEvent::BudgetExceeded {
                kind: BudgetKind::TokenLimit,
                ..
            }
        )),
        "the token-limit event must be persisted: {events:?}"
    );
    let outer = read_events(&database, &run.session(1));
    assert!(
        outer.iter().any(|event| matches!(
            event,
            AgentEvent::SessionEnded {
                reason: SessionEndReason::BudgetExceeded,
                ..
            }
        )),
        "the final outer session must report BudgetExceeded: {outer:?}"
    );

    let (_, child_session) = child_trace("costly task", &[], ALL_TOOLS);
    assert_session_absent(&database, &child_session);
}

#[test]
fn oversized_child_answer_is_truncated_to_a_utf8_prefix() {
    let temp = TempDir::new("truncate");
    let config = temp.path.join("harness.toml");
    let settings = Settings {
        max_result_bytes: 256,
        ..Settings::default()
    };
    write_config(&config, &enabled_config(&settings));

    let long_answer = "答案🙂".repeat(100);
    let run = run_turn(
        &temp.path,
        &config,
        "delegate a long answer",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "answer verbosely", "tools": []}),
            )])),
            Plan::Sse(text_response(&long_answer)),
            Plan::Sse(text_response("parent done")),
        ],
    );

    assert_success(&run.output);
    let results = tool_result_jsons(run.parent_requests()[1]);
    let result = &results[0];
    assert_eq!(result["status"], "truncated");
    assert_eq!(
        result.as_object().expect("result object").len(),
        3,
        "truncated results have exactly status, answer, and original_bytes"
    );
    assert!(result["original_bytes"].as_u64().expect("original size") > 256);

    let answer = result["answer"].as_str().expect("answer string");
    assert!(
        long_answer.starts_with(answer),
        "the retained answer must be a UTF-8 prefix"
    );
    assert!(
        serde_json::to_string(result).expect("result JSON").len() <= 256,
        "the serialized result must stay within its budget"
    );

    let events = read_events(&temp.path.join("events.sqlite"), &run.session(0));
    let (_, delegate_results) = delegate_events(&events);
    assert_eq!(delegate_results.len(), 1);
    assert!(
        !delegate_results[0].is_error,
        "a truncated successful result is not an error"
    );
}

#[test]
fn unauthorized_child_tool_is_rejected_before_any_child_request() {
    let temp = TempDir::new("permission");
    let config = temp.path.join("harness.toml");
    let settings = Settings {
        policy: &["read_file", "write_file", "delegate_task"],
        ..Settings::default()
    };
    write_config(&config, &enabled_config(&settings));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate bash",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "run bash", "tools": ["bash"]}),
            )])),
            Plan::Sse(text_response("parent saw the denial")),
        ],
    );

    assert_success(&run.output);
    assert_eq!(run.child_requests().len(), 0);
    let parent = run.parent_requests();
    assert_eq!(parent.len(), 2);
    let results = tool_result_jsons(parent[1]);
    assert_eq!(results[0]["status"], "error");
    let error = results[0]["error"].as_str().expect("error string");
    assert!(
        error.contains("not permitted by the parent allowlist"),
        "the permission failure must be named: {error}"
    );
}

#[test]
fn two_delegations_in_one_round_keep_their_transcripts_separate() {
    let temp = TempDir::new("parallel");
    std::fs::write(temp.path.join("ALPHA.txt"), "ALPHA-ONLY").expect("alpha file");
    std::fs::write(temp.path.join("BETA.txt"), "BETA-ONLY").expect("beta file");
    let config = temp.path.join("harness.toml");
    write_config(&config, &enabled_config(&Settings::default()));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate two reads",
        vec![
            Plan::Sse(tool_calls_response(&[
                (
                    "delegate_alpha",
                    DELEGATE_TASK_TOOL_NAME,
                    json!({"task": "task alpha", "tools": ["read_file"]}),
                ),
                (
                    "delegate_beta",
                    DELEGATE_TASK_TOOL_NAME,
                    json!({"task": "task beta", "tools": ["read_file"]}),
                ),
            ])),
            Plan::Sse(tool_calls_response(&[(
                "alpha_read",
                "read_file",
                json!({"path": "ALPHA.txt"}),
            )])),
            Plan::Sse(text_response("answer alpha")),
            Plan::Sse(tool_calls_response(&[(
                "beta_read",
                "read_file",
                json!({"path": "BETA.txt"}),
            )])),
            Plan::Sse(text_response("answer beta")),
            Plan::Sse(text_response("parent done")),
        ],
    );

    assert_success(&run.output);
    assert_eq!(run.requests.len(), 6, "parent, two children, parent");

    let children = run.child_requests();
    assert_eq!(children.len(), 4);
    assert_child_request(children[0], "task alpha", &["read_file"]);
    assert_child_request(children[2], "task beta", &["read_file"]);

    let alpha_follow_up = request_messages(children[1]);
    assert_eq!(alpha_follow_up.len(), 4);
    assert_eq!(alpha_follow_up[3]["content"], "1\tALPHA-ONLY\n");
    assert!(
        !children[1].contains("BETA-ONLY"),
        "alpha must not see beta state"
    );

    let beta_follow_up = request_messages(children[3]);
    assert_eq!(beta_follow_up.len(), 4);
    assert_eq!(beta_follow_up[3]["content"], "1\tBETA-ONLY\n");
    assert!(
        !children[3].contains("ALPHA-ONLY"),
        "beta must not see alpha state"
    );

    let results = tool_result_jsons(run.parent_requests()[1]);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["status"], "ok");
    assert_eq!(results[0]["answer"], "answer alpha");
    assert_eq!(results[1]["status"], "ok");
    assert_eq!(results[1]["answer"], "answer beta");

    let events = read_events(&temp.path.join("events.sqlite"), &run.session(0));
    let (calls, delegate_results) = delegate_events(&events);
    assert_eq!(calls.len(), 2);
    assert_eq!(delegate_results.len(), 2);
    assert_eq!(calls[0].arguments["task"], "task alpha");
    assert_eq!(calls[1].arguments["task"], "task beta");
    for (index, call) in calls.iter().enumerate() {
        assert_eq!(delegate_results[index].tool_call_id, call.id);
    }

    let database = temp.path.join("events.sqlite");
    for task in ["task alpha", "task beta"] {
        let (_, child_session) = child_trace(task, &["read_file"], ALL_TOOLS);
        assert_session_absent(&database, &child_session);
    }
}

#[test]
fn a_forged_nested_delegation_is_unknown_to_the_child() {
    let temp = TempDir::new("nested");
    let config = temp.path.join("harness.toml");
    write_config(&config, &enabled_config(&Settings::default()));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate without nesting",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "no nesting", "tools": ["read_file"]}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "forged_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "nested task", "tools": []}),
            )])),
            Plan::Sse(text_response("child after unknown")),
            Plan::Sse(text_response("parent done")),
        ],
    );

    assert_success(&run.output);
    assert_eq!(run.requests.len(), 4, "no grandchild provider call");
    let children = run.child_requests();
    assert_eq!(children.len(), 2);
    assert_tool_names(children[0], &["read_file"]);
    assert!(
        !request_tool_names(children[0])
            .iter()
            .any(|name| name == DELEGATE_TASK_TOOL_NAME),
        "the child must never receive delegate_task"
    );
    let follow_up = request_messages(children[1]);
    let tool_content = follow_up[3]["content"].as_str().expect("tool content");
    assert!(
        tool_content.contains("unknown tool `delegate_task`"),
        "a forged delegation is just an unknown tool: {tool_content}"
    );

    let results = tool_result_jsons(run.parent_requests()[1]);
    assert_eq!(results[0]["status"], "error");
    let error = results[0]["error"].as_str().expect("error string");
    assert!(error.contains("child tool failure"));
}

#[test]
fn disabled_delegation_never_exposes_the_tool() {
    let temp = TempDir::new("disabled");
    let config = temp.path.join("harness.toml");
    write_config(&config, &disabled_config());

    let run = run_turn(
        &temp.path,
        &config,
        "try to delegate",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "should not run", "tools": []}),
            )])),
            Plan::Sse(text_response("parent recovered")),
        ],
    );

    assert_success(&run.output);
    assert_eq!(run.requests.len(), 2);
    assert_eq!(run.child_requests().len(), 0);
    assert_tool_names(
        run.parent_requests()[0],
        &["bash", "read_file", "write_file"],
    );
    let parent = request_messages(run.parent_requests()[1]);
    assert_eq!(parent[3]["role"], "tool");
    let content = parent[3]["content"].as_str().expect("tool content");
    assert!(
        content.contains("unknown tool `delegate_task`"),
        "disabled delegation is an unknown tool: {content}"
    );
}

#[test]
fn replay_is_deterministic_and_completed_resume_makes_no_calls() {
    let temp = TempDir::new("replay-resume");
    std::fs::write(temp.path.join("NOTE.txt"), "REPLAY-NOTE").expect("note should be written");
    let config = temp.path.join("harness.toml");
    write_config(&config, &enabled_config(&Settings::default()));

    let run = run_turn(
        &temp.path,
        &config,
        "delegate for replay",
        vec![
            Plan::Sse(tool_calls_response(&[(
                "call_delegate",
                DELEGATE_TASK_TOOL_NAME,
                json!({"task": "read for replay", "tools": ["read_file"]}),
            )])),
            Plan::Sse(tool_calls_response(&[(
                "child_read",
                "read_file",
                json!({"path": "NOTE.txt"}),
            )])),
            Plan::Sse(text_response("child replay answer")),
            Plan::Sse(text_response("parent replay answer")),
        ],
    );
    assert_success(&run.output);

    let database = temp.path.join("events.sqlite");
    let session = run.session(0);
    let recorded = read_events(&database, &session);
    assert!(recorded.len() >= 5, "the session should be recorded");

    let first_path = temp.path.join("first.jsonl");
    let second_path = temp.path.join("second.jsonl");
    for path in [&first_path, &second_path] {
        let output = run_replay(&temp.path, &config, &session, path);
        assert_success(&output);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains(&format!("wrote {} events", recorded.len())),
            "replay must report every recorded event: {stdout}"
        );
    }

    let first = std::fs::read(&first_path).expect("first replay output");
    let second = std::fs::read(&second_path).expect("second replay output");
    assert_eq!(first, second, "replay must be byte-identical");
    let expected = recorded
        .iter()
        .map(|event| {
            format!(
                "{}\n",
                serde_json::to_string(event).expect("recorded event should serialize")
            )
        })
        .collect::<String>();
    assert_eq!(
        String::from_utf8(first).expect("replay output should be UTF-8"),
        expected,
        "replay must reproduce only the parent event stream"
    );

    let (resume_output, requests) = run_resume(&temp.path, &config, &session);
    assert_success(&resume_output);
    assert!(
        String::from_utf8_lossy(&resume_output.stdout).contains("already ended"),
        "a completed resume must not start a turn"
    );
    assert!(
        requests.is_empty(),
        "a completed resume must make zero provider calls"
    );
    assert_eq!(
        read_events(&database, &session),
        recorded,
        "resume must not append events to a completed session"
    );
}

#[test]
fn resume_rejects_an_interrupted_delegate_call_without_running_the_child() {
    let temp = TempDir::new("interrupted");
    let database = temp.path.join("events.sqlite");
    let session = SessionId::new("cli-interrupted-delegation");
    seed_interrupted_delegation(&database, &session);

    let config = temp.path.join("harness.toml");
    write_config(&config, &enabled_config(&Settings::default()));

    let (output, requests) = run_resume(&temp.path, &config, &session);
    assert_eq!(
        output.status.code(),
        Some(1),
        "an incomplete delegate round must be rejected"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("leaves tool call") && stderr.contains("without a result"),
        "recovery must reject the incomplete delegate call: {stderr}"
    );
    assert!(
        requests.is_empty(),
        "resume must not execute the child or call the provider"
    );

    let store = EventStore::open(&database).expect("event store should open");
    assert_eq!(store.last_seq(&session).expect("last seq"), Some(2));
    let (_, child_session) = child_trace("unfinished task", &[], ALL_TOOLS);
    assert_session_absent(&database, &child_session);
}

/// Seeds a session whose stream stops after a delegate tool call starts.
fn seed_interrupted_delegation(database: &Path, session: &SessionId) {
    let mut store = EventStore::open(database).expect("event store should open");
    store
        .record_session(
            session,
            &[
                Message::system("interrupted delegation system prompt"),
                Message::user("finish the unfinished task"),
            ],
            Timestamp::from_millis(1),
        )
        .expect("session metadata should be recorded");

    let call = ToolCall::new(
        "call_delegate",
        DELEGATE_TASK_TOOL_NAME,
        json!({"task": "unfinished task", "tools": []}),
    );
    for (index, event) in [
        AgentEvent::SessionStarted {
            session_id: session.clone(),
            messages: Vec::new(),
        },
        AgentEvent::ToolCallStarted { call },
    ]
    .into_iter()
    .enumerate()
    {
        store
            .append(session, &event, Timestamp::from_millis(2 + index as i64))
            .expect("seeded event should append");
    }
}

/// Returns one SSE response carrying `content` and a stop finish reason.
fn text_response(content: &str) -> String {
    sse(&[content_chunk(content), finish_chunk("stop")])
}

/// Returns one SSE response carrying text and a usage record.
fn text_response_with_usage(content: &str, prompt_tokens: u64, completion_tokens: u64) -> String {
    sse(&[
        content_chunk(content),
        usage_chunk(prompt_tokens, completion_tokens),
        finish_chunk("stop"),
    ])
}

/// Returns one SSE response requesting one or more tools.
fn tool_calls_response(calls: &[(&str, &str, Value)]) -> String {
    let mut payloads = Vec::with_capacity(calls.len() + 1);
    for (index, (id, name, arguments)) in calls.iter().enumerate() {
        payloads.push(tool_call_chunk(index, id, name, &arguments.to_string()));
    }
    payloads.push(finish_chunk("tool_calls"));
    sse(&payloads)
}

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
