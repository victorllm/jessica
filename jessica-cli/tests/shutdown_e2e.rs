//! End-to-end acceptance for process-level graceful shutdown.
//!
//! The scenarios spawn the compiled CLI and deliver real Unix signals:
//!
//! - SIGTERM during a turn cancels the turn, persists its terminal
//!   `SessionEnded(Cancelled)` event, and exits 143.
//! - SIGINT on an idle REPL wakes the pending stdin read and exits 130 without
//!   creating a session or calling the provider.
//! - A first signal drains the skill worker; a second signal forces an
//!   immediate exit while the in-flight job remains recoverable.
//!
//! No test needs a real provider: the scripted HTTP server answers, holds, or
//! stalls requests as each scenario requires.

#![cfg(unix)]

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use jessica_core::types::{AgentEvent, SessionEndReason, SessionId};
use jessica_memory::{EventStore, SkillJobStatus, SkillLog, SqliteSkillStore};
use serde_json::json;

/// Overall budget for one CLI child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the scripted server.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);

/// Temporary directory removed when the test finishes.
struct TempDir {
    /// Directory path.
    path: PathBuf,
}

impl TempDir {
    /// Creates a uniquely named directory below the system temp directory.
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-cli-shutdown-e2e-{label}-{}-{unique}",
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

/// One scripted server action.
enum Plan {
    /// A complete OpenAI-compatible SSE response.
    Sse(String),
    /// A complete SSE response sent after waiting, so shutdown can start while
    /// the client is still waiting for the answer.
    SseAfter {
        /// HTTP response body.
        response: String,
        /// Delay before the response is written.
        wait: Duration,
    },
    /// Hold the connection open without sending any response bytes.
    Hang,
}

/// Scripted HTTP server that records every request body.
struct ScriptedServer {
    /// Address the CLI should use as its provider base URL.
    address: SocketAddr,
    /// Flag that stops the accept loop and any hanging response.
    stop: Arc<AtomicBool>,
    /// Number of requests read so far.
    requests_seen: Arc<AtomicUsize>,
    /// Join handle returning all request bodies.
    handle: thread::JoinHandle<Vec<String>>,
}

impl ScriptedServer {
    /// Starts the server with one plan per provider request.
    fn start(plans: Vec<Plan>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
        let address = listener.local_addr().expect("mock server address");
        let stop = Arc::new(AtomicBool::new(false));
        let requests_seen = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&stop);
        let seen = Arc::clone(&requests_seen);
        let handle = thread::spawn(move || serve(listener, flag, seen, plans));
        Self {
            address,
            stop,
            requests_seen,
            handle,
        }
    }

    /// Waits until at least `count` requests have been read.
    fn wait_for_requests(&self, count: usize) {
        let deadline = Instant::now() + SERVER_TIMEOUT;
        while self.requests_seen.load(Ordering::SeqCst) < count {
            if Instant::now() >= deadline {
                panic!("timed out waiting for {count} requests");
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Stops the server and returns every request body in arrival order.
    fn finish(self) -> Vec<String> {
        self.stop.store(true, Ordering::SeqCst);
        self.handle.join().expect("mock server should finish")
    }
}

/// Accepts requests until stopped, answering from the plan queue.
fn serve(
    listener: TcpListener,
    stop: Arc<AtomicBool>,
    requests_seen: Arc<AtomicUsize>,
    plans: Vec<Plan>,
) -> Vec<String> {
    listener
        .set_nonblocking(true)
        .expect("listener should become non-blocking");
    let deadline = Instant::now() + SERVER_TIMEOUT;
    let mut plans: VecDeque<Plan> = plans.into();
    let mut requests = Vec::new();

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
                    requests.push(body);
                    requests_seen.fetch_add(1, Ordering::SeqCst);
                    match plans.pop_front() {
                        Some(Plan::Sse(response)) => {
                            let _ = stream.write_all(response.as_bytes());
                            let _ = stream.flush();
                        }
                        Some(Plan::SseAfter { response, wait }) => {
                            thread::sleep(wait);
                            let _ = stream.write_all(response.as_bytes());
                            let _ = stream.flush();
                        }
                        Some(Plan::Hang) => {
                            // The CLI may cancel the request while this side is
                            // still holding the connection. Keep the connection
                            // open until the test stops the server, so the
                            // server thread can always join.
                            while !stop.load(Ordering::SeqCst) && Instant::now() < deadline {
                                thread::sleep(Duration::from_millis(10));
                            }
                        }
                        None => {
                            let _ = stream.write_all(
                                b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\
                                  Connection: close\r\n\r\n",
                            );
                            let _ = stream.flush();
                        }
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("mock server accept failed: {error}"),
        }
    }

    requests
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

/// Spawns the CLI in REPL mode with piped stdin and output.
fn spawn_repl(workspace: &Path, config: &Path, address: SocketAddr) -> Child {
    Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .env_remove("JESSICA_PROVIDER")
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .arg("--config")
        .arg(config)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should start")
}

/// Sends one line to the REPL and flushes it.
fn send_line(stdin: &mut ChildStdin, line: &str) {
    writeln!(stdin, "{line}").expect("REPL input should be written");
    stdin.flush().expect("REPL input should be flushed");
}

/// Sends one Unix signal to `pid`.
fn send_signal(pid: u32, signal: &str) {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .status()
        .expect("kill should run");
    assert!(status.success(), "kill -{signal} {pid} failed: {status}");
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

/// Collects the child output after it has exited.
fn collect_output(child: Child, status: std::process::ExitStatus) -> Output {
    let mut output = child
        .wait_with_output()
        .expect("CLI output should be collected");
    output.status = status;
    output
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

fn write_config(path: &Path, body: &str) {
    std::fs::write(path, body).expect("configuration should be written");
}

/// Returns one SSE response carrying `content` and a stop finish reason.
fn text_response(content: &str) -> String {
    let payloads = [
        json!({
            "choices": [{
                "index": 0,
                "delta": {"content": content},
                "finish_reason": null
            }]
        })
        .to_string(),
        json!({
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
        })
        .to_string(),
    ];
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

/// Asserts that neither captured stream contains a Rust panic message.
fn assert_no_panic(stdout: &str, stderr: &str) {
    assert!(
        !stdout.contains("panicked at") && !stderr.contains("panicked at"),
        "shutdown must not panic\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn sigterm_during_a_turn_persists_cancelled_and_exits_143() {
    let temp = TempDir::new("sigterm-turn");
    let config = temp.path.join("harness.toml");
    write_config(&config, "[events]\npath = \"events.sqlite\"\n");
    let server = ScriptedServer::start(vec![Plan::Hang]);
    let mut child = spawn_repl(&temp.path, &config, server.address);
    let pid = child.id();
    let mut stdin = child.stdin.take().expect("stdin should be piped");

    send_line(&mut stdin, "hang until interrupted");
    server.wait_for_requests(1);
    send_signal(pid, "TERM");

    let status = wait_with_timeout(&mut child);
    let output = collect_output(child, status);
    let requests = server.finish();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(143),
        "SIGTERM must produce exit code 143\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        requests.len(),
        1,
        "the cancelled turn must not issue another request\nstderr:\n{stderr}"
    );
    assert_no_panic(&stdout, &stderr);
    assert!(
        stderr.contains("SIGTERM received; stopping new turns and draining"),
        "the first signal must be visible on stderr:\n{stderr}"
    );

    let session = SessionId::new(format!("cli-{pid}-0"));
    let events = read_events(&temp.path.join("events.sqlite"), &session);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::SessionStarted { .. })),
        "the cancelled turn must have started: {events:?}"
    );
    let reasons: Vec<SessionEndReason> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::SessionEnded { reason, .. } => Some(*reason),
            _ => None,
        })
        .collect();
    assert_eq!(
        reasons,
        vec![SessionEndReason::Cancelled],
        "the terminal event must be persisted exactly once before exit: {events:?}"
    );
    assert!(
        matches!(
            events.last(),
            Some(AgentEvent::SessionEnded {
                reason: SessionEndReason::Cancelled,
                ..
            })
        ),
        "SessionEnded(Cancelled) must be the last persisted event: {events:?}"
    );
}

/// Runs the idle-REPL SIGINT scenario once.
///
/// Returns the temporary directory (kept alive for later database assertions),
/// the child pid, and the captured output.
fn run_idle_sigint(label: &str) -> (TempDir, u32, Output) {
    let temp = TempDir::new(label);
    let config = temp.path.join("harness.toml");
    write_config(&config, "[events]\npath = \"events.sqlite\"\n");
    // Bind but never accept: the CLI must not connect at all in this test.
    let listener = TcpListener::bind("127.0.0.1:0").expect("unused listener should bind");
    let address = listener.local_addr().expect("listener address");
    let mut child = spawn_repl(&temp.path, &config, address);
    let pid = child.id();

    // The event store is opened after the signal handlers are installed, so
    // its existence proves a signal sent now cannot hit the default action.
    let database = temp.path.join("events.sqlite");
    let deadline = Instant::now() + SERVER_TIMEOUT;
    while !database.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the event store to be created"
        );
        thread::sleep(Duration::from_millis(10));
    }

    send_signal(pid, "INT");
    let status = wait_with_timeout(&mut child);
    let output = collect_output(child, status);
    drop(listener);
    (temp, pid, output)
}

#[test]
fn sigint_on_an_idle_repl_exits_130_without_creating_a_turn() {
    let (temp, pid, output) = run_idle_sigint("sigint-idle");
    let database = temp.path.join("events.sqlite");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(130),
        "SIGINT must produce exit code 130\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("SIGINT received; stopping new turns and draining"),
        "the first signal must be visible on stderr:\n{stderr}"
    );
    assert!(
        !stdout.contains("[session] started"),
        "an idle REPL must not create a turn on shutdown\nstdout:\n{stdout}"
    );
    assert_no_panic(&stdout, &stderr);

    let store = EventStore::open(&database).expect("event store should open");
    let session = SessionId::new(format!("cli-{pid}-0"));
    assert!(
        store
            .events(&session)
            .expect("events should be readable")
            .is_empty(),
        "shutdown must not append events without a turn"
    );
    assert!(
        store
            .session_messages(&session)
            .expect("session metadata should be readable")
            .is_none(),
        "shutdown must not record session metadata without a turn"
    );
}

#[test]
fn every_idle_sigint_writes_its_diagnostic_before_exiting() {
    // The diagnostic used to be written after the controller recorded the
    // signal. The REPL could wake and exit in that window, producing exit 130
    // with no stderr line (CI run 44431198725). Repeat the real process
    // scenario so the ordering is guarded by behaviour, not only by the
    // controller unit tests.
    for iteration in 0..20 {
        let (_temp, _pid, output) = run_idle_sigint("sigint-idle-stress");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(130),
            "iteration {iteration}: SIGINT must produce exit code 130\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stderr.contains("SIGINT received; stopping new turns and draining"),
            "iteration {iteration}: the first-signal diagnostic must not race with exit\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert_no_panic(&stdout, &stderr);
    }
}

#[test]
fn sigterm_waits_for_the_skill_worker_to_drain_an_in_flight_job() {
    let temp = TempDir::new("skill-drain");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[events]\npath = \"events.sqlite\"\n\n\
         [skills]\n\
         enabled = true\n\
         path = \"skills.db\"\n\
         min_tool_calls = 0\n\
         min_rounds = 1\n\
         distill_timeout_ms = 10000\n\
         drain_timeout_ms = 10000\n",
    );
    // The turn completes, then the distiller waits for a response that arrives
    // only after the first signal has started the drain.
    let server = ScriptedServer::start(vec![
        Plan::Sse(text_response("finished")),
        Plan::SseAfter {
            response: text_response(
                "{\"name\":\"shutdown test skill\",\
                 \"description\":\"a reusable shutdown procedure\",\
                 \"instructions\":\"wait for in-flight work before exiting\",\
                 \"tags\":[\"shutdown\"]}",
            ),
            wait: Duration::from_millis(500),
        },
    ]);
    let mut child = spawn_repl(&temp.path, &config, server.address);
    let pid = child.id();
    let mut stdin = child.stdin.take().expect("stdin should be piped");

    send_line(&mut stdin, "distill this completed turn");
    server.wait_for_requests(2);
    send_signal(pid, "TERM");

    let status = wait_with_timeout(&mut child);
    let output = collect_output(child, status);
    let requests = server.finish();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(143),
        "the drained CLI must keep the SIGTERM exit code\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        requests.len(),
        2,
        "the in-flight distillation must finish instead of being aborted\nstderr:\n{stderr}"
    );
    assert_no_panic(&stdout, &stderr);

    let session = SessionId::new(format!("cli-{pid}-0"));
    let events = read_events(&temp.path.join("events.sqlite"), &session);
    assert!(
        matches!(
            events.last(),
            Some(AgentEvent::SessionEnded {
                reason: SessionEndReason::Completed,
                ..
            })
        ),
        "the completed turn must be persisted before the drain: {events:?}"
    );

    // The worker finished the job during the graceful drain: the job is Done
    // rather than left pending or processing.
    let skills_database = temp.path.join("skills.db");
    let workspace = std::fs::canonicalize(&temp.path).expect("workspace must canonicalize");
    let store = SqliteSkillStore::open(&skills_database, &workspace)
        .expect("skill store should open after the CLI exits");
    let scope = store.workspace_scope().clone();
    drop(store);
    let log = SkillLog::open(&skills_database).expect("skill log should open");
    let jobs = log.list_jobs(&scope, 10).expect("jobs should be readable");
    assert_eq!(jobs.len(), 1, "exactly one skill job must exist: {jobs:?}");
    assert_eq!(
        jobs[0].status(),
        SkillJobStatus::Done,
        "the graceful drain must complete the in-flight job"
    );

    let skills = log
        .list_recent(&scope, 10)
        .expect("skills should be readable");
    assert_eq!(skills.len(), 1, "the drained job must persist one skill");
    assert_eq!(skills[0].name(), "shutdown test skill");
}

#[test]
fn a_second_signal_forces_exit_while_the_skill_drain_is_pending() {
    let temp = TempDir::new("second-signal");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[events]\npath = \"events.sqlite\"\n\n\
         [skills]\n\
         enabled = true\n\
         path = \"skills.db\"\n\
         min_tool_calls = 0\n\
         min_rounds = 1\n\
         distill_timeout_ms = 60000\n\
         drain_timeout_ms = 60000\n",
    );
    // The first request completes a turn and enqueues a skill job; the second
    // request is the distiller, which hangs for the rest of the test.
    let server = ScriptedServer::start(vec![Plan::Sse(text_response("finished")), Plan::Hang]);
    let mut child = spawn_repl(&temp.path, &config, server.address);
    let pid = child.id();
    let mut stdin = child.stdin.take().expect("stdin should be piped");

    send_line(&mut stdin, "distill this completed turn");
    server.wait_for_requests(2);
    send_signal(pid, "TERM");

    // The first signal cancels the REPL and starts the skill drain. The
    // distiller is still waiting on the hanging server, so the process must
    // stay alive rather than exit without draining.
    thread::sleep(Duration::from_millis(700));
    assert!(
        child
            .try_wait()
            .expect("CLI status should be readable")
            .is_none(),
        "the first signal must drain the skill worker instead of exiting immediately"
    );

    send_signal(pid, "TERM");
    let status = wait_with_timeout(&mut child);
    let output = collect_output(child, status);
    let requests = server.finish();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(143),
        "the second SIGTERM must keep the SIGTERM exit code\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        requests.len(),
        2,
        "one turn request and one hanging distillation request\nstderr:\n{stderr}"
    );
    assert_no_panic(&stdout, &stderr);
    assert!(
        stderr.contains("second SIGTERM received; exiting immediately"),
        "the forced exit must be visible on stderr:\n{stderr}"
    );

    let session = SessionId::new(format!("cli-{pid}-0"));
    let events = read_events(&temp.path.join("events.sqlite"), &session);
    assert!(
        matches!(
            events.last(),
            Some(AgentEvent::SessionEnded {
                reason: SessionEndReason::Completed,
                ..
            })
        ),
        "the completed turn must be persisted before the forced exit: {events:?}"
    );

    // The forced exit kills the in-flight distillation without marking the
    // job done or failed; it stays recoverable through the stale-claim path.
    let skills_database = temp.path.join("skills.db");
    let workspace = std::fs::canonicalize(&temp.path).expect("workspace must canonicalize");
    let store = SqliteSkillStore::open(&skills_database, &workspace)
        .expect("skill store should open after the CLI exits");
    let scope = store.workspace_scope().clone();
    drop(store);
    let log = SkillLog::open(&skills_database).expect("skill log should open");
    let jobs = log.list_jobs(&scope, 10).expect("jobs should be readable");

    assert_eq!(
        jobs.len(),
        1,
        "exactly one skill job must be enqueued: {jobs:?}"
    );
    assert_eq!(
        jobs[0].session_id(),
        &session,
        "the job must belong to the completed turn"
    );
    assert!(
        matches!(
            jobs[0].status(),
            SkillJobStatus::Pending | SkillJobStatus::Processing
        ),
        "a forced exit must leave the job recoverable, not {:?}",
        jobs[0].status()
    );
}
