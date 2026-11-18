//! End-to-end acceptance for provider retries in the real CLI.
//!
//! The scenarios drive the compiled binary against a scripted HTTP server.
//! They pin the observable retry boundary: enabling `[reliability]` retries a
//! server failure before any text and then records exactly one completed
//! session with one copy of the final text; a client error is not retried; and
//! omitting the section leaves the previous single-request behavior unchanged.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use jessica_core::types::{AgentEvent, SessionEndReason, SessionId};
use jessica_memory::EventStore;
use serde_json::{Value, json};

/// Overall budget for one CLI child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock server to observe requests.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);

/// Temporary directory removed when the test finishes.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-cli-reliability-e2e-{label}-{}-{unique}",
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
    /// An HTTP error response with no body.
    Status {
        /// HTTP status code.
        code: u16,
        /// HTTP reason phrase.
        reason: &'static str,
    },
    /// A complete OpenAI-compatible SSE response.
    Sse(String),
}

/// One HTTP request observed by the scripted server.
struct ObservedRequest {
    /// Time the server finished reading the request.
    at: Instant,
    /// Raw request body, retained for boundary assertions.
    #[allow(dead_code)]
    body: String,
}

/// Scripted HTTP server that records every request and its arrival time.
struct ScriptedServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<Vec<ObservedRequest>>,
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

    fn finish(self) -> Vec<ObservedRequest> {
        self.stop.store(true, Ordering::SeqCst);
        self.handle.join().expect("mock server should finish")
    }
}

/// Accepts requests until stopped, answering from the plan queue.
fn serve(listener: TcpListener, stop: Arc<AtomicBool>, plans: Vec<Plan>) -> Vec<ObservedRequest> {
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
                    requests.push(ObservedRequest {
                        at: Instant::now(),
                        body,
                    });
                    let response = match plans.pop_front() {
                        Some(Plan::Status { code, reason }) => {
                            format!(
                                "HTTP/1.1 {code} {reason}\r\nContent-Length: 0\r\n\
                                 Connection: close\r\n\r\n"
                            )
                        }
                        Some(Plan::Sse(response)) => response,
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

/// One CLI run and the requests the mock server observed.
struct CliRun {
    output: Output,
    requests: Vec<ObservedRequest>,
    pid: u32,
}

impl CliRun {
    /// Session id allocated by the CLI for its first one-shot turn.
    fn session(&self) -> SessionId {
        self.session_at(0)
    }

    /// Session id allocated for the given zero-based REPL turn.
    fn session_at(&self, turn: usize) -> SessionId {
        SessionId::new(format!("cli-{}-{turn}", self.pid))
    }
}

/// Runs one CLI prompt against a scripted server.
fn run_cli(workspace: &Path, config: &Path, prompt: &str, plans: Vec<Plan>) -> CliRun {
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
    CliRun {
        output,
        requests,
        pid,
    }
}

/// Runs the CLI REPL with scripted stdin against a scripted server.
fn run_cli_repl(workspace: &Path, config: &Path, lines: &[&str], plans: Vec<Plan>) -> CliRun {
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
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should start");

    let pid = child.id();
    {
        let stdin = child.stdin.take().expect("stdin should be piped");
        let mut stdin = stdin;
        for line in lines {
            writeln!(stdin, "{line}").expect("REPL input should be written");
        }
    }
    let status = wait_with_timeout(&mut child);
    let mut output = child
        .wait_with_output()
        .expect("CLI output should be collected");
    output.status = status;
    let requests = server.finish();
    CliRun {
        output,
        requests,
        pid,
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

fn enabled_config() -> &'static str {
    "[events]\npath = \"events.sqlite\"\n\n\
     [reliability]\n\
     enabled = true\n\
     max_attempts = 3\n\
     initial_backoff_ms = 1\n\
     max_backoff_ms = 4\n\
     request_timeout_ms = 5000\n\
     stream_idle_timeout_ms = 5000\n"
}

fn disabled_config() -> &'static str {
    "[events]\npath = \"events.sqlite\"\n"
}

/// Reliability config with one attempt and a circuit that stays open.
fn circuit_config(max_attempts: u32, threshold: u32) -> String {
    format!(
        "[events]\npath = \"events.sqlite\"\n\n\
         [reliability]\n\
         enabled = true\n\
         max_attempts = {max_attempts}\n\
         initial_backoff_ms = 1\n\
         max_backoff_ms = 4\n\
         request_timeout_ms = 5000\n\
         stream_idle_timeout_ms = 5000\n\
         circuit_failure_threshold = {threshold}\n\
         circuit_cooldown_ms = 60000\n"
    )
}

/// Reliability config with one attempt and an explicit request rate.
fn rate_limit_config(requests_per_minute: u32, burst: u32) -> String {
    format!(
        "[events]\npath = \"events.sqlite\"\n\n\
         [reliability]\n\
         enabled = true\n\
         max_attempts = 1\n\
         initial_backoff_ms = 1\n\
         max_backoff_ms = 4\n\
         request_timeout_ms = 5000\n\
         stream_idle_timeout_ms = 5000\n\
         requests_per_minute = {requests_per_minute}\n\
         burst = {burst}\n"
    )
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

#[test]
fn reliability_retries_two_server_errors_and_completes_once() {
    let temp = TempDir::new("retry-success");
    let config = temp.path.join("harness.toml");
    write_config(&config, enabled_config());

    let run = run_cli(
        &temp.path,
        &config,
        "recover from transient failures",
        vec![
            Plan::Status {
                code: 500,
                reason: "Internal Server Error",
            },
            Plan::Status {
                code: 500,
                reason: "Internal Server Error",
            },
            Plan::Sse(text_response("recovered after retries")),
        ],
    );

    let stdout = String::from_utf8_lossy(&run.output.stdout);
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(
        run.output.status.success(),
        "CLI failed with {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        run.output.status
    );
    assert_eq!(
        run.requests.len(),
        3,
        "expected exactly three provider requests\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let events = read_events(&temp.path.join("events.sqlite"), &run.session());
    let deltas: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ModelDelta { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        deltas,
        vec!["recovered after retries"],
        "the failed attempts must not duplicate text"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            AgentEvent::SessionEnded {
                reason: SessionEndReason::Completed,
                ..
            }
        )),
        "the session must end exactly once as Completed: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::SessionEnded { .. }))
            .count(),
        1
    );
}

#[test]
fn reliability_does_not_retry_a_client_error() {
    let temp = TempDir::new("client-error");
    let config = temp.path.join("harness.toml");
    write_config(&config, enabled_config());

    let run = run_cli(
        &temp.path,
        &config,
        "this request is unauthorized",
        vec![
            Plan::Status {
                code: 401,
                reason: "Unauthorized",
            },
            Plan::Sse(text_response("must not be reached")),
        ],
    );

    let stdout = String::from_utf8_lossy(&run.output.stdout);
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(
        !run.output.status.success(),
        "401 must fail the CLI\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        run.requests.len(),
        1,
        "401 must not be retried\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("401"),
        "the original status must be visible in stderr:\n{stderr}"
    );

    let events = read_events(&temp.path.join("events.sqlite"), &run.session());
    assert!(
        events.iter().any(|event| matches!(
            event,
            AgentEvent::SessionEnded {
                reason: SessionEndReason::Error,
                ..
            }
        )),
        "the session must end as Error: {events:?}"
    );
}

#[test]
fn omitted_reliability_section_keeps_single_request_behavior() {
    let temp = TempDir::new("default-disabled");
    let config = temp.path.join("harness.toml");
    write_config(&config, disabled_config());

    let run = run_cli(
        &temp.path,
        &config,
        "do not retry by default",
        vec![
            Plan::Status {
                code: 500,
                reason: "Internal Server Error",
            },
            Plan::Sse(text_response("must not be reached")),
        ],
    );

    let stdout = String::from_utf8_lossy(&run.output.stdout);
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(
        !run.output.status.success(),
        "the first 500 must fail without retries\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        run.requests.len(),
        1,
        "omitting [reliability] must keep the single-request behavior\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn open_circuit_rejects_the_next_turn_without_an_http_request() {
    let temp = TempDir::new("circuit-open");
    let config = temp.path.join("harness.toml");
    write_config(&config, &circuit_config(1, 2));

    let run = run_cli_repl(
        &temp.path,
        &config,
        &[
            "first failure",
            "second failure",
            "third must fail fast",
            "exit",
        ],
        vec![
            Plan::Status {
                code: 503,
                reason: "Service Unavailable",
            },
            Plan::Status {
                code: 503,
                reason: "Service Unavailable",
            },
        ],
    );

    let stdout = String::from_utf8_lossy(&run.output.stdout);
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(
        run.output.status.success(),
        "the REPL itself should exit cleanly\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        run.requests.len(),
        2,
        "the third turn must not reach the provider\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("circuit breaker open"),
        "the fast failure must be visible:\n{stderr}"
    );

    let events = read_events(&temp.path.join("events.sqlite"), &run.session_at(2));
    assert!(
        events.iter().any(|event| matches!(
            event,
            AgentEvent::SessionEnded {
                reason: SessionEndReason::Error,
                ..
            }
        )),
        "the third turn must end as Error: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ModelDelta { .. })),
        "an open circuit must not emit model output: {events:?}"
    );
}

#[test]
fn client_errors_do_not_open_the_circuit_between_turns() {
    let temp = TempDir::new("circuit-4xx");
    let config = temp.path.join("harness.toml");
    write_config(&config, &circuit_config(1, 2));

    let run = run_cli_repl(
        &temp.path,
        &config,
        &[
            "first unauthorized",
            "second unauthorized",
            "this one should succeed",
            "exit",
        ],
        vec![
            Plan::Status {
                code: 401,
                reason: "Unauthorized",
            },
            Plan::Status {
                code: 401,
                reason: "Unauthorized",
            },
            Plan::Sse(text_response("client errors do not open the circuit")),
        ],
    );

    let stdout = String::from_utf8_lossy(&run.output.stdout);
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(
        run.output.status.success(),
        "the REPL itself should exit cleanly\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        run.requests.len(),
        3,
        "two 401 turns must not open the circuit\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let events = read_events(&temp.path.join("events.sqlite"), &run.session_at(2));
    let deltas: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ModelDelta { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(deltas, vec!["client errors do not open the circuit"]);
    assert!(
        events.iter().any(|event| matches!(
            event,
            AgentEvent::SessionEnded {
                reason: SessionEndReason::Completed,
                ..
            }
        )),
        "the third turn must complete: {events:?}"
    );
}

#[test]
fn requests_per_minute_spaces_repl_turns_by_the_refill_rate() {
    let temp = TempDir::new("rate-limit");
    let config = temp.path.join("harness.toml");
    write_config(&config, &rate_limit_config(60, 1));

    let run = run_cli_repl(
        &temp.path,
        &config,
        &["first turn", "second turn", "exit"],
        vec![
            Plan::Sse(text_response("first reply")),
            Plan::Sse(text_response("second reply")),
        ],
    );

    let stdout = String::from_utf8_lossy(&run.output.stdout);
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(
        run.output.status.success(),
        "the REPL itself should exit cleanly\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        run.requests.len(),
        2,
        "both turns must reach the provider\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // 60 requests per minute with a burst of one means the second turn needs
    // a refilled token. The lower bound is deliberately loose.
    let gap = run.requests[1].at.duration_since(run.requests[0].at);
    assert!(
        gap >= Duration::from_millis(800),
        "the second turn must wait for a refilled token, but the gap was {gap:?}"
    );

    for (turn, expected) in [(0, "first reply"), (1, "second reply")] {
        let events = read_events(&temp.path.join("events.sqlite"), &run.session_at(turn));
        let deltas: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ModelDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(deltas, vec![expected], "turn {turn}: {events:?}");
        assert!(
            events.iter().any(|event| matches!(
                event,
                AgentEvent::SessionEnded {
                    reason: SessionEndReason::Completed,
                    ..
                }
            )),
            "turn {turn} must complete: {events:?}"
        );
    }
}

#[test]
fn zero_requests_per_minute_behaves_like_disabled_reliability() {
    let temp = TempDir::new("rate-limit-disabled");
    let config = temp.path.join("harness.toml");
    write_config(&config, &rate_limit_config(0, 0));

    let run = run_cli_repl(
        &temp.path,
        &config,
        &["first turn", "second turn", "exit"],
        vec![
            Plan::Sse(text_response("first reply")),
            Plan::Sse(text_response("second reply")),
        ],
    );

    let stdout = String::from_utf8_lossy(&run.output.stdout);
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(
        run.output.status.success(),
        "the REPL itself should exit cleanly\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        run.requests.len(),
        2,
        "a zero rpm must not reject requests\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // A zero rate constructs no bucket, so the turns are not spaced by one
    // refill interval. The upper bound is deliberately loose.
    let gap = run.requests[1].at.duration_since(run.requests[0].at);
    assert!(
        gap < Duration::from_millis(800),
        "a zero rpm must not delay requests, but the gap was {gap:?}"
    );

    let events = read_events(&temp.path.join("events.sqlite"), &run.session_at(1));
    assert!(
        events.iter().any(|event| matches!(
            event,
            AgentEvent::SessionEnded {
                reason: SessionEndReason::Completed,
                ..
            }
        )),
        "the second turn must complete: {events:?}"
    );
}

/// Parses a request body only to prove it is valid JSON where assertions need it.
#[allow(dead_code)]
fn request_json(body: &str) -> Value {
    serde_json::from_str(body).expect("request body should be JSON")
}
