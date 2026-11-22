//! End-to-end acceptance for REPL configuration hot reload.
//!
//! The scenarios drive the compiled binary against a scripted HTTP server.
//! They pin the observable boundary: a reloadable change modifies the next
//! turn's system prompt and tool schema, immutable sections reject the whole
//! reload, invalid TOML leaves the previous snapshot active, and one-shot runs
//! never start the watcher.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// Overall budget for one CLI child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock server to observe requests.
const SERVER_TIMEOUT: Duration = Duration::from_secs(20);
/// Time allowed for one watcher poll to observe a file change.
const POLL_SETTLE: Duration = Duration::from_millis(1600);
/// Marker rendered by the memory guide message.
const MEMORY_TITLE: &str = "[记忆参考";

/// Initial configuration: default native allowlist, delegation disabled.
const INITIAL_CONFIG: &str = "\
[budget]
max_rounds = 4
tool_timeout_ms = 9000

[policy]
allowed_tools = [\"bash\", \"read_file\", \"write_file\"]

[tools.bash]
enabled = true

[memory]
enabled = true
path = \".jessica/memory.db\"
recall_limit = 5
max_inject_bytes = 4096
embedder = \"deterministic\"
write_policy = \"completed\"
";

/// Reloadable change: the allowlist gains `delegate_task` and delegation is
/// enabled, so both the system prompt and the tool schema must change. The
/// immutable memory section is byte-identical, so the reload is accepted.
const RELOADED_CONFIG: &str = "\
[budget]
max_rounds = 6
tool_timeout_ms = 9000

[policy]
allowed_tools = [\"bash\", \"read_file\", \"write_file\", \"delegate_task\"]

[delegation]
enabled = true

[tools.bash]
enabled = true

[memory]
enabled = true
path = \".jessica/memory.db\"
recall_limit = 5
max_inject_bytes = 4096
embedder = \"deterministic\"
write_policy = \"completed\"
";

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
            "jessica-cli-hot-reload-e2e-{label}-{}-{unique}",
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

/// Spawns one one-shot CLI turn against `address`.
fn spawn_one_shot(workspace: &Path, config: &Path, address: SocketAddr, prompt: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_jessica"))
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
        .expect("CLI should start")
}

/// Seeds memory through a real completed turn, using the configured embedder.
fn seed_memory(workspace: &Path, config: &Path) {
    let server = ScriptedServer::start(vec![Plan::Sse(text_response(
        "The launch code is ORANGE-7.",
    ))]);
    let mut child = spawn_one_shot(
        workspace,
        config,
        server.address,
        "Remember this fact: the launch code is ORANGE-7.",
    );
    let status = wait_with_timeout(&mut child);
    let output = collect_output(child, status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "memory seed turn failed: {}\nstderr:\n{stderr}",
        output.status
    );
    assert_eq!(server.finish().len(), 1);
}

/// One scripted server action.
enum Plan {
    /// A complete OpenAI-compatible SSE response.
    Sse(String),
    /// A response preceded by a configuration rewrite.
    SseWithMutation {
        /// HTTP response body.
        response: String,
        /// Configuration file to rewrite.
        path: PathBuf,
        /// New configuration contents.
        body: String,
        /// Delay after the rewrite and before the response.
        wait: Duration,
    },
}

/// Scripted HTTP server that records every request body.
struct ScriptedServer {
    /// Address the CLI should use as its provider base URL.
    address: SocketAddr,
    /// Flag that stops the accept loop.
    stop: Arc<AtomicBool>,
    /// Number of requests read so far.
    requests_seen: Arc<AtomicUsize>,
    /// Number of configuration mutations applied so far.
    mutations_done: Arc<AtomicUsize>,
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
        let mutations_done = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&stop);
        let seen = Arc::clone(&requests_seen);
        let mutated = Arc::clone(&mutations_done);
        let handle = thread::spawn(move || serve(listener, flag, seen, mutated, plans));
        Self {
            address,
            stop,
            requests_seen,
            mutations_done,
            handle,
        }
    }

    /// Waits until at least `count` requests have been read.
    fn wait_for_requests(&self, count: usize) {
        wait_until(
            || self.requests_seen.load(Ordering::SeqCst) >= count,
            "requests",
        );
    }

    /// Waits until at least `count` configuration mutations have been applied.
    fn wait_for_mutations(&self, count: usize) {
        wait_until(
            || self.mutations_done.load(Ordering::SeqCst) >= count,
            "mutations",
        );
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
    mutations_done: Arc<AtomicUsize>,
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
                    let response = match plans.pop_front() {
                        Some(Plan::Sse(response)) => response,
                        Some(Plan::SseWithMutation {
                            response,
                            path,
                            body,
                            wait,
                        }) => {
                            write_config(&path, &body);
                            mutations_done.fetch_add(1, Ordering::SeqCst);
                            if !wait.is_zero() {
                                thread::sleep(wait);
                            }
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

/// Waits until `condition` holds or the server budget expires.
fn wait_until(mut condition: impl FnMut() -> bool, label: &str) {
    let deadline = Instant::now() + SERVER_TIMEOUT;
    while !condition() {
        if Instant::now() >= deadline {
            panic!("timed out waiting for {label}");
        }
        thread::sleep(Duration::from_millis(10));
    }
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

/// Atomically replaces the configuration file contents.
///
/// Writing a sibling file and renaming it avoids the watcher reading a
/// half-written TOML document.
fn write_config(path: &Path, body: &str) {
    let temporary = path.with_extension("toml.tmp");
    std::fs::write(&temporary, body).expect("temporary configuration should be written");
    std::fs::rename(&temporary, path).expect("configuration should be replaced");
}

/// Returns one SSE response carrying `content` and a stop finish reason.
fn text_response(content: &str) -> String {
    let payloads = [content_chunk(content), finish_chunk()];
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

/// Returns one SSE chunk that finishes the response.
fn finish_chunk() -> String {
    json!({
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
    })
    .to_string()
}

/// Returns one SSE chunk carrying assistant text.
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

/// Parses one provider request body.
fn request_json(body: &str) -> Value {
    serde_json::from_str(body).expect("provider request should be JSON")
}

/// Returns the role sequence of one provider request.
fn roles(request: &Value) -> Vec<&str> {
    request["messages"]
        .as_array()
        .expect("messages should be an array")
        .iter()
        .map(|message| {
            message["role"]
                .as_str()
                .expect("message role should be a string")
        })
        .collect()
}

/// Returns the system prompt of one provider request.
fn system_prompt(request: &Value) -> &str {
    request["messages"]
        .as_array()
        .expect("messages should be an array")
        .first()
        .expect("the request must include a system message")["content"]
        .as_str()
        .expect("system content should be a string")
}

/// Returns the tool schema names of one provider request.
fn tool_names(request: &Value) -> Vec<&str> {
    request["tools"]
        .as_array()
        .expect("tools should be an array")
        .iter()
        .map(|tool| {
            tool["function"]["name"]
                .as_str()
                .expect("tool name should be a string")
        })
        .collect()
}

#[test]
fn repl_reload_changes_system_prompt_and_tool_schema_between_turns() {
    let temp = TempDir::new("repl");
    let config = temp.path.join("harness.toml");
    write_config(&config, INITIAL_CONFIG);
    seed_memory(&temp.path, &config);
    let server = ScriptedServer::start(vec![
        Plan::SseWithMutation {
            response: text_response("first answer"),
            path: config.clone(),
            body: RELOADED_CONFIG.to_owned(),
            wait: Duration::ZERO,
        },
        Plan::Sse(text_response("second answer")),
    ]);
    let mut child = spawn_repl(&temp.path, &config, server.address);
    let mut stdin = child.stdin.take().expect("stdin should be piped");

    send_line(&mut stdin, "What is the launch code?");
    server.wait_for_mutations(1);
    // The watcher polls once per second; wait for the next poll to publish.
    thread::sleep(POLL_SETTLE);
    send_line(&mut stdin, "What is the launch code now?");
    send_line(&mut stdin, "exit");
    drop(stdin);

    let status = wait_with_timeout(&mut child);
    let output = collect_output(child, status);
    let requests = server.finish();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "CLI failed with {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert_eq!(
        requests.len(),
        2,
        "each text turn must make exactly one provider request\nstderr:\n{stderr}"
    );

    let first = request_json(&requests[0]);
    let second = request_json(&requests[1]);
    assert!(
        system_prompt(&first).contains("bash, read_file, write_file"),
        "the first turn must use the startup allowlist: {}",
        system_prompt(&first)
    );
    assert!(!system_prompt(&first).contains("delegate_task"));
    assert_eq!(
        tool_names(&first),
        vec!["bash", "read_file", "write_file"],
        "delegation was disabled on the first turn"
    );

    assert!(
        system_prompt(&second).contains("delegate_task"),
        "the second turn's system prompt must use the reloaded allowlist: {}",
        system_prompt(&second)
    );
    assert_eq!(
        tool_names(&second),
        vec!["bash", "delegate_task", "read_file", "write_file"],
        "the second turn's schema must include the newly enabled tool"
    );
    assert_eq!(
        roles(&first),
        vec!["system", "system", "user"],
        "the first turn must contain one system prompt and one memory guide"
    );
    assert_eq!(
        roles(&second),
        vec!["system", "system", "user"],
        "hot reload must not add, remove, or reorder messages"
    );
    let memory_guides = second["messages"]
        .as_array()
        .expect("messages should be an array")
        .iter()
        .filter(|message| {
            message["content"]
                .as_str()
                .is_some_and(|content| content.contains(MEMORY_TITLE))
        })
        .count();
    assert_eq!(
        memory_guides, 1,
        "hot reload must still recall memory exactly once per turn"
    );
    assert!(
        second["messages"][1]["content"]
            .as_str()
            .expect("memory content should be a string")
            .contains("ORANGE-7")
    );
    assert_eq!(
        second["messages"][2]["content"],
        "What is the launch code now?"
    );
    assert!(
        stderr.contains("[config] reloaded"),
        "a successful reload must be logged\nstderr:\n{stderr}"
    );
}

#[test]
fn repl_reload_can_enable_bash_for_the_next_turn() {
    const DISABLED: &str = "\
[budget]
max_rounds = 4
tool_timeout_ms = 9000

[policy]
allowed_tools = [\"bash\", \"read_file\", \"write_file\"]
";
    const ENABLED: &str = "\
[budget]
max_rounds = 4
tool_timeout_ms = 9000

[policy]
allowed_tools = [\"bash\", \"read_file\", \"write_file\"]

[tools.bash]
enabled = true
";

    let temp = TempDir::new("enable-gate");
    let config = temp.path.join("harness.toml");
    write_config(&config, DISABLED);
    let server = ScriptedServer::start(vec![
        Plan::SseWithMutation {
            response: text_response("first answer"),
            path: config.clone(),
            body: ENABLED.to_owned(),
            wait: Duration::ZERO,
        },
        Plan::Sse(text_response("second answer")),
    ]);
    let mut child = spawn_repl(&temp.path, &config, server.address);
    let mut stdin = child.stdin.take().expect("stdin should be piped");

    send_line(&mut stdin, "first question");
    server.wait_for_mutations(1);
    thread::sleep(POLL_SETTLE);
    send_line(&mut stdin, "second question");
    send_line(&mut stdin, "exit");
    drop(stdin);

    let status = wait_with_timeout(&mut child);
    let output = collect_output(child, status);
    let requests = server.finish();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "CLI failed with {}\nstderr:\n{stderr}",
        output.status
    );
    assert_eq!(
        requests.len(),
        2,
        "each text turn must make exactly one provider request\nstderr:\n{stderr}"
    );

    let first = request_json(&requests[0]);
    let second = request_json(&requests[1]);
    assert_eq!(
        tool_names(&first),
        vec!["read_file", "write_file"],
        "bash must stay hidden until the reload"
    );
    assert!(
        !system_prompt(&first).contains("bash"),
        "the first prompt must not advertise disabled bash: {}",
        system_prompt(&first)
    );
    assert_eq!(
        tool_names(&second),
        vec!["bash", "read_file", "write_file"],
        "the reloaded enable gate must expose bash"
    );
    assert!(
        system_prompt(&second).contains("bash"),
        "the next prompt must advertise the enabled bash tool"
    );
    assert!(
        stderr.contains("[config] reloaded"),
        "the enable gate is nested in [tools] and must reload\nstderr:\n{stderr}"
    );
}

#[test]
fn immutable_and_invalid_changes_keep_the_old_config_and_repl_alive() {
    let temp = TempDir::new("rejected");
    let config = temp.path.join("harness.toml");
    write_config(&config, INITIAL_CONFIG);
    let server = ScriptedServer::start(vec![
        Plan::Sse(text_response("first answer")),
        Plan::Sse(text_response("second answer")),
    ]);
    let mut child = spawn_repl(&temp.path, &config, server.address);
    let mut stdin = child.stdin.take().expect("stdin should be piped");

    send_line(&mut stdin, "first question");
    server.wait_for_requests(1);
    thread::sleep(Duration::from_millis(300));

    // An immutable section rejects the entire document, even though the
    // reloadable budget section in the same file is also valid.
    write_config(
        &config,
        "\
[budget]
max_rounds = 12
tool_timeout_ms = 9000

[policy]
allowed_tools = [\"bash\", \"read_file\", \"write_file\"]

[events]
path = \"events.sqlite\"
",
    );
    thread::sleep(POLL_SETTLE);

    // A malformed document must be ignored without stopping the REPL.
    write_config(&config, "[budget\nmax_rounds = nope");
    thread::sleep(POLL_SETTLE);

    // Recovery: the next valid reloadable document is published.
    write_config(&config, RELOADED_CONFIG);
    thread::sleep(POLL_SETTLE);

    send_line(&mut stdin, "second question");
    send_line(&mut stdin, "exit");
    drop(stdin);

    let status = wait_with_timeout(&mut child);
    let output = collect_output(child, status);
    let requests = server.finish();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "the REPL must stay alive across rejected reloads: {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert_eq!(
        requests.len(),
        2,
        "the REPL must still accept a second turn\nstderr:\n{stderr}"
    );
    let second = request_json(&requests[1]);
    assert!(
        system_prompt(&second).contains("delegate_task"),
        "the recovered reload must reach the second turn: {}",
        system_prompt(&second)
    );
    assert_eq!(
        tool_names(&second),
        vec!["bash", "delegate_task", "read_file", "write_file"]
    );
    assert!(
        stderr.contains("`events` is not hot-reloadable"),
        "the rejection log must name the immutable field\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("config reload failed"),
        "a malformed file must be logged\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("[config] reloaded"),
        "the later valid file must still be published\nstderr:\n{stderr}"
    );
}

#[test]
fn one_shot_mode_never_starts_the_config_watcher() {
    let temp = TempDir::new("one-shot");
    let config = temp.path.join("harness.toml");
    write_config(&config, INITIAL_CONFIG);
    // The server rewrites the config while the single provider call is in
    // flight. A running watcher would observe it and log a reload.
    let server = ScriptedServer::start(vec![Plan::SseWithMutation {
        response: text_response("one-shot answer"),
        path: config.clone(),
        body: RELOADED_CONFIG.to_owned(),
        wait: Duration::from_millis(1200),
    }]);
    let mut child = spawn_one_shot(&temp.path, &config, server.address, "one-shot question");

    let status = wait_with_timeout(&mut child);
    let output = collect_output(child, status);
    let requests = server.finish();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "one-shot CLI failed with {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert_eq!(requests.len(), 1, "one-shot mode makes one provider call");
    assert!(
        !stderr.contains("[config] reloaded"),
        "one-shot mode must not start the watcher\nstderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("config reload failed"),
        "one-shot mode must not parse the file a second time\nstderr:\n{stderr}"
    );
    let request = request_json(&requests[0]);
    assert!(
        !system_prompt(&request).contains("delegate_task"),
        "the one-shot turn must keep its startup snapshot"
    );
    assert_eq!(
        tool_names(&request),
        vec!["bash", "read_file", "write_file"]
    );
}
