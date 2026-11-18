//! End-to-end acceptance for the `replay` subcommand.
//!
//! The test records a completed session in SQLite, runs the real CLI twice with
//! no API key or network endpoint, and requires the two JSONL files to be
//! byte-identical and to reproduce the recorded runtime events. The replayed
//! output can only come from the event store: no provider is configured and no
//! tool implementation is registered.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId, ToolCall, ToolResult};
use jessica_memory::{EventStore, Timestamp};
use serde_json::json;

/// Overall budget for one child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);

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
fn replay_writes_byte_identical_jsonl_without_a_provider_or_tools() {
    let temp = TempDir::new("replay");
    let database = temp.path.join("events.sqlite");
    let session = SessionId::new("cli-replay-session");
    let recorded = seed_completed_session(&database, &session);

    let config = temp.path.join("harness.toml");
    std::fs::write(
        &config,
        format!("[events]\npath = \"{}\"\n", database.display()),
    )
    .expect("config should be written");

    let first = temp.path.join("a.jsonl");
    let second = temp.path.join("b.jsonl");
    for output_path in [&first, &second] {
        let output = run_replay(&config, &session, output_path);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "replay failed with {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status
        );
        assert!(
            stdout.contains("wrote 6 events"),
            "replay did not report its event count:\n{stdout}"
        );
    }

    let first_bytes = std::fs::read(&first).expect("first JSONL should exist");
    let second_bytes = std::fs::read(&second).expect("second JSONL should exist");
    assert_eq!(
        first_bytes, second_bytes,
        "two replays of the same session must produce byte-identical JSONL"
    );

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
        String::from_utf8(first_bytes).expect("JSONL output should be UTF-8"),
        expected,
        "replay must reproduce the recorded runtime event stream"
    );
}

/// Runs the real CLI in replay mode without any provider configuration.
fn run_replay(config: &Path, session: &SessionId, output_path: &Path) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace_root())
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

/// Writes a completed session with one tool round and one final text round.
fn seed_completed_session(database: &Path, session: &SessionId) -> Vec<AgentEvent> {
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
    let events = vec![
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
    events
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
