//! End-to-end acceptance for `jessica trace verify`.
//!
//! Every test seeds a real SQLite event store, runs the real CLI binary without
//! a provider or network endpoint, and asserts concrete report statuses and
//! problem kinds. The verification path must open the store read-only: it may
//! not create a missing database or migrate a database without the trace
//! schema.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use jessica_core::trace::{TraceEvent, TraceGapReason, TracePayload, TraceTurnMode, TurnId};
use jessica_core::types::{
    AgentEvent, Message, PermissionDecision, SessionEndReason, SessionId, TokenUsage, ToolCall,
    ToolResult,
};
use jessica_memory::{EventStore, Timestamp};
use serde_json::{Value, json};

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
            "jessica-trace-{label}-{}-{unique}",
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
fn json_report_distinguishes_ok_open_gap_and_corrupt_turns() {
    let temp = TempDir::new("json");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_valid_turn(&mut store, "turn-ok", "session-ok", 1_000);
    seed_open_turn(&mut store, "turn-open", "session-open", 2_000);
    seed_gap_turn(&mut store, "turn-gap", "session-gap", 3_000);
    drop(store);
    seed_corrupt_head(&database, "turn-corrupt", 4_000);
    let config = write_config(&temp, &database);

    let output = run_verify(&config, &["--all", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "invalid and open traces must fail without --allow-open\nstderr:\n{stderr}"
    );

    let report = json_report(&output);
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["summary"]["turns"], 4);
    assert_eq!(report["summary"]["ok"], 1);
    assert_eq!(report["summary"]["open"], 1);
    assert_eq!(report["summary"]["invalid"], 2);
    assert_eq!(report["summary"]["problems"], 2);

    let ok = report_turn(&report, "turn-ok");
    assert_eq!(ok["status"], "ok");
    assert_eq!(ok["schema_version"], 2);
    assert_eq!(ok["entries"], 10);
    assert_eq!(
        ok["problems"]
            .as_array()
            .expect("problems must be an array"),
        &Vec::<Value>::new()
    );

    let open = report_turn(&report, "turn-open");
    assert_eq!(open["status"], "open");
    assert_eq!(open["schema_version"], 2);
    assert_eq!(open["entries"], 2);

    let gap = report_turn(&report, "turn-gap");
    assert_eq!(gap["status"], "invalid");
    assert_eq!(problem_kinds(gap), vec!["trace_gap"]);

    let corrupt = report_turn(&report, "turn-corrupt");
    assert_eq!(corrupt["status"], "invalid");
    assert!(corrupt["schema_version"].is_null());
    assert!(corrupt["entries"].is_null());
    assert_eq!(problem_kinds(corrupt), vec!["trace_structure"]);

    // --allow-open relaxes only the open status. The gap and the corrupt head
    // must keep the run failing.
    let allowed = run_verify(&config, &["--all", "--json", "--allow-open"]);
    assert!(
        !allowed.status.success(),
        "--allow-open must never accept an invalid trace"
    );
    let allowed_report = json_report(&allowed);
    assert_eq!(allowed_report["summary"]["invalid"], 2);
    assert_eq!(allowed_report["summary"]["open"], 1);
}

#[test]
fn allow_open_accepts_a_clean_open_trace() {
    let temp = TempDir::new("allow-open");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_valid_turn(&mut store, "turn-ok", "session-ok", 1_000);
    seed_open_turn(&mut store, "turn-open", "session-open", 2_000);
    drop(store);
    let config = write_config(&temp, &database);

    let rejected = run_verify(&config, &["--all"]);
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(!rejected.status.success());
    assert!(
        stderr.contains("--allow-open"),
        "the failure should explain how to accept open traces:\n{stderr}"
    );

    let single = run_verify(&config, &["--turn", "turn-open"]);
    let stderr = String::from_utf8_lossy(&single.stderr);
    assert!(!single.status.success());
    assert!(
        !stderr.contains("--allow-open"),
        "single-turn verification must not suggest a flag it rejects:\n{stderr}"
    );

    let allowed = run_verify(&config, &["--all", "--allow-open"]);
    let stdout = String::from_utf8_lossy(&allowed.stdout);
    let stderr = String::from_utf8_lossy(&allowed.stderr);
    assert!(
        allowed.status.success(),
        "an open trace with no findings must pass under --allow-open\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("summary: 2 turns: 1 ok, 1 open, 0 invalid, 0 problems"),
        "unexpected summary:\n{stdout}"
    );
    assert!(
        stderr.is_empty(),
        "success must not write to stderr:\n{stderr}"
    );
}

#[test]
fn turn_selection_verifies_one_trace_and_unknown_turns_are_errors() {
    let temp = TempDir::new("turn");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_valid_turn(&mut store, "turn-ok", "session-ok", 1_000);
    seed_gap_turn(&mut store, "turn-gap", "session-gap", 2_000);
    drop(store);
    let config = write_config(&temp, &database);

    let ok = run_verify(&config, &["--turn", "turn-ok", "--json"]);
    assert!(
        ok.status.success(),
        "a complete trace must verify:\n{}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let report = json_report(&ok);
    assert_eq!(
        report["turns"]
            .as_array()
            .expect("turns must be an array")
            .len(),
        1
    );
    assert_eq!(report_turn(&report, "turn-ok")["status"], "ok");

    let gap = run_verify(&config, &["--turn", "turn-gap", "--json"]);
    assert!(!gap.status.success());
    let report = json_report(&gap);
    assert_eq!(report_turn(&report, "turn-gap")["status"], "invalid");
    assert_eq!(
        problem_kinds(report_turn(&report, "turn-gap")),
        vec!["trace_gap"]
    );

    let missing = run_verify(&config, &["--turn", "turn-missing"]);
    let stderr = String::from_utf8_lossy(&missing.stderr);
    assert!(!missing.status.success());
    assert!(
        empty_stdout(&missing),
        "an unknown turn must not produce a report"
    );
    assert!(
        stderr.contains("turn-missing") && stderr.contains("not found"),
        "unknown turn error must name the turn:\n{stderr}"
    );
}

#[test]
fn an_empty_store_is_a_successful_zero_turn_report() {
    let temp = TempDir::new("empty");
    let database = temp.path.join("events.sqlite");
    drop(EventStore::open(&database).expect("event store should open"));
    let config = write_config(&temp, &database);

    let output = run_verify(&config, &["--all", "--json"]);
    assert!(
        output.status.success(),
        "an empty store has no failing turn:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = json_report(&output);
    assert_eq!(report["summary"]["turns"], 0);
    assert_eq!(report["summary"]["ok"], 0);
    assert_eq!(report["summary"]["open"], 0);
    assert_eq!(report["summary"]["invalid"], 0);
    assert_eq!(report["summary"]["problems"], 0);
    assert_eq!(
        report["turns"].as_array().expect("turns must be an array"),
        &Vec::<Value>::new()
    );
}

#[test]
fn verification_never_creates_the_event_store() {
    let temp = TempDir::new("missing");
    let database = temp.path.join("missing.sqlite");
    let config = write_config(&temp, &database);

    let output = run_verify(&config, &["--all", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        !database.exists(),
        "verification must not create the database it audits"
    );
    assert!(
        stderr.contains("failed to open event store"),
        "expected an open failure:\n{stderr}"
    );
}

#[test]
fn a_database_without_the_trace_schema_is_rejected_untouched() {
    let temp = TempDir::new("no-schema");
    let database = temp.path.join("empty.sqlite");
    std::fs::write(&database, b"").expect("empty database should be written");
    let config = write_config(&temp, &database);

    let output = run_verify(&config, &["--all"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("no trace schema"),
        "expected a schema rejection:\n{stderr}"
    );
    let length = std::fs::metadata(&database)
        .expect("the database file must still exist")
        .len();
    assert_eq!(length, 0, "verification must not migrate the database");
}

#[test]
fn trace_requires_an_event_store_path() {
    let temp = TempDir::new("no-events");
    let config = temp.path.join("harness.toml");
    std::fs::write(&config, "").expect("config should be written");

    let output = run_verify(&config, &["--all"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("requires an `[events]` path"),
        "expected a configuration error:\n{stderr}"
    );
}

/// Runs the real CLI in trace-verification mode without provider configuration.
fn run_verify(config: &Path, arguments: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace_root())
        .env_remove("JESSICA_PROVIDER")
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("OPENAI_MODEL")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_BASE_URL")
        .env_remove("ANTHROPIC_MODEL")
        .arg("--config")
        .arg(config)
        .arg("trace")
        .arg("verify")
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

fn write_config(temp: &TempDir, database: &Path) -> PathBuf {
    let config = temp.path.join("harness.toml");
    std::fs::write(
        &config,
        format!("[events]\npath = \"{}\"\n", database.display()),
    )
    .expect("config should be written");
    config
}

fn json_report(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout is not a JSON report: {error}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn report_turn<'a>(report: &'a Value, turn_id: &str) -> &'a Value {
    report["turns"]
        .as_array()
        .expect("turns must be an array")
        .iter()
        .find(|turn| turn["turn_id"].as_str() == Some(turn_id))
        .unwrap_or_else(|| panic!("turn `{turn_id}` is missing from the report"))
}

fn problem_kinds(turn: &Value) -> Vec<&str> {
    turn["problems"]
        .as_array()
        .expect("problems must be an array")
        .iter()
        .map(|problem| {
            problem["kind"]
                .as_str()
                .expect("every problem must have a string kind")
        })
        .collect()
}

fn empty_stdout(output: &Output) -> bool {
    output.stdout.is_empty()
}

/// Writes a complete trace that satisfies every verification layer.
fn seed_valid_turn(store: &mut EventStore, turn: &str, session_name: &str, base: i64) {
    let turn_id = TurnId::new(turn);
    let session = SessionId::new(session_name);
    append(
        store,
        &TraceEvent::new(
            turn_id.clone(),
            None,
            Some(session.clone()),
            TracePayload::TurnStarted {
                mode: TraceTurnMode::Fresh,
                messages: vec![Message::user("trace verification")],
            },
        ),
        None,
        base,
    );

    let session_started = AgentEvent::SessionStarted {
        session_id: session.clone(),
        messages: Vec::new(),
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, session_started.clone()),
        Some((&session, &session_started)),
        base + 1,
    );

    let round_started = AgentEvent::RoundStarted { round: 1 };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, round_started.clone()),
        Some((&session, &round_started)),
        base + 2,
    );

    let delta = AgentEvent::ModelDelta {
        text: "hello".to_owned(),
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, delta.clone()),
        Some((&session, &delta)),
        base + 3,
    );

    let round_finished = AgentEvent::RoundFinished {
        round: 1,
        usage: Some(TokenUsage::new(12, 3)),
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, round_finished.clone()),
        Some((&session, &round_finished)),
        base + 4,
    );

    let call = ToolCall::new("call-1", "read_file", json!({"path": "README.md"}));
    let tool_started = AgentEvent::ToolCallStarted { call: call.clone() };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, tool_started.clone()),
        Some((&session, &tool_started)),
        base + 5,
    );

    let permission = AgentEvent::PermissionDecided {
        call: call.clone(),
        decision: PermissionDecision::Allow,
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, permission),
        None,
        base + 6,
    );

    let tool_finished = AgentEvent::ToolCallFinished {
        result: ToolResult::success("call-1", "ok"),
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, tool_finished.clone()),
        Some((&session, &tool_finished)),
        base + 7,
    );

    let session_ended = AgentEvent::SessionEnded {
        session_id: session.clone(),
        reason: SessionEndReason::Completed,
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, session_ended.clone()),
        Some((&session, &session_ended)),
        base + 8,
    );

    append(
        store,
        &TraceEvent::new(
            turn_id,
            None,
            None,
            TracePayload::TurnEnded {
                reason: SessionEndReason::Completed,
            },
        ),
        None,
        base + 9,
    );
}

/// Writes a trace that stops after `session_started` and therefore has no
/// `turn_ended` marker.
fn seed_open_turn(store: &mut EventStore, turn: &str, session_name: &str, base: i64) {
    let turn_id = TurnId::new(turn);
    let session = SessionId::new(session_name);
    append(
        store,
        &TraceEvent::new(
            turn_id.clone(),
            None,
            Some(session.clone()),
            TracePayload::TurnStarted {
                mode: TraceTurnMode::Fresh,
                messages: vec![Message::user("open trace")],
            },
        ),
        None,
        base,
    );

    let session_started = AgentEvent::SessionStarted {
        session_id: session.clone(),
        messages: Vec::new(),
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, session_started.clone()),
        Some((&session, &session_started)),
        base + 1,
    );
}

/// Writes a closed trace whose recorder observed a gap.
fn seed_gap_turn(store: &mut EventStore, turn: &str, session_name: &str, base: i64) {
    let turn_id = TurnId::new(turn);
    let session = SessionId::new(session_name);
    append(
        store,
        &TraceEvent::new(
            turn_id.clone(),
            None,
            Some(session),
            TracePayload::TurnStarted {
                mode: TraceTurnMode::Fresh,
                messages: vec![Message::user("gap trace")],
            },
        ),
        None,
        base,
    );
    append(
        store,
        &TraceEvent::new(
            turn_id.clone(),
            None,
            None,
            TracePayload::Gap {
                skipped: 2,
                reason: TraceGapReason::BroadcastLagged,
            },
        ),
        None,
        base + 1,
    );
    append(
        store,
        &TraceEvent::new(
            turn_id,
            None,
            None,
            TracePayload::TurnEnded {
                reason: SessionEndReason::Completed,
            },
        ),
        None,
        base + 2,
    );
}

/// Inserts a trace head with no entries, which the read path rejects.
///
/// The public API cannot produce this state: its validation and append-only
/// triggers prevent it. Seeding it directly proves that one corrupt turn does
/// not stop an `--all` run.
fn seed_corrupt_head(database: &Path, turn: &str, created_at: i64) {
    let connection =
        rusqlite::Connection::open(database).expect("raw database connection should open");
    connection
        .execute(
            "INSERT INTO trace_heads (turn_id, last_seq, closed, created_at) \
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![turn, 1_i64, 1_i64, created_at],
        )
        .expect("corrupt trace head should be inserted");
}

fn traced_agent(turn: &TurnId, session: &SessionId, attempt: u32, event: AgentEvent) -> TraceEvent {
    TraceEvent::new(
        turn.clone(),
        Some(attempt),
        Some(session.clone()),
        TracePayload::Agent { event },
    )
}

fn append(
    store: &mut EventStore,
    trace: &TraceEvent,
    legacy: Option<(&SessionId, &AgentEvent)>,
    millis: i64,
) {
    store
        .append_traced(trace, legacy, Timestamp::from_millis(millis))
        .expect("trace append should satisfy the storage contract");
}
