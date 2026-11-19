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
    assert_eq!(report["report_version"], 3);
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
    assert!(
        ok["replay"].is_null(),
        "replay must stay absent unless --replay is requested"
    );
    let ok_digest = ok["digest"]
        .as_str()
        .expect("a verified turn must carry its chain digest");
    assert!(
        is_lower_hex_64(ok_digest),
        "the digest must be canonical lowercase hex: {ok_digest}"
    );

    let open = report_turn(&report, "turn-open");
    assert_eq!(open["status"], "open");
    assert_eq!(open["schema_version"], 2);
    assert_eq!(open["entries"], 2);
    let open_digest = open["digest"]
        .as_str()
        .expect("an open turn's chain is still digestible");
    assert!(is_lower_hex_64(open_digest));
    assert_ne!(
        ok_digest, open_digest,
        "different turns must not share one digest"
    );

    let gap = report_turn(&report, "turn-gap");
    assert_eq!(gap["status"], "invalid");
    assert_eq!(problem_kinds(gap), vec!["trace_gap"]);
    assert!(
        is_lower_hex_64(gap["digest"].as_str().expect("digest must survive a gap")),
        "a semantic gap does not break the chain"
    );

    let corrupt = report_turn(&report, "turn-corrupt");
    assert_eq!(corrupt["status"], "invalid");
    assert!(corrupt["schema_version"].is_null());
    assert!(corrupt["entries"].is_null());
    assert!(
        corrupt["digest"].is_null(),
        "a structurally invalid trace has no digest"
    );
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
fn replay_reproduces_a_closed_fresh_trace_and_reports_it() {
    let temp = TempDir::new("replay-ok");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_replayable_turn(&mut store, "turn-replay", "session-replay", 1_000);
    drop(store);
    let config = write_config(&temp, &database);

    let text = run_verify(&config, &["--turn", "turn-replay", "--replay"]);
    let stdout = String::from_utf8_lossy(&text.stdout);
    let stderr = String::from_utf8_lossy(&text.stderr);
    assert!(
        text.status.success(),
        "a replayable trace must pass\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("replay: ok attempts=1 events=5"),
        "text output must summarize execution parity:\n{stdout}"
    );
    assert!(
        stderr.is_empty(),
        "success must not write to stderr:\n{stderr}"
    );

    let json = run_verify(&config, &["--turn", "turn-replay", "--replay", "--json"]);
    assert!(json.status.success());
    let report = json_report(&json);
    assert_eq!(report["report_version"], 3);
    let turn = report_turn(&report, "turn-replay");
    assert_eq!(turn["status"], "ok");
    assert_eq!(turn["replay"]["status"], "ok");
    assert_eq!(turn["replay"]["attempts"], 1);
    assert_eq!(turn["replay"]["events"], 5);
    assert!(turn["replay"]["reason"].is_null());
}

#[test]
fn replay_mismatches_make_the_turn_invalid() {
    let temp = TempDir::new("replay-mismatch");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    // Structurally valid, but its one tool round is followed directly by a
    // `Completed` end. A real `AgentLoop` with max_rounds = 1 ends on the round
    // budget instead, so execution parity must reject the claim.
    seed_valid_turn(&mut store, "turn-mismatch", "session-mismatch", 1_000);
    drop(store);
    let config = write_config(&temp, &database);

    let output = run_verify(&config, &["--turn", "turn-mismatch", "--replay", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "a parity mismatch must fail\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );

    let report = json_report(&output);
    let turn = report_turn(&report, "turn-mismatch");
    assert_eq!(turn["status"], "invalid");
    assert_eq!(turn["replay"]["status"], "mismatch");
    assert!(
        problem_kinds(turn).contains(&"replay_mismatch"),
        "the mismatch must be reported as a problem: {turn}"
    );
    assert!(
        stderr.contains("trace verification failed"),
        "the CLI must report verification failure:\n{stderr}"
    );
}

#[test]
fn replay_supports_version_one_fresh_traces() {
    let temp = TempDir::new("replay-v1");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_v1_turn(&mut store, "turn-v1", "session-v1", 1_000);
    drop(store);
    let config = write_config(&temp, &database);

    let text = run_verify(&config, &["--turn", "turn-v1", "--replay"]);
    let stdout = String::from_utf8_lossy(&text.stdout);
    let stderr = String::from_utf8_lossy(&text.stderr);
    assert!(
        text.status.success(),
        "a version-1 trace must replay from its turn bootstrap\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("replay: ok attempts=1 events=2"),
        "the version-1 session start and end must match:\n{stdout}"
    );
    assert!(
        stderr.is_empty(),
        "success must not write to stderr:\n{stderr}"
    );

    let json = run_verify(&config, &["--turn", "turn-v1", "--replay", "--json"]);
    assert!(json.status.success());
    let report = json_report(&json);
    let turn = report_turn(&report, "turn-v1");
    assert_eq!(turn["status"], "ok");
    assert_eq!(turn["replay"]["status"], "ok");
    assert_eq!(turn["replay"]["events"], 2);
    assert!(turn["replay"]["reason"].is_null());
}

#[test]
fn batch_replay_covers_fresh_version_one_and_resumed_traces() {
    let temp = TempDir::new("replay-batch-ok");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_replayable_turn(&mut store, "turn-fresh", "session-fresh", 1_000);
    seed_v1_turn(&mut store, "turn-v1", "session-v1", 2_000);
    seed_resumed_turn(&mut store, "turn-resumed", "session-resumed", 3_000);
    drop(store);
    let config = write_config(&temp, &database);

    let output = run_verify(&config, &["--all", "--replay", "--json"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "every replayable trace must pass\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.is_empty(),
        "success must not write to stderr:\n{stderr}"
    );

    let report = json_report(&output);
    assert_eq!(report["summary"]["turns"], 3);
    assert_eq!(report["summary"]["ok"], 3);
    assert_eq!(report["summary"]["invalid"], 0);
    for (turn_id, events) in [("turn-fresh", 5), ("turn-v1", 2), ("turn-resumed", 4)] {
        let turn = report_turn(&report, turn_id);
        assert_eq!(turn["status"], "ok", "{turn}");
        assert_eq!(turn["replay"]["status"], "ok", "{turn}");
        assert_eq!(turn["replay"]["attempts"], 1, "{turn}");
        assert_eq!(turn["replay"]["events"], events, "{turn}");
    }
}

#[test]
fn batch_replay_continues_after_a_mismatch() {
    let temp = TempDir::new("replay-batch-mismatch");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_replayable_turn(&mut store, "turn-ok-a", "session-ok-a", 1_000);
    seed_valid_turn(&mut store, "turn-mismatch", "session-mismatch", 2_000);
    seed_v1_turn(&mut store, "turn-ok-b", "session-ok-b", 3_000);
    drop(store);
    let config = write_config(&temp, &database);

    let output = run_verify(&config, &["--all", "--replay", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "a batch with a parity mismatch must fail\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );

    let report = json_report(&output);
    assert_eq!(report["summary"]["turns"], 3);
    assert_eq!(report["summary"]["ok"], 2);
    assert_eq!(report["summary"]["invalid"], 1);
    assert_eq!(report_turn(&report, "turn-ok-a")["replay"]["status"], "ok");
    let mismatch = report_turn(&report, "turn-mismatch");
    assert_eq!(mismatch["status"], "invalid");
    assert_eq!(mismatch["replay"]["status"], "mismatch");
    assert!(problem_kinds(mismatch).contains(&"replay_mismatch"));
    assert_eq!(
        report_turn(&report, "turn-ok-b")["replay"]["status"],
        "ok",
        "the batch must keep replaying after a mismatch"
    );
    assert!(
        stderr.contains("trace verification failed"),
        "the CLI must report verification failure:\n{stderr}"
    );
}

#[test]
fn batch_replay_reports_unsupported_turns_without_invalidating_them() {
    let temp = TempDir::new("replay-batch-unsupported");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_replayable_turn(&mut store, "turn-ok", "session-ok", 1_000);
    seed_unreplayable_resumed_turn(&mut store, "turn-resumed", "session-resumed", 2_000);
    drop(store);
    let config = write_config(&temp, &database);

    let output = run_verify(&config, &["--all", "--replay", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "an unsupported valid trace must make batch replay exit non-zero\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );

    let report = json_report(&output);
    assert_eq!(report["summary"]["turns"], 2);
    assert_eq!(report["summary"]["ok"], 2);
    assert_eq!(report["summary"]["invalid"], 0);
    assert_eq!(report_turn(&report, "turn-ok")["replay"]["status"], "ok");
    let unsupported = report_turn(&report, "turn-resumed");
    assert_eq!(
        unsupported["status"], "ok",
        "unsupported replay must not invalidate valid evidence"
    );
    assert_eq!(unsupported["replay"]["status"], "unsupported");
    assert!(
        unsupported["replay"]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("RoundStarted")),
        "the report must explain the unsupported subset: {unsupported}"
    );
    assert!(
        stderr.contains("cannot be replay-verified")
            && stderr.contains("turn-resumed")
            && !stderr.contains("trace verification failed"),
        "the CLI must report only the replay limitation:\n{stderr}"
    );
}

#[test]
fn batch_replay_rejects_allow_open() {
    let temp = TempDir::new("replay-batch-open");
    let database = temp.path.join("events.sqlite");
    drop(EventStore::open(&database).expect("event store should open"));
    let config = write_config(&temp, &database);

    let output = run_verify(&config, &["--all", "--replay", "--allow-open"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        empty_stdout(&output),
        "a rejected command must not produce a report"
    );
    assert!(
        stderr.contains("cannot be combined"),
        "the error must explain the incompatible flags:\n{stderr}"
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

    let replay = run_verify(&config, &["--all", "--replay", "--json"]);
    assert!(
        replay.status.success(),
        "batch replay over an empty store has nothing to reject:\n{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    let report = json_report(&replay);
    assert_eq!(report["summary"]["turns"], 0);
    assert_eq!(
        report["turns"].as_array().expect("turns must be an array"),
        &Vec::<Value>::new()
    );
}

#[test]
fn digest_prints_the_same_digest_that_verify_reports() {
    let temp = TempDir::new("digest-single");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_valid_turn(&mut store, "turn-ok", "session-ok", 1_000);
    drop(store);
    let config = write_config(&temp, &database);

    let text = run_trace(&config, "digest", &["--turn", "turn-ok"]);
    let stdout = String::from_utf8_lossy(&text.stdout);
    let stderr = String::from_utf8_lossy(&text.stderr);
    assert!(
        text.status.success(),
        "a valid chain must digest\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.is_empty(),
        "success must not write to stderr:\n{stderr}"
    );
    let digest = stdout.trim();
    assert!(
        is_lower_hex_64(digest),
        "single-turn output must be just the digest: {stdout:?}"
    );

    let verify = run_verify(&config, &["--turn", "turn-ok", "--json"]);
    let report = json_report(&verify);
    assert_eq!(
        report_turn(&report, "turn-ok")["digest"].as_str(),
        Some(digest),
        "verify and digest must report one digest for one stored chain"
    );

    let json = run_trace(&config, "digest", &["--turn", "turn-ok", "--json"]);
    assert!(json.status.success());
    let report = json_report(&json);
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["digest_format"], "jessica.trace.digest.v1");
    let turns = report["turns"].as_array().expect("turns must be an array");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0]["turn_id"], "turn-ok");
    assert_eq!(turns[0]["digest"], digest);
    assert_eq!(turns[0]["closed"], true);
    assert_eq!(turns[0]["entries"], 10);
}

#[test]
fn digest_all_reports_every_turn_in_ascending_order() {
    let temp = TempDir::new("digest-all");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_valid_turn(&mut store, "turn-alpha", "session-alpha", 1_000);
    seed_open_turn(&mut store, "turn-beta", "session-beta", 2_000);
    drop(store);
    let config = write_config(&temp, &database);

    let text = run_trace(&config, "digest", &["--all"]);
    let stdout = String::from_utf8_lossy(&text.stdout);
    assert!(
        text.status.success(),
        "an open turn still has a digestible chain:\n{}",
        String::from_utf8_lossy(&text.stderr)
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "unexpected text output:\n{stdout}");
    let mut split = lines[0].split('\t');
    assert_eq!(split.next(), Some("turn-alpha"));
    assert!(
        split.next().is_some_and(is_lower_hex_64),
        "the first line must carry the alpha digest: {stdout}"
    );
    let mut split = lines[1].split('\t');
    assert_eq!(split.next(), Some("turn-beta"));
    assert!(
        split.next().is_some_and(is_lower_hex_64),
        "the second line must carry the beta digest: {stdout}"
    );

    let json = run_trace(&config, "digest", &["--all", "--json"]);
    assert!(json.status.success());
    let report = json_report(&json);
    let turns = report["turns"].as_array().expect("turns must be an array");
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["turn_id"], "turn-alpha");
    assert_eq!(turns[0]["closed"], true);
    assert_eq!(turns[1]["turn_id"], "turn-beta");
    assert_eq!(turns[1]["closed"], false);
    assert_eq!(turns[1]["entries"], 2);
}

#[test]
fn digest_of_an_empty_store_is_a_successful_empty_report() {
    let temp = TempDir::new("digest-empty");
    let database = temp.path.join("events.sqlite");
    drop(EventStore::open(&database).expect("event store should open"));
    let config = write_config(&temp, &database);

    let text = run_trace(&config, "digest", &["--all"]);
    assert!(
        text.status.success(),
        "an empty store has no failing chain:\n{}",
        String::from_utf8_lossy(&text.stderr)
    );
    assert!(
        text.stdout.is_empty(),
        "an empty all-turns digest prints nothing"
    );

    let json = run_trace(&config, "digest", &["--all", "--json"]);
    assert!(json.status.success());
    let report = json_report(&json);
    assert_eq!(
        report["turns"].as_array().expect("turns must be an array"),
        &Vec::<Value>::new()
    );
}

#[test]
fn digest_rejects_an_unknown_turn() {
    let temp = TempDir::new("digest-missing");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_valid_turn(&mut store, "turn-ok", "session-ok", 1_000);
    drop(store);
    let config = write_config(&temp, &database);

    let output = run_trace(&config, "digest", &["--turn", "turn-missing"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        empty_stdout(&output),
        "an unknown turn must not produce a digest"
    );
    assert!(
        stderr.contains("turn-missing") && stderr.contains("not found"),
        "the error must name the missing turn:\n{stderr}"
    );
}

#[test]
fn digest_detects_a_tampered_chain() {
    let temp = TempDir::new("digest-tamper");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_valid_turn(&mut store, "turn-ok", "session-ok", 1_000);
    drop(store);

    {
        let connection =
            rusqlite::Connection::open(&database).expect("raw database connection should open");
        connection
            .execute_batch("DROP TRIGGER trace_chain_append_only_delete;")
            .expect("the delete guard should drop for the fixture");
        let deleted = connection
            .execute(
                "DELETE FROM trace_chain WHERE turn_id = 'turn-ok' AND seq = 3",
                [],
            )
            .expect("the fixture deletion should apply");
        assert_eq!(deleted, 1, "the fixture must delete exactly one link");
    }

    let config = write_config(&temp, &database);
    let output = run_trace(&config, "digest", &["--turn", "turn-ok"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "a missing chain link must not produce a digest\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        empty_stdout(&output),
        "a broken chain must not print an unverified digest"
    );
    assert!(
        stderr.contains("turn-ok") && stderr.contains("chain"),
        "the error must name the turn and the broken chain:\n{stderr}"
    );
}

#[test]
fn verify_expect_accepts_only_the_anchored_digest() {
    let temp = TempDir::new("expect");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_valid_turn(&mut store, "turn-ok", "session-ok", 1_000);
    drop(store);
    let config = write_config(&temp, &database);

    let baseline = run_trace(&config, "digest", &["--turn", "turn-ok"]);
    assert!(baseline.status.success());
    let digest = String::from_utf8_lossy(&baseline.stdout).trim().to_owned();
    assert!(is_lower_hex_64(&digest));

    let matching = run_verify(
        &config,
        &["--turn", "turn-ok", "--json", "--expect", &digest],
    );
    let stderr = String::from_utf8_lossy(&matching.stderr);
    assert!(
        matching.status.success(),
        "the recorded digest must pass verification\nstderr:\n{stderr}"
    );
    assert_eq!(
        report_turn(&json_report(&matching), "turn-ok")["status"],
        "ok"
    );

    let uppercase = digest.to_ascii_uppercase();
    let normalized = run_verify(
        &config,
        &["--turn", "turn-ok", "--json", "--expect", &uppercase],
    );
    assert!(
        normalized.status.success(),
        "an uppercase expected digest must be normalized:\n{}",
        String::from_utf8_lossy(&normalized.stderr)
    );

    let wrong = if digest == "0".repeat(64) {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    let mismatch = run_verify(
        &config,
        &["--turn", "turn-ok", "--json", "--expect", &wrong],
    );
    let stderr = String::from_utf8_lossy(&mismatch.stderr);
    assert!(
        !mismatch.status.success(),
        "an unanchored digest must fail\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&mismatch.stdout)
    );
    let report = json_report(&mismatch);
    let turn = report_turn(&report, "turn-ok");
    assert_eq!(turn["status"], "invalid");
    assert_eq!(turn["digest"], digest);
    assert_eq!(problem_kinds(turn), vec!["digest_mismatch"]);
    assert!(
        stderr.contains("trace verification failed"),
        "the CLI must report verification failure:\n{stderr}"
    );
}

#[test]
fn verify_expect_cannot_anchor_a_structurally_invalid_trace() {
    let temp = TempDir::new("expect-unavailable");
    let database = temp.path.join("events.sqlite");
    drop(EventStore::open(&database).expect("event store should open"));
    seed_corrupt_head(&database, "turn-corrupt", 2_000);
    let config = write_config(&temp, &database);

    let expected = "0".repeat(64);
    let output = run_verify(
        &config,
        &["--turn", "turn-corrupt", "--json", "--expect", &expected],
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "an unverifiable trace cannot be anchored\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
    let report = json_report(&output);
    let turn = report_turn(&report, "turn-corrupt");
    assert_eq!(turn["status"], "invalid");
    assert!(turn["digest"].is_null());
    let kinds = problem_kinds(turn);
    assert!(
        kinds.contains(&"trace_structure"),
        "the structural failure must remain visible: {turn}"
    );
    assert!(
        kinds.contains(&"digest_unavailable"),
        "the missing digest must be reported, not assumed to match: {turn}"
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
    run_trace(config, "verify", arguments)
}

/// Runs the real CLI in one trace subcommand without provider configuration.
fn run_trace(config: &Path, subcommand: &str, arguments: &[&str]) -> Output {
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
        .arg(subcommand)
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

/// Returns whether `value` is 64 lowercase hexadecimal characters.
fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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

/// Writes a closed fresh trace that the deterministic replay engine can
/// reproduce: one model round with text and no tool calls, ending `Completed`.
fn seed_replayable_turn(store: &mut EventStore, turn: &str, session_name: &str, base: i64) {
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
                messages: vec![Message::user("replay")],
            },
        ),
        None,
        base,
    );

    let session_started = AgentEvent::SessionStarted {
        session_id: session.clone(),
        messages: vec![Message::user("do it")],
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
        text: "done".to_owned(),
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, delta.clone()),
        Some((&session, &delta)),
        base + 3,
    );

    let round_finished = AgentEvent::RoundFinished {
        round: 1,
        usage: Some(TokenUsage::new(5, 2)),
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, round_finished.clone()),
        Some((&session, &round_finished)),
        base + 4,
    );

    let session_ended = AgentEvent::SessionEnded {
        session_id: session.clone(),
        reason: SessionEndReason::Completed,
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, session_ended.clone()),
        Some((&session, &session_ended)),
        base + 5,
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
        base + 6,
    );
}

/// Writes a structurally valid version-1 trace. It has no explicit rounds or
/// usage; replay reconstructs its single model turn from the turn bootstrap.
fn seed_v1_turn(store: &mut EventStore, turn: &str, session_name: &str, base: i64) {
    let turn_id = TurnId::new(turn);
    let session = SessionId::new(session_name);
    append(
        store,
        &v1(TraceEvent::new(
            turn_id.clone(),
            None,
            Some(session.clone()),
            TracePayload::TurnStarted {
                mode: TraceTurnMode::Fresh,
                messages: vec![Message::user("legacy")],
            },
        )),
        None,
        base,
    );

    let session_started = AgentEvent::SessionStarted {
        session_id: session.clone(),
        messages: Vec::new(),
    };
    append(
        store,
        &v1(traced_agent(&turn_id, &session, 1, session_started.clone())),
        Some((&session, &session_started)),
        base + 1,
    );

    let session_ended = AgentEvent::SessionEnded {
        session_id: session.clone(),
        reason: SessionEndReason::Completed,
    };
    append(
        store,
        &v1(traced_agent(&turn_id, &session, 1, session_ended.clone())),
        Some((&session, &session_ended)),
        base + 2,
    );

    append(
        store,
        &v1(TraceEvent::new(
            turn_id,
            None,
            None,
            TracePayload::TurnEnded {
                reason: SessionEndReason::Completed,
            },
        )),
        None,
        base + 3,
    );
}

/// Writes a closed resumed trace that continues at model round two.
fn seed_resumed_turn(store: &mut EventStore, turn: &str, session_name: &str, base: i64) {
    let turn_id = TurnId::new(turn);
    let session = SessionId::new(session_name);
    append(
        store,
        &TraceEvent::new(
            turn_id.clone(),
            None,
            Some(session.clone()),
            TracePayload::TurnStarted {
                mode: TraceTurnMode::Resumed,
                messages: vec![
                    Message::system("resume test"),
                    Message::user("continue"),
                    Message::assistant("working"),
                    Message::tool("call-1", "recorded output"),
                ],
            },
        ),
        None,
        base,
    );

    let round_started = AgentEvent::RoundStarted { round: 2 };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, round_started.clone()),
        Some((&session, &round_started)),
        base + 1,
    );

    let delta = AgentEvent::ModelDelta {
        text: "resumed done".to_owned(),
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, delta.clone()),
        Some((&session, &delta)),
        base + 2,
    );

    let round_finished = AgentEvent::RoundFinished {
        round: 2,
        usage: Some(TokenUsage::new(7, 3)),
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, round_finished.clone()),
        Some((&session, &round_finished)),
        base + 3,
    );

    let session_ended = AgentEvent::SessionEnded {
        session_id: session.clone(),
        reason: SessionEndReason::Completed,
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, session_ended.clone()),
        Some((&session, &session_ended)),
        base + 4,
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
        base + 5,
    );
}

/// Writes a valid resumed trace with no model round. Replay cannot infer the
/// checkpoint's completed-round count from such a stream, so it is unsupported.
fn seed_unreplayable_resumed_turn(
    store: &mut EventStore,
    turn: &str,
    session_name: &str,
    base: i64,
) {
    let turn_id = TurnId::new(turn);
    let session = SessionId::new(session_name);
    append(
        store,
        &TraceEvent::new(
            turn_id.clone(),
            None,
            Some(session.clone()),
            TracePayload::TurnStarted {
                mode: TraceTurnMode::Resumed,
                messages: vec![Message::user("continue")],
            },
        ),
        None,
        base,
    );

    let session_ended = AgentEvent::SessionEnded {
        session_id: session.clone(),
        reason: SessionEndReason::Completed,
    };
    append(
        store,
        &traced_agent(&turn_id, &session, 1, session_ended.clone()),
        Some((&session, &session_ended)),
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

fn v1(mut trace: TraceEvent) -> TraceEvent {
    trace.schema_version = 1;
    trace
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
