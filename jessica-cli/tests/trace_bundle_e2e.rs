//! End-to-end acceptance for portable trace evidence bundles.
//!
//! Every test uses a real SQLite event store and the real CLI binary. Export
//! runs read-only against the store; offline verification must succeed without
//! an `[events]` configuration and must never create a database.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use jessica_core::trace::{TraceEvent, TraceGapReason, TracePayload, TraceTurnMode, TurnId};
use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId, TokenUsage};
use jessica_memory::{EventStore, Timestamp};
use serde_json::{Value, json};

/// Overall budget for one child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);

/// Bundle size limit used by the CLI.
const MAX_BUNDLE_BYTES: u64 = 64 * 1024 * 1024;

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
            "jessica-bundle-{label}-{}-{unique}",
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
fn export_and_offline_verify_round_trip_without_an_event_store() {
    let temp = TempDir::new("round-trip");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_closed_turn(&mut store, "turn-alpha", "session-alpha", 1_000);
    seed_closed_turn(&mut store, "turn-beta", "session-beta", 2_000);
    drop(store);
    let config = write_config(&temp, &database);
    let bundle = temp.path.join("evidence.json");

    let exported = run_export(
        &config,
        &["--all", "--out", bundle.to_str().expect("utf-8 path")],
    );
    let stdout = String::from_utf8_lossy(&exported.stdout);
    let stderr = String::from_utf8_lossy(&exported.stderr);
    assert!(
        exported.status.success(),
        "exporting valid turns must succeed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("exported 2 turn(s) to"),
        "export must report the turn count:\n{stdout}"
    );
    let aggregate = aggregate_from_export(&exported);
    assert!(is_lower_hex_64(&aggregate));

    let parsed: Value = read_json(&bundle);
    assert_eq!(parsed["bundle_version"], 1);
    assert_eq!(parsed["bundle_format"], "jessica.trace.bundle.v1");
    assert_eq!(parsed["aggregate_digest"], aggregate);
    let turns = parsed["turns"].as_array().expect("turns must be an array");
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["turn_id"], "turn-alpha");
    assert_eq!(turns[0]["closed"], true);
    assert_eq!(turns[0]["entries"].as_array().expect("entries").len(), 7);
    assert_eq!(turns[0]["legacy"].as_array().expect("legacy").len(), 5);
    assert!(is_lower_hex_64(
        turns[0]["digest"].as_str().expect("digest")
    ));
    assert_eq!(turns[1]["turn_id"], "turn-beta");

    // Offline verification uses a configuration with no [events] path at all.
    let offline_config = write_empty_config(&temp);
    let verified = run_verify_bundle(&offline_config, &["--bundle", path(&bundle), "--json"]);
    let stdout = String::from_utf8_lossy(&verified.stdout);
    let stderr = String::from_utf8_lossy(&verified.stderr);
    assert!(
        verified.status.success(),
        "a valid bundle must verify offline\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let report: Value = serde_json::from_slice(&verified.stdout).expect("report must be JSON");
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["bundle_format"], "jessica.trace.bundle.v1");
    assert_eq!(report["bundle"]["declared_digest"], aggregate);
    assert_eq!(report["bundle"]["computed_digest"], aggregate);
    assert_eq!(
        report["bundle"]["problems"]
            .as_array()
            .expect("bundle problems"),
        &Vec::<Value>::new()
    );
    assert_eq!(report["summary"]["turns"], 2);
    assert_eq!(report["summary"]["ok"], 2);
    assert_eq!(report["turns"][0]["digest"], turns[0]["digest"]);

    let text = run_verify_bundle(&offline_config, &["--bundle", path(&bundle)]);
    let stdout = String::from_utf8_lossy(&text.stdout);
    assert!(
        text.status.success(),
        "the text report must pass:\n{}",
        String::from_utf8_lossy(&text.stderr)
    );
    assert!(
        stdout.contains(&format!("bundle {} turns=2", bundle.display())),
        "text output must identify the bundle and count:\n{stdout}"
    );
    assert!(
        stdout.contains(&format!("aggregate {aggregate}"))
            && stdout.contains(&format!("declared {aggregate}")),
        "text output must print the recomputed and declared aggregates:\n{stdout}"
    );
    assert!(
        stdout.contains("summary: 2 turns: 2 ok, 0 open, 0 invalid, 0 problems"),
        "text output must summarize every turn:\n{stdout}"
    );

    let no_config = run_jessica(
        None,
        &[
            "trace",
            "verify-bundle",
            "--bundle",
            path(&bundle),
            "--json",
        ],
    );
    assert!(
        no_config.status.success(),
        "offline verification must not require a config file:\n{}",
        String::from_utf8_lossy(&no_config.stderr)
    );
}

#[test]
fn exported_bundles_are_deterministic_and_turns_are_sorted() {
    let temp = TempDir::new("deterministic");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_closed_turn(&mut store, "turn-zeta", "session-zeta", 2_000);
    seed_closed_turn(&mut store, "turn-alpha", "session-alpha", 1_000);
    drop(store);
    let config = write_config(&temp, &database);
    let first = temp.path.join("first.json");
    let second = temp.path.join("second.json");

    for path in [&first, &second] {
        let output = run_export(
            &config,
            &["--all", "--out", path.to_str().expect("utf-8 path")],
        );
        assert!(
            output.status.success(),
            "export must succeed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert_eq!(
        std::fs::read(&first).expect("first bundle should be readable"),
        std::fs::read(&second).expect("second bundle should be readable"),
        "the same store must produce byte-identical bundles"
    );
    let parsed: Value = read_json(&first);
    let ids: Vec<&str> = parsed["turns"]
        .as_array()
        .expect("turns must be an array")
        .iter()
        .map(|turn| turn["turn_id"].as_str().expect("turn id"))
        .collect();
    assert_eq!(ids, vec!["turn-alpha", "turn-zeta"]);
}

#[test]
fn open_traces_require_allow_open_for_export_and_offline_verification() {
    let temp = TempDir::new("open");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_closed_turn(&mut store, "turn-closed", "session-closed", 1_000);
    seed_open_turn(&mut store, "turn-open", "session-open", 2_000);
    drop(store);
    let config = write_config(&temp, &database);
    let bundle = temp.path.join("open.json");
    let single = temp.path.join("open-single.json");

    let single_rejected = run_export(&config, &["--turn", "turn-open", "--out", path(&single)]);
    let stderr = String::from_utf8_lossy(&single_rejected.stderr);
    assert!(!single_rejected.status.success());
    assert!(
        stderr.contains("only be exported with `--all --allow-open`"),
        "a single open trace must explain the only supported path:\n{stderr}"
    );
    assert!(
        !single.exists(),
        "a refused single-turn export must not write a bundle"
    );

    let rejected = run_export(&config, &["--all", "--out", path(&bundle)]);
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(!rejected.status.success());
    assert!(
        stderr.contains("--allow-open") && stderr.contains("turn-open"),
        "the refusal must name the open turn and the flag:\n{stderr}"
    );
    assert!(
        !bundle.exists(),
        "a refused export must not write a partial bundle"
    );

    let exported = run_export(&config, &["--all", "--allow-open", "--out", path(&bundle)]);
    assert!(
        exported.status.success(),
        "allow-open must export the open trace:\n{}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let parsed: Value = read_json(&bundle);
    let open = parsed["turns"]
        .as_array()
        .expect("turns")
        .iter()
        .find(|turn| turn["turn_id"] == "turn-open")
        .expect("open turn must be present");
    assert_eq!(open["closed"], false);

    let offline = run_verify_bundle(&config, &["--bundle", path(&bundle)]);
    let stderr = String::from_utf8_lossy(&offline.stderr);
    assert!(
        !offline.status.success(),
        "offline verification must reject an open trace by default:\n{stderr}"
    );
    assert!(
        stderr.contains("trace verification failed"),
        "the CLI must report verification failure:\n{stderr}"
    );

    let allowed = run_verify_bundle(&config, &["--bundle", path(&bundle), "--allow-open"]);
    assert!(
        allowed.status.success(),
        "allow-open must accept the exported open trace:\n{}",
        String::from_utf8_lossy(&allowed.stderr)
    );
}

#[test]
fn invalid_traces_cannot_be_exported() {
    let temp = TempDir::new("invalid");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_gap_turn(&mut store, "turn-gap", "session-gap", 1_000);
    drop(store);
    let config = write_config(&temp, &database);
    let bundle = temp.path.join("invalid.json");

    let output = run_export(&config, &["--turn", "turn-gap", "--out", path(&bundle)]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("turn-gap") && stderr.contains("trace_gap"),
        "the refusal must name the turn and the semantic problem:\n{stderr}"
    );
    assert!(
        !bundle.exists(),
        "an invalid trace must not produce a bundle"
    );
}

#[test]
fn offline_verification_detects_tampered_trace_legacy_and_aggregate() {
    let temp = TempDir::new("tamper");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_closed_turn(&mut store, "turn-alpha", "session-alpha", 1_000);
    drop(store);
    let config = write_config(&temp, &database);
    let clean = temp.path.join("clean.json");
    let exported = run_export(&config, &["--turn", "turn-alpha", "--out", path(&clean)]);
    assert!(
        exported.status.success(),
        "baseline export must succeed:\n{}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let baseline = read_json(&clean);

    let mut tampered_trace = baseline.clone();
    let entries = tampered_trace["turns"][0]["entries"]
        .as_array_mut()
        .expect("entries must be an array");
    let delta = entries
        .iter_mut()
        .find(|entry| {
            entry["payload_json"]
                .as_str()
                .is_some_and(|payload| payload.contains("hello"))
        })
        .expect("the seeded model delta must be in the bundle");
    let old_payload = delta["payload_json"]
        .as_str()
        .expect("payload must be a string")
        .to_owned();
    delta["payload_json"] = Value::String(old_payload.replace("hello", "tampered"));
    let trace_bundle = temp.path.join("tampered-trace.json");
    write_json(&trace_bundle, &tampered_trace);
    let output = run_verify_bundle(&config, &["--bundle", path(&trace_bundle), "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "a tampered trace entry must fail\nstderr:\n{stderr}"
    );
    let report: Value = serde_json::from_slice(&output.stdout).expect("report must be JSON");
    let kinds = problem_kinds(&report["turns"][0]);
    assert!(
        kinds.contains(&"digest_mismatch"),
        "the recomputed turn digest must disagree: {kinds:?}"
    );
    let computed = report["bundle"]["computed_digest"]
        .as_str()
        .expect("the recomputed aggregate must be visible");
    assert!(is_lower_hex_64(computed));
    assert_ne!(
        computed,
        baseline["aggregate_digest"]
            .as_str()
            .expect("baseline aggregate"),
        "the offline recomputation must reflect the edited trace"
    );
    assert!(
        bundle_problem_kinds(&report).contains(&"digest_mismatch"),
        "the declared aggregate must no longer match"
    );

    let mut tampered_legacy = baseline.clone();
    tampered_legacy["turns"][0]["legacy"][0]["payload_json"] =
        Value::String("{\"tampered\":true}".to_owned());
    let legacy_bundle = temp.path.join("tampered-legacy.json");
    write_json(&legacy_bundle, &tampered_legacy);
    let output = run_verify_bundle(&config, &["--bundle", path(&legacy_bundle), "--json"]);
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).expect("report must be JSON");
    let kinds = problem_kinds(&report["turns"][0]);
    assert!(
        kinds.contains(&"missing_legacy_mirror"),
        "the mirror check must reject the legacy edit: {kinds:?}"
    );
    assert_eq!(
        report["bundle"]["computed_digest"].as_str(),
        Some(baseline["aggregate_digest"].as_str().expect("aggregate"))
    );

    let mut tampered_aggregate = baseline.clone();
    let aggregate = tampered_aggregate["aggregate_digest"]
        .as_str()
        .expect("aggregate must be a string")
        .to_owned();
    let replacement = if aggregate.starts_with('0') { "1" } else { "0" };
    tampered_aggregate["aggregate_digest"] =
        Value::String(format!("{replacement}{}", &aggregate[1..]));
    let aggregate_bundle = temp.path.join("tampered-aggregate.json");
    write_json(&aggregate_bundle, &tampered_aggregate);
    let output = run_verify_bundle(&config, &["--bundle", path(&aggregate_bundle), "--json"]);
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).expect("report must be JSON");
    assert_eq!(report["turns"][0]["status"], "ok");
    assert_eq!(
        bundle_problem_kinds(&report),
        vec!["digest_mismatch"],
        "the aggregate mismatch must be a bundle-level finding"
    );
}

#[test]
fn bundle_turn_digest_and_expect_anchor_the_package() {
    let temp = TempDir::new("anchors");
    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_closed_turn(&mut store, "turn-alpha", "session-alpha", 1_000);
    seed_closed_turn(&mut store, "turn-beta", "session-beta", 2_000);
    drop(store);
    let config = write_config(&temp, &database);
    let bundle = temp.path.join("evidence.json");
    let exported = run_export(&config, &["--all", "--out", path(&bundle)]);
    assert!(exported.status.success());
    let aggregate = aggregate_from_export(&exported);

    let matching = run_verify_bundle(
        &config,
        &["--bundle", path(&bundle), "--expect", &aggregate],
    );
    assert!(
        matching.status.success(),
        "the externally recorded aggregate must pass:\n{}",
        String::from_utf8_lossy(&matching.stderr)
    );

    let uppercase = aggregate.to_ascii_uppercase();
    let normalized = run_verify_bundle(
        &config,
        &["--bundle", path(&bundle), "--expect", &uppercase],
    );
    assert!(
        normalized.status.success(),
        "an uppercase expected digest must be normalized:\n{}",
        String::from_utf8_lossy(&normalized.stderr)
    );

    let wrong = if aggregate == "0".repeat(64) {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    let mismatch = run_verify_bundle(
        &config,
        &["--bundle", path(&bundle), "--json", "--expect", &wrong],
    );
    let stderr = String::from_utf8_lossy(&mismatch.stderr);
    assert!(!mismatch.status.success());
    assert!(
        stderr.contains("trace verification failed"),
        "the CLI must fail on an unanchored bundle:\n{stderr}"
    );
    let report: Value = serde_json::from_slice(&mismatch.stdout).expect("report must be JSON");
    assert_eq!(report["summary"]["ok"], 2);
    assert_eq!(report["bundle"]["expected_digest"], wrong);
    assert_eq!(bundle_problem_kinds(&report), vec!["digest_mismatch"]);

    let mut declared_turn = read_json(&bundle);
    declared_turn["turns"][0]["digest"] = Value::String("0".repeat(64));
    let turn_bundle = temp.path.join("bad-turn-digest.json");
    write_json(&turn_bundle, &declared_turn);
    let output = run_verify_bundle(&config, &["--bundle", path(&turn_bundle), "--json"]);
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).expect("report must be JSON");
    assert_eq!(report["turns"][0]["status"], "invalid");
    assert!(
        problem_kinds(&report["turns"][0]).contains(&"digest_mismatch"),
        "the declared turn digest must be compared"
    );
}

#[test]
fn malformed_bundles_are_rejected_with_specific_errors() {
    let temp = TempDir::new("malformed");
    let offline_config = write_empty_config(&temp);
    let missing = temp.path.join("missing.json");
    let output = run_verify_bundle(&offline_config, &["--bundle", path(&missing)]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("failed to read trace bundle"),
        "a missing bundle must report the read failure:\n{stderr}"
    );

    let database = temp.path.join("events.sqlite");
    let mut store = EventStore::open(&database).expect("event store should open");
    seed_closed_turn(&mut store, "turn-alpha", "session-alpha", 1_000);
    drop(store);
    let config = write_config(&temp, &database);
    let clean = temp.path.join("clean.json");
    let exported = run_export(&config, &["--all", "--out", path(&clean)]);
    assert!(exported.status.success());
    let baseline = read_json(&clean);

    for (label, mutate, expected) in [
        (
            "version",
            Box::new(|bundle: &mut Value| bundle["bundle_version"] = json!(2))
                as Box<dyn Fn(&mut Value)>,
            "unsupported",
        ),
        (
            "format",
            Box::new(|bundle: &mut Value| bundle["bundle_format"] = json!("other.format")),
            "format",
        ),
        (
            "aggregate",
            Box::new(|bundle: &mut Value| bundle["aggregate_digest"] = json!("z".repeat(64))),
            "64 lowercase",
        ),
        (
            "duplicate",
            Box::new(|bundle: &mut Value| {
                let turn = bundle["turns"][0].clone();
                bundle["turns"].as_array_mut().expect("turns").push(turn);
            }),
            "more than once",
        ),
        (
            "json",
            Box::new(|bundle: &mut Value| {
                bundle["turns"][0]["entries"] = json!("not-an-array");
            }),
            "failed to decode",
        ),
    ] {
        let mut candidate = baseline.clone();
        mutate(&mut candidate);
        let path = temp.path.join(format!("{label}.json"));
        write_json(&path, &candidate);
        let output = run_verify_bundle(&config, &["--bundle", path.to_str().expect("utf-8 path")]);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !output.status.success(),
            "the `{label}` bundle must be rejected"
        );
        assert!(
            stderr.contains(expected),
            "the `{label}` error must mention `{expected}`:\n{stderr}"
        );
    }

    let oversized = temp.path.join("oversized.json");
    let file = std::fs::File::create(&oversized).expect("oversized file should be created");
    file.set_len(MAX_BUNDLE_BYTES + 1)
        .expect("sparse size should be set");
    drop(file);
    let output = run_verify_bundle(&config, &["--bundle", path(&oversized)]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("exceeding") && stderr.contains("limit"),
        "the size limit must be reported:\n{stderr}"
    );
}

#[test]
fn an_empty_store_exports_an_empty_verifiable_bundle() {
    let temp = TempDir::new("empty");
    let database = temp.path.join("events.sqlite");
    drop(EventStore::open(&database).expect("event store should open"));
    let config = write_config(&temp, &database);
    let bundle = temp.path.join("empty.json");

    let exported = run_export(&config, &["--all", "--out", path(&bundle)]);
    let stdout = String::from_utf8_lossy(&exported.stdout);
    assert!(
        exported.status.success(),
        "an empty store has no invalid turn:\n{stdout}\n{}",
        String::from_utf8_lossy(&exported.stderr)
    );
    assert!(stdout.contains("exported 0 turn(s) to"), "{stdout}");
    let parsed: Value = read_json(&bundle);
    assert_eq!(
        parsed["turns"].as_array().expect("turns must be an array"),
        &Vec::<Value>::new()
    );
    assert!(is_lower_hex_64(
        parsed["aggregate_digest"].as_str().expect("aggregate")
    ));

    let verified = run_verify_bundle(&config, &["--bundle", path(&bundle), "--json"]);
    let stderr = String::from_utf8_lossy(&verified.stderr);
    assert!(
        verified.status.success(),
        "an empty bundle must verify:\n{stderr}"
    );
    let report: Value = serde_json::from_slice(&verified.stdout).expect("report must be JSON");
    assert_eq!(report["summary"]["turns"], 0);
    assert_eq!(
        report["bundle"]["problems"].as_array().expect("problems"),
        &Vec::<Value>::new()
    );
}

#[test]
fn export_requires_an_event_store_and_never_creates_it() {
    let temp = TempDir::new("missing-store");
    let database = temp.path.join("missing.sqlite");
    let config = write_config(&temp, &database);
    let bundle = temp.path.join("evidence.json");

    let output = run_export(&config, &["--all", "--out", path(&bundle)]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("failed to open event store"),
        "the missing store must be reported:\n{stderr}"
    );
    assert!(!database.exists(), "export must not create the database");
    assert!(!bundle.exists(), "a failed export must not write a bundle");

    let empty_config = write_empty_config(&temp);
    let output = run_export(&empty_config, &["--all", "--out", path(&bundle)]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("requires an `[events]` path"),
        "export must require [events]"
    );
}

/// Runs `trace export`.
fn run_export(config: &Path, arguments: &[&str]) -> Output {
    run_jessica(Some(config), &[&["trace", "export"], arguments].concat())
}

/// Runs `trace verify-bundle`.
fn run_verify_bundle(config: &Path, arguments: &[&str]) -> Output {
    run_jessica(
        Some(config),
        &[&["trace", "verify-bundle"], arguments].concat(),
    )
}

/// Runs the real CLI without provider configuration.
fn run_jessica(config: Option<&Path>, arguments: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_jessica"));
    command
        .current_dir(workspace_root())
        .env_remove("JESSICA_PROVIDER")
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("OPENAI_MODEL")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_BASE_URL")
        .env_remove("ANTHROPIC_MODEL");
    if let Some(config) = config {
        command.arg("--config").arg(config);
    }
    let mut child = command
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

fn write_empty_config(temp: &TempDir) -> PathBuf {
    let config = temp.path.join("empty.toml");
    std::fs::write(&config, "").expect("empty config should be written");
    config
}

fn read_json(path: &Path) -> Value {
    let bytes = std::fs::read(path).expect("bundle should be readable");
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "bundle is not JSON: {error}\n{}",
            String::from_utf8_lossy(&bytes)
        )
    })
}

fn write_json(path: &Path, value: &Value) {
    let mut bytes = serde_json::to_vec_pretty(value).expect("value should serialize");
    bytes.push(b'\n');
    std::fs::write(path, bytes).expect("bundle should be written");
}

fn path(path: &Path) -> &str {
    path.to_str().expect("test paths must be UTF-8")
}

fn aggregate_from_export(output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("aggregate "))
        .unwrap_or_else(|| panic!("export output must contain the aggregate:\n{stdout}"))
        .trim()
        .to_owned()
}

fn bundle_problem_kinds(report: &Value) -> Vec<&str> {
    report["bundle"]["problems"]
        .as_array()
        .expect("bundle problems must be an array")
        .iter()
        .map(|problem| {
            problem["kind"]
                .as_str()
                .expect("every bundle problem must have a kind")
        })
        .collect()
}

fn problem_kinds(turn: &Value) -> Vec<&str> {
    turn["problems"]
        .as_array()
        .expect("turn problems must be an array")
        .iter()
        .map(|problem| {
            problem["kind"]
                .as_str()
                .expect("every turn problem must have a kind")
        })
        .collect()
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Writes a closed, semantically valid turn with five mirrored agent events.
fn seed_closed_turn(store: &mut EventStore, turn: &str, session_name: &str, base: i64) {
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
                messages: vec![Message::user("bundle")],
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

/// Writes a trace with no `turn_ended` marker.
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
                messages: vec![Message::user("open")],
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
                messages: vec![Message::user("gap")],
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
                skipped: 1,
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
