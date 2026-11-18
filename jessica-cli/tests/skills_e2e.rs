//! End-to-end acceptance for skill triggering, distillation, and injection.
//!
//! Every scenario drives the real CLI binary against a scripted SSE server and
//! a temporary workspace. The server classifies each request by body: the
//! scripted turn responses drive the agent, and a distillation request is
//! answered with one strict skill JSON document. No API key or network access
//! is involved.
//!
//! The scenarios pin the Task 5.2.6 boundaries:
//!
//! - thresholds use structured counts and below-threshold turns create no job;
//! - a triggered turn stores a draft but never injects a draft;
//! - manual activation makes a draft injectable in the fixed
//!   `system → memory → skills → user` order;
//! - invalid distillation output fails the job and never changes the turn;
//! - resume never injects but enqueues idempotently through the queue's unique
//!   index;
//! - replay never opens the skill database;
//! - failure to open the database, a drain timeout, and duplicate writes are
//!   fail-open;
//! - review subcommands never initialize a provider or the event store.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId, ToolCall, ToolResult};
use jessica_harness::{DistillSource, DistillSourceInput, TurnSignals};
use jessica_memory::{
    EventStore, MemoryLog, NewMemoryEntry, NewSkill, NewSkillJob, SkillJob, SkillJobStatus,
    SkillLog, SkillSource as StoredSkillSource, SkillStatus, SqliteMemoryStore, SqliteSkillStore,
    Timestamp, WorkspaceScope,
};
use serde_json::{Value, json};

/// Overall budget for one child process.
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for the mock server to keep accepting requests.
const SERVER_TIMEOUT: Duration = Duration::from_secs(30);

/// Marker present only in the fixed distillation system prompt.
const DISTILL_MARKER: &str = "You distill one completed agent turn";
/// Title rendered by `SkillGuide`.
const SKILL_TITLE: &str = "[技能参考";
/// Title rendered by `MemoryGuide`.
const MEMORY_TITLE: &str = "[记忆参考";

/// Skill content returned by the scripted distiller.
const SKILL_NAME: &str = "ORANGE workflow";
const SKILL_DESCRIPTION: &str = "Reusable ORANGE and MAGENTA inspection procedure";
const SKILL_INSTRUCTIONS: &str = "1. Read NOTES.txt.\n2. Report the ORANGE summary.";
/// Tool output that must never reach `skills.db` while outputs are disabled.
const TOOL_SECRET: &str = "SECRET-TOOL-OUTPUT";

/// Temporary directory removed when the test finishes.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-cli-skills-e2e-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temporary directory should be created");
        Self { path }
    }

    fn skills_db(&self) -> PathBuf {
        self.path.join(".jessica/skills.db")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[test]
fn below_threshold_turns_create_no_job_and_no_skill() {
    let temp = TempDir::new("below-threshold");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[budget]\nmax_rounds = 4\n\n\
         [skills]\nenabled = true\npath = \"skills.db\"\n\
         min_tool_calls = 1\nmin_rounds = 2\n",
    );

    let server = ScriptedServer::start(|body| {
        assert!(
            !is_distill_request(body),
            "a below-threshold turn must not request distillation"
        );
        text_response("plain answer without tools")
    });
    let output = run_cli(
        server.address,
        &temp.path,
        &config,
        &["summarize the ORANGE notes"],
    );
    let requests = server.finish();

    assert_success(&output);
    assert_eq!(requests.len(), 1);
    assert_eq!(request_messages(&requests[0]).len(), 2);

    let database = temp.path.join("skills.db");
    assert!(database.is_file(), "an enabled store is opened eagerly");
    assert!(list_jobs(&database, &temp.path).is_empty());
    assert!(list_skills(&database, &temp.path).is_empty());
}

#[test]
fn threshold_turn_stores_a_draft_but_never_injects_it_and_writes_no_skill_events() {
    let temp = TempDir::new("threshold");
    prepare_notes(&temp.path);
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[budget]\nmax_rounds = 4\n\n\
         [events]\npath = \"events.sqlite\"\n\n\
         [skills]\nenabled = true\npath = \"skills.db\"\n\
         min_tool_calls = 1\nmin_rounds = 2\ninclude_tool_outputs = false\n",
    );

    let mut turn = 0;
    let server = ScriptedServer::start(move |body| {
        if is_distill_request(body) {
            return distill_response();
        }
        let response = if turn == 0 {
            tool_call_response("call_read", "NOTES.txt")
        } else {
            text_response("the ORANGE notes were read")
        };
        turn += 1;
        response
    });

    let output = run_cli(
        server.address,
        &temp.path,
        &config,
        &["read the ORANGE notes"],
    );
    let requests = server.finish();
    assert_success(&output);
    assert_eq!(
        requests
            .iter()
            .filter(|body| !is_distill_request(body))
            .count(),
        2,
        "the scripted turn needs one tool round and one answer"
    );

    let database = temp.path.join("skills.db");
    let jobs = list_jobs(&database, &temp.path);
    assert_eq!(jobs.len(), 1, "one completed turn must enqueue one job");
    let job = &jobs[0];
    assert_eq!(job.status(), SkillJobStatus::Done);
    assert_eq!(job.turn_index(), 0);
    assert!(
        job.source_json().contains("read_file: ok"),
        "the persisted source must carry structured tool summaries: {}",
        job.source_json()
    );
    assert!(
        !job.source_json().contains(TOOL_SECRET),
        "full tool outputs must not reach skills.db: {}",
        job.source_json()
    );
    assert!(job.source_json().contains("\"tool_call_count\":1"));
    assert!(job.source_json().contains("\"assistant_rounds\":2"));

    let skills = list_skills(&database, &temp.path);
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].name(), SKILL_NAME);
    assert_eq!(
        skills[0].status(),
        SkillStatus::Draft,
        "auto_activate defaults to false"
    );

    let events = EventStore::open(temp.path.join("events.sqlite"))
        .expect("event store should open")
        .events(job.session_id())
        .expect("recorded events should load");
    assert!(!events.is_empty());
    for event in events {
        // Bootstrap messages legitimately carry the workspace path, which can
        // contain "skills"; only the skill-job event vocabulary or payload is
        // forbidden here.
        assert!(
            !event.event_type().to_ascii_lowercase().contains("skill"),
            "skill-job state must never enter the agent event stream: {}",
            event.event_type()
        );
        let payload = event.payload_json();
        for marker in [
            SKILL_NAME,
            SKILL_DESCRIPTION,
            SKILL_INSTRUCTIONS,
            SKILL_TITLE,
        ] {
            assert!(
                !payload.contains(marker),
                "skill-job state must never enter the agent event stream: {payload}"
            );
        }
        assert!(
            !payload.contains("\"tool_call_count\"") && !payload.contains("\"assistant_rounds\""),
            "skill-job source state must never enter the agent event stream: {payload}"
        );
    }

    // A second fresh turn queries the same ORANGE keyword. The stored draft
    // would match, but drafts are never injectable.
    let server = ScriptedServer::start(|body| {
        assert!(
            !is_distill_request(body),
            "the draft-only turn is below threshold and must not distill"
        );
        text_response("draft is not used")
    });
    let second = run_cli(server.address, &temp.path, &config, &["ORANGE"]);
    let second_requests = server.finish();
    assert_success(&second);
    assert_eq!(second_requests.len(), 1);
    let messages = request_messages(&second_requests[0]);
    assert_eq!(
        messages.len(),
        2,
        "a draft must not inject a skill message: {second_requests:?}"
    );
    assert!(!second_requests[0].contains(SKILL_TITLE));
}

#[test]
fn manual_review_commands_and_activation_inject_memory_then_skills() {
    let temp = TempDir::new("activation");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[budget]\nmax_rounds = 4\n\n\
         [memory]\nenabled = true\npath = \".jessica/memory.db\"\n\n\
         [skills]\nenabled = true\npath = \".jessica/skills.db\"\n",
    );
    std::fs::create_dir_all(temp.path.join(".jessica")).expect("skill directory should exist");
    let database = temp.skills_db();
    let id = seed_skill(&database, &temp.path, "seed-skill-session", 0);
    seed_memory(
        &temp.path.join(".jessica/memory.db"),
        &temp.path,
        "The launch code is ORANGE MAGENTA.",
    );

    let list = run_skill_command(&temp.path, &config, &["list"]);
    assert_success(&list);
    assert!(String::from_utf8_lossy(&list.stdout).contains(SKILL_NAME));

    let show = run_skill_command(&temp.path, &config, &["show", &id.to_string()]);
    assert_success(&show);
    let shown = String::from_utf8_lossy(&show.stdout);
    assert!(shown.contains(SKILL_NAME));
    assert!(shown.contains(SKILL_DESCRIPTION));
    assert!(shown.contains(SKILL_INSTRUCTIONS));

    let activate = run_skill_command(&temp.path, &config, &["activate", &id.to_string()]);
    assert_success(&activate);
    assert!(String::from_utf8_lossy(&activate.stdout).contains("active"));

    let active = run_skill_command(&temp.path, &config, &["list", "--status", "active"]);
    assert_success(&active);
    assert!(String::from_utf8_lossy(&active.stdout).contains(SKILL_NAME));

    let server = ScriptedServer::start(|body| {
        assert!(
            !is_distill_request(body),
            "a plain one-round turn must not distill"
        );
        text_response("ORANGE and MAGENTA are available")
    });
    let output = run_cli(server.address, &temp.path, &config, &["ORANGE MAGENTA"]);
    let requests = server.finish();
    assert_success(&output);
    assert_eq!(requests.len(), 1);

    let messages = request_messages(&requests[0]);
    assert_eq!(
        messages.len(),
        4,
        "expected system, memory, skills, and user messages: {requests:?}"
    );
    assert_eq!(messages[0]["role"], "system");
    assert!(
        messages[0]["content"]
            .as_str()
            .expect("system content")
            .contains("You are Jessica")
    );
    assert_eq!(messages[1]["role"], "system");
    let memory = messages[1]["content"].as_str().expect("memory content");
    assert!(memory.contains(MEMORY_TITLE));
    assert!(memory.contains("ORANGE MAGENTA"));
    assert_eq!(messages[2]["role"], "system");
    let skills = messages[2]["content"].as_str().expect("skill content");
    assert!(skills.contains(SKILL_TITLE));
    assert!(skills.contains("仅供参考"));
    assert!(skills.contains(SKILL_NAME));
    assert_eq!(messages[3]["role"], "user");
    assert_eq!(messages[3]["content"], "ORANGE MAGENTA");
    assert!(
        !temp.path.join(".jessica/events.sqlite").exists(),
        "review and turn paths must not create an event store without one"
    );

    let delete = run_skill_command(&temp.path, &config, &["delete", &id.to_string()]);
    assert_success(&delete);
    assert!(String::from_utf8_lossy(&delete.stdout).contains("deleted"));

    let after = run_skill_command(&temp.path, &config, &["list"]);
    assert_success(&after);
    assert!(String::from_utf8_lossy(&after.stdout).contains("no skills"));

    let shown = run_skill_command(&temp.path, &config, &["show", &id.to_string()]);
    assert_success(&shown);
    let shown = String::from_utf8_lossy(&shown.stdout);
    assert!(
        shown.contains(SKILL_NAME),
        "soft-deleted skills stay readable"
    );
    assert!(shown.contains("deleted_at_ms: "));
    assert!(
        !shown.contains("deleted_at_ms: -"),
        "soft delete must set a time"
    );

    let missing = run_skill_command(&temp.path, &config, &["show", "999"]);
    assert!(
        !missing.status.success(),
        "a missing skill must produce a non-zero exit"
    );
    assert!(String::from_utf8_lossy(&missing.stderr).contains("999"));
}

#[test]
fn disabled_review_commands_fail_cleanly_without_a_provider_or_database() {
    let temp = TempDir::new("disabled-command");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[skills]\nenabled = false\npath = \".jessica/skills.db\"\n",
    );

    let output = run_skill_command(&temp.path, &config, &["list"]);

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("skills are disabled"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!temp.path.join(".jessica").exists());
}

#[test]
fn invalid_distillation_json_fails_the_job_without_changing_the_turn() {
    let temp = TempDir::new("invalid-json");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[skills]\nenabled = true\npath = \"skills.db\"\n\
         min_tool_calls = 0\nmin_rounds = 0\nmax_attempts = 2\n",
    );

    let server = ScriptedServer::start(|body| {
        if is_distill_request(body) {
            return text_response("this is not a skill JSON document");
        }
        text_response("the turn still completed")
    });
    let output = run_cli(server.address, &temp.path, &config, &["finish normally"]);
    let requests = server.finish();

    assert_success(&output);
    assert!(String::from_utf8_lossy(&output.stdout).contains("the turn still completed"));
    assert_eq!(
        requests
            .iter()
            .filter(|body| !is_distill_request(body))
            .count(),
        1
    );

    let database = temp.path.join("skills.db");
    let jobs = list_jobs(&database, &temp.path);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].status(), SkillJobStatus::Failed);
    assert!(
        jobs[0]
            .last_error()
            .is_some_and(|error| error.contains("not valid JSON")),
        "the stored error must be bounded and descriptive: {:?}",
        jobs[0].last_error()
    );
    assert!(list_skills(&database, &temp.path).is_empty());
}

#[test]
fn resume_never_injects_skills_and_enqueues_the_same_turn_idempotently() {
    let temp = TempDir::new("resume");
    prepare_notes(&temp.path);
    let events = temp.path.join("events.sqlite");
    let session = SessionId::new("cli-skills-resume-session");
    seed_interrupted_session(&events, &session);

    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[budget]\nmax_rounds = 4\n\n\
         [events]\npath = \"events.sqlite\"\n\n\
         [skills]\nenabled = true\npath = \"skills.db\"\n\
         min_tool_calls = 0\nmin_rounds = 0\n",
    );

    let database = temp.path.join("skills.db");
    let signals = TurnSignals::new(SessionEndReason::Completed, 1, 2);
    let source = DistillSource::new(
        DistillSourceInput::new(
            session.clone(),
            0,
            "seeded resume goal",
            "seeded resume answer",
            &signals,
        ),
        4096,
    );
    let seeded_json = seed_pending_job(&database, &temp.path, &session, 0, &source);

    let server = ScriptedServer::start(|body| {
        if is_distill_request(body) {
            return distill_response();
        }
        text_response("resume completed the ORANGE turn")
    });
    let output = run_cli(
        server.address,
        &temp.path,
        &config,
        &["--resume", session.as_str()],
    );
    let requests = server.finish();

    assert_success(&output);
    let turn_requests: Vec<&String> = requests
        .iter()
        .filter(|body| !is_distill_request(body))
        .collect();
    assert_eq!(turn_requests.len(), 1);
    assert!(
        !turn_requests[0].contains(SKILL_TITLE),
        "resume must never inject skills: {}",
        turn_requests[0]
    );
    assert!(!turn_requests[0].contains(SKILL_NAME));

    let jobs = list_jobs(&database, &temp.path);
    assert_eq!(
        jobs.len(),
        1,
        "the completed resumed turn must not create a second job"
    );
    let job = &jobs[0];
    assert_eq!(job.session_id(), &session);
    assert_eq!(job.turn_index(), 0);
    assert_eq!(
        job.source_json(),
        seeded_json,
        "the unique index must keep the original source unchanged"
    );
    assert_eq!(
        job.created_at().as_millis(),
        1,
        "a duplicate enqueue must not replace the existing row"
    );
    assert_eq!(job.status(), SkillJobStatus::Done);

    let skills = list_skills(&database, &temp.path);
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].name(), SKILL_NAME);
    assert_eq!(skills[0].source().session_id, session);
    assert_eq!(skills[0].source().turn_index, 0);
}

#[test]
fn replay_never_opens_the_skill_database_and_consecutive_replays_ignore_it() {
    let temp = TempDir::new("replay");
    let events = temp.path.join("events.sqlite");
    let session = SessionId::new("cli-skills-replay-session");
    seed_completed_session(&events, &session);

    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[events]\npath = \"events.sqlite\"\n\n\
         [skills]\nenabled = true\npath = \".jessica/skills.db\"\n",
    );

    let first = run_replay(&temp.path, &config, &session, "first.jsonl");
    assert_success(&first);
    assert!(
        !temp.path.join(".jessica").exists(),
        "replay must not create the skill directory or database"
    );

    // Seed a pending job, then verify replay leaves the database byte-for-byte
    // untouched and still succeeds as many times as it is called.
    std::fs::create_dir_all(temp.path.join(".jessica")).expect("skill directory should exist");
    let database = temp.skills_db();
    let pending_session = SessionId::new("replay-pending-session");
    let signals = TurnSignals::new(SessionEndReason::Completed, 1, 1);
    let source = DistillSource::new(
        DistillSourceInput::new(
            pending_session.clone(),
            0,
            "replay pending goal",
            "replay pending answer",
            &signals,
        ),
        4096,
    );
    seed_pending_job(&database, &temp.path, &pending_session, 0, &source);
    let before = std::fs::read(&database).expect("skill database should be readable");
    let before_entries = directory_entries(&temp.path.join(".jessica"));

    for output_path in ["second.jsonl", "third.jsonl"] {
        let output = run_replay(&temp.path, &config, &session, output_path);
        assert_success(&output);
    }

    assert_eq!(
        std::fs::read(&database).expect("skill database should stay readable"),
        before,
        "replay must not modify skills.db"
    );
    assert_eq!(
        directory_entries(&temp.path.join(".jessica")),
        before_entries,
        "replay must not create WAL side files"
    );

    let jobs = list_jobs(&database, &temp.path);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].status(), SkillJobStatus::Pending);
    assert_eq!(jobs[0].attempts(), 0);
    assert!(list_skills(&database, &temp.path).is_empty());
}

#[test]
fn skill_open_failure_degrades_without_changing_the_turn() {
    let temp = TempDir::new("open-failure");
    let blocker = temp.path.join(".jessica");
    std::fs::write(&blocker, b"not a directory").expect("blocking file should be written");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[skills]\nenabled = true\npath = \".jessica/skills.db\"\n\
         min_tool_calls = 0\nmin_rounds = 0\n",
    );

    let server = ScriptedServer::start(|body| {
        assert!(
            !is_distill_request(body),
            "no skill store means no distillation"
        );
        text_response("still works without skills")
    });
    let output = run_cli(server.address, &temp.path, &config, &["hello"]);
    let requests = server.finish();

    assert_success(&output);
    assert!(String::from_utf8_lossy(&output.stdout).contains("still works without skills"));
    assert_eq!(requests.len(), 1);
    assert_eq!(request_messages(&requests[0]).len(), 2);
    assert_eq!(
        std::fs::read(&blocker).expect("blocking file should remain readable"),
        b"not a directory"
    );
}

#[test]
fn drain_timeout_leaves_the_job_recoverable_and_keeps_exit_code_zero() {
    let temp = TempDir::new("drain-timeout");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[skills]\nenabled = true\npath = \"skills.db\"\n\
         min_tool_calls = 0\nmin_rounds = 0\nmax_attempts = 1\n\
         distill_timeout_ms = 5000\ndrain_timeout_ms = 300\n",
    );

    let server = ScriptedServer::start(|body| {
        if is_distill_request(body) {
            thread::sleep(Duration::from_millis(1500));
            return distill_response();
        }
        text_response("the turn finished before the drain timeout")
    });
    let output = run_cli(
        server.address,
        &temp.path,
        &config,
        &["trigger a slow distill"],
    );
    let requests = server.finish();

    assert_success(&output);
    assert!(String::from_utf8_lossy(&output.stdout).contains("the turn finished"));
    assert_eq!(
        requests
            .iter()
            .filter(|body| !is_distill_request(body))
            .count(),
        1
    );

    let database = temp.path.join("skills.db");
    let jobs = list_jobs(&database, &temp.path);
    assert_eq!(jobs.len(), 1);
    assert!(
        matches!(
            jobs[0].status(),
            SkillJobStatus::Pending | SkillJobStatus::Processing
        ),
        "an undrained job must remain recoverable, got {:?}",
        jobs[0].status()
    );
    assert!(list_skills(&database, &temp.path).is_empty());
}

#[test]
fn duplicate_sink_writes_do_not_duplicate_skills() {
    let temp = TempDir::new("duplicate");
    prepare_notes(&temp.path);
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[skills]\nenabled = true\npath = \"skills.db\"\n\
         min_tool_calls = 1\nmin_rounds = 2\n",
    );
    let database = temp.path.join("skills.db");
    seed_skill(&database, &temp.path, "seed-duplicate-session", 0);

    let mut turn = 0;
    let server = ScriptedServer::start(move |body| {
        if is_distill_request(body) {
            return distill_response();
        }
        let response = if turn == 0 {
            tool_call_response("call_read", "NOTES.txt")
        } else {
            text_response("the ORANGE turn completed")
        };
        turn += 1;
        response
    });
    let output = run_cli(
        server.address,
        &temp.path,
        &config,
        &["read the ORANGE notes"],
    );
    server.finish();

    assert_success(&output);
    let skills = list_skills(&database, &temp.path);
    assert_eq!(
        skills.len(),
        1,
        "an identical distilled skill must be absorbed, not duplicated"
    );
    assert_eq!(
        skills[0].source().session_id,
        SessionId::new("seed-duplicate-session"),
        "the existing row must not be replaced"
    );

    let jobs = list_jobs(&database, &temp.path);
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].status(),
        SkillJobStatus::Done,
        "an absorbed duplicate is still a successful write"
    );
}

#[test]
fn auto_activate_makes_a_new_skill_injectable_without_a_manual_command() {
    let temp = TempDir::new("auto-activate");
    let config = temp.path.join("harness.toml");
    write_config(
        &config,
        "[skills]\nenabled = true\npath = \"skills.db\"\n\
         min_tool_calls = 0\nmin_rounds = 0\nauto_activate = true\n",
    );

    let server = ScriptedServer::start(|body| {
        if is_distill_request(body) {
            return distill_response();
        }
        text_response("stored the ORANGE knowledge")
    });
    let first = run_cli(
        server.address,
        &temp.path,
        &config,
        &["store ORANGE knowledge"],
    );
    server.finish();
    assert_success(&first);

    let database = temp.path.join("skills.db");
    let skills = list_skills(&database, &temp.path);
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].status(), SkillStatus::Active);

    let server = ScriptedServer::start(|body| {
        if is_distill_request(body) {
            return distill_response();
        }
        text_response("used the ORANGE knowledge")
    });
    let second = run_cli(server.address, &temp.path, &config, &["ORANGE"]);
    let requests = server.finish();
    assert_success(&second);

    let turn_request = requests
        .iter()
        .find(|body| !is_distill_request(body))
        .expect("the second turn must reach the model");
    let messages = request_messages(turn_request);
    assert_eq!(messages.len(), 3, "system, skill, and user messages");
    assert_eq!(messages[0]["role"], "system");
    assert_eq!(messages[1]["role"], "system");
    let content = messages[1]["content"].as_str().expect("skill content");
    assert!(content.contains(SKILL_TITLE));
    assert!(content.contains("仅供参考"));
    assert!(content.contains(SKILL_NAME));

    assert_eq!(
        list_skills(&database, &temp.path).len(),
        1,
        "the second distillation is absorbed as a duplicate"
    );
}

/// Starts a scripted HTTP server that answers requests until stopped.
struct ScriptedServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<Vec<String>>,
}

impl ScriptedServer {
    fn start<F>(respond: F) -> Self
    where
        F: FnMut(&str) -> String + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
        let address = listener.local_addr().expect("mock server address");
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = thread::spawn(move || serve(listener, flag, respond));
        Self {
            address,
            stop,
            handle,
        }
    }

    fn finish(self) -> Vec<String> {
        self.stop.store(true, Ordering::SeqCst);
        self.handle.join().expect("mock server should finish")
    }
}

/// Accepts requests until `stop` is set and returns every request body.
fn serve<F>(listener: TcpListener, stop: Arc<AtomicBool>, mut respond: F) -> Vec<String>
where
    F: FnMut(&str) -> String,
{
    listener
        .set_nonblocking(true)
        .expect("listener should become non-blocking");
    let deadline = Instant::now() + SERVER_TIMEOUT;
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
                    let response = respond(&body);
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
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

fn is_distill_request(body: &str) -> bool {
    body.contains(DISTILL_MARKER)
}

/// Returns one SSE response carrying `content` and a stop finish reason.
fn text_response(content: &str) -> String {
    sse(&[content_chunk(content), finish_chunk("stop")])
}

/// Returns one SSE response requesting the `read_file` tool.
fn tool_call_response(id: &str, path: &str) -> String {
    let arguments = json!({"path": path}).to_string();
    sse(&[
        tool_call_chunk(id, "read_file", &arguments),
        finish_chunk("tool_calls"),
    ])
}

/// Returns a valid distilled-skill JSON document as a model answer.
fn distill_response() -> String {
    let document = json!({
        "name": SKILL_NAME,
        "description": SKILL_DESCRIPTION,
        "instructions": SKILL_INSTRUCTIONS,
        "tags": ["orange", "workflow"],
    })
    .to_string();
    text_response(&document)
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
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn tool_call_chunk(id: &str, name: &str, arguments: &str) -> String {
    json!({
        "choices": [{
            "index": 0,
            "delta": {
                "tool_calls": [{
                    "index": 0,
                    "id": id,
                    "function": {"name": name, "arguments": arguments}
                }]
            },
            "finish_reason": null
        }]
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

fn write_config(path: &Path, body: &str) {
    std::fs::write(path, body).expect("configuration should be written");
}

/// Writes a file the scripted `read_file` tool reads.
fn prepare_notes(workspace: &Path) {
    std::fs::write(workspace.join("NOTES.txt"), TOOL_SECRET)
        .expect("tool input file should be written");
}

/// Runs one prompt turn against the scripted server.
fn run_cli(address: SocketAddr, workspace: &Path, config: &Path, args: &[&str]) -> Output {
    let child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .env_remove("JESSICA_PROVIDER")
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_BASE_URL", format!("http://{address}/v1"))
        .env("OPENAI_MODEL", "test-model")
        .arg("--config")
        .arg(config)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should start");
    finish_child(child)
}

/// Runs a skill review command without any provider configuration.
fn run_skill_command(workspace: &Path, config: &Path, args: &[&str]) -> Output {
    run_without_provider(workspace, config, &[args], "skills")
}

/// Runs the CLI in replay mode without any provider configuration.
fn run_replay(workspace: &Path, config: &Path, session: &SessionId, output: &str) -> Output {
    run_without_provider(
        workspace,
        config,
        &[&["--session", session.as_str(), "--out", output]],
        "replay",
    )
}

fn run_without_provider(
    workspace: &Path,
    config: &Path,
    args: &[&[&str]],
    subcommand: &str,
) -> Output {
    let child = Command::new(env!("CARGO_BIN_EXE_jessica"))
        .current_dir(workspace)
        .env_remove("OPENAI_API_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("OPENAI_MODEL")
        .env_remove("JESSICA_PROVIDER")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_BASE_URL")
        .env_remove("ANTHROPIC_MODEL")
        .arg("--config")
        .arg(config)
        .arg(subcommand)
        .args(args.iter().flat_map(|arguments| arguments.iter().copied()))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should start");
    finish_child(child)
}

fn finish_child(mut child: Child) -> Output {
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

fn assert_success(output: &Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "CLI failed with {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
}

fn request_messages(body: &str) -> Vec<Value> {
    let parsed: Value = serde_json::from_str(body).expect("request body should be JSON");
    parsed["messages"]
        .as_array()
        .expect("request body should contain messages")
        .clone()
}

fn workspace_scope(database: &Path, workspace: &Path) -> WorkspaceScope {
    let canonical = std::fs::canonicalize(workspace).expect("workspace should canonicalize");
    SqliteSkillStore::open(database, canonical)
        .expect("skill store should open")
        .workspace_scope()
        .clone()
}

fn list_jobs(database: &Path, workspace: &Path) -> Vec<SkillJob> {
    let scope = workspace_scope(database, workspace);
    SkillLog::open(database)
        .expect("skill log should open")
        .list_jobs(&scope, 50)
        .expect("jobs should list")
}

fn list_skills(database: &Path, workspace: &Path) -> Vec<jessica_memory::Skill> {
    let scope = workspace_scope(database, workspace);
    SkillLog::open(database)
        .expect("skill log should open")
        .list_recent(&scope, 50)
        .expect("skills should list")
}

/// Seeds one draft skill with the standard fixture content.
fn seed_skill(database: &Path, workspace: &Path, session: &str, turn_index: u64) -> i64 {
    let scope = workspace_scope(database, workspace);
    let log = SkillLog::open(database).expect("skill log should open");
    log.append(
        NewSkill::new(
            scope,
            StoredSkillSource::new(SessionId::new(session), turn_index, 1, 2, 0),
            SKILL_NAME,
            SKILL_DESCRIPTION,
            SKILL_INSTRUCTIONS,
            ["orange", "workflow"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            Timestamp::from_millis(1),
        )
        .expect("seed skill should construct"),
    )
    .expect("seed skill should append")
    .get()
}

/// Seeds one short-term memory entry.
fn seed_memory(database: &Path, workspace: &Path, content: &str) {
    let canonical = std::fs::canonicalize(workspace).expect("workspace should canonicalize");
    let store =
        SqliteMemoryStore::open(database, canonical, None).expect("memory store should open");
    let scope = store.workspace_scope().clone();
    drop(store);
    let log = MemoryLog::open(database).expect("memory log should open");
    log.append(
        NewMemoryEntry::new(
            SessionId::new("seed-memory-session"),
            scope,
            content,
            Timestamp::from_millis(1),
        )
        .expect("memory entry should construct"),
    )
    .expect("memory entry should append");
}

/// Seeds one pending job and returns its persisted source JSON.
fn seed_pending_job(
    database: &Path,
    workspace: &Path,
    session: &SessionId,
    turn_index: u64,
    source: &DistillSource,
) -> String {
    let scope = workspace_scope(database, workspace);
    let source_json = source.to_persisted_json();
    let log = SkillLog::open(database).expect("skill log should open");
    log.enqueue_job(
        NewSkillJob::new(
            scope,
            session.clone(),
            turn_index,
            source_json.clone(),
            Timestamp::from_millis(1),
        )
        .expect("seed job should construct"),
    )
    .expect("seed job should enqueue");
    source_json
}

/// Writes a session that stopped after its first complete tool batch.
fn seed_interrupted_session(database: &Path, session: &SessionId) {
    let mut store = EventStore::open(database).expect("event store should open");
    store
        .record_session(
            session,
            &[
                Message::system("resume test system prompt"),
                Message::user("finish the ORANGE resume"),
            ],
            Timestamp::from_millis(1),
        )
        .expect("session metadata should be recorded");

    let call = ToolCall::new("call_1", "read_file", json!({"path": "NOTES.txt"}));
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

/// Returns the sorted file names in `directory`.
fn directory_entries(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .expect("directory should be readable")
        .map(|entry| {
            entry
                .expect("directory entry should be readable")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort_unstable();
    names
}
