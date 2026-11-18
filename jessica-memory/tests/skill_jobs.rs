//! Behavior tests for the persistent skill-job queue.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, TryRecvError};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use jessica_core::types::{AgentEvent, SessionId};
use jessica_memory::{
    EventStore, MAX_LAST_ERROR_BYTES, MAX_SOURCE_JSON_BYTES, NewSkillJob, SkillJob, SkillJobError,
    SkillJobId, SkillJobStatus, SkillLog, SqliteSkillStore, Timestamp, WorkspaceScope,
};
use rusqlite::Connection;

/// Process-local counter so parallel tests never share a database file.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A temporary skills database removed on drop.
struct TempDatabase {
    /// Database file path.
    path: PathBuf,
}

impl TempDatabase {
    /// Reserves a unique database path in the system temporary directory.
    fn new(label: &str) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-skill-jobs-{label}-{}-{id}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }

    /// Returns the database file path.
    fn path(&self) -> &Path {
        &self.path
    }

    /// Removes the database and all SQLite sidecar files.
    fn remove_files(&self) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut path = self.path.clone().into_os_string();
            path.push(suffix);
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        self.remove_files();
    }
}

/// Builds a non-empty workspace scope.
fn workspace(scope: &str) -> WorkspaceScope {
    WorkspaceScope::new(scope).expect("test workspace scope should be valid")
}

/// Builds a source document with a fixed goal.
fn source_json(session: &str, turn_index: u64) -> String {
    source_json_with_goal(session, turn_index, "goal")
}

/// Builds a serialized `DistillSource` with an explicit goal.
fn source_json_with_goal(session: &str, turn_index: u64, goal: &str) -> String {
    serde_json::json!({
        "session_id": session,
        "turn_index": turn_index,
        "user_goal": goal,
        "final_answer": "answer",
        "tool_summaries": ["summary"],
        "tool_call_count": 5,
        "assistant_rounds": 2,
        "acceptance_failures": 0,
    })
    .to_string()
}

/// Builds a validated job input.
fn new_job(
    workspace_scope: &WorkspaceScope,
    session: &str,
    turn_index: u64,
    created_at: i64,
) -> NewSkillJob {
    NewSkillJob::new(
        workspace_scope.clone(),
        SessionId::new(session),
        turn_index,
        source_json(session, turn_index),
        Timestamp::from_millis(created_at),
    )
    .expect("test job should be valid")
}

/// Enqueues one job and returns the stored row.
fn enqueue(
    log: &SkillLog,
    workspace_scope: &WorkspaceScope,
    session: &str,
    turn_index: u64,
    created_at: i64,
) -> SkillJob {
    log.enqueue_job(new_job(workspace_scope, session, turn_index, created_at))
        .expect("enqueue should succeed")
}

#[test]
fn duplicate_enqueue_returns_the_existing_job_without_a_new_row() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-duplicate-job");

    let first = enqueue(&log, &scope, "session-duplicate", 1, 100);
    assert_eq!(first.status(), SkillJobStatus::Pending);
    assert_eq!(first.created_at(), Timestamp::from_millis(100));

    let duplicate = NewSkillJob::new(
        scope.clone(),
        SessionId::new("session-duplicate"),
        1,
        source_json_with_goal("session-duplicate", 1, "replacement goal"),
        Timestamp::from_millis(200),
    )
    .expect("duplicate job should validate");
    let second = log
        .enqueue_job(duplicate)
        .expect("duplicate enqueue should return the existing job");

    assert_eq!(second.id(), first.id());
    assert_eq!(second.created_at(), Timestamp::from_millis(100));
    assert_eq!(second.source_json(), first.source_json());
    assert_eq!(
        log.list_jobs(&scope, 10)
            .expect("list should succeed")
            .len(),
        1
    );
}

#[test]
fn different_turns_enqueue_separately() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-different-turns");

    let first = enqueue(&log, &scope, "session-turns", 1, 100);
    let second = enqueue(&log, &scope, "session-turns", 2, 100);
    let third = enqueue(&log, &scope, "session-other", 1, 100);

    assert_ne!(first.id(), second.id());
    assert_ne!(first.id(), third.id());
    assert_ne!(second.id(), third.id());
    assert_eq!(
        log.list_jobs(&scope, 10)
            .expect("list should succeed")
            .len(),
        3
    );
}

#[test]
fn done_and_failed_jobs_are_not_replaced_by_reenqueue() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-terminal");

    let done_job = enqueue(&log, &scope, "session-done", 1, 100);
    let claimed = log
        .claim_next_job(&scope, Timestamp::from_millis(110), Duration::from_secs(60))
        .expect("claim should succeed")
        .expect("a pending job should be claimable");
    assert_eq!(claimed.id(), done_job.id());
    let completed = log
        .complete_job(&scope, done_job.id(), Timestamp::from_millis(120))
        .expect("completion should succeed");
    assert_eq!(completed.status(), SkillJobStatus::Done);

    let done_duplicate = NewSkillJob::new(
        scope.clone(),
        SessionId::new("session-done"),
        1,
        source_json_with_goal("session-done", 1, "different source"),
        Timestamp::from_millis(130),
    )
    .expect("duplicate source should validate");
    let returned = log
        .enqueue_job(done_duplicate)
        .expect("duplicate enqueue should return the terminal job");
    assert_eq!(returned.status(), SkillJobStatus::Done);
    assert_eq!(returned.source_json(), done_job.source_json());
    assert_eq!(returned.attempts(), 1);

    let failed_job = enqueue(&log, &scope, "session-failed", 1, 200);
    let claimed = log
        .claim_next_job(&scope, Timestamp::from_millis(210), Duration::from_secs(60))
        .expect("claim should succeed")
        .expect("the second job should be claimable");
    assert_eq!(claimed.id(), failed_job.id());
    let failed = log
        .fail_job(
            &scope,
            failed_job.id(),
            Timestamp::from_millis(220),
            "provider timeout",
        )
        .expect("failure should succeed");
    assert_eq!(failed.status(), SkillJobStatus::Failed);
    assert_eq!(failed.last_error(), Some("provider timeout"));

    let failed_duplicate = NewSkillJob::new(
        scope.clone(),
        SessionId::new("session-failed"),
        1,
        source_json_with_goal("session-failed", 1, "different source"),
        Timestamp::from_millis(230),
    )
    .expect("duplicate source should validate");
    let returned = log
        .enqueue_job(failed_duplicate)
        .expect("duplicate enqueue should return the terminal job");
    assert_eq!(returned.status(), SkillJobStatus::Failed);
    assert_eq!(returned.last_error(), Some("provider timeout"));
    assert_eq!(returned.source_json(), failed_job.source_json());
    assert_eq!(
        log.list_jobs(&scope, 10)
            .expect("list should succeed")
            .len(),
        2
    );
}

#[test]
fn claim_order_is_deterministic_by_created_at_then_id() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-claim-order");

    let latest = enqueue(&log, &scope, "session-order", 3, 300);
    let oldest = enqueue(&log, &scope, "session-order", 1, 100);
    let middle = enqueue(&log, &scope, "session-order", 2, 100);

    let first = log
        .claim_next_job(
            &scope,
            Timestamp::from_millis(1000),
            Duration::from_secs(3600),
        )
        .expect("claim should succeed")
        .expect("a job should be claimable");
    let second = log
        .claim_next_job(
            &scope,
            Timestamp::from_millis(1000),
            Duration::from_secs(3600),
        )
        .expect("claim should succeed")
        .expect("a job should be claimable");
    let third = log
        .claim_next_job(
            &scope,
            Timestamp::from_millis(1000),
            Duration::from_secs(3600),
        )
        .expect("claim should succeed")
        .expect("a job should be claimable");
    let fourth = log
        .claim_next_job(
            &scope,
            Timestamp::from_millis(1000),
            Duration::from_secs(3600),
        )
        .expect("claim should succeed");

    assert_eq!(first.id(), oldest.id());
    assert_eq!(second.id(), middle.id());
    assert_eq!(third.id(), latest.id());
    assert!(fourth.is_none());
    for job in [first, second, third] {
        assert_eq!(job.attempts(), 1);
        assert_eq!(job.claimed_at(), Some(Timestamp::from_millis(1000)));
    }
}

#[test]
fn concurrent_claims_never_assign_the_same_job() {
    let database = TempDatabase::new("claim-race");
    let scope = workspace("workspace-claim-race");
    {
        let log = SkillLog::open(database.path()).expect("skill log should open");
        enqueue(&log, &scope, "session-race", 1, 100);
        enqueue(&log, &scope, "session-race", 2, 100);
    }

    let log_a = SkillLog::open(database.path()).expect("first claim log should open");
    let log_b = SkillLog::open(database.path()).expect("second claim log should open");
    let barrier = Arc::new(Barrier::new(3));

    let scope_a = scope.clone();
    let barrier_a = Arc::clone(&barrier);
    let claim_a = thread::spawn(move || {
        barrier_a.wait();
        log_a
            .claim_next_job(
                &scope_a,
                Timestamp::from_millis(1000),
                Duration::from_secs(3600),
            )
            .expect("first concurrent claim should succeed")
    });

    let scope_b = scope.clone();
    let barrier_b = Arc::clone(&barrier);
    let claim_b = thread::spawn(move || {
        barrier_b.wait();
        log_b
            .claim_next_job(
                &scope_b,
                Timestamp::from_millis(1000),
                Duration::from_secs(3600),
            )
            .expect("second concurrent claim should succeed")
    });

    barrier.wait();
    let first = claim_a
        .join()
        .expect("first claim thread should not panic")
        .expect("first claim should receive a job");
    let second = claim_b
        .join()
        .expect("second claim thread should not panic")
        .expect("second claim should receive a job");

    let ids: HashSet<_> = [first.id().get(), second.id().get()].into_iter().collect();
    assert_eq!(ids.len(), 2, "both claims returned the same job: {ids:?}");
}

#[test]
fn stale_processing_claims_are_recoverable_at_the_threshold() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-stale");
    let job = enqueue(&log, &scope, "session-stale", 1, 100);

    let first_claim = log
        .claim_next_job(&scope, Timestamp::from_millis(100), Duration::from_secs(60))
        .expect("initial claim should succeed")
        .expect("pending job should be claimable");
    assert_eq!(first_claim.attempts(), 1);
    assert_eq!(first_claim.claimed_at(), Some(Timestamp::from_millis(100)));

    let before_threshold = log
        .claim_next_job(
            &scope,
            Timestamp::from_millis(60_099),
            Duration::from_secs(60),
        )
        .expect("not-yet-stale claim should succeed");
    assert!(before_threshold.is_none());

    let at_threshold = log
        .claim_next_job(
            &scope,
            Timestamp::from_millis(60_100),
            Duration::from_secs(60),
        )
        .expect("stale claim should succeed")
        .expect("a stale processing job should be recoverable");
    assert_eq!(at_threshold.id(), job.id());
    assert_eq!(at_threshold.attempts(), 2);
    assert_eq!(
        at_threshold.claimed_at(),
        Some(Timestamp::from_millis(60_100))
    );

    let immediately_after = log
        .claim_next_job(
            &scope,
            Timestamp::from_millis(60_101),
            Duration::from_secs(60),
        )
        .expect("fresh claim should succeed");
    assert!(immediately_after.is_none());

    let completed = log
        .complete_job(&scope, job.id(), Timestamp::from_millis(60_200))
        .expect("completion should succeed");
    assert_eq!(completed.status(), SkillJobStatus::Done);
    assert_eq!(completed.attempts(), 2);
}

#[test]
fn success_and_failure_transitions_do_not_write_events() {
    let mut events = EventStore::open_in_memory().expect("event store should open");
    let session = SessionId::new("session-queue-events");
    events
        .append(
            &session,
            &AgentEvent::SessionStarted {
                session_id: session.clone(),
                messages: Vec::new(),
            },
            Timestamp::from_millis(1),
        )
        .expect("event append should succeed");
    let before = events.events(&session).expect("events should be readable");

    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-queue-events");

    let completed_job = enqueue(&log, &scope, "session-completed", 1, 100);
    log.claim_next_job(&scope, Timestamp::from_millis(110), Duration::from_secs(60))
        .expect("claim should succeed")
        .expect("completed job should be claimable");
    log.complete_job(&scope, completed_job.id(), Timestamp::from_millis(120))
        .expect("completion should succeed");

    let failed_job = enqueue(&log, &scope, "session-failed", 1, 200);
    log.claim_next_job(&scope, Timestamp::from_millis(210), Duration::from_secs(60))
        .expect("claim should succeed")
        .expect("failed job should be claimable");
    log.fail_job(
        &scope,
        failed_job.id(),
        Timestamp::from_millis(220),
        "summary only",
    )
    .expect("failure should succeed");

    let after = events
        .events(&session)
        .expect("events should remain readable");
    assert_eq!(before, after);
    assert_eq!(
        log.get_job(completed_job.id())
            .expect("completed job should be readable")
            .expect("completed job should exist")
            .status(),
        SkillJobStatus::Done
    );
    assert_eq!(
        log.get_job(failed_job.id())
            .expect("failed job should be readable")
            .expect("failed job should exist")
            .status(),
        SkillJobStatus::Failed
    );
}

#[test]
fn corrupt_source_json_is_reported_as_corrupt_without_panicking() {
    let database = TempDatabase::new("corrupt-source");
    let log = SkillLog::open(database.path()).expect("skill log should open");
    let scope = workspace("workspace-corrupt-source");
    let job = enqueue(&log, &scope, "session-corrupt", 1, 100);

    let raw = Connection::open(database.path()).expect("raw connection should open");
    raw.execute(
        "UPDATE skill_jobs SET source_json = ?1 WHERE id = ?2",
        rusqlite::params!["{not-json", job.id().get()],
    )
    .expect("corrupt update should succeed");

    let error = log
        .claim_next_job(&scope, Timestamp::from_millis(110), Duration::from_secs(60))
        .expect_err("corrupt source must fail the claim");
    match error {
        SkillJobError::Corrupt { id, .. } => assert_eq!(id, job.id().get()),
        other => panic!("unexpected claim error: {other:?}"),
    }

    let read_error = log
        .get_job(job.id())
        .expect_err("corrupt source must fail the read");
    match read_error {
        SkillJobError::Corrupt { id, .. } => assert_eq!(id, job.id().get()),
        other => panic!("unexpected read error: {other:?}"),
    }

    raw.execute(
        "UPDATE skill_jobs SET source_json = ?1 WHERE id = ?2",
        rusqlite::params![
            "{\"session_id\":\"session-corrupt\",\"turn_index\":1}",
            job.id().get()
        ],
    )
    .expect("second corrupt update should succeed");
    let missing_fields = log
        .get_job(job.id())
        .expect_err("missing source fields must be corrupt");
    assert!(matches!(missing_fields, SkillJobError::Corrupt { .. }));
}

#[test]
fn invalid_and_oversized_source_documents_are_rejected_at_enqueue() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-invalid-source");

    let invalid_json = NewSkillJob::new(
        scope.clone(),
        SessionId::new("session-invalid"),
        1,
        "{",
        Timestamp::from_millis(1),
    )
    .expect_err("invalid JSON must be rejected");
    assert!(matches!(invalid_json, SkillJobError::InvalidSource { .. }));

    let mismatched_session = NewSkillJob::new(
        scope.clone(),
        SessionId::new("session-invalid"),
        1,
        source_json("other-session", 1),
        Timestamp::from_millis(1),
    )
    .expect_err("a mismatched session must be rejected");
    assert!(matches!(
        mismatched_session,
        SkillJobError::InvalidSource { .. }
    ));

    let mismatched_turn = NewSkillJob::new(
        scope.clone(),
        SessionId::new("session-invalid"),
        1,
        source_json("session-invalid", 2),
        Timestamp::from_millis(1),
    )
    .expect_err("a mismatched turn must be rejected");
    assert!(matches!(
        mismatched_turn,
        SkillJobError::InvalidSource { .. }
    ));

    let extra_field = serde_json::json!({
        "session_id": "session-invalid",
        "turn_index": 1,
        "user_goal": "goal",
        "final_answer": "answer",
        "tool_summaries": [],
        "tool_call_count": 5,
        "assistant_rounds": 2,
        "acceptance_failures": 0,
        "raw_provider_response": "must not be accepted",
    })
    .to_string();
    let extra = NewSkillJob::new(
        scope.clone(),
        SessionId::new("session-invalid"),
        1,
        extra_field,
        Timestamp::from_millis(1),
    )
    .expect_err("unknown source fields must be rejected");
    assert!(matches!(extra, SkillJobError::InvalidSource { .. }));

    let oversized = NewSkillJob::new(
        scope.clone(),
        SessionId::new("session-invalid"),
        1,
        "x".repeat(MAX_SOURCE_JSON_BYTES + 1),
        Timestamp::from_millis(1),
    )
    .expect_err("oversized source must be rejected");
    assert!(matches!(oversized, SkillJobError::SourceTooLarge { .. }));

    assert!(
        log.list_jobs(&scope, 10)
            .expect("list should succeed")
            .is_empty()
    );
}

#[test]
fn last_error_is_redacted_and_truncated_before_storage() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-redaction");

    let job = enqueue(&log, &scope, "session-redaction", 1, 100);
    log.claim_next_job(&scope, Timestamp::from_millis(110), Duration::from_secs(60))
        .expect("claim should succeed")
        .expect("job should be claimable");
    let failed = log
        .fail_job(
            &scope,
            job.id(),
            Timestamp::from_millis(120),
            "Authorization: Bearer sk-supersecret-12345 api_key=topsecret \
             password='hunter2' sk-second-secret",
        )
        .expect("failure should succeed");
    let stored = failed.last_error().expect("failure should store a message");

    assert!(stored.contains("[REDACTED]"), "stored error: {stored}");
    for secret in [
        "sk-supersecret-12345",
        "topsecret",
        "hunter2",
        "sk-second-secret",
    ] {
        assert!(
            !stored.contains(secret),
            "secret {secret:?} leaked in {stored:?}"
        );
    }
    assert!(stored.len() <= MAX_LAST_ERROR_BYTES);

    let unicode_job = enqueue(&log, &scope, "session-redaction-unicode", 1, 200);
    log.claim_next_job(&scope, Timestamp::from_millis(210), Duration::from_secs(60))
        .expect("claim should succeed")
        .expect("unicode job should be claimable");
    let failed = log
        .fail_job(
            &scope,
            unicode_job.id(),
            Timestamp::from_millis(220),
            &"你".repeat(400),
        )
        .expect("unicode failure should succeed");
    assert_eq!(
        failed.last_error(),
        Some("你".repeat(170).as_str()),
        "truncation must stop on a UTF-8 boundary"
    );
}

#[test]
fn wal_and_busy_timeout_are_effective() {
    let database = TempDatabase::new("wal-timeout");
    let log = SkillLog::open(database.path()).expect("skill log should open");
    let raw = Connection::open(database.path()).expect("raw connection should open");
    let journal_mode: String = raw
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode should be readable");
    assert_eq!(journal_mode, "wal");

    let (locked_tx, locked_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let blocker_path = database.path().to_path_buf();
    let blocker = thread::spawn(move || {
        let connection = Connection::open(blocker_path).expect("blocker should open");
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .expect("blocker should acquire the write lock");
        locked_tx.send(()).expect("lock signal should send");
        release_rx.recv().expect("release signal should arrive");
        connection
            .execute_batch("ROLLBACK")
            .expect("blocker rollback should succeed");
    });
    locked_rx.recv().expect("blocker should acquire the lock");

    let scope = workspace("workspace-wal-timeout");
    let job = new_job(&scope, "session-wal", 1, 100);
    let (result_tx, result_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        started_tx.send(()).expect("start signal should send");
        let result = log.enqueue_job(job);
        result_tx.send(result).expect("enqueue result should send");
    });
    started_rx.recv().expect("worker should start");

    thread::sleep(Duration::from_millis(250));
    assert!(
        matches!(result_rx.try_recv(), Err(TryRecvError::Empty)),
        "enqueue returned before the write lock was released; busy_timeout was not effective"
    );

    release_tx.send(()).expect("release should send");
    let result = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("enqueue should finish after the lock is released");
    assert!(result.is_ok(), "enqueue failed: {result:?}");

    worker.join().expect("worker should not panic");
    blocker.join().expect("blocker should not panic");
}

#[test]
fn reopen_preserves_jobs_and_the_unique_index() {
    let database = TempDatabase::new("reopen");
    let scope = workspace("workspace-reopen-jobs");
    let job = {
        let log = SkillLog::open(database.path()).expect("skill log should open");
        let job = enqueue(&log, &scope, "session-reopen-job", 1, 100);
        log.claim_next_job(&scope, Timestamp::from_millis(110), Duration::from_secs(60))
            .expect("claim should succeed")
            .expect("job should be claimable");
        log.complete_job(&scope, job.id(), Timestamp::from_millis(120))
            .expect("completion should succeed");
        job
    };

    {
        let raw = Connection::open(database.path()).expect("raw connection should open");
        let version: i64 = raw
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user_version should be readable");
        assert_eq!(version, 1);

        let mut statement = raw
            .prepare("PRAGMA index_list(skill_jobs)")
            .expect("index list should prepare");
        let mut rows = statement.query([]).expect("index list should query");
        let mut has_unique_index = false;
        while let Some(row) = rows.next().expect("index row should read") {
            let unique: i64 = row.get(2).expect("unique flag should read");
            has_unique_index |= unique == 1;
        }
        assert!(has_unique_index, "skill_jobs must keep a unique index");
    }

    let reopened = SkillLog::open(database.path()).expect("skill log should reopen");
    let stored = reopened
        .get_job(job.id())
        .expect("job should be readable")
        .expect("job should survive reopen");
    assert_eq!(stored.status(), SkillJobStatus::Done);
    assert_eq!(stored.attempts(), 1);
    assert_eq!(stored.claimed_at(), Some(Timestamp::from_millis(110)));

    let duplicate = reopened
        .enqueue_job(
            NewSkillJob::new(
                scope.clone(),
                SessionId::new("session-reopen-job"),
                1,
                source_json_with_goal("session-reopen-job", 1, "after reopen"),
                Timestamp::from_millis(130),
            )
            .expect("duplicate should validate"),
        )
        .expect("duplicate enqueue should return the existing job");
    assert_eq!(duplicate.id(), job.id());
    assert_eq!(duplicate.status(), SkillJobStatus::Done);
    assert_eq!(
        reopened
            .list_jobs(&scope, 10)
            .expect("list should succeed")
            .len(),
        1
    );
}

#[test]
fn deleting_skills_db_removes_resume_style_idempotency() {
    let database = TempDatabase::new("delete");
    let scope = workspace("workspace-delete");
    let original = {
        let log = SkillLog::open(database.path()).expect("skill log should open");
        let job = enqueue(&log, &scope, "session-delete", 1, 100);
        log.claim_next_job(&scope, Timestamp::from_millis(110), Duration::from_secs(60))
            .expect("claim should succeed")
            .expect("job should be claimable");
        log.complete_job(&scope, job.id(), Timestamp::from_millis(120))
            .expect("completion should succeed")
    };

    database.remove_files();

    let reopened = SkillLog::open(database.path()).expect("new skill log should open");
    assert!(
        reopened
            .list_jobs(&scope, 10)
            .expect("list should succeed")
            .is_empty()
    );
    let replacement = enqueue(&reopened, &scope, "session-delete", 1, 200);
    assert_eq!(replacement.status(), SkillJobStatus::Pending);
    assert_eq!(replacement.attempts(), 0);
    assert_eq!(replacement.created_at(), Timestamp::from_millis(200));
    assert_eq!(original.status(), SkillJobStatus::Done);
    assert_eq!(original.created_at(), Timestamp::from_millis(100));
}

#[test]
fn invalid_state_transitions_are_rejected_without_changing_the_row() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-transitions");
    let job = enqueue(&log, &scope, "session-transitions", 1, 100);

    let pending_complete = log
        .complete_job(&scope, job.id(), Timestamp::from_millis(110))
        .expect_err("pending jobs cannot complete");
    match pending_complete {
        SkillJobError::InvalidTransition { from, to, .. } => {
            assert_eq!(from, SkillJobStatus::Pending);
            assert_eq!(to, SkillJobStatus::Done);
        }
        other => panic!("unexpected transition error: {other:?}"),
    }
    assert_eq!(
        log.get_job(job.id())
            .expect("job should be readable")
            .expect("job should exist")
            .status(),
        SkillJobStatus::Pending
    );

    log.claim_next_job(&scope, Timestamp::from_millis(120), Duration::from_secs(60))
        .expect("claim should succeed")
        .expect("job should be claimable");
    log.complete_job(&scope, job.id(), Timestamp::from_millis(130))
        .expect("completion should succeed");

    let repeated_complete = log
        .complete_job(&scope, job.id(), Timestamp::from_millis(140))
        .expect_err("done jobs cannot complete again");
    assert!(matches!(
        repeated_complete,
        SkillJobError::InvalidTransition {
            from: SkillJobStatus::Done,
            to: SkillJobStatus::Done,
            ..
        }
    ));

    let done_fail = log
        .fail_job(&scope, job.id(), Timestamp::from_millis(150), "late error")
        .expect_err("done jobs cannot fail");
    assert!(matches!(
        done_fail,
        SkillJobError::InvalidTransition {
            from: SkillJobStatus::Done,
            to: SkillJobStatus::Failed,
            ..
        }
    ));

    let missing = log
        .complete_job(
            &scope,
            SkillJobId::from_raw(9_999),
            Timestamp::from_millis(160),
        )
        .expect_err("missing jobs must be reported");
    assert!(matches!(missing, SkillJobError::NotFound { .. }));
}

#[tokio::test]
async fn async_store_scopes_jobs_to_its_workspace() {
    let store_a = SqliteSkillStore::open_in_memory("/tmp/jessica-skill-job-workspace-a")
        .expect("store A should open");
    let store_b = SqliteSkillStore::open_in_memory("/tmp/jessica-skill-job-workspace-b")
        .expect("store B should open");

    let scope = store_a.workspace_scope().clone();
    let queued = store_a
        .enqueue_job(new_job(&scope, "session-async", 1, 100))
        .await
        .expect("async enqueue should succeed");
    assert_eq!(queued.status(), SkillJobStatus::Pending);

    let claimed = store_a
        .claim_next_job(Timestamp::from_millis(110), Duration::from_secs(60))
        .await
        .expect("async claim should succeed")
        .expect("job should be claimable");
    assert_eq!(claimed.id(), queued.id());

    let completed = store_a
        .complete_job(queued.id(), Timestamp::from_millis(120))
        .await
        .expect("async completion should succeed");
    assert_eq!(completed.status(), SkillJobStatus::Done);

    assert!(
        store_b
            .get_job(queued.id())
            .await
            .expect("cross-workspace read should not error")
            .is_none()
    );
    assert!(
        store_b
            .claim_next_job(Timestamp::from_millis(130), Duration::from_secs(60),)
            .await
            .expect("cross-workspace claim should not error")
            .is_none()
    );
    let missing = store_b
        .complete_job(queued.id(), Timestamp::from_millis(140))
        .await
        .expect_err("cross-workspace completion must fail");
    assert!(matches!(missing, SkillJobError::NotFound { .. }));

    let failed = store_a
        .enqueue_job(new_job(&scope, "session-async", 2, 200))
        .await
        .expect("second enqueue should succeed");
    store_a
        .claim_next_job(Timestamp::from_millis(210), Duration::from_secs(60))
        .await
        .expect("claim should succeed")
        .expect("second job should be claimable");
    let failed = store_a
        .fail_job(failed.id(), Timestamp::from_millis(220), "async failure")
        .await
        .expect("async failure should succeed");
    assert_eq!(failed.status(), SkillJobStatus::Failed);
}
