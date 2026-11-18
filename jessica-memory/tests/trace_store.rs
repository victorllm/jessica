//! Behavioral tests for the versioned trace store.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_core::trace::{TraceEvent, TraceGapReason, TracePayload, TraceTurnMode, TurnId};
use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId};
use jessica_memory::{EventStore, EventStoreError, Timestamp};
use rusqlite::{Connection, params};
use serde_json::json;

/// Unique database file removed when the test finishes.
struct TempDatabase {
    /// Path passed to [`EventStore::open`].
    path: PathBuf,
}

impl TempDatabase {
    /// Creates an unused path in the system temporary directory.
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-trace-{label}-{}-{unique}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }

    /// Returns the database path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        for suffix in ["-journal", "-wal", "-shm"] {
            let mut sidecar = self.path.as_os_str().to_owned();
            sidecar.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(sidecar));
        }
    }
}

/// Builds a `turn_started` entry for `turn`.
fn started(turn: &TurnId, session: &SessionId) -> TraceEvent {
    TraceEvent::new(
        turn.clone(),
        None,
        Some(session.clone()),
        TracePayload::TurnStarted {
            mode: TraceTurnMode::Fresh,
            messages: vec![Message::system("system prompt"), Message::user("fix it")],
        },
    )
}

/// Builds an attempt-scoped agent entry.
fn agent(turn: &TurnId, attempt: u32, session: &SessionId, event: AgentEvent) -> TraceEvent {
    TraceEvent::new(
        turn.clone(),
        Some(attempt),
        Some(session.clone()),
        TracePayload::Agent { event },
    )
}

/// Builds a `turn_ended` entry for `turn`.
fn ended(turn: &TurnId, reason: SessionEndReason) -> TraceEvent {
    TraceEvent::new(turn.clone(), None, None, TracePayload::TurnEnded { reason })
}

/// Builds a valid `turn_started` payload as raw JSON for corruption tests.
fn started_json(turn: &TurnId, session: &SessionId) -> String {
    serde_json::to_string(&started(turn, session)).expect("trace envelope must serialize")
}

#[test]
fn a_complete_turn_is_written_losslessly_and_reads_back_closed() {
    let mut store = EventStore::open_in_memory().expect("store should open");
    let turn = TurnId::new("turn-complete");
    let session = SessionId::new("cli-complete");
    let started_event = started(&turn, &session);
    let session_started = AgentEvent::SessionStarted {
        session_id: session.clone(),
    };
    let delta = AgentEvent::ModelDelta {
        text: "hi".to_owned(),
    };
    let acceptance = AgentEvent::AcceptanceFailed {
        attempt: 1,
        feedback: "try again".to_owned(),
    };
    let session_ended = AgentEvent::SessionEnded {
        session_id: session.clone(),
        reason: SessionEndReason::Completed,
    };
    let ended_event = ended(&turn, SessionEndReason::Completed);

    let appended = vec![
        store
            .append_traced(&started_event, None, Timestamp::from_millis(100))
            .expect("turn_started should append"),
        store
            .append_traced(
                &agent(&turn, 1, &session, session_started.clone()),
                Some((&session, &session_started)),
                Timestamp::from_millis(101),
            )
            .expect("session_started should dual-write"),
        store
            .append_traced(
                &agent(&turn, 1, &session, delta.clone()),
                Some((&session, &delta)),
                Timestamp::from_millis(102),
            )
            .expect("model delta should dual-write"),
        store
            .append_traced(
                &agent(&turn, 1, &session, acceptance.clone()),
                None,
                Timestamp::from_millis(103),
            )
            .expect("acceptance failure should be trace-only"),
        store
            .append_traced(
                &agent(&turn, 1, &session, session_ended.clone()),
                Some((&session, &session_ended)),
                Timestamp::from_millis(104),
            )
            .expect("session_ended should dual-write"),
        store
            .append_traced(&ended_event, None, Timestamp::from_millis(105))
            .expect("turn_ended should append"),
    ];

    assert_eq!(
        appended.iter().map(|entry| entry.seq()).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6]
    );
    for entry in &appended {
        assert_eq!(
            entry.payload_json(),
            serde_json::to_string(entry.event()).expect("envelope must serialize"),
            "the returned payload must be the persisted bytes"
        );
    }

    let legacy = store.events(&session).expect("legacy events should load");
    assert_eq!(
        legacy
            .iter()
            .map(|event| event.event().clone())
            .collect::<Vec<_>>(),
        vec![session_started, delta, session_ended],
        "acceptance failures must not enter the legacy stream"
    );

    let trace = store
        .trace_turn(&turn)
        .expect("trace should read")
        .expect("trace should exist");
    assert!(trace.closed());
    assert_eq!(trace.turn_id(), &turn);
    assert_eq!(trace.events().len(), 6);
    assert_eq!(
        trace
            .events()
            .iter()
            .map(|entry| entry.event().payload.kind())
            .collect::<Vec<_>>(),
        vec![
            "turn_started",
            "agent",
            "agent",
            "agent",
            "agent",
            "turn_ended"
        ]
    );
    assert_eq!(
        trace.events()[3].event().attempt,
        Some(1),
        "the acceptance failure must name the attempt it judged"
    );
    assert_eq!(
        trace.events()[3].event().session_id.as_ref(),
        Some(&session),
        "the acceptance failure must name the failed attempt's session"
    );
    assert_eq!(trace.events()[5].event().session_id, None);
    assert_eq!(trace.events()[5].timestamp(), Timestamp::from_millis(105));

    let summaries = store.trace_turns(10).expect("turns should list");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].turn_id(), &turn);
    assert_eq!(summaries[0].last_seq(), 6);
    assert!(summaries[0].closed());
    assert_eq!(summaries[0].created_at(), Timestamp::from_millis(100));

    let raw: String = appended[3].payload_json().to_owned();
    let raw: serde_json::Value = serde_json::from_str(&raw).expect("payload must be JSON");
    assert_eq!(raw["schema_version"], json!(1));
    assert_eq!(raw["attempt"], json!(1));
    assert_eq!(raw["session_id"], json!("cli-complete"));
    assert_eq!(raw["payload"]["kind"], json!("agent"));
    assert_eq!(raw["payload"]["event"]["type"], json!("acceptance_failed"));
}

#[test]
fn an_unterminated_trace_is_readable_and_reports_incomplete() {
    let mut store = EventStore::open_in_memory().expect("store should open");
    let turn = TurnId::new("turn-open");
    let session = SessionId::new("cli-open");

    store
        .append_traced(&started(&turn, &session), None, Timestamp::from_millis(10))
        .expect("turn_started should append");

    let trace = store
        .trace_turn(&turn)
        .expect("trace should read")
        .expect("trace should exist");
    assert!(!trace.closed(), "a missing turn_ended must be visible");
    assert_eq!(trace.events().len(), 1);

    let summaries = store.trace_turns(10).expect("turns should list");
    assert_eq!(summaries.len(), 1);
    assert!(!summaries[0].closed());

    assert!(
        store
            .trace_turn(&TurnId::new("turn-missing"))
            .expect("missing trace should not be an error")
            .is_none()
    );
}

#[test]
fn lifecycle_rules_are_enforced_without_writing_partial_rows() {
    let mut store = EventStore::open_in_memory().expect("store should open");
    let session = SessionId::new("cli-lifecycle");
    let turn = TurnId::new("turn-lifecycle");
    let delta = AgentEvent::ModelDelta {
        text: "x".to_owned(),
    };

    let error = store
        .append_traced(
            &agent(&turn, 1, &session, delta.clone()),
            Some((&session, &delta)),
            Timestamp::from_millis(1),
        )
        .expect_err("a trace must start with turn_started");
    assert!(matches!(error, EventStoreError::TraceLifecycle { .. }));
    assert!(
        store
            .trace_turn(&turn)
            .expect("read should succeed")
            .is_none(),
        "a rejected first entry must leave no trace head"
    );
    assert!(
        store.events(&session).expect("legacy read").is_empty(),
        "a rejected first entry must leave no legacy event"
    );

    store
        .append_traced(&started(&turn, &session), None, Timestamp::from_millis(2))
        .expect("turn_started should append");

    let error = store
        .append_traced(&started(&turn, &session), None, Timestamp::from_millis(3))
        .expect_err("a second turn_started must be rejected");
    assert!(matches!(error, EventStoreError::TraceLifecycle { .. }));

    store
        .append_traced(
            &ended(&turn, SessionEndReason::Completed),
            None,
            Timestamp::from_millis(4),
        )
        .expect("turn_ended should append");

    let error = store
        .append_traced(
            &agent(&turn, 1, &session, delta.clone()),
            Some((&session, &delta)),
            Timestamp::from_millis(5),
        )
        .expect_err("no entry may follow turn_ended");
    assert!(matches!(error, EventStoreError::TraceLifecycle { .. }));

    let trace = store
        .trace_turn(&turn)
        .expect("trace should read")
        .expect("trace should exist");
    assert!(trace.closed());
    assert_eq!(
        trace.events().len(),
        2,
        "rejected appends must not add rows"
    );
    assert!(
        store.events(&session).expect("legacy read").is_empty(),
        "rejected appends must not add legacy rows"
    );

    let error = store
        .append_traced(
            &ended(
                &TurnId::new("turn-ended-first"),
                SessionEndReason::Completed,
            ),
            None,
            Timestamp::from_millis(6),
        )
        .expect_err("turn_ended cannot be the first entry");
    assert!(matches!(error, EventStoreError::TraceLifecycle { .. }));
}

#[test]
fn envelope_context_is_validated_before_storage() {
    let mut store = EventStore::open_in_memory().expect("store should open");
    let session = SessionId::new("cli-envelope");
    let other = SessionId::new("cli-other");
    let turn = TurnId::new("turn-envelope");
    let delta = AgentEvent::ModelDelta {
        text: "x".to_owned(),
    };

    let empty_turn = started(&TurnId::new(""), &session);
    let error = store
        .append_traced(&empty_turn, None, Timestamp::from_millis(1))
        .expect_err("an empty turn id must be rejected");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let mut future = started(&turn, &session);
    future.schema_version = 2;
    let error = store
        .append_traced(&future, None, Timestamp::from_millis(2))
        .expect_err("an unknown schema version must be rejected");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let mut attempted_start = started(&turn, &session);
    attempted_start.attempt = Some(1);
    let error = store
        .append_traced(&attempted_start, None, Timestamp::from_millis(3))
        .expect_err("turn_started cannot belong to an attempt");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let mut anonymous_start = started(&turn, &session);
    anonymous_start.session_id = None;
    let error = store
        .append_traced(&anonymous_start, None, Timestamp::from_millis(4))
        .expect_err("turn_started needs a session");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let mut attributed_end = ended(&turn, SessionEndReason::Completed);
    attributed_end.session_id = Some(session.clone());
    let error = store
        .append_traced(&attributed_end, None, Timestamp::from_millis(5))
        .expect_err("turn_ended cannot belong to a session");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let empty_gap = TraceEvent::new(
        turn.clone(),
        None,
        None,
        TracePayload::Gap {
            skipped: 0,
            reason: TraceGapReason::BroadcastLagged,
        },
    );
    let error = store
        .append_traced(&empty_gap, None, Timestamp::from_millis(6))
        .expect_err("a gap must report a positive count");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let missing_mirror = agent(&turn, 1, &session, delta.clone());
    let error = store
        .append_traced(&missing_mirror, None, Timestamp::from_millis(7))
        .expect_err("a session-scoped agent event needs a legacy mirror");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let mismatched_session = agent(&turn, 1, &session, delta.clone());
    let error = store
        .append_traced(
            &mismatched_session,
            Some((&other, &delta)),
            Timestamp::from_millis(8),
        )
        .expect_err("the legacy session must match the envelope");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let mismatched_event = agent(&turn, 1, &session, delta.clone());
    let different = AgentEvent::ModelDelta {
        text: "different".to_owned(),
    };
    let error = store
        .append_traced(
            &mismatched_event,
            Some((&session, &different)),
            Timestamp::from_millis(9),
        )
        .expect_err("the legacy event must match the envelope payload");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let acceptance = agent(
        &turn,
        1,
        &session,
        AgentEvent::AcceptanceFailed {
            attempt: 1,
            feedback: "retry".to_owned(),
        },
    );
    let acceptance_event = match &acceptance.payload {
        TracePayload::Agent { event } => event.clone(),
        other => panic!("expected an agent payload, found {other:?}"),
    };
    let error = store
        .append_traced(
            &acceptance,
            Some((&session, &acceptance_event)),
            Timestamp::from_millis(10),
        )
        .expect_err("an acceptance failure must never enter the legacy stream");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    let error = store
        .append_traced(
            &started(&turn, &session),
            Some((&session, &delta)),
            Timestamp::from_millis(11),
        )
        .expect_err("only agent payloads may mirror legacy events");
    assert!(matches!(
        error,
        EventStoreError::TraceInvalid { seq: 0, .. }
    ));

    assert!(
        store.trace_turns(10).expect("turns should list").is_empty(),
        "no invalid envelope may create a trace head"
    );
    assert!(
        store.events(&session).expect("legacy read").is_empty(),
        "no invalid envelope may create a legacy event"
    );
}

#[test]
fn a_trace_append_and_its_legacy_mirror_roll_back_together() {
    let database = TempDatabase::new("atomic");
    let session = SessionId::new("cli-atomic");
    let turn = TurnId::new("turn-atomic");
    let session_started = AgentEvent::SessionStarted {
        session_id: session.clone(),
    };
    let delta = AgentEvent::ModelDelta {
        text: "must roll back".to_owned(),
    };

    let mut store = EventStore::open(database.path()).expect("store should open");
    store
        .append_traced(&started(&turn, &session), None, Timestamp::from_millis(1))
        .expect("turn_started should append");
    store
        .append_traced(
            &agent(&turn, 1, &session, session_started.clone()),
            Some((&session, &session_started)),
            Timestamp::from_millis(2),
        )
        .expect("session_started should dual-write");

    let competing = Connection::open(database.path()).expect("raw connection should open");
    competing
        .execute(
            "INSERT INTO trace_events \
             (turn_id, seq, schema_version, payload_kind, payload_json, timestamp) \
             VALUES (?1, 3, 1, 'gap', '{}', 0)",
            [turn.as_str()],
        )
        .expect("the competing row should insert");

    let error = store
        .append_traced(
            &agent(&turn, 1, &session, delta.clone()),
            Some((&session, &delta)),
            Timestamp::from_millis(3),
        )
        .expect_err("the duplicate trace sequence must fail the whole append");
    assert!(matches!(error, EventStoreError::TraceStore { .. }));

    let legacy = store.events(&session).expect("legacy events should load");
    assert_eq!(
        legacy.len(),
        1,
        "the legacy mirror must roll back when the trace insert fails"
    );
    let stored_payload: String = competing
        .query_row(
            "SELECT payload_json FROM events WHERE session_id = ?1 AND seq = 1",
            [session.as_str()],
            |row| row.get(0),
        )
        .expect("the first legacy row should remain");
    assert_eq!(
        serde_json::from_str::<AgentEvent>(&stored_payload).expect("payload should decode"),
        session_started
    );
    let head_seq: i64 = competing
        .query_row(
            "SELECT last_seq FROM trace_heads WHERE turn_id = ?1",
            [turn.as_str()],
            |row| row.get(0),
        )
        .expect("the head should remain");
    assert_eq!(
        head_seq, 2,
        "a failed trace append must not advance the head"
    );
}

#[test]
fn append_only_triggers_reject_trace_mutation() {
    let database = TempDatabase::new("guards");
    let session = SessionId::new("cli-guards");
    let turn = TurnId::new("turn-guards");

    {
        let mut store = EventStore::open(database.path()).expect("store should open");
        store
            .append_traced(&started(&turn, &session), None, Timestamp::from_millis(1))
            .expect("turn_started should append");
        store
            .append_traced(
                &ended(&turn, SessionEndReason::Completed),
                None,
                Timestamp::from_millis(2),
            )
            .expect("turn_ended should append");
    }

    let connection = Connection::open(database.path()).expect("raw connection should open");
    for statement in [
        "UPDATE trace_events SET payload_json = '{}'",
        "DELETE FROM trace_events",
        "DELETE FROM trace_heads",
        "UPDATE trace_heads SET closed = 0",
        "UPDATE trace_heads SET last_seq = 1",
    ] {
        let error = connection
            .execute(statement, [])
            .expect_err("trace mutation must be rejected");
        assert_eq!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::ConstraintViolation),
            "`{statement}` should hit the append-only guards"
        );
    }
}

#[test]
fn a_legacy_database_is_upgraded_without_rewriting_its_events() {
    let database = TempDatabase::new("upgrade");
    let session = SessionId::new("legacy-session");
    let event = AgentEvent::ModelDelta {
        text: "before upgrade".to_owned(),
    };
    let payload = serde_json::to_string(&event).expect("legacy event should serialize");

    let connection = Connection::open(database.path()).expect("raw connection should open");
    connection
        .execute_batch(
            "CREATE TABLE events (\
                 session_id TEXT NOT NULL,\
                 seq INTEGER NOT NULL CHECK (seq >= 1),\
                 event_type TEXT NOT NULL,\
                 payload_json TEXT NOT NULL,\
                 timestamp INTEGER NOT NULL,\
                 PRIMARY KEY (session_id, seq)\
             ) WITHOUT ROWID;",
        )
        .expect("legacy schema should install");
    connection
        .execute(
            "INSERT INTO events (session_id, seq, event_type, payload_json, timestamp) \
             VALUES (?1, 1, 'model_delta', ?2, 42)",
            params![session.as_str(), payload],
        )
        .expect("legacy event should insert");
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("version should read");
    assert_eq!(version, 0, "the fixture must start as a pre-trace database");
    drop(connection);

    let turn = TurnId::new("turn-after-upgrade");
    {
        let mut store = EventStore::open(database.path()).expect("store should upgrade");
        let legacy = store.events(&session).expect("legacy events should load");
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].event(), &event);
        assert_eq!(
            legacy[0].payload_json(),
            serde_json::to_string(&event).expect("event should serialize"),
            "upgrading must preserve the exact legacy bytes"
        );

        store
            .append_traced(&started(&turn, &session), None, Timestamp::from_millis(43))
            .expect("new trace writes should work after upgrade");
    }

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("version should read");
    assert_eq!(version, 1);
    let table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type = 'table' AND name IN ('trace_events', 'trace_heads')",
            [],
            |row| row.get(0),
        )
        .expect("trace tables should be visible");
    assert_eq!(table_count, 2);

    let reopened = EventStore::open(database.path()).expect("upgraded store should reopen");
    let trace = reopened
        .trace_turn(&turn)
        .expect("trace should read")
        .expect("trace should exist");
    assert!(!trace.closed());
    assert_eq!(trace.events().len(), 1);
}

#[test]
fn a_database_from_a_newer_schema_is_rejected() {
    let database = TempDatabase::new("future");
    let connection = Connection::open(database.path()).expect("raw connection should open");
    connection
        .execute_batch("PRAGMA user_version = 2;")
        .expect("future version should set");
    drop(connection);

    let error = EventStore::open(database.path()).expect_err("a future schema must be rejected");
    match error {
        EventStoreError::UnsupportedVersion { found, supported } => {
            assert_eq!(found, 2);
            assert_eq!(supported, 1);
        }
        other => panic!("expected UnsupportedVersion, found {other:?}"),
    }
}

#[test]
fn turn_summaries_are_newest_first_with_a_deterministic_tie_break() {
    let mut store = EventStore::open_in_memory().expect("store should open");
    let session = SessionId::new("cli-order");
    let turn_a = TurnId::new("turn-a");
    let turn_b = TurnId::new("turn-b");
    let turn_c = TurnId::new("turn-c");

    for (turn, timestamp) in [
        (&turn_a, Timestamp::from_millis(20)),
        (&turn_b, Timestamp::from_millis(30)),
        (&turn_c, Timestamp::from_millis(20)),
    ] {
        store
            .append_traced(&started(turn, &session), None, timestamp)
            .expect("turn_started should append");
    }

    let all = store.trace_turns(10).expect("turns should list");
    assert_eq!(
        all.iter()
            .map(|turn| turn.turn_id().as_str())
            .collect::<Vec<_>>(),
        vec!["turn-b", "turn-c", "turn-a"]
    );

    let limited = store.trace_turns(2).expect("turns should list");
    assert_eq!(
        limited
            .iter()
            .map(|turn| turn.turn_id().as_str())
            .collect::<Vec<_>>(),
        vec!["turn-b", "turn-c"]
    );
    assert!(store.trace_turns(0).expect("zero limit").is_empty());
}

#[test]
fn independent_turns_allocate_independent_sequences() {
    let mut store = EventStore::open_in_memory().expect("store should open");
    let session_a = SessionId::new("cli-a");
    let session_b = SessionId::new("cli-b");
    let turn_a = TurnId::new("turn-a");
    let turn_b = TurnId::new("turn-b");

    let first_a = store
        .append_traced(
            &started(&turn_a, &session_a),
            None,
            Timestamp::from_millis(1),
        )
        .expect("turn a should start");
    let first_b = store
        .append_traced(
            &started(&turn_b, &session_b),
            None,
            Timestamp::from_millis(1),
        )
        .expect("turn b should start");
    assert_eq!(first_a.seq(), 1);
    assert_eq!(first_b.seq(), 1);

    let last_a = store
        .append_traced(
            &ended(&turn_a, SessionEndReason::Completed),
            None,
            Timestamp::from_millis(2),
        )
        .expect("turn a should end");
    assert_eq!(last_a.seq(), 2);
    assert_eq!(
        store.trace_turn(&turn_b).unwrap().unwrap().events().len(),
        1
    );
    assert_eq!(
        store.trace_turn(&turn_a).unwrap().unwrap().events().len(),
        2
    );
}

#[test]
fn payload_bytes_survive_reopening_the_database() {
    let database = TempDatabase::new("reopen");
    let turn = TurnId::new("turn-reopen");
    let session = SessionId::new("cli-reopen");
    let delta = AgentEvent::ModelDelta {
        text: "persisted".to_owned(),
    };
    let expected = agent(&turn, 1, &session, delta.clone());

    {
        let mut store = EventStore::open(database.path()).expect("store should open");
        store
            .append_traced(&started(&turn, &session), None, Timestamp::from_millis(1))
            .expect("turn_started should append");
        store
            .append_traced(
                &expected,
                Some((&session, &delta)),
                Timestamp::from_millis(2),
            )
            .expect("agent event should append");
        store
            .append_traced(
                &ended(&turn, SessionEndReason::Completed),
                None,
                Timestamp::from_millis(3),
            )
            .expect("turn_ended should append");
    }

    let reopened = EventStore::open(database.path()).expect("store should reopen");
    let trace = reopened
        .trace_turn(&turn)
        .expect("trace should read")
        .expect("trace should exist");
    assert!(trace.closed());
    assert_eq!(trace.events()[1].event(), &expected);
    assert_eq!(
        trace.events()[1].payload_json(),
        serde_json::to_string(&expected).expect("envelope should serialize")
    );
    assert_eq!(trace.events()[1].timestamp(), Timestamp::from_millis(2));
}

#[test]
fn structurally_corrupt_traces_are_rejected_instead_of_partially_returned() {
    let database = TempDatabase::new("corrupt");
    EventStore::open(database.path()).expect("store should install the schema");
    let connection = Connection::open(database.path()).expect("raw connection should open");

    let missing_start = TurnId::new("turn-missing-start");
    let session = SessionId::new("cli-corrupt");
    let agent_payload = serde_json::to_string(&TraceEvent::new(
        missing_start.clone(),
        None,
        None,
        TracePayload::Agent {
            event: AgentEvent::ModelDelta {
                text: "orphan".to_owned(),
            },
        },
    ))
    .expect("envelope should serialize");
    connection
        .execute(
            "INSERT INTO trace_heads (turn_id, last_seq, closed, created_at) VALUES (?1, 1, 0, 0)",
            [missing_start.as_str()],
        )
        .expect("head should insert");
    connection
        .execute(
            "INSERT INTO trace_events \
             (turn_id, seq, schema_version, payload_kind, event_type, payload_json, timestamp) \
             VALUES (?1, 1, 1, 'agent', 'model_delta', ?2, 0)",
            params![missing_start.as_str(), agent_payload],
        )
        .expect("row should insert");

    let gap_turn = TurnId::new("turn-gap");
    let start_payload = started_json(&gap_turn, &session);
    let end_payload = serde_json::to_string(&ended(&gap_turn, SessionEndReason::Completed))
        .expect("envelope should serialize");
    connection
        .execute(
            "INSERT INTO trace_heads (turn_id, last_seq, closed, created_at) VALUES (?1, 3, 0, 0)",
            [gap_turn.as_str()],
        )
        .expect("head should insert");
    for (seq, payload) in [(1_i64, start_payload), (3_i64, end_payload)] {
        connection
            .execute(
                "INSERT INTO trace_events \
                 (turn_id, seq, schema_version, payload_kind, event_type, attempt, session_id, \
                  payload_json, timestamp) \
                 VALUES (?1, ?2, 1, ?3, ?4, NULL, ?5, ?6, 0)",
                params![
                    gap_turn.as_str(),
                    seq,
                    if seq == 1 {
                        "turn_started"
                    } else {
                        "turn_ended"
                    },
                    Option::<&str>::None,
                    if seq == 1 {
                        Some(session.as_str())
                    } else {
                        None
                    },
                    payload
                ],
            )
            .expect("row should insert");
    }

    let wrong_kind_turn = TurnId::new("turn-wrong-kind");
    let wrong_kind_payload = started_json(&wrong_kind_turn, &session);
    connection
        .execute(
            "INSERT INTO trace_heads (turn_id, last_seq, closed, created_at) VALUES (?1, 1, 0, 0)",
            [wrong_kind_turn.as_str()],
        )
        .expect("head should insert");
    connection
        .execute(
            "INSERT INTO trace_events \
             (turn_id, seq, schema_version, payload_kind, event_type, payload_json, timestamp) \
             VALUES (?1, 1, 1, 'gap', NULL, ?2, 0)",
            params![wrong_kind_turn.as_str(), wrong_kind_payload],
        )
        .expect("row should insert");

    let wrong_closed_turn = TurnId::new("turn-wrong-closed");
    let wrong_closed_payload = started_json(&wrong_closed_turn, &session);
    connection
        .execute(
            "INSERT INTO trace_heads (turn_id, last_seq, closed, created_at) VALUES (?1, 1, 1, 0)",
            [wrong_closed_turn.as_str()],
        )
        .expect("head should insert");
    connection
        .execute(
            "INSERT INTO trace_events \
             (turn_id, seq, schema_version, payload_kind, event_type, session_id, payload_json, \
              timestamp) \
             VALUES (?1, 1, 1, 'turn_started', NULL, ?2, ?3, 0)",
            params![
                wrong_closed_turn.as_str(),
                session.as_str(),
                wrong_closed_payload
            ],
        )
        .expect("row should insert");
    drop(connection);

    let store = EventStore::open(database.path()).expect("store should open");
    let error = store
        .trace_turn(&missing_start)
        .expect_err("a trace without turn_started is corrupt");
    assert!(matches!(error, EventStoreError::TraceLifecycle { .. }));

    let error = store
        .trace_turn(&gap_turn)
        .expect_err("a sequence gap must be reported");
    assert!(matches!(
        error,
        EventStoreError::TraceSequence {
            expected: 2,
            found: 3,
            ..
        }
    ));

    let error = store
        .trace_turn(&wrong_kind_turn)
        .expect_err("a payload-kind mismatch is corruption");
    assert!(matches!(error, EventStoreError::TraceInvalid { .. }));

    let error = store
        .trace_turn(&wrong_closed_turn)
        .expect_err("closed must agree with the terminal entry");
    assert!(matches!(error, EventStoreError::TraceLifecycle { .. }));
}
