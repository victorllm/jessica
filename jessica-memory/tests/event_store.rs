//! Behavioral tests for the append-only SQLite event store.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_core::types::{
    AgentEvent, BudgetKind, Message, SessionEndReason, SessionId, ToolCall, ToolResult,
};
use jessica_memory::{EventStore, EventStoreError, StoredEvent, Timestamp};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

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
            "jessica-memory-{label}-{}-{unique}.sqlite",
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

/// Builds one event of every [`AgentEvent`] variant.
fn sample_events(session: &SessionId) -> Vec<AgentEvent> {
    vec![
        AgentEvent::SessionStarted {
            session_id: session.clone(),
        },
        AgentEvent::ModelDelta {
            text: "hello".to_owned(),
        },
        AgentEvent::ToolCallStarted {
            call: ToolCall::new("call-1", "read_file", json!({"path": "Cargo.toml"})),
        },
        AgentEvent::ToolCallFinished {
            result: ToolResult::success("call-1", "workspace content"),
        },
        AgentEvent::PolicyDenied {
            call: ToolCall::new("call-2", "bash", json!({"command": "rm -rf /"})),
            reason: "tool is not in the allowlist".to_owned(),
        },
        AgentEvent::BudgetExceeded {
            kind: BudgetKind::TokenLimit,
            message: "token budget exhausted".to_owned(),
        },
        AgentEvent::AcceptanceFailed {
            attempt: 2,
            feedback: "fix the spelling error".to_owned(),
        },
        AgentEvent::Error {
            message: "provider stream ended".to_owned(),
        },
        AgentEvent::SessionEnded {
            session_id: session.clone(),
            reason: SessionEndReason::Completed,
        },
    ]
}

/// Serde tags expected for [`sample_events`], in the same order.
const EXPECTED_TYPES: [&str; 9] = [
    "session_started",
    "model_delta",
    "tool_call_started",
    "tool_call_finished",
    "policy_denied",
    "budget_exceeded",
    "acceptance_failed",
    "error",
    "session_ended",
];

#[test]
fn every_event_variant_round_trips_in_sequence_order() {
    let mut store = EventStore::open_in_memory().expect("in-memory store should open");
    let session = SessionId::new("session-all-variants");
    let events = sample_events(&session);
    assert_eq!(events.len(), EXPECTED_TYPES.len());

    let mut appended = Vec::new();
    for (index, event) in events.iter().enumerate() {
        let timestamp = Timestamp::from_millis(1_700_000_000_000 + index as i64);
        let stored = store
            .append(&session, event, timestamp)
            .unwrap_or_else(|error| panic!("append of {event:?} failed: {error}"));

        assert_eq!(stored.session_id(), &session);
        assert_eq!(stored.seq(), index as u64 + 1);
        assert_eq!(stored.event(), event);
        assert_eq!(stored.timestamp(), timestamp);
        assert_eq!(stored.event_type(), EXPECTED_TYPES[index]);
        assert_eq!(
            stored.payload_json(),
            serde_json::to_string(event).expect("event should serialize"),
            "the persisted JSON must match a direct serialization"
        );

        let payload: Value =
            serde_json::from_str(stored.payload_json()).expect("payload should be JSON");
        assert_eq!(
            payload["type"].as_str(),
            Some(EXPECTED_TYPES[index]),
            "stored event_type must match the serde tag in the payload"
        );
        appended.push(stored);
    }

    let read_back = store.events(&session).expect("events should load");
    assert_eq!(
        read_back, appended,
        "read-back records must preserve every field, including payload bytes"
    );
    assert_eq!(store.last_seq(&session).unwrap(), Some(9));
}

#[test]
fn sessions_have_independent_sequences_and_survive_reopen() {
    let database = TempDatabase::new("reopen");
    let session_a = SessionId::new("session-a");
    let session_b = SessionId::new("session-b");

    {
        let mut store = EventStore::open(database.path()).expect("store should open");
        store
            .append(
                &session_a,
                &AgentEvent::ModelDelta {
                    text: "a1".to_owned(),
                },
                Timestamp::from_millis(10),
            )
            .expect("first append should succeed");
        store
            .append(
                &session_b,
                &AgentEvent::ModelDelta {
                    text: "b1".to_owned(),
                },
                Timestamp::from_millis(20),
            )
            .expect("second append should succeed");
        store
            .append(
                &session_a,
                &AgentEvent::ModelDelta {
                    text: "a2".to_owned(),
                },
                Timestamp::from_millis(30),
            )
            .expect("third append should succeed");

        assert_eq!(store.last_seq(&session_a).unwrap(), Some(2));
        assert_eq!(store.last_seq(&session_b).unwrap(), Some(1));
        assert_eq!(
            store.last_seq(&SessionId::new("missing")).unwrap(),
            None,
            "an unknown session has no sequence"
        );
    }

    let reopened = EventStore::open(database.path()).expect("store should reopen");
    let events_a = reopened.events(&session_a).expect("events should load");
    assert_eq!(
        events_a.iter().map(StoredEvent::seq).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        events_a
            .iter()
            .map(StoredEvent::timestamp)
            .collect::<Vec<_>>(),
        vec![Timestamp::from_millis(10), Timestamp::from_millis(30)]
    );
    assert_eq!(
        events_a[1].event(),
        &AgentEvent::ModelDelta {
            text: "a2".to_owned(),
        }
    );

    let events_b = reopened.events(&session_b).expect("events should load");
    assert_eq!(events_b.len(), 1);
    assert_eq!(events_b[0].seq(), 1);
    assert!(
        reopened
            .events(&SessionId::new("missing"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn the_schema_has_the_required_columns_and_rejects_mutation() {
    let database = TempDatabase::new("append-only");
    let session = SessionId::new("guarded");
    let mut store = EventStore::open(database.path()).expect("store should open");
    store
        .append(
            &session,
            &AgentEvent::SessionStarted {
                session_id: session.clone(),
            },
            Timestamp::from_millis(99),
        )
        .expect("append should succeed");

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let columns = {
        let mut statement = connection
            .prepare("PRAGMA table_info(events)")
            .expect("table info should prepare");
        statement
            .query_map([], |row| row.get::<_, String>(1))
            .expect("table info should query")
            .collect::<Result<Vec<_>, _>>()
            .expect("column names should decode")
    };
    assert_eq!(
        columns,
        [
            "session_id",
            "seq",
            "event_type",
            "payload_json",
            "timestamp"
        ]
    );

    let update_error = connection
        .execute("UPDATE events SET payload_json = '{}'", [])
        .expect_err("UPDATE must be rejected");
    assert!(
        update_error.to_string().contains("append-only"),
        "unexpected UPDATE error: {update_error}"
    );

    let delete_error = connection
        .execute("DELETE FROM events", [])
        .expect_err("DELETE must be rejected");
    assert!(
        delete_error.to_string().contains("append-only"),
        "unexpected DELETE error: {delete_error}"
    );

    let events = store.events(&session).expect("events should still load");
    assert_eq!(events.len(), 1, "rejected mutations must not remove rows");
    assert_eq!(events[0].timestamp(), Timestamp::from_millis(99));
}

#[test]
fn reads_follow_sequence_order_even_for_out_of_order_inserts() {
    let database = TempDatabase::new("ordering");
    let session = SessionId::new("out-of-order");
    let store = EventStore::open(database.path()).expect("store should open");
    let connection = Connection::open(database.path()).expect("raw connection should open");

    for (seq, text) in [(2_i64, "second"), (1_i64, "first")] {
        let payload = serde_json::to_string(&AgentEvent::ModelDelta {
            text: text.to_owned(),
        })
        .expect("event should serialize");
        connection
            .execute(
                "INSERT INTO events (session_id, seq, event_type, payload_json, timestamp) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![session.as_str(), seq, "model_delta", payload, seq],
            )
            .expect("raw insert should succeed");
    }

    let events = store.events(&session).expect("events should load");
    assert_eq!(
        events.iter().map(StoredEvent::seq).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        events[1].event(),
        &AgentEvent::ModelDelta {
            text: "second".to_owned(),
        }
    );
    assert_eq!(store.last_seq(&session).unwrap(), Some(2));
}

#[test]
fn session_metadata_is_immutable_and_survives_reopen() {
    let database = TempDatabase::new("session-metadata");
    let session = SessionId::new("resumable");
    let bootstrap = vec![
        Message::system("you are a helpful agent"),
        Message::user("calculate the total"),
    ];

    {
        let mut store = EventStore::open(database.path()).expect("store should open");
        assert_eq!(
            store
                .session_messages(&session)
                .expect("lookup should succeed"),
            None,
            "a session without metadata must report None"
        );
        store
            .record_session(&session, &bootstrap, Timestamp::from_millis(5))
            .expect("recording bootstrap messages should succeed");
        assert_eq!(
            store
                .session_messages(&session)
                .expect("lookup should succeed"),
            Some(bootstrap.clone())
        );
        store
            .record_session(&session, &bootstrap, Timestamp::from_millis(999))
            .expect("re-recording identical messages must be idempotent");
        assert_eq!(
            store
                .session_messages(&session)
                .expect("lookup should succeed"),
            Some(bootstrap.clone()),
            "the original timestamp and messages must be retained"
        );

        let conflicting = vec![Message::user("a different question")];
        match store.record_session(&session, &conflicting, Timestamp::from_millis(1_000)) {
            Err(EventStoreError::SessionConflict { session_id }) => {
                assert_eq!(session_id, "resumable")
            }
            other => panic!("expected a SessionConflict error, got {other:?}"),
        }
        assert_eq!(
            store
                .session_messages(&session)
                .expect("lookup should succeed"),
            Some(bootstrap.clone()),
            "a rejected conflict must not overwrite the original metadata"
        );
    }

    let reopened = EventStore::open(database.path()).expect("store should reopen");
    assert_eq!(
        reopened
            .session_messages(&session)
            .expect("lookup should succeed"),
        Some(bootstrap),
        "bootstrap messages must survive reopening"
    );

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let update_error = connection
        .execute("UPDATE sessions SET messages_json = '[]'", [])
        .expect_err("UPDATE must be rejected");
    assert!(
        update_error.to_string().contains("immutable"),
        "unexpected UPDATE error: {update_error}"
    );
    let delete_error = connection
        .execute("DELETE FROM sessions", [])
        .expect_err("DELETE must be rejected");
    assert!(
        delete_error.to_string().contains("immutable"),
        "unexpected DELETE error: {delete_error}"
    );
}

#[test]
fn corrupt_payloads_are_reported_with_their_session_and_sequence() {
    let database = TempDatabase::new("corrupt");
    let store = EventStore::open(database.path()).expect("store should open");
    let connection = Connection::open(database.path()).expect("raw connection should open");

    connection
        .execute(
            "INSERT INTO events (session_id, seq, event_type, payload_json, timestamp) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["broken-json", 7_i64, "model_delta", "{not json", 0_i64],
        )
        .expect("raw insert should succeed");

    match store.events(&SessionId::new("broken-json")) {
        Err(EventStoreError::Decode {
            session_id,
            seq,
            source,
        }) => {
            assert_eq!(session_id, "broken-json");
            assert_eq!(seq, 7);
            assert!(
                source.to_string().contains("key must be a string")
                    || source.to_string().contains("expected")
                    || source.to_string().contains("EOF"),
                "unexpected decode error: {source}"
            );
        }
        other => panic!("expected a Decode error, got {other:?}"),
    }

    let payload = serde_json::to_string(&AgentEvent::ModelDelta {
        text: "typed".to_owned(),
    })
    .expect("event should serialize");
    connection
        .execute(
            "INSERT INTO events (session_id, seq, event_type, payload_json, timestamp) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                "mismatched-type",
                1_i64,
                "tool_call_started",
                payload,
                0_i64
            ],
        )
        .expect("raw insert should succeed");

    match store.events(&SessionId::new("mismatched-type")) {
        Err(EventStoreError::EventTypeMismatch {
            session_id,
            seq,
            stored,
            payload,
        }) => {
            assert_eq!(session_id, "mismatched-type");
            assert_eq!(seq, 1);
            assert_eq!(stored, "tool_call_started");
            assert_eq!(payload, "model_delta");
        }
        other => panic!("expected an EventTypeMismatch error, got {other:?}"),
    }
}
