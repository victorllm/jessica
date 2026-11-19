//! Behavioral tests for the trace hash chain and its migration.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_core::trace::{TraceEvent, TracePayload, TraceTurnMode, TurnId};
use jessica_core::types::{AgentEvent, SessionEndReason, SessionId};
use jessica_memory::{EventStore, EventStoreError, Timestamp};
use rusqlite::{Connection, params};

/// Unique database file removed when the test finishes.
struct TempDatabase {
    /// Path passed to [`EventStore`].
    path: PathBuf,
}

impl TempDatabase {
    /// Creates an unused path in the system temporary directory.
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-trace-chain-{label}-{}-{unique}.sqlite",
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

/// Builds a `turn_started` entry.
fn started(turn: &TurnId, session: &SessionId) -> TraceEvent {
    TraceEvent::new(
        turn.clone(),
        None,
        Some(session.clone()),
        TracePayload::TurnStarted {
            mode: TraceTurnMode::Fresh,
            messages: Vec::new(),
        },
    )
}

/// Builds an attempt-scoped agent entry with the given delta text.
fn agent(turn: &TurnId, session: &SessionId, text: &str) -> (TraceEvent, AgentEvent) {
    let event = AgentEvent::ModelDelta {
        text: text.to_owned(),
    };
    (
        TraceEvent::new(
            turn.clone(),
            Some(1),
            Some(session.clone()),
            TracePayload::Agent {
                event: event.clone(),
            },
        ),
        event,
    )
}

/// Builds a `turn_ended` entry.
fn ended(turn: &TurnId) -> TraceEvent {
    TraceEvent::new(
        turn.clone(),
        None,
        None,
        TracePayload::TurnEnded {
            reason: SessionEndReason::Completed,
        },
    )
}

/// Appends a complete three-entry turn.
fn append_closed_turn(store: &mut EventStore, turn: &TurnId, session: &SessionId, base: i64) {
    let (agent_trace, legacy) = agent(turn, session, "original");
    store
        .append_traced(&started(turn, session), None, Timestamp::from_millis(base))
        .expect("turn_started should append");
    store
        .append_traced(
            &agent_trace,
            Some((session, &legacy)),
            Timestamp::from_millis(base + 1),
        )
        .expect("agent entry should append");
    store
        .append_traced(&ended(turn), None, Timestamp::from_millis(base + 2))
        .expect("turn_ended should append");
}

/// Returns every chain row for `turn`, ordered by sequence.
fn chain_rows(connection: &Connection, turn: &TurnId) -> Vec<(i64, String, String)> {
    let mut statement = connection
        .prepare(
            "SELECT seq, prev_hash, entry_hash FROM trace_chain \
             WHERE turn_id = ?1 ORDER BY seq ASC",
        )
        .expect("chain query should prepare");
    let rows = statement
        .query_map([turn.as_str()], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("chain query should run");
    rows.map(|row| row.expect("chain row should decode"))
        .collect()
}

/// Reads a turn digest, requiring the turn to exist.
fn digest(store: &EventStore, turn: &TurnId) -> String {
    store
        .trace_turn(turn)
        .expect("trace should validate")
        .expect("trace should exist")
        .digest()
        .to_owned()
}

/// Returns whether `value` is 64 lowercase hexadecimal characters.
fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[test]
fn appended_entries_form_a_linked_chain_and_the_digest_survives_reopen() {
    let database = TempDatabase::new("links");
    let session = SessionId::new("chain-session");
    let turn = TurnId::new("turn-chain");
    let mut store = EventStore::open(database.path()).expect("store should open");

    store
        .append_traced(&started(&turn, &session), None, Timestamp::from_millis(1))
        .expect("turn_started should append");
    let digest_after_start = digest(&store, &turn);
    assert!(
        is_lower_hex_64(&digest_after_start),
        "digest must be canonical lowercase hex: {digest_after_start}"
    );

    let (agent_trace, legacy) = agent(&turn, &session, "first");
    store
        .append_traced(
            &agent_trace,
            Some((&session, &legacy)),
            Timestamp::from_millis(2),
        )
        .expect("agent entry should append");
    let digest_after_agent = digest(&store, &turn);
    assert_ne!(
        digest_after_start, digest_after_agent,
        "appending an entry must change the turn digest"
    );

    store
        .append_traced(&ended(&turn), None, Timestamp::from_millis(3))
        .expect("turn_ended should append");
    let closed_digest = digest(&store, &turn);
    drop(store);

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let rows = chain_rows(&connection, &turn);
    assert_eq!(rows.len(), 3, "every trace entry must have one chain link");
    assert_eq!(
        rows[0].1,
        "0".repeat(64),
        "the first link must start at the genesis hash"
    );
    assert_eq!(
        rows[1].1, rows[0].2,
        "each link must name the previous entry hash"
    );
    assert_eq!(
        rows[2].1, rows[1].2,
        "each link must name the previous entry hash"
    );
    for (_, previous, entry) in &rows {
        assert!(is_lower_hex_64(previous), "previous hash must be canonical");
        assert!(is_lower_hex_64(entry), "entry hash must be canonical");
    }
    drop(connection);

    let reopened = EventStore::open(database.path()).expect("store should reopen");
    assert_eq!(
        digest(&reopened, &turn),
        closed_digest,
        "the digest must be stable across reopen"
    );
}

#[test]
fn migration_backfills_the_same_digest_and_read_only_audit_requires_it() {
    let database = TempDatabase::new("migration");
    let session = SessionId::new("migration-session");
    let closed = TurnId::new("turn-migration-closed");
    let open = TurnId::new("turn-migration-open");

    let (closed_digest, open_digest) = {
        let mut store = EventStore::open(database.path()).expect("store should open");
        append_closed_turn(&mut store, &closed, &session, 10);
        store
            .append_traced(&started(&open, &session), None, Timestamp::from_millis(20))
            .expect("open turn should append");
        (digest(&store, &closed), digest(&store, &open))
    };

    // Reproduce the schema shape left by the previous release: trace entries
    // exist, but the chain table and its version do not.
    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute_batch("DROP TABLE trace_chain; PRAGMA user_version = 1;")
            .expect("the version-one shape should be reproducible");
    }

    let error = EventStore::open_read_only(database.path())
        .expect_err("a version-one database lacks the chain and must not be audited");
    match error {
        EventStoreError::TraceSchemaOutdated {
            found, required, ..
        } => {
            assert_eq!(found, 1);
            assert_eq!(required, 2);
        }
        other => panic!("expected TraceSchemaOutdated, found {other:?}"),
    }

    let migrated = EventStore::open(database.path()).expect("read-write open should migrate");
    assert_eq!(
        digest(&migrated, &closed),
        closed_digest,
        "backfill must reproduce the live append digest for a closed turn"
    );
    assert_eq!(
        digest(&migrated, &open),
        open_digest,
        "backfill must reproduce the live append digest for an open turn"
    );
    drop(migrated);

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("version should read");
    assert_eq!(version, 2);
    let chain_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM trace_chain", [], |row| row.get(0))
        .expect("chain rows should count");
    assert_eq!(chain_count, 4, "one chain link per migrated trace entry");
    drop(connection);

    let audited = EventStore::open_read_only(database.path())
        .expect("a migrated database should be auditable");
    assert_eq!(digest(&audited, &closed), closed_digest);
    assert_eq!(digest(&audited, &open), open_digest);
}

#[test]
fn editing_a_stored_payload_is_detected_by_the_chain() {
    let database = TempDatabase::new("payload-tamper");
    let session = SessionId::new("tamper-session");
    let turn = TurnId::new("turn-tamper");
    {
        let mut store = EventStore::open(database.path()).expect("store should open");
        append_closed_turn(&mut store, &turn, &session, 100);
    }

    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute_batch("DROP TRIGGER trace_events_append_only_update;")
            .expect("the update guard should drop for the fixture");
        let changed = connection
            .execute(
                "UPDATE trace_events SET payload_json = replace(payload_json, '\"original\"', '\"tampered\"') \
                 WHERE turn_id = ?1 AND seq = 2",
                [turn.as_str()],
            )
            .expect("the fixture edit should apply");
        assert_eq!(changed, 1, "the fixture must edit exactly one entry");
    }

    let audited = EventStore::open_read_only(database.path()).expect("audit open should succeed");
    let error = audited
        .trace_turn(&turn)
        .expect_err("an edited payload must break the chain");
    assert!(
        matches!(&error, EventStoreError::TraceInvalid { message, .. } if message.contains("chain")),
        "unexpected error: {error:?}"
    );
}

#[test]
fn deleting_a_chain_link_is_detected() {
    let database = TempDatabase::new("chain-delete");
    let session = SessionId::new("delete-session");
    let turn = TurnId::new("turn-delete");
    {
        let mut store = EventStore::open(database.path()).expect("store should open");
        append_closed_turn(&mut store, &turn, &session, 200);
    }

    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute_batch("DROP TRIGGER trace_chain_append_only_delete;")
            .expect("the delete guard should drop for the fixture");
        let deleted = connection
            .execute(
                "DELETE FROM trace_chain WHERE turn_id = ?1 AND seq = 2",
                [turn.as_str()],
            )
            .expect("the fixture deletion should apply");
        assert_eq!(deleted, 1, "the fixture must delete exactly one link");
    }

    let audited = EventStore::open_read_only(database.path()).expect("audit open should succeed");
    let error = audited
        .trace_turn(&turn)
        .expect_err("a missing chain link must invalidate the turn");
    assert!(
        matches!(&error, EventStoreError::TraceInvalid { message, .. } if message.contains("chain")),
        "unexpected error: {error:?}"
    );
}

#[test]
fn an_extra_chain_link_is_detected() {
    let database = TempDatabase::new("chain-insert");
    let session = SessionId::new("extra-session");
    let turn = TurnId::new("turn-extra");
    {
        let mut store = EventStore::open(database.path()).expect("store should open");
        append_closed_turn(&mut store, &turn, &session, 300);
    }

    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute(
                "INSERT INTO trace_chain (turn_id, seq, prev_hash, entry_hash) \
                 VALUES (?1, 4, ?2, ?3)",
                params![turn.as_str(), "0".repeat(64), "0".repeat(64)],
            )
            .expect("the fixture chain row should insert");
    }

    let audited = EventStore::open_read_only(database.path()).expect("audit open should succeed");
    let error = audited
        .trace_turn(&turn)
        .expect_err("an extra chain link must invalidate the turn");
    assert!(
        matches!(&error, EventStoreError::TraceInvalid { message, .. } if message.contains("chain")),
        "unexpected error: {error:?}"
    );
}

#[test]
fn a_failed_chain_backfill_rolls_back_the_migration() {
    let database = TempDatabase::new("backfill-rollback");
    let session = SessionId::new("rollback-session");
    let turn = TurnId::new("turn-rollback");
    let expected_digest = {
        let mut store = EventStore::open(database.path()).expect("store should open");
        append_closed_turn(&mut store, &turn, &session, 400);
        digest(&store, &turn)
    };

    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute_batch(
                "DROP TABLE trace_chain; \
                 PRAGMA user_version = 1; \
                 CREATE TABLE trace_chain (wrong_shape TEXT);",
            )
            .expect("the conflicting version-one fixture should install");
    }

    let error = EventStore::open(database.path()).expect_err("the backfill must fail");
    assert!(
        matches!(error, EventStoreError::Schema(_)),
        "unexpected error: {error:?}"
    );

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("version should read");
    assert_eq!(
        version, 1,
        "a failed migration must not advance user_version"
    );
    let trace_rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM trace_events", [], |row| row.get(0))
        .expect("trace entries should count");
    assert_eq!(trace_rows, 3, "the failed migration must not touch entries");
    connection
        .execute_batch("DROP TABLE trace_chain;")
        .expect("the fixture should be removable");
    drop(connection);

    let retried = EventStore::open(database.path()).expect("a retry after rollback should work");
    assert_eq!(
        digest(&retried, &turn),
        expected_digest,
        "the retried backfill must reproduce the original digest"
    );
}
