//! Behavioral tests for EventStore WAL mode and transactional migrations.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_core::types::{AgentEvent, SessionId};
use jessica_memory::{EventStore, EventStoreError, Timestamp};
use rusqlite::Connection;

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
            "jessica-durability-{label}-{}-{unique}.sqlite",
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
            let _ = std::fs::remove_file(sidecar(&self.path, suffix));
        }
    }
}

/// Builds the path of a SQLite sidecar file.
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

/// Builds a simple event for durability tests.
fn model_delta(text: &str) -> AgentEvent {
    AgentEvent::ModelDelta {
        text: text.to_owned(),
    }
}

#[test]
fn file_databases_use_wal_and_a_clean_close_removes_sidecars() {
    let database = TempDatabase::new("wal");
    let session = SessionId::new("wal-session");
    {
        let mut store = EventStore::open(database.path()).expect("store should open");
        store
            .append(&session, &model_delta("first"), Timestamp::from_millis(1))
            .expect("append should succeed");
        assert!(
            sidecar(database.path(), "-wal").exists(),
            "a file database must create a WAL sidecar while writing"
        );
    }
    assert!(
        !sidecar(database.path(), "-wal").exists(),
        "a clean close must checkpoint and remove the WAL sidecar"
    );
    assert!(
        !sidecar(database.path(), "-shm").exists(),
        "a clean close must remove the SHM sidecar"
    );

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode should read");
    assert_eq!(
        journal_mode, "wal",
        "the WAL setting must persist across connections"
    );
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE session_id = ?1",
            [session.as_str()],
            |row| row.get(0),
        )
        .expect("events should read");
    assert_eq!(count, 1, "the committed event must survive the clean close");
}

#[test]
fn a_future_schema_is_rejected_before_wal_or_schema_changes() {
    let database = TempDatabase::new("future");
    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute_batch("PRAGMA user_version = 2;")
            .expect("future version should set");
    }

    let error = EventStore::open(database.path()).expect_err("a future schema must be rejected");
    assert!(
        matches!(
            error,
            EventStoreError::UnsupportedVersion {
                found: 2,
                supported: 1
            }
        ),
        "unexpected error: {error:?}"
    );

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("version should read");
    assert_eq!(version, 2, "the rejected database must keep its version");
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode should read");
    assert_eq!(
        journal_mode, "delete",
        "rejecting a future schema must not enable WAL"
    );
    let tables: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type = 'table' \
               AND name IN ('events', 'sessions', 'trace_events', 'trace_heads')",
            [],
            |row| row.get(0),
        )
        .expect("schema should read");
    assert_eq!(
        tables, 0,
        "rejecting a future schema must not install tables"
    );
}

#[test]
fn a_failed_migration_rolls_back_ddl_and_user_version() {
    let database = TempDatabase::new("failed-migration");
    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute_batch(
                "CREATE TABLE fixture (x TEXT);\
                 CREATE INDEX sessions ON fixture (x);\
                 PRAGMA user_version = 0;",
            )
            .expect("the conflicting fixture should install");
    }

    let error = EventStore::open(database.path()).expect_err("the migration must fail");
    assert!(
        matches!(error, EventStoreError::Schema(_)),
        "unexpected error: {error:?}"
    );

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("version should read");
    assert_eq!(
        version, 0,
        "a failed migration must not advance user_version"
    );
    let created_tables: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type = 'table' \
               AND name IN ('events', 'sessions', 'trace_events', 'trace_heads')",
            [],
            |row| row.get(0),
        )
        .expect("schema should read");
    assert_eq!(
        created_tables, 0,
        "DDL from the failed migration must roll back"
    );
    let conflicting_indexes: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type = 'index' AND name = 'sessions'",
            [],
            |row| row.get(0),
        )
        .expect("index should read");
    assert_eq!(
        conflicting_indexes, 1,
        "the fixture outside the migration transaction must remain"
    );
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode should read");
    assert_eq!(
        journal_mode, "wal",
        "WAL setup precedes the migration transaction and is not rolled back"
    );

    connection
        .execute_batch("DROP INDEX sessions;")
        .expect("the conflicting fixture should drop");
    drop(connection);

    let store = EventStore::open(database.path()).expect("a retry after the rollback should work");
    assert_eq!(
        store
            .last_seq(&SessionId::new("no-events"))
            .expect("sequence should read"),
        None
    );
}

#[test]
fn reopening_a_current_database_repairs_a_missing_trigger() {
    let database = TempDatabase::new("repair");
    {
        let _store = EventStore::open(database.path()).expect("store should open");
    }
    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute_batch("DROP TRIGGER events_append_only_update;")
            .expect("the trigger should drop");
    }

    let store = EventStore::open(database.path()).expect("reopen should repair the schema");
    drop(store);

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let triggers: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type = 'trigger' AND name = 'events_append_only_update'",
            [],
            |row| row.get(0),
        )
        .expect("trigger should be visible");
    assert_eq!(triggers, 1, "the current schema must be reinstalled");
}
