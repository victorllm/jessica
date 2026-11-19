//! Event-sourced persistence for Jessica sessions.
//!
//! [`EventStore`] appends [`AgentEvent`]s to a SQLite table in per-session
//! sequence order and reads them back without rewriting the log. The table is
//! append-only at the database level: triggers reject `UPDATE` and `DELETE`.
//!
//! Each row carries the session id, a one-based sequence number, the serde type
//! tag, the exact JSON payload, and a caller-supplied millisecond timestamp:
//!
//! | column | meaning |
//! | --- | --- |
//! | `session_id` | owning session |
//! | `seq` | one-based position within the session |
//! | `event_type` | serde tag such as `tool_call_finished` |
//! | `payload_json` | serialized [`AgentEvent`] |
//! | `timestamp` | milliseconds since the Unix epoch |
//!
//! The store also keeps immutable session bootstrap metadata (the system and
//! user messages a session started with) in a `sessions` table. `AgentEvent`
//! has no user-message variant, so those messages cannot be recovered from the
//! event log alone; recording them separately lets recovery rebuild the exact
//! conversation without changing the frozen core event model.
//!
//! The API is synchronous because SQLite is synchronous. Callers inside a Tokio
//! runtime must invoke it through `tokio::task::spawn_blocking`, and the store
//! never reads the host clock itself so callers control event timestamps.
//!
//! File databases use WAL journaling with `synchronous = FULL`. WAL lets
//! readers proceed while the single writer commits, and every commit is
//! fsynced; the `-wal` and `-shm` sidecars exist while the store is open and a
//! clean close checkpoints and removes them. In-memory databases keep the
//! `memory` journal mode.
//!
//! [`MemoryLog`] is the append-only storage foundation for layered memory. It
//! stores raw entries scoped by session and workspace, indexes them with FTS5,
//! and permits only one-way soft deletion.
//!
//! [`Embedder`] and [`VectorIndex`] add the deterministic vector layer:
//! [`DeterministicEmbedder`] produces reproducible, non-semantic vectors and
//! [`BruteForceIndex`] ranks stored vectors by cosine similarity.
//!
//! [`SqliteMemoryStore`] is the asynchronous adapter that implements the frozen
//! [`jessica_core::memory::MemoryStore`] contract. It fuses short-term session
//! entries, mid-term workspace keyword matches, and long-term vector matches
//! with reciprocal rank fusion while degrading individual failures to warnings.

#![deny(missing_docs)]

mod embedding;
mod fts_query;
mod memory_log;
mod memory_store;
mod skill_jobs;
mod skill_log;
mod trace_store;
mod workspace;

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use jessica_core::types::{AgentEvent, Message, SessionId};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use thiserror::Error;

pub use embedding::{
    BruteForceIndex, DeterministicEmbedder, EmbedError, Embedder, VectorError, VectorIndex,
    VectorIndexError, VectorMatch, decode_vector, encode_vector,
};
pub use memory_log::{
    MemoryEntryId, MemoryLog, MemoryLogError, MemoryRecord, NewMemoryEntry, WorkspaceScope,
};
pub use memory_store::{RecallRequest, SqliteMemoryStore, SqliteMemoryStoreError};
pub use skill_jobs::{
    MAX_LAST_ERROR_BYTES, MAX_SOURCE_JSON_BYTES, NewSkillJob, SkillJob, SkillJobError, SkillJobId,
    SkillJobStatus,
};
pub use skill_log::{
    NewSkill, Skill, SkillId, SkillLog, SkillLogError, SkillSource, SkillStatus, SqliteSkillStore,
    SqliteSkillStoreError,
};
pub use trace_store::{StoredTraceEvent, StoredTraceTurn, TraceTurnSummary};

/// SQLite schema and the append-only guards installed on every open.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq >= 1),
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    PRIMARY KEY (session_id, seq)
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS events_append_only_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS events_append_only_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only: DELETE is not allowed');
END;

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    messages_json TEXT NOT NULL,
    started_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS sessions_append_only_update
BEFORE UPDATE ON sessions
BEGIN
    SELECT RAISE(ABORT, 'sessions are immutable: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS sessions_append_only_delete
BEFORE DELETE ON sessions
BEGIN
    SELECT RAISE(ABORT, 'sessions are immutable: DELETE is not allowed');
END;
"#;

/// How long SQLite waits for a competing writer before returning `Busy`.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Schema version stored in the event database's `user_version` pragma.
///
/// Version 1 adds the versioned trace tables without changing or rewriting the
/// legacy `events` and `sessions` tables. Migrations are applied one version at
/// a time inside a transaction; a schema change bumps this constant and adds
/// the matching arm in `apply_migration`.
const SCHEMA_VERSION: i64 = 1;

/// Milliseconds since the Unix epoch, supplied by the caller.
///
/// The store never reads the host clock itself. Recording an explicit timestamp
/// keeps event capture deterministic and lets replay preserve the original
/// wall-clock values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Creates a timestamp from milliseconds since the Unix epoch.
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Returns milliseconds since the Unix epoch.
    pub const fn as_millis(self) -> i64 {
        self.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// One event recovered from the store.
///
/// The exact JSON text is retained alongside the decoded event so replay and
/// audit paths can reproduce the persisted bytes without re-serializing.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredEvent {
    /// Owning session.
    session_id: SessionId,
    /// One-based position within the session.
    seq: u64,
    /// Serde tag of the event.
    event_type: String,
    /// Exact JSON text persisted in the store.
    payload_json: String,
    /// Caller-supplied event timestamp.
    timestamp: Timestamp,
    /// Decoded event.
    event: AgentEvent,
}

impl StoredEvent {
    /// Returns the session this event belongs to.
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the one-based position within the session's event stream.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Returns the serde tag of the event, such as `"tool_call_finished"`.
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// Returns the exact JSON text persisted in the store.
    pub fn payload_json(&self) -> &str {
        &self.payload_json
    }

    /// Returns the caller-supplied event timestamp.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the decoded event.
    pub fn event(&self) -> &AgentEvent {
        &self.event
    }
}

/// Append-only event log backed by SQLite.
///
/// Opening a database installs or migrates the schema and enables WAL
/// journaling with `synchronous = FULL` for file databases. Appends allocate
/// the next sequence number in a transaction; reads never modify the log.
#[derive(Debug)]
pub struct EventStore {
    /// SQLite connection used for every operation on this store.
    connection: Connection,
}

impl EventStore {
    /// Opens or creates a database at `path`.
    ///
    /// Missing parent directories are not created. The schema and append-only
    /// triggers are installed or migrated if needed, and file databases use WAL
    /// journaling with `synchronous = FULL`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EventStoreError> {
        let path = path.as_ref();
        let connection = Connection::open(path).map_err(|source| EventStoreError::Open {
            path: path.to_path_buf(),
            source,
        })?;
        Self::initialize(connection)
    }

    /// Opens a private in-memory database, primarily for tests and short-lived
    /// tools.
    pub fn open_in_memory() -> Result<Self, EventStoreError> {
        let connection = Connection::open_in_memory().map_err(EventStoreError::InMemory)?;
        Self::initialize(connection)
    }

    /// Opens an existing database without creating, migrating, or modifying it.
    ///
    /// This is the audit path used by `jessica trace verify`. Unlike
    /// [`EventStore::open`], a missing file is an error rather than a new
    /// database, and a database without the trace schema is rejected instead of
    /// being upgraded. Databases written by a newer build are rejected for the
    /// same reason as [`EventStore::open`].
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, EventStoreError> {
        let path = path.as_ref();
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|source| EventStoreError::Open {
                path: path.to_path_buf(),
                source,
            })?;
        connection
            .busy_timeout(BUSY_TIMEOUT)
            .map_err(EventStoreError::Schema)?;
        let version = read_user_version(&connection)?;
        if version > SCHEMA_VERSION {
            return Err(EventStoreError::UnsupportedVersion {
                found: version,
                supported: SCHEMA_VERSION,
            });
        }
        let trace_table_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type = 'table' AND name IN ('trace_events', 'trace_heads')",
                [],
                |row| row.get(0),
            )
            .map_err(EventStoreError::Schema)?;
        if version < 1 || trace_table_count != 2 {
            return Err(EventStoreError::TraceSchemaMissing {
                path: path.to_path_buf(),
                version,
            });
        }
        Ok(Self { connection })
    }

    /// Installs or upgrades the schema, enables WAL, and wraps the connection.
    ///
    /// The schema version is read before any write, so a database from a newer
    /// build is rejected without changing its journal mode. Databases written
    /// before trace support report `user_version = 0`; they gain the trace
    /// tables without any event rewrite. Migrations and their `user_version`
    /// updates commit in one transaction.
    fn initialize(mut connection: Connection) -> Result<Self, EventStoreError> {
        connection
            .busy_timeout(BUSY_TIMEOUT)
            .map_err(EventStoreError::Schema)?;
        let version = read_user_version(&connection)?;
        if version > SCHEMA_VERSION {
            return Err(EventStoreError::UnsupportedVersion {
                found: version,
                supported: SCHEMA_VERSION,
            });
        }
        configure_wal(&connection)?;
        install_schema(&mut connection, version)?;
        Ok(Self { connection })
    }

    /// Records the immutable bootstrap messages for `session_id`.
    ///
    /// Recording the same session with byte-identical messages twice is
    /// idempotent. Recording different messages for an existing session returns
    /// [`EventStoreError::SessionConflict`] and leaves the original metadata in
    /// place.
    pub fn record_session(
        &mut self,
        session_id: &SessionId,
        messages: &[Message],
        timestamp: Timestamp,
    ) -> Result<(), EventStoreError> {
        let messages_json = serde_json::to_string(messages)?;
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| session_error(session_id, source))?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO sessions (session_id, messages_json, started_at) \
                 VALUES (?1, ?2, ?3)",
                params![session_id.as_str(), messages_json, timestamp.as_millis()],
            )
            .map_err(|source| session_error(session_id, source))?;
        let stored: String = transaction
            .query_row(
                "SELECT messages_json FROM sessions WHERE session_id = ?1",
                [session_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|source| session_error(session_id, source))?;
        if stored != messages_json {
            return Err(EventStoreError::SessionConflict {
                session_id: session_id.to_string(),
            });
        }
        transaction
            .commit()
            .map_err(|source| session_error(session_id, source))?;
        Ok(())
    }

    /// Returns the bootstrap messages recorded for `session_id`.
    ///
    /// Returns `None` when the session has no metadata row.
    pub fn session_messages(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<Vec<Message>>, EventStoreError> {
        let messages_json: Option<String> = self
            .connection
            .query_row(
                "SELECT messages_json FROM sessions WHERE session_id = ?1",
                [session_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| session_error(session_id, source))?;
        messages_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|source| EventStoreError::SessionDecode {
                    session_id: session_id.to_string(),
                    source,
                })
            })
            .transpose()
    }

    /// Appends `event` to `session_id`'s stream and returns the stored record.
    ///
    /// Sequence numbers start at 1 and are allocated inside a transaction, so
    /// the log for a session has no gaps. Any append error leaves the log
    /// unchanged.
    pub fn append(
        &mut self,
        session_id: &SessionId,
        event: &AgentEvent,
        timestamp: Timestamp,
    ) -> Result<StoredEvent, EventStoreError> {
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| append_error(session_id, source))?;
        let stored = append_in_transaction(&transaction, session_id, event, timestamp)?;
        transaction
            .commit()
            .map_err(|source| append_error(session_id, source))?;
        Ok(stored)
    }

    /// Returns every event for `session_id` in ascending sequence order.
    ///
    /// Payloads are decoded on read. A payload that is not a valid
    /// [`AgentEvent`], or whose tag disagrees with the stored `event_type`,
    /// produces an error instead of silently skipping the row.
    pub fn events(&self, session_id: &SessionId) -> Result<Vec<StoredEvent>, EventStoreError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT seq, event_type, payload_json, timestamp FROM events \
                 WHERE session_id = ?1 ORDER BY seq ASC",
            )
            .map_err(|source| read_error(session_id, source))?;
        let mut rows = statement
            .query([session_id.as_str()])
            .map_err(|source| read_error(session_id, source))?;

        let mut events = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|source| read_error(session_id, source))?
        {
            let seq_value: i64 = row
                .get(0)
                .map_err(|source| read_error(session_id, source))?;
            let seq = u64::try_from(seq_value).map_err(|_| EventStoreError::Sequence {
                session_id: session_id.to_string(),
                value: seq_value,
            })?;
            let stored_type: String = row
                .get(1)
                .map_err(|source| read_error(session_id, source))?;
            let payload_json: String = row
                .get(2)
                .map_err(|source| read_error(session_id, source))?;
            let timestamp_millis: i64 = row
                .get(3)
                .map_err(|source| read_error(session_id, source))?;
            let event: AgentEvent =
                serde_json::from_str(&payload_json).map_err(|source| EventStoreError::Decode {
                    session_id: session_id.to_string(),
                    seq,
                    source,
                })?;
            let payload_type = event_type_tag(&event);
            if payload_type != stored_type {
                return Err(EventStoreError::EventTypeMismatch {
                    session_id: session_id.to_string(),
                    seq,
                    stored: stored_type,
                    payload: payload_type.to_owned(),
                });
            }
            events.push(StoredEvent {
                session_id: session_id.clone(),
                seq,
                event_type: stored_type,
                payload_json,
                timestamp: Timestamp::from_millis(timestamp_millis),
                event,
            });
        }
        Ok(events)
    }

    /// Returns the highest sequence number stored for `session_id`.
    ///
    /// Returns `None` when the session has no events.
    pub fn last_seq(&self, session_id: &SessionId) -> Result<Option<u64>, EventStoreError> {
        let value: Option<i64> = self
            .connection
            .query_row(
                "SELECT MAX(seq) FROM events WHERE session_id = ?1",
                [session_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|source| read_error(session_id, source))?;
        match value {
            None => Ok(None),
            Some(value) => u64::try_from(value)
                .map(Some)
                .map_err(|_| EventStoreError::Sequence {
                    session_id: session_id.to_string(),
                    value,
                }),
        }
    }
}

/// Applies pending migrations, then repairs the current schema idempotently.
///
/// Each migration runs inside its own transaction together with the
/// `user_version` update, so an interrupted or failing upgrade leaves the
/// previous version intact. A database already at [`SCHEMA_VERSION`] keeps the
/// previous `IF NOT EXISTS` installation so a partially removed schema is
/// repaired on the next open.
fn install_schema(connection: &mut Connection, version: i64) -> Result<(), EventStoreError> {
    let mut current = version;
    while current < SCHEMA_VERSION {
        let next = current + 1;
        let transaction = connection.transaction().map_err(EventStoreError::Schema)?;
        apply_migration(&transaction, next)?;
        transaction
            .pragma_update(None, "user_version", next)
            .map_err(EventStoreError::Schema)?;
        transaction.commit().map_err(EventStoreError::Schema)?;
        current = next;
    }

    if version == SCHEMA_VERSION {
        let transaction = connection.transaction().map_err(EventStoreError::Schema)?;
        apply_migration(&transaction, SCHEMA_VERSION)?;
        transaction.commit().map_err(EventStoreError::Schema)?;
    }
    Ok(())
}

/// Applies the schema changes for one target version inside `transaction`.
fn apply_migration(
    transaction: &rusqlite::Transaction<'_>,
    version: i64,
) -> Result<(), EventStoreError> {
    match version {
        1 => {
            transaction
                .execute_batch(SCHEMA)
                .map_err(EventStoreError::Schema)?;
            trace_store::install(transaction)
        }
        _ => Err(EventStoreError::MigrationUnavailable { version }),
    }
}

/// Configures WAL journaling and full write durability on a writable connection.
///
/// File databases must report `wal`; in-memory databases report `memory`.
/// `synchronous = FULL` makes every commit fsync the WAL, trading throughput for
/// durability.
fn configure_wal(connection: &Connection) -> Result<(), EventStoreError> {
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(EventStoreError::Schema)?;
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(EventStoreError::Schema)?;
    if journal_mode != "wal" && journal_mode != "memory" {
        return Err(EventStoreError::JournalMode {
            found: journal_mode,
        });
    }
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(EventStoreError::Schema)?;
    Ok(())
}

/// Reads the database's `user_version` pragma.
fn read_user_version(connection: &Connection) -> Result<i64, EventStoreError> {
    connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(EventStoreError::Schema)
}

/// Appends one legacy event through an already-open transaction.
///
/// Both [`EventStore::append`] and the trace dual-write path use this helper so
/// legacy sequence allocation and payload encoding stay identical.
pub(crate) fn append_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    event: &AgentEvent,
    timestamp: Timestamp,
) -> Result<StoredEvent, EventStoreError> {
    let payload_json = serde_json::to_string(event)?;
    let event_type = event_type_tag(event);
    let next_seq: i64 = transaction
        .query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE session_id = ?1",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(|source| append_error(session_id, source))?;
    transaction
        .execute(
            "INSERT INTO events (session_id, seq, event_type, payload_json, timestamp) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session_id.as_str(),
                next_seq,
                event_type,
                payload_json,
                timestamp.as_millis()
            ],
        )
        .map_err(|source| append_error(session_id, source))?;

    let seq = u64::try_from(next_seq).map_err(|_| EventStoreError::Sequence {
        session_id: session_id.to_string(),
        value: next_seq,
    })?;
    Ok(StoredEvent {
        session_id: session_id.clone(),
        seq,
        event_type: event_type.to_owned(),
        payload_json,
        timestamp,
        event: event.clone(),
    })
}

/// Returns the serde tag stored in the `event_type` column.
pub(crate) fn event_type_tag(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::SessionStarted { .. } => "session_started",
        AgentEvent::RoundStarted { .. } => "round_started",
        AgentEvent::RoundFinished { .. } => "round_finished",
        AgentEvent::ModelDelta { .. } => "model_delta",
        AgentEvent::ToolCallStarted { .. } => "tool_call_started",
        AgentEvent::ToolCallFinished { .. } => "tool_call_finished",
        AgentEvent::PermissionDecided { .. } => "permission_decided",
        AgentEvent::ChildSessionStarted { .. } => "child_session_started",
        AgentEvent::ChildSessionEnded { .. } => "child_session_ended",
        AgentEvent::PolicyDenied { .. } => "policy_denied",
        AgentEvent::BudgetExceeded { .. } => "budget_exceeded",
        AgentEvent::AcceptanceFailed { .. } => "acceptance_failed",
        AgentEvent::Error { .. } => "error",
        AgentEvent::SessionEnded { .. } => "session_ended",
    }
}

/// Builds an append error for `session_id`.
fn append_error(session_id: &SessionId, source: rusqlite::Error) -> EventStoreError {
    EventStoreError::Append {
        session_id: session_id.to_string(),
        source,
    }
}

/// Builds a read error for `session_id`.
fn read_error(session_id: &SessionId, source: rusqlite::Error) -> EventStoreError {
    EventStoreError::Read {
        session_id: session_id.to_string(),
        source,
    }
}

/// Builds a session-metadata error for `session_id`.
fn session_error(session_id: &SessionId, source: rusqlite::Error) -> EventStoreError {
    EventStoreError::Session {
        session_id: session_id.to_string(),
        source,
    }
}

/// Errors raised while opening, appending to, or reading the event store.
#[derive(Debug, Error)]
pub enum EventStoreError {
    /// The database file could not be opened.
    #[error("failed to open event store `{path}`: {source}")]
    Open {
        /// Requested database path.
        path: PathBuf,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// The in-memory database could not be created.
    #[error("failed to create an in-memory event store: {0}")]
    InMemory(#[source] rusqlite::Error),
    /// The schema or append-only triggers could not be installed.
    #[error("failed to initialize the event store schema: {0}")]
    Schema(#[source] rusqlite::Error),
    /// SQLite did not enable WAL journaling for a file database.
    #[error("event store journal mode is `{found}`, expected `wal` or `memory`")]
    JournalMode {
        /// Journal mode reported by SQLite.
        found: String,
    },
    /// No migration is registered for a version the store needs to apply.
    #[error("no migration is registered for event store schema version {version}")]
    MigrationUnavailable {
        /// Target version without a registered migration.
        version: i64,
    },
    /// The database was written by a build with a newer schema.
    ///
    /// The store refuses to open it rather than guessing at semantics it does
    /// not know.
    #[error("event store schema version {found} is newer than the supported version {supported}")]
    UnsupportedVersion {
        /// Version stored in the database's `user_version`.
        found: i64,
        /// Highest version this build supports.
        supported: i64,
    },
    /// A read-only open found a database without the trace schema.
    ///
    /// The caller asked for audit access, so silently creating or migrating the
    /// database would change the evidence being inspected.
    #[error(
        "event store `{path}` has no trace schema (user_version = {version}); open it once with a current read-write build"
    )]
    TraceSchemaMissing {
        /// Database path that was opened.
        path: PathBuf,
        /// Version stored in the database's `user_version`.
        version: i64,
    },
    /// The event could not be serialized to JSON.
    #[error("failed to serialize the agent event: {0}")]
    Serialize(#[from] serde_json::Error),
    /// The event could not be appended.
    #[error("failed to append an event for session `{session_id}`: {source}")]
    Append {
        /// Session the append targeted.
        session_id: String,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Events could not be read.
    #[error("failed to read events for session `{session_id}`: {source}")]
    Read {
        /// Session the read targeted.
        session_id: String,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Session metadata could not be recorded or read.
    #[error("failed to access metadata for session `{session_id}`: {source}")]
    Session {
        /// Session the operation targeted.
        session_id: String,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Stored session metadata is not valid JSON for a list of messages.
    #[error("metadata for session `{session_id}` is not a valid message list: {source}")]
    SessionDecode {
        /// Session whose metadata is corrupt.
        session_id: String,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// An attempt was made to change the immutable bootstrap messages of a
    /// session that already has metadata.
    #[error("session `{session_id}` already has different bootstrap messages")]
    SessionConflict {
        /// Session whose metadata was rejected.
        session_id: String,
    },
    /// A stored payload is not a valid [`AgentEvent`].
    #[error("event {seq} for session `{session_id}` is not a valid agent event: {source}")]
    Decode {
        /// Session the event belongs to.
        session_id: String,
        /// Sequence number of the corrupt row.
        seq: u64,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// The stored type tag disagrees with the payload's serde tag.
    #[error(
        "event {seq} for session `{session_id}` has type `{stored}` but its payload is `{payload}`"
    )]
    EventTypeMismatch {
        /// Session the event belongs to.
        session_id: String,
        /// Sequence number of the inconsistent row.
        seq: u64,
        /// Value stored in the `event_type` column.
        stored: String,
        /// Serde tag found in `payload_json`.
        payload: String,
    },
    /// SQLite returned a sequence number outside the `u64` range.
    #[error("event sequence {value} for session `{session_id}` is out of range")]
    Sequence {
        /// Session the sequence belongs to.
        session_id: String,
        /// Value read from SQLite.
        value: i64,
    },
    /// A trace envelope could not be serialized to JSON.
    #[error("failed to serialize an entry for turn `{turn_id}`: {source}")]
    TraceSerialize {
        /// Turn whose envelope could not be serialized.
        turn_id: String,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// Trace rows could not be written or read for one turn.
    #[error("failed to access trace turn `{turn_id}`: {source}")]
    TraceStore {
        /// Turn the operation targeted.
        turn_id: String,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// The trace transaction could not be started, committed, or read.
    #[error("failed to access stored traces: {source}")]
    TraceDatabase {
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// A stored trace payload is not valid JSON for a [`jessica_core::trace::TraceEvent`].
    #[error("trace entry {seq} for turn `{turn_id}` is not a valid trace event: {source}")]
    TraceDecode {
        /// Turn whose row is corrupt.
        turn_id: String,
        /// Sequence number of the corrupt row.
        seq: u64,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// A trace envelope or stored row violates the trace contract.
    ///
    /// `seq` is the offending entry, or `0` when the envelope as a whole is
    /// invalid.
    #[error("trace entry {seq} for turn `{turn_id}` is invalid: {message}")]
    TraceInvalid {
        /// Turn whose entry is invalid.
        turn_id: String,
        /// Offending sequence number, or `0` for an envelope-level problem.
        seq: u64,
        /// Description of the violated rule.
        message: String,
    },
    /// Stored trace sequence numbers are not contiguous.
    #[error("trace turn `{turn_id}` expected sequence {expected} but found {found}")]
    TraceSequence {
        /// Turn whose sequence is broken.
        turn_id: String,
        /// Next sequence number required for a complete trace.
        expected: u64,
        /// Sequence number found in the store.
        found: u64,
    },
    /// A trace lifecycle invariant is broken.
    #[error("trace turn `{turn_id}` has an invalid lifecycle: {message}")]
    TraceLifecycle {
        /// Turn whose lifecycle is invalid.
        turn_id: String,
        /// Description of the violated invariant.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_databases_use_the_memory_journal_and_full_synchronous() {
        let store = EventStore::open_in_memory().expect("in-memory store should open");

        let journal_mode: String = store
            .connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal mode should read");
        assert_eq!(journal_mode, "memory");

        let synchronous: i64 = store
            .connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("synchronous level should read");
        assert_eq!(synchronous, 2, "SQLite encodes synchronous=FULL as 2");
    }
}
