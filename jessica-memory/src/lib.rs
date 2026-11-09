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

#![deny(missing_docs)]

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use jessica_core::types::{AgentEvent, Message, SessionId};
use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

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
/// Opening a database installs the `events` table and its append-only triggers
/// if they are absent. Appends allocate the next sequence number in a
/// transaction; reads never modify the log.
#[derive(Debug)]
pub struct EventStore {
    /// SQLite connection used for every operation on this store.
    connection: Connection,
}

impl EventStore {
    /// Opens or creates a database at `path`.
    ///
    /// Missing parent directories are not created. The schema and append-only
    /// triggers are installed if absent.
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

    /// Installs the schema and wraps the connection.
    fn initialize(connection: Connection) -> Result<Self, EventStoreError> {
        connection
            .busy_timeout(BUSY_TIMEOUT)
            .map_err(EventStoreError::Schema)?;
        connection
            .execute_batch(SCHEMA)
            .map_err(EventStoreError::Schema)?;
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
        let payload_json = serde_json::to_string(event)?;
        let event_type = event_type_tag(event);
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| append_error(session_id, source))?;
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
        transaction
            .commit()
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

/// Returns the serde tag stored in the `event_type` column.
fn event_type_tag(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::SessionStarted { .. } => "session_started",
        AgentEvent::ModelDelta { .. } => "model_delta",
        AgentEvent::ToolCallStarted { .. } => "tool_call_started",
        AgentEvent::ToolCallFinished { .. } => "tool_call_finished",
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
}
