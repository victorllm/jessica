//! Append-only SQLite storage for layered memory entries.
//!
//! [`MemoryLog`] is the synchronous storage foundation for the future
//! [`jessica_core::memory::MemoryStore`] implementation. It stores raw text
//! entries scoped to a session and a workspace, indexes them with SQLite FTS5,
//! and exposes two deterministic recall paths:
//!
//! - [`MemoryLog::recent_for_session`] returns the newest entries for one
//!   session and workspace.
//! - [`MemoryLog::search_keyword`] runs a sanitized FTS5 trigram query over one
//!   workspace, with a `LIKE` fallback for tokens shorter than three characters.
//!
//! Entry rows are append-only. The only permitted mutation is a one-way soft
//! delete that sets `deleted_at`; triggers reject every other `UPDATE` and
//! every `DELETE`. A partial unique index deduplicates identical content within
//! a session and workspace while still allowing the content to be stored again
//! after it was soft-deleted.
//!
//! The FTS5 trigram tokenizer was verified against the bundled SQLite build
//! used by this workspace. The store never reads the host clock: callers supply
//! [`Timestamp`] values explicitly. The API is synchronous because SQLite is
//! synchronous; callers inside Tokio must use `tokio::task::spawn_blocking`.
//!
//! Since schema version 2 the database also contains `memory_embeddings`, a
//! side table keyed by entry and model. The synchronous accessors used by
//! [`crate::BruteForceIndex`] replace a stored vector for the same entry and
//! model, and every vector scan joins the parent entry so soft-deleted entries
//! never produce vector hits.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use jessica_core::types::SessionId;
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use serde_json::Value;
use thiserror::Error;

use crate::Timestamp;
use crate::fts_query::{like_pattern, quote_fts_token, split_query};

/// Version stored in SQLite's `user_version` pragma.
///
/// Version 1 introduced entries, FTS5, and append-only guards. Version 2 adds
/// the `memory_embeddings` side table.
const SCHEMA_VERSION: i64 = 2;

/// How long SQLite waits for a competing writer before returning `Busy`.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Tables, indexes, FTS5 index, and append-only guards installed on every open.
///
/// `memory_entries` is the source of truth. `memory_fts` is an external-content
/// FTS5 table: it reads content from `memory_entries` and is kept in sync by
/// the three triggers below. The `UPDATE` trigger removes the old FTS row and
/// only re-inserts it when the entry is still live, which makes soft deletion
/// visible to keyword search without rewriting the source row's content.
/// `memory_embeddings` stores at most one vector per entry and model; the
/// foreign key prevents vectors for missing entries when foreign-key
/// enforcement is active.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS memory_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    workspace_scope TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    deleted_at INTEGER NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS memory_entries_unique_active
ON memory_entries (session_id, workspace_scope, content)
WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS memory_embeddings (
    entry_id INTEGER NOT NULL REFERENCES memory_entries(id),
    model TEXT NOT NULL,
    dims INTEGER NOT NULL,
    vector BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (entry_id, model)
);

CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
    content,
    content='memory_entries',
    content_rowid='id',
    tokenize='trigram'
);

CREATE TRIGGER IF NOT EXISTS memory_entries_soft_delete_only
BEFORE UPDATE ON memory_entries
WHEN NOT (
    OLD.deleted_at IS NULL
    AND NEW.deleted_at IS NOT NULL
    AND NEW.id IS OLD.id
    AND NEW.session_id IS OLD.session_id
    AND NEW.workspace_scope IS OLD.workspace_scope
    AND NEW.content IS OLD.content
    AND NEW.metadata_json IS OLD.metadata_json
    AND NEW.created_at IS OLD.created_at
)
BEGIN
    SELECT RAISE(ABORT, 'memory entries are append-only: only one-way soft deletion is allowed');
END;

CREATE TRIGGER IF NOT EXISTS memory_entries_immutable_delete
BEFORE DELETE ON memory_entries
BEGIN
    SELECT RAISE(ABORT, 'memory entries are append-only: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS memory_entries_fts_insert
AFTER INSERT ON memory_entries
BEGIN
    INSERT INTO memory_fts (rowid, content) VALUES (NEW.id, NEW.content);
END;

CREATE TRIGGER IF NOT EXISTS memory_entries_fts_update
AFTER UPDATE ON memory_entries
BEGIN
    INSERT INTO memory_fts (memory_fts, rowid, content)
    VALUES ('delete', OLD.id, OLD.content);
    INSERT INTO memory_fts (rowid, content)
    SELECT NEW.id, NEW.content WHERE NEW.deleted_at IS NULL;
END;

CREATE TRIGGER IF NOT EXISTS memory_entries_fts_delete
AFTER DELETE ON memory_entries
BEGIN
    INSERT INTO memory_fts (memory_fts, rowid, content)
    VALUES ('delete', OLD.id, OLD.content);
END;
"#;

/// Columns selected for every [`MemoryRecord`].
const SELECT_COLUMNS: &str = "m.id, m.session_id, m.workspace_scope, m.content, \
                              m.metadata_json, m.created_at, m.deleted_at";

/// Identifier of one stored memory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemoryEntryId(i64);

impl MemoryEntryId {
    /// Wraps a raw SQLite row id.
    ///
    /// Callers normally receive identifiers from [`MemoryLog::append`]; this
    /// constructor exists for storage adapters and tests that read raw rows.
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// Returns the raw SQLite row id.
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for MemoryEntryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Deterministic workspace partition for memory entries.
///
/// Task 5.1.3 derives this value from the canonical workspace path. The storage
/// layer only requires a stable, non-empty string so that entries from
/// different workspaces never mix.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceScope(String);

impl WorkspaceScope {
    /// Creates a workspace scope from a non-empty string.
    ///
    /// Whitespace-only values are rejected.
    pub fn new(scope: impl Into<String>) -> Result<Self, MemoryLogError> {
        let scope = scope.into();
        if scope.trim().is_empty() {
            return Err(MemoryLogError::EmptyWorkspace);
        }
        Ok(Self(scope))
    }

    /// Returns the scope as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkspaceScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// An entry waiting to be appended.
///
/// Construction validates the content; metadata is serialized as JSON when the
/// entry is appended. The timestamp is supplied by the caller so the storage
/// layer never reads the host clock.
#[derive(Debug, Clone)]
pub struct NewMemoryEntry {
    /// Session that owns the entry.
    session_id: SessionId,
    /// Workspace partition used by keyword recall.
    workspace_scope: WorkspaceScope,
    /// Raw text content.
    content: String,
    /// Structured metadata preserved as JSON.
    metadata: Value,
    /// Caller-supplied creation time.
    created_at: Timestamp,
}

impl NewMemoryEntry {
    /// Creates an entry with `Value::Null` metadata.
    ///
    /// Returns [`MemoryLogError::EmptyContent`] when `content` is empty or
    /// whitespace-only.
    pub fn new(
        session_id: SessionId,
        workspace_scope: WorkspaceScope,
        content: impl Into<String>,
        created_at: Timestamp,
    ) -> Result<Self, MemoryLogError> {
        let content = content.into();
        if content.trim().is_empty() {
            return Err(MemoryLogError::EmptyContent);
        }
        Ok(Self {
            session_id,
            workspace_scope,
            content,
            metadata: Value::Null,
            created_at,
        })
    }

    /// Replaces the entry metadata.
    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// A memory entry read back from storage.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecord {
    /// SQLite row id.
    id: MemoryEntryId,
    /// Session that owns the entry.
    session_id: SessionId,
    /// Workspace partition.
    workspace_scope: WorkspaceScope,
    /// Raw text content.
    content: String,
    /// Structured metadata.
    metadata: Value,
    /// Caller-supplied creation time.
    created_at: Timestamp,
    /// Soft-deletion time, or `None` while the entry is live.
    deleted_at: Option<Timestamp>,
}

impl MemoryRecord {
    /// Returns the entry identifier.
    pub const fn id(&self) -> MemoryEntryId {
        self.id
    }

    /// Returns the owning session.
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the workspace scope.
    pub fn workspace_scope(&self) -> &WorkspaceScope {
        &self.workspace_scope
    }

    /// Returns the raw text content.
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Returns the structured metadata.
    pub fn metadata(&self) -> &Value {
        &self.metadata
    }

    /// Returns the caller-supplied creation time.
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Returns the soft-deletion time, if the entry was deleted.
    pub const fn deleted_at(&self) -> Option<Timestamp> {
        self.deleted_at
    }
}

/// Append-only memory storage backed by SQLite.
///
/// Opening a database installs the schema, FTS5 index, and append-only
/// triggers if they are absent. Appends ignore exact duplicates within the same
/// session and workspace; reads never modify the log. Entry rows are immutable
/// after insertion except for [`MemoryLog::soft_delete`]; the separate
/// `memory_embeddings` side table may be upserted per entry and model.
#[derive(Debug)]
pub struct MemoryLog {
    /// SQLite connection used for every operation on this log.
    connection: Connection,
}

impl MemoryLog {
    /// Opens or creates a memory database at `path`.
    ///
    /// Missing parent directories are not created. The schema is installed if
    /// absent, and a `user_version` newer than this build supports is rejected.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MemoryLogError> {
        let path = path.as_ref();
        let connection = Connection::open(path).map_err(|source| MemoryLogError::Open {
            path: path.to_path_buf(),
            source,
        })?;
        Self::initialize(connection)
    }

    /// Opens a private in-memory database, primarily for tests and short-lived
    /// tools.
    pub fn open_in_memory() -> Result<Self, MemoryLogError> {
        let connection = Connection::open_in_memory().map_err(MemoryLogError::InMemory)?;
        Self::initialize(connection)
    }

    /// Installs the schema and verifies the schema version.
    fn initialize(connection: Connection) -> Result<Self, MemoryLogError> {
        connection
            .busy_timeout(BUSY_TIMEOUT)
            .map_err(MemoryLogError::Schema)?;
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(MemoryLogError::Schema)?;

        let found: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(MemoryLogError::Schema)?;
        if found > SCHEMA_VERSION {
            return Err(MemoryLogError::UnsupportedVersion {
                found,
                supported: SCHEMA_VERSION,
            });
        }

        connection
            .execute_batch(SCHEMA)
            .map_err(MemoryLogError::Schema)?;
        if found < SCHEMA_VERSION {
            connection
                .execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))
                .map_err(MemoryLogError::Schema)?;
        }

        Ok(Self { connection })
    }

    /// Appends `entry`, returning its identifier.
    ///
    /// Returns `Ok(None)` when an identical live entry already exists for the
    /// same session and workspace. A soft-deleted entry does not block a new
    /// append of the same content.
    pub fn append(&self, entry: NewMemoryEntry) -> Result<Option<MemoryEntryId>, MemoryLogError> {
        let metadata_json = serde_json::to_string(&entry.metadata)?;
        let inserted = self
            .connection
            .execute(
                "INSERT OR IGNORE INTO memory_entries \
                 (session_id, workspace_scope, content, metadata_json, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    entry.session_id.as_str(),
                    entry.workspace_scope.as_str(),
                    entry.content,
                    metadata_json,
                    entry.created_at.as_millis()
                ],
            )
            .map_err(MemoryLogError::Insert)?;

        if inserted == 0 {
            return Ok(None);
        }
        Ok(Some(MemoryEntryId::from_raw(
            self.connection.last_insert_rowid(),
        )))
    }

    /// Returns the entry with `id`, including soft-deleted entries.
    ///
    /// Returns `Ok(None)` when no row has that identifier.
    pub fn get(&self, id: MemoryEntryId) -> Result<Option<MemoryRecord>, MemoryLogError> {
        let raw = self
            .connection
            .query_row(
                &format!("SELECT {SELECT_COLUMNS} FROM memory_entries m WHERE m.id = ?1"),
                [id.get()],
                decode_raw,
            )
            .optional()
            .map_err(MemoryLogError::Query)?;
        raw.map(decode_record).transpose()
    }

    /// Returns up to `limit` live entries for `session` and `workspace`.
    ///
    /// Entries are ordered newest first. Ties on `created_at` are broken by
    /// descending row id so the result is deterministic.
    pub fn recent_for_session(
        &self,
        session: &SessionId,
        workspace: &WorkspaceScope,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, MemoryLogError> {
        let mut statement = self
            .connection
            .prepare(&format!(
                "SELECT {SELECT_COLUMNS} FROM memory_entries m \
                 WHERE m.session_id = ?1 AND m.workspace_scope = ?2 AND m.deleted_at IS NULL \
                 ORDER BY m.created_at DESC, m.id DESC LIMIT ?3"
            ))
            .map_err(MemoryLogError::Query)?;
        let rows = statement
            .query_map(
                params![session.as_str(), workspace.as_str(), limit_to_i64(limit)],
                decode_raw,
            )
            .map_err(MemoryLogError::Query)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(decode_record(row.map_err(MemoryLogError::Query)?)?);
        }
        Ok(records)
    }

    /// Searches live entries in `workspace` for `query`.
    ///
    /// Tokens of at least three characters use the FTS5 trigram index; shorter
    /// tokens use an escaped `LIKE` filter. Every token must match. Results are
    /// ordered newest first, with row id as the deterministic tie-breaker.
    ///
    /// User input is never interpolated into the `MATCH` expression: it is
    /// tokenized, then each token is quoted. Queries with no alphanumeric
    /// tokens return an empty result instead of an error.
    pub fn search_keyword(
        &self,
        workspace: &WorkspaceScope,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, MemoryLogError> {
        let tokens = split_query(query);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let mut values = Vec::new();
        let mut statement = String::new();
        let mut next_parameter;

        if tokens.long.is_empty() {
            statement.push_str(&format!(
                "SELECT {SELECT_COLUMNS} FROM memory_entries m \
                 WHERE m.workspace_scope = ?1 AND m.deleted_at IS NULL"
            ));
            values.push(SqlValue::Text(workspace.as_str().to_owned()));
            next_parameter = 2;
        } else {
            let match_expression = tokens
                .long
                .iter()
                .map(|token| quote_fts_token(token))
                .collect::<Vec<_>>()
                .join(" AND ");
            statement.push_str(&format!(
                "SELECT {SELECT_COLUMNS} FROM memory_fts \
                 JOIN memory_entries m ON m.id = memory_fts.rowid \
                 WHERE memory_fts MATCH ?1 AND m.workspace_scope = ?2 AND m.deleted_at IS NULL"
            ));
            values.push(SqlValue::Text(match_expression));
            values.push(SqlValue::Text(workspace.as_str().to_owned()));
            next_parameter = 3;
        }

        for token in tokens.short {
            statement.push_str(&format!(
                " AND m.content LIKE ?{next_parameter} ESCAPE '\\'"
            ));
            values.push(SqlValue::Text(like_pattern(&token)));
            next_parameter += 1;
        }

        statement.push_str(&format!(
            " ORDER BY m.created_at DESC, m.id DESC LIMIT ?{next_parameter}"
        ));
        values.push(SqlValue::Integer(limit_to_i64(limit)));

        let mut prepared = self
            .connection
            .prepare(&statement)
            .map_err(MemoryLogError::Query)?;
        let rows = prepared
            .query_map(params_from_iter(values.iter()), decode_raw)
            .map_err(MemoryLogError::Query)?;

        let mut records = Vec::new();
        for row in rows {
            records.push(decode_record(row.map_err(MemoryLogError::Query)?)?);
        }
        Ok(records)
    }

    /// Soft-deletes the entry with `id` at `deleted_at`.
    ///
    /// The row and its audit trail are kept; recall paths exclude it. A second
    /// soft delete of the same row is rejected, as is a missing identifier.
    pub fn soft_delete(
        &mut self,
        id: MemoryEntryId,
        deleted_at: Timestamp,
    ) -> Result<(), MemoryLogError> {
        let raw_id = id.get();
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| MemoryLogError::Delete { id: raw_id, source })?;

        let state: Option<Option<i64>> = transaction
            .query_row(
                "SELECT deleted_at FROM memory_entries WHERE id = ?1",
                [raw_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| MemoryLogError::Delete { id: raw_id, source })?;

        match state {
            None => return Err(MemoryLogError::NotFound { id: raw_id }),
            Some(Some(_)) => return Err(MemoryLogError::AlreadyDeleted { id: raw_id }),
            Some(None) => {}
        }

        transaction
            .execute(
                "UPDATE memory_entries SET deleted_at = ?1 WHERE id = ?2",
                params![deleted_at.as_millis(), raw_id],
            )
            .map_err(|source| MemoryLogError::Delete { id: raw_id, source })?;
        transaction
            .commit()
            .map_err(|source| MemoryLogError::Delete { id: raw_id, source })?;

        Ok(())
    }

    /// Stores or replaces the embedding for `entry_id` and `model`.
    ///
    /// The vector is expected to be a little-endian `f32` blob produced by
    /// [`crate::encode_vector`] with exactly `dims` components. This method is
    /// synchronous and is only used by the in-crate vector index.
    pub(crate) fn put_embedding(
        &self,
        entry_id: MemoryEntryId,
        model: &str,
        dims: i64,
        encoded: &[u8],
        created_at: Timestamp,
    ) -> Result<(), MemoryLogError> {
        let raw_id = entry_id.get();
        self.connection
            .execute(
                "INSERT INTO memory_embeddings (entry_id, model, dims, vector, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(entry_id, model) DO UPDATE SET \
                     dims = excluded.dims, \
                     vector = excluded.vector, \
                     created_at = excluded.created_at",
                params![raw_id, model, dims, encoded, created_at.as_millis()],
            )
            .map_err(|source| MemoryLogError::EmbeddingStore {
                entry_id: raw_id,
                source,
            })?;
        Ok(())
    }

    /// Returns `(entry_id, encoded_vector)` pairs for live entries in
    /// `workspace` that have an embedding for exactly `model` and `dims`.
    ///
    /// Rows are ordered by ascending entry id so callers can apply a stable
    /// score sort. Soft-deleted entries are excluded through a join with
    /// `memory_entries`.
    pub(crate) fn scan_embeddings(
        &self,
        workspace: &WorkspaceScope,
        model: &str,
        dims: i64,
    ) -> Result<Vec<(MemoryEntryId, Vec<u8>)>, MemoryLogError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.entry_id, e.vector \
                 FROM memory_embeddings e \
                 JOIN memory_entries m ON m.id = e.entry_id \
                 WHERE m.workspace_scope = ?1 \
                   AND e.model = ?2 \
                   AND e.dims = ?3 \
                   AND m.deleted_at IS NULL \
                 ORDER BY e.entry_id ASC",
            )
            .map_err(MemoryLogError::EmbeddingQuery)?;
        let rows = statement
            .query_map(params![workspace.as_str(), model, dims], |row| {
                let entry_id: i64 = row.get(0)?;
                let vector: Vec<u8> = row.get(1)?;
                Ok((MemoryEntryId::from_raw(entry_id), vector))
            })
            .map_err(MemoryLogError::EmbeddingQuery)?;

        let mut embeddings = Vec::new();
        for row in rows {
            embeddings.push(row.map_err(MemoryLogError::EmbeddingQuery)?);
        }
        Ok(embeddings)
    }
}

/// Raw row values before metadata decoding.
#[derive(Debug)]
struct RawMemoryRecord {
    /// SQLite row id.
    id: i64,
    /// Owning session id.
    session_id: String,
    /// Workspace partition.
    workspace_scope: String,
    /// Raw text content.
    content: String,
    /// Serialized metadata.
    metadata_json: String,
    /// Caller-supplied creation time.
    created_at: i64,
    /// Soft-deletion time.
    deleted_at: Option<i64>,
}

/// Reads one row in [`SELECT_COLUMNS`] order.
fn decode_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawMemoryRecord> {
    Ok(RawMemoryRecord {
        id: row.get(0)?,
        session_id: row.get(1)?,
        workspace_scope: row.get(2)?,
        content: row.get(3)?,
        metadata_json: row.get(4)?,
        created_at: row.get(5)?,
        deleted_at: row.get(6)?,
    })
}

/// Decodes metadata and converts a raw row into a public record.
fn decode_record(raw: RawMemoryRecord) -> Result<MemoryRecord, MemoryLogError> {
    let metadata = serde_json::from_str(&raw.metadata_json)
        .map_err(|source| MemoryLogError::Decode { id: raw.id, source })?;
    let workspace_scope =
        WorkspaceScope::new(raw.workspace_scope).map_err(|_| MemoryLogError::Corrupt {
            id: raw.id,
            message: "workspace scope is empty".to_owned(),
        })?;

    Ok(MemoryRecord {
        id: MemoryEntryId::from_raw(raw.id),
        session_id: SessionId::new(raw.session_id),
        workspace_scope,
        content: raw.content,
        metadata,
        created_at: Timestamp::from_millis(raw.created_at),
        deleted_at: raw.deleted_at.map(Timestamp::from_millis),
    })
}

/// Converts a row limit into the signed integer SQLite expects.
fn limit_to_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

/// Errors raised while opening or accessing a [`MemoryLog`].
#[derive(Debug, Error)]
pub enum MemoryLogError {
    /// The database file could not be opened.
    #[error("failed to open memory database `{path}`: {source}")]
    Open {
        /// Requested database path.
        path: PathBuf,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// The in-memory database could not be created.
    #[error("failed to create an in-memory memory database: {0}")]
    InMemory(#[source] rusqlite::Error),
    /// The schema, FTS5 index, or append-only triggers could not be installed.
    #[error("failed to initialize the memory schema: {0}")]
    Schema(#[source] rusqlite::Error),
    /// The database was written by a newer schema version.
    #[error(
        "memory database schema version {found} is newer than the supported version {supported}"
    )]
    UnsupportedVersion {
        /// Version found in `PRAGMA user_version`.
        found: i64,
        /// Highest version this build supports.
        supported: i64,
    },
    /// The entry content was empty or whitespace-only.
    #[error("memory entry content must not be empty")]
    EmptyContent,
    /// The workspace scope was empty or whitespace-only.
    #[error("workspace scope must not be empty")]
    EmptyWorkspace,
    /// Entry metadata could not be serialized to JSON.
    #[error("failed to serialize memory metadata: {0}")]
    Serialize(#[from] serde_json::Error),
    /// The entry could not be inserted.
    #[error("failed to append a memory entry: {0}")]
    Insert(#[source] rusqlite::Error),
    /// A read query failed.
    #[error("failed to query memory entries: {0}")]
    Query(#[source] rusqlite::Error),
    /// An embedding could not be stored.
    #[error("failed to store an embedding for memory entry {entry_id}: {source}")]
    EmbeddingStore {
        /// Entry the embedding belongs to.
        entry_id: i64,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Embeddings could not be scanned.
    #[error("failed to scan memory embeddings: {0}")]
    EmbeddingQuery(#[source] rusqlite::Error),
    /// Stored metadata is not valid JSON.
    #[error("memory entry {id} contains invalid JSON metadata: {source}")]
    Decode {
        /// Row id of the corrupt entry.
        id: i64,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// A stored row violates an invariant enforced by this layer.
    #[error("memory entry {id} is corrupt: {message}")]
    Corrupt {
        /// Row id of the corrupt entry.
        id: i64,
        /// Description of the violated invariant.
        message: String,
    },
    /// The requested entry does not exist.
    #[error("memory entry {id} does not exist")]
    NotFound {
        /// Requested row id.
        id: i64,
    },
    /// The requested entry was already soft-deleted.
    #[error("memory entry {id} is already deleted")]
    AlreadyDeleted {
        /// Requested row id.
        id: i64,
    },
    /// The soft deletion failed.
    #[error("failed to soft-delete memory entry {id}: {source}")]
    Delete {
        /// Requested row id.
        id: i64,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
}
