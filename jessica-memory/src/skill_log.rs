//! Append-only skill-library storage with FTS5 keyword search.
//!
//! [`SkillLog`] is a synchronous SQLite store for distilled skills. It is kept
//! in a database separate from the memory and event stores, and it is scoped to
//! one workspace at the query boundary. Skill rows are append-only: the only
//! permitted mutations are a one-way soft deletion and a one-way transition
//! from `draft` to `active`. Triggers reject every other `UPDATE` and every
//! `DELETE`.
//!
//! A skill is deduplicated in two independent ways while it is live:
//!
//! - the source tuple `(workspace_scope, source_session, source_turn_index)`
//!   makes distillation idempotent; and
//! - a content hash over `name`, `description`, `instructions`, and the
//!   canonical tag JSON makes the same skill reusable across sessions.
//!
//! New skills are `draft` by default and are invisible to keyword retrieval
//! until [`SkillLog::activate`] is called. Auto-activation is available only at
//! insertion time through [`NewSkill::with_auto_activate`]; a duplicate insert
//! never activates an existing draft.
//!
//! [`SkillLog::search_keyword`] returns only active, live rows. It returns the
//! raw SQLite `bm25()` score alongside each skill; lower values are better.
//! A `min_score` of `None` or `0.0` disables the floor. A positive `min_score`
//! is applied to the relevance `-bm25`, so a hit is kept when
//! `-bm25 >= min_score`. Queries made entirely of short tokens use the escaped
//! `LIKE` fallback and report the deterministic sentinel score `0.0`; those
//! hits therefore do not pass a positive floor.
//!
//! [`SqliteSkillStore`] is the asynchronous adapter. Every rusqlite call made
//! by its asynchronous methods runs inside
//! [`tokio::task::spawn_blocking`], and the single connection is serialized by
//! a mutex, so SQLite never blocks a Tokio worker thread. The synchronous
//! `open` constructors perform schema setup; call them during startup or from a
//! blocking context.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use jessica_core::types::SessionId;
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::Timestamp;
use crate::fts_query::{like_pattern, quote_fts_token, split_query};
use crate::memory_log::WorkspaceScope;
use crate::workspace::{WorkspaceScopeError, workspace_scope_for};

/// Version stored in SQLite's `user_version` pragma.
const SCHEMA_VERSION: i64 = 1;

/// How long SQLite waits for a competing writer before returning `Busy`.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum byte length of a skill name.
const MAX_NAME_BYTES: usize = 128;

/// Maximum byte length of a skill description.
const MAX_DESCRIPTION_BYTES: usize = 512;

/// Maximum byte length of skill instructions.
const MAX_INSTRUCTIONS_BYTES: usize = 8192;

/// Maximum number of canonical tags.
const MAX_TAGS: usize = 16;

/// Maximum byte length of one canonical tag.
const MAX_TAG_BYTES: usize = 64;

/// Score reported for a short-token `LIKE` fallback hit.
///
/// The fallback has no bm25 value. Using a fixed sentinel keeps the returned
/// score deterministic and makes the positive `min_score` floor reject these
/// lower-confidence hits consistently.
const LIKE_SCORE: f64 = 0.0;

/// Tables, indexes, FTS5 index, and append-only guards installed on every open.
///
/// `skills` is the source of truth. `skills_fts` is an external-content FTS5
/// table kept in sync by the triggers below. The update trigger removes the old
/// FTS row and re-inserts it only while the skill is live, so soft-deleted rows
/// disappear from keyword search without rewriting the source row's content.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS skills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_scope TEXT NOT NULL,
    source_session TEXT NOT NULL,
    source_turn_index INTEGER NOT NULL,
    source_tool_calls INTEGER NOT NULL,
    source_rounds INTEGER NOT NULL,
    source_acceptance_failures INTEGER NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    instructions TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('draft','active')),
    created_at INTEGER NOT NULL,
    deleted_at INTEGER NULL,
    activated_at INTEGER NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS skills_unique_live_source
ON skills (workspace_scope, source_session, source_turn_index)
WHERE deleted_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS skills_unique_live_content
ON skills (workspace_scope, content_hash)
WHERE deleted_at IS NULL;

CREATE VIRTUAL TABLE IF NOT EXISTS skills_fts USING fts5(
    name,
    description,
    instructions,
    tags_json,
    content='skills',
    content_rowid='id',
    tokenize='trigram'
);

CREATE TRIGGER IF NOT EXISTS skills_append_only_update
BEFORE UPDATE ON skills
WHEN NOT (
    (
        OLD.deleted_at IS NULL
        AND NEW.deleted_at IS NOT NULL
        AND NEW.id IS OLD.id
        AND NEW.workspace_scope IS OLD.workspace_scope
        AND NEW.source_session IS OLD.source_session
        AND NEW.source_turn_index IS OLD.source_turn_index
        AND NEW.source_tool_calls IS OLD.source_tool_calls
        AND NEW.source_rounds IS OLD.source_rounds
        AND NEW.source_acceptance_failures IS OLD.source_acceptance_failures
        AND NEW.name IS OLD.name
        AND NEW.description IS OLD.description
        AND NEW.instructions IS OLD.instructions
        AND NEW.tags_json IS OLD.tags_json
        AND NEW.content_hash IS OLD.content_hash
        AND NEW.status IS OLD.status
        AND NEW.created_at IS OLD.created_at
        AND NEW.activated_at IS OLD.activated_at
    )
    OR
    (
        OLD.status = 'draft'
        AND NEW.status = 'active'
        AND OLD.deleted_at IS NULL
        AND NEW.deleted_at IS NULL
        AND OLD.activated_at IS NULL
        AND NEW.activated_at IS NOT NULL
        AND NEW.id IS OLD.id
        AND NEW.workspace_scope IS OLD.workspace_scope
        AND NEW.source_session IS OLD.source_session
        AND NEW.source_turn_index IS OLD.source_turn_index
        AND NEW.source_tool_calls IS OLD.source_tool_calls
        AND NEW.source_rounds IS OLD.source_rounds
        AND NEW.source_acceptance_failures IS OLD.source_acceptance_failures
        AND NEW.name IS OLD.name
        AND NEW.description IS OLD.description
        AND NEW.instructions IS OLD.instructions
        AND NEW.tags_json IS OLD.tags_json
        AND NEW.content_hash IS OLD.content_hash
        AND NEW.created_at IS OLD.created_at
    )
)
BEGIN
    SELECT RAISE(ABORT, 'skills are append-only: only soft deletion or draft activation is allowed');
END;

CREATE TRIGGER IF NOT EXISTS skills_immutable_delete
BEFORE DELETE ON skills
BEGIN
    SELECT RAISE(ABORT, 'skills are append-only: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS skills_fts_insert
AFTER INSERT ON skills
BEGIN
    INSERT INTO skills_fts (rowid, name, description, instructions, tags_json)
    VALUES (NEW.id, NEW.name, NEW.description, NEW.instructions, NEW.tags_json);
END;

CREATE TRIGGER IF NOT EXISTS skills_fts_update
AFTER UPDATE ON skills
BEGIN
    INSERT INTO skills_fts (skills_fts, rowid, name, description, instructions, tags_json)
    VALUES ('delete', OLD.id, OLD.name, OLD.description, OLD.instructions, OLD.tags_json);
    INSERT INTO skills_fts (rowid, name, description, instructions, tags_json)
    SELECT NEW.id, NEW.name, NEW.description, NEW.instructions, NEW.tags_json
    WHERE NEW.deleted_at IS NULL;
END;

CREATE TRIGGER IF NOT EXISTS skills_fts_delete
AFTER DELETE ON skills
BEGIN
    INSERT INTO skills_fts (skills_fts, rowid, name, description, instructions, tags_json)
    VALUES ('delete', OLD.id, OLD.name, OLD.description, OLD.instructions, OLD.tags_json);
END;
"#;

/// Columns selected for one [`Skill`], in [`RawSkill`] order.
const SELECT_COLUMNS: &str = "m.id, m.workspace_scope, m.source_session, \
                              m.source_turn_index, m.source_tool_calls, m.source_rounds, \
                              m.source_acceptance_failures, m.name, m.description, \
                              m.instructions, m.tags_json, m.content_hash, m.status, \
                              m.created_at, m.deleted_at, m.activated_at";

/// Identifier of one stored skill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SkillId(i64);

impl SkillId {
    /// Wraps a raw SQLite row id.
    ///
    /// Callers normally receive identifiers from [`SkillLog::append`]; this
    /// constructor exists for storage adapters and tests that read raw rows.
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// Returns the raw SQLite row id.
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for SkillId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Lifecycle state of a stored skill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkillStatus {
    /// Stored but not eligible for retrieval.
    Draft,
    /// Eligible for keyword retrieval.
    Active,
}

impl SkillStatus {
    /// Returns the value stored in the `status` column.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
        }
    }

    /// Decodes a stored status value.
    fn from_db(value: &str, id: i64) -> Result<Self, SkillLogError> {
        match value {
            "draft" => Ok(Self::Draft),
            "active" => Ok(Self::Active),
            other => Err(SkillLogError::Corrupt {
                id,
                message: format!("unknown skill status `{other}`"),
            }),
        }
    }
}

impl fmt::Display for SkillStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Structured provenance for one distilled skill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillSource {
    /// Session in which the source turn ran.
    pub session_id: SessionId,
    /// Stable turn index within the session.
    pub turn_index: u64,
    /// Number of tool calls observed in the turn.
    pub tool_calls: u64,
    /// Number of assistant rounds observed in the turn.
    pub rounds: u64,
    /// Number of failed acceptance attempts observed in the turn.
    pub acceptance_failures: u64,
}

impl SkillSource {
    /// Creates provenance for one distilled turn.
    pub fn new(
        session_id: SessionId,
        turn_index: u64,
        tool_calls: u64,
        rounds: u64,
        acceptance_failures: u64,
    ) -> Self {
        Self {
            session_id,
            turn_index,
            tool_calls,
            rounds,
            acceptance_failures,
        }
    }
}

/// A skill waiting to be appended.
///
/// Construction validates all text limits, canonicalizes tags, and computes the
/// content hash. The timestamp is supplied by the caller so the storage layer
/// never reads the host clock.
#[derive(Debug, Clone)]
pub struct NewSkill {
    /// Workspace partition that owns the skill.
    workspace_scope: WorkspaceScope,
    /// Structured provenance.
    source: SkillSource,
    /// Human-readable name.
    name: String,
    /// Short description used by retrieval and display.
    description: String,
    /// Full skill instructions.
    instructions: String,
    /// Canonical JSON representation of the sorted tags.
    tags_json: String,
    /// Content hash over all content fields.
    content_hash: String,
    /// Caller-supplied creation time.
    created_at: Timestamp,
    /// Whether the inserted row starts active.
    auto_activate: bool,
}

impl NewSkill {
    /// Creates a draft skill and validates every bounded field.
    ///
    /// `tags` are trimmed, sorted, and de-duplicated before the content hash is
    /// computed. Empty names, descriptions, or instructions are rejected.
    /// Returns a structured [`SkillLogError`] when a byte limit is exceeded;
    /// values are never silently truncated.
    pub fn new(
        workspace_scope: WorkspaceScope,
        source: SkillSource,
        name: impl Into<String>,
        description: impl Into<String>,
        instructions: impl Into<String>,
        tags: Vec<String>,
        created_at: Timestamp,
    ) -> Result<Self, SkillLogError> {
        let name = name.into();
        let description = description.into();
        let instructions = instructions.into();

        validate_text_field("name", &name, MAX_NAME_BYTES)?;
        validate_text_field("description", &description, MAX_DESCRIPTION_BYTES)?;
        validate_text_field("instructions", &instructions, MAX_INSTRUCTIONS_BYTES)?;

        let tags = canonicalize_tags(tags)?;
        let tags_json = serde_json::to_string(&tags)?;
        let content_hash = compute_content_hash(&name, &description, &instructions, &tags_json);

        Ok(Self {
            workspace_scope,
            source,
            name,
            description,
            instructions,
            tags_json,
            content_hash,
            created_at,
            auto_activate: false,
        })
    }

    /// Inserts the skill directly as `active` when `auto_activate` is `true`.
    ///
    /// The default is `false`. Auto-activation never applies to a duplicate
    /// insert: an existing draft remains a draft.
    pub fn with_auto_activate(mut self, auto_activate: bool) -> Self {
        self.auto_activate = auto_activate;
        self
    }

    /// Returns the fixed workspace scope for this skill.
    pub(crate) fn workspace_scope(&self) -> &WorkspaceScope {
        &self.workspace_scope
    }
}

/// A skill read back from storage.
#[derive(Debug, Clone, PartialEq)]
pub struct Skill {
    /// SQLite row id.
    id: SkillId,
    /// Workspace partition.
    workspace_scope: WorkspaceScope,
    /// Structured provenance.
    source: SkillSource,
    /// Human-readable name.
    name: String,
    /// Short description.
    description: String,
    /// Full instructions.
    instructions: String,
    /// Canonical tags.
    tags: Vec<String>,
    /// Content hash over the content fields.
    content_hash: String,
    /// Lifecycle state.
    status: SkillStatus,
    /// Caller-supplied creation time.
    created_at: Timestamp,
    /// Soft-deletion time, or `None` while live.
    deleted_at: Option<Timestamp>,
    /// Activation time, or `None` while still a draft.
    activated_at: Option<Timestamp>,
}

impl Skill {
    /// Returns the skill identifier.
    pub const fn id(&self) -> SkillId {
        self.id
    }

    /// Returns the owning workspace scope.
    pub fn workspace_scope(&self) -> &WorkspaceScope {
        &self.workspace_scope
    }

    /// Returns the structured provenance.
    pub fn source(&self) -> &SkillSource {
        &self.source
    }

    /// Returns the human-readable name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the short description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the full instructions.
    pub fn instructions(&self) -> &str {
        &self.instructions
    }

    /// Returns the canonical tags.
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// Returns the content hash.
    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    /// Returns the lifecycle state.
    pub const fn status(&self) -> SkillStatus {
        self.status
    }

    /// Returns the caller-supplied creation time.
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Returns the soft-deletion time, if the skill was deleted.
    pub const fn deleted_at(&self) -> Option<Timestamp> {
        self.deleted_at
    }

    /// Returns the activation time, if the skill is active.
    pub const fn activated_at(&self) -> Option<Timestamp> {
        self.activated_at
    }
}

/// Append-only skill storage backed by SQLite.
///
/// Opening a database installs the schema, FTS5 index, and append-only guards
/// if they are absent. Reads never modify the log. The only mutations are
/// [`SkillLog::soft_delete`] and [`SkillLog::activate`].
#[derive(Debug)]
pub struct SkillLog {
    /// SQLite connection used for every operation on this log.
    connection: Connection,
}

impl SkillLog {
    /// Opens or creates a skill database at `path`.
    ///
    /// Missing parent directories are not created. The schema is installed if
    /// absent, and a `user_version` newer than this build supports is rejected.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SkillLogError> {
        let path = path.as_ref();
        let connection = Connection::open(path).map_err(|source| SkillLogError::Open {
            path: path.to_path_buf(),
            source,
        })?;
        Self::initialize(connection)
    }

    /// Opens a private in-memory database, primarily for tests and short-lived
    /// tools.
    pub fn open_in_memory() -> Result<Self, SkillLogError> {
        let connection = Connection::open_in_memory().map_err(SkillLogError::InMemory)?;
        Self::initialize(connection)
    }

    /// Installs the schema and verifies the schema version.
    fn initialize(connection: Connection) -> Result<Self, SkillLogError> {
        connection
            .busy_timeout(BUSY_TIMEOUT)
            .map_err(SkillLogError::Schema)?;

        let found: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(SkillLogError::Schema)?;
        if found > SCHEMA_VERSION {
            return Err(SkillLogError::UnsupportedVersion {
                found,
                supported: SCHEMA_VERSION,
            });
        }

        connection
            .execute_batch(SCHEMA)
            .map_err(SkillLogError::Schema)?;
        if found < SCHEMA_VERSION {
            connection
                .execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))
                .map_err(SkillLogError::Schema)?;
        }

        Ok(Self { connection })
    }

    /// Appends `skill`, returning its identifier.
    ///
    /// A live row with the same source tuple or the same content hash is not
    /// overwritten. In that case [`SkillLogError::Duplicate`] carries the id of
    /// the existing row, whose source and status remain unchanged. Soft-deleted
    /// rows do not block an append.
    pub fn append(&self, skill: NewSkill) -> Result<SkillId, SkillLogError> {
        let turn_index = to_sql_integer("source_turn_index", skill.source.turn_index)?;
        let tool_calls = to_sql_integer("source_tool_calls", skill.source.tool_calls)?;
        let rounds = to_sql_integer("source_rounds", skill.source.rounds)?;
        let acceptance_failures = to_sql_integer(
            "source_acceptance_failures",
            skill.source.acceptance_failures,
        )?;
        let status = if skill.auto_activate {
            SkillStatus::Active
        } else {
            SkillStatus::Draft
        };
        let activated_at = if skill.auto_activate {
            Some(skill.created_at.as_millis())
        } else {
            None
        };

        let inserted = self
            .connection
            .execute(
                "INSERT OR IGNORE INTO skills \
                 (workspace_scope, source_session, source_turn_index, source_tool_calls, \
                  source_rounds, source_acceptance_failures, name, description, instructions, \
                  tags_json, content_hash, status, created_at, activated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    skill.workspace_scope.as_str(),
                    skill.source.session_id.as_str(),
                    turn_index,
                    tool_calls,
                    rounds,
                    acceptance_failures,
                    skill.name,
                    skill.description,
                    skill.instructions,
                    skill.tags_json,
                    skill.content_hash,
                    status.as_str(),
                    skill.created_at.as_millis(),
                    activated_at,
                ],
            )
            .map_err(SkillLogError::Insert)?;

        if inserted == 0 {
            if let Some(id) = self.live_id_by_source(&skill)? {
                return Err(SkillLogError::Duplicate {
                    id: SkillId::from_raw(id),
                });
            }
            if let Some(id) = self.live_id_by_content(&skill)? {
                return Err(SkillLogError::Duplicate {
                    id: SkillId::from_raw(id),
                });
            }
            return Err(SkillLogError::DuplicateLookup);
        }

        Ok(SkillId::from_raw(self.connection.last_insert_rowid()))
    }

    /// Returns the skill with `id`, including soft-deleted skills.
    ///
    /// Returns `Ok(None)` when no row has that identifier.
    pub fn get(&self, id: SkillId) -> Result<Option<Skill>, SkillLogError> {
        let raw = self
            .connection
            .query_row(
                &format!("SELECT {SELECT_COLUMNS} FROM skills m WHERE m.id = ?1"),
                [id.get()],
                decode_raw,
            )
            .optional()
            .map_err(SkillLogError::Query)?;
        raw.map(decode_skill).transpose()
    }

    /// Returns up to `limit` live skills in `workspace`, newest first.
    ///
    /// Both drafts and active skills are returned. Ties on `created_at` are
    /// broken by descending row id so the result is deterministic.
    pub fn list_recent(
        &self,
        workspace: &WorkspaceScope,
        limit: usize,
    ) -> Result<Vec<Skill>, SkillLogError> {
        self.list(
            "m.workspace_scope = ?1 AND m.deleted_at IS NULL",
            &[SqlValue::Text(workspace.as_str().to_owned())],
            limit,
        )
    }

    /// Returns up to `limit` live skills with `status` in `workspace`, newest
    /// first.
    pub fn list_by_status(
        &self,
        workspace: &WorkspaceScope,
        status: SkillStatus,
        limit: usize,
    ) -> Result<Vec<Skill>, SkillLogError> {
        self.list(
            "m.workspace_scope = ?1 AND m.status = ?2 AND m.deleted_at IS NULL",
            &[
                SqlValue::Text(workspace.as_str().to_owned()),
                SqlValue::Text(status.as_str().to_owned()),
            ],
            limit,
        )
    }

    /// Runs a list query with the supplied `WHERE` clause.
    fn list(
        &self,
        where_clause: &str,
        values: &[SqlValue],
        limit: usize,
    ) -> Result<Vec<Skill>, SkillLogError> {
        let mut parameters = values.to_vec();
        let limit_parameter = parameters.len() + 1;
        parameters.push(SqlValue::Integer(limit_to_i64(limit)));
        let mut statement = self
            .connection
            .prepare(&format!(
                "SELECT {SELECT_COLUMNS} FROM skills m WHERE {where_clause} \
                 ORDER BY m.created_at DESC, m.id DESC LIMIT ?{limit_parameter}"
            ))
            .map_err(SkillLogError::Query)?;
        let rows = statement
            .query_map(params_from_iter(parameters.iter()), decode_raw)
            .map_err(SkillLogError::Query)?;

        let mut skills = Vec::new();
        for row in rows {
            skills.push(decode_skill(row.map_err(SkillLogError::Query)?)?);
        }
        Ok(skills)
    }

    /// Searches active, live skills in `workspace` for `query`.
    ///
    /// Tokens of at least three characters use the FTS5 trigram index; shorter
    /// tokens use an escaped `LIKE` filter across all text columns. Every token
    /// must match. FTS results are ordered by ascending raw `bm25()` score and
    /// then ascending row id; short-token-only results use the deterministic
    /// sentinel [`LIKE_SCORE`] and are ordered newest first.
    ///
    /// The returned score is the raw SQLite bm25 value (lower is better).
    /// `min_score` semantics are documented on the module.
    ///
    /// User input is never interpolated into the `MATCH` expression: it is
    /// tokenized, then each token is quoted. Queries with no alphanumeric
    /// tokens return an empty result instead of an error.
    pub fn search_keyword(
        &self,
        workspace: &WorkspaceScope,
        query: &str,
        limit: usize,
        min_score: Option<f64>,
    ) -> Result<Vec<(Skill, f64)>, SkillLogError> {
        let threshold = normalize_min_score(min_score)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let tokens = split_query(query);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let mut values = Vec::new();
        let mut statement = String::new();
        let mut next_parameter;
        let order_by;

        if tokens.long.is_empty() {
            statement.push_str(&format!(
                "SELECT {SELECT_COLUMNS}, ?1 AS score FROM skills m \
                 WHERE m.workspace_scope = ?2 AND m.status = 'active' AND m.deleted_at IS NULL"
            ));
            values.push(SqlValue::Real(LIKE_SCORE));
            values.push(SqlValue::Text(workspace.as_str().to_owned()));
            next_parameter = 3;
            order_by = "m.created_at DESC, m.id DESC";
        } else {
            let match_expression = tokens
                .long
                .iter()
                .map(|token| quote_fts_token(token))
                .collect::<Vec<_>>()
                .join(" AND ");
            statement.push_str(&format!(
                "SELECT {SELECT_COLUMNS}, bm25(skills_fts) AS score FROM skills_fts \
                 JOIN skills m ON m.id = skills_fts.rowid \
                 WHERE skills_fts MATCH ?1 AND m.workspace_scope = ?2 \
                   AND m.status = 'active' AND m.deleted_at IS NULL"
            ));
            values.push(SqlValue::Text(match_expression));
            values.push(SqlValue::Text(workspace.as_str().to_owned()));
            next_parameter = 3;
            order_by = "score ASC, m.id ASC";
        }

        for token in tokens.short {
            statement.push_str(&format!(
                " AND (m.name LIKE ?{next_parameter} ESCAPE '\\' \
                   OR m.description LIKE ?{next_parameter} ESCAPE '\\' \
                   OR m.instructions LIKE ?{next_parameter} ESCAPE '\\' \
                   OR m.tags_json LIKE ?{next_parameter} ESCAPE '\\')"
            ));
            values.push(SqlValue::Text(like_pattern(&token)));
            next_parameter += 1;
        }

        statement.push_str(&format!(" ORDER BY {order_by} LIMIT ?{next_parameter}"));
        values.push(SqlValue::Integer(limit_to_i64(limit)));

        let mut prepared = self
            .connection
            .prepare(&statement)
            .map_err(SkillLogError::Query)?;
        let rows = prepared
            .query_map(params_from_iter(values.iter()), decode_raw_with_score)
            .map_err(SkillLogError::Query)?;

        let mut hits = Vec::new();
        for row in rows {
            let (raw, score) = row.map_err(SkillLogError::Query)?;
            if let Some(threshold) = threshold
                && -score < threshold
            {
                continue;
            }
            hits.push((decode_skill(raw)?, score));
        }
        Ok(hits)
    }

    /// Soft-deletes the skill with `id` at `deleted_at`.
    ///
    /// The row and its audit trail are kept; retrieval paths exclude it. A
    /// second soft delete of the same row is rejected, as is a missing
    /// identifier.
    pub fn soft_delete(&mut self, id: SkillId, deleted_at: Timestamp) -> Result<(), SkillLogError> {
        let raw_id = id.get();
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| SkillLogError::Delete { id: raw_id, source })?;

        let state: Option<Option<i64>> = transaction
            .query_row(
                "SELECT deleted_at FROM skills WHERE id = ?1",
                [raw_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| SkillLogError::Delete { id: raw_id, source })?;

        match state {
            None => return Err(SkillLogError::NotFound { id: raw_id }),
            Some(Some(_)) => return Err(SkillLogError::AlreadyDeleted { id: raw_id }),
            Some(None) => {}
        }

        transaction
            .execute(
                "UPDATE skills SET deleted_at = ?1 WHERE id = ?2",
                params![deleted_at.as_millis(), raw_id],
            )
            .map_err(|source| SkillLogError::Delete { id: raw_id, source })?;
        transaction
            .commit()
            .map_err(|source| SkillLogError::Delete { id: raw_id, source })?;

        Ok(())
    }

    /// Activates a draft skill at `activated_at`.
    ///
    /// Activating an already active skill is idempotent: it returns `Ok(())`
    /// and leaves the original `activated_at` unchanged. A missing or
    /// soft-deleted skill is rejected.
    pub fn activate(&mut self, id: SkillId, activated_at: Timestamp) -> Result<(), SkillLogError> {
        let raw_id = id.get();
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| SkillLogError::Activate { id: raw_id, source })?;

        let state: Option<(String, Option<i64>)> = transaction
            .query_row(
                "SELECT status, deleted_at FROM skills WHERE id = ?1",
                [raw_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|source| SkillLogError::Activate { id: raw_id, source })?;

        let status = match state {
            None => return Err(SkillLogError::NotFound { id: raw_id }),
            Some((_, Some(_))) => return Err(SkillLogError::AlreadyDeleted { id: raw_id }),
            Some((status, None)) => SkillStatus::from_db(&status, raw_id)?,
        };

        if status == SkillStatus::Active {
            return Ok(());
        }

        let updated = transaction
            .execute(
                "UPDATE skills SET status = 'active', activated_at = ?1 \
                 WHERE id = ?2 AND status = 'draft' AND deleted_at IS NULL",
                params![activated_at.as_millis(), raw_id],
            )
            .map_err(|source| SkillLogError::Activate { id: raw_id, source })?;
        if updated == 0 {
            return Err(SkillLogError::NotFound { id: raw_id });
        }
        transaction
            .commit()
            .map_err(|source| SkillLogError::Activate { id: raw_id, source })?;

        Ok(())
    }

    /// Returns the live skill distilled from `session` and `turn_index` in
    /// `workspace`.
    ///
    /// Soft-deleted rows are ignored, which matches the partial unique index
    /// used for append idempotency.
    pub fn find_by_source(
        &self,
        workspace: &WorkspaceScope,
        session: &SessionId,
        turn_index: u64,
    ) -> Result<Option<Skill>, SkillLogError> {
        let turn_index = to_sql_integer("source_turn_index", turn_index)?;
        let raw = self
            .connection
            .query_row(
                &format!(
                    "SELECT {SELECT_COLUMNS} FROM skills m \
                     WHERE m.workspace_scope = ?1 AND m.source_session = ?2 \
                       AND m.source_turn_index = ?3 AND m.deleted_at IS NULL \
                     ORDER BY m.id ASC LIMIT 1"
                ),
                params![workspace.as_str(), session.as_str(), turn_index],
                decode_raw,
            )
            .optional()
            .map_err(SkillLogError::Query)?;
        raw.map(decode_skill).transpose()
    }

    /// Returns the live row id matching a skill's source tuple.
    fn live_id_by_source(&self, skill: &NewSkill) -> Result<Option<i64>, SkillLogError> {
        let turn_index = to_sql_integer("source_turn_index", skill.source.turn_index)?;
        self.connection
            .query_row(
                "SELECT id FROM skills \
                 WHERE workspace_scope = ?1 AND source_session = ?2 \
                   AND source_turn_index = ?3 AND deleted_at IS NULL",
                params![
                    skill.workspace_scope.as_str(),
                    skill.source.session_id.as_str(),
                    turn_index
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(SkillLogError::Query)
    }

    /// Returns the live row id matching a skill's content hash.
    fn live_id_by_content(&self, skill: &NewSkill) -> Result<Option<i64>, SkillLogError> {
        self.connection
            .query_row(
                "SELECT id FROM skills \
                 WHERE workspace_scope = ?1 AND content_hash = ?2 AND deleted_at IS NULL",
                params![skill.workspace_scope.as_str(), skill.content_hash.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(SkillLogError::Query)
    }
}

/// Asynchronous adapter over [`SkillLog`].
///
/// The workspace scope is fixed at construction time. Every operation runs on
/// [`tokio::task::spawn_blocking`], and the single SQLite connection is
/// serialized by a mutex.
#[derive(Clone, Debug)]
pub struct SqliteSkillStore {
    /// Shared synchronous log.
    log: Arc<Mutex<SkillLog>>,
    /// Fixed workspace partition for this store.
    workspace_scope: WorkspaceScope,
}

impl SqliteSkillStore {
    /// Opens or creates a skill database at `database_path` and binds it to
    /// `canonical_workspace`.
    ///
    /// The caller must supply an already-canonicalized workspace path. Missing
    /// parent directories are not created. This constructor performs
    /// synchronous SQLite initialization; call it during startup or from a
    /// blocking context rather than from an async hot path.
    pub fn open(
        database_path: impl AsRef<Path>,
        canonical_workspace: impl AsRef<Path>,
    ) -> Result<Self, SqliteSkillStoreError> {
        let log = SkillLog::open(database_path)?;
        Self::with_log(log, canonical_workspace.as_ref())
    }

    /// Opens a private in-memory skill database with the same workspace
    /// semantics as [`SqliteSkillStore::open`].
    pub fn open_in_memory(
        canonical_workspace: impl AsRef<Path>,
    ) -> Result<Self, SqliteSkillStoreError> {
        let log = SkillLog::open_in_memory()?;
        Self::with_log(log, canonical_workspace.as_ref())
    }

    /// Wraps an already-open log and derives its fixed workspace scope.
    fn with_log(log: SkillLog, canonical_workspace: &Path) -> Result<Self, SqliteSkillStoreError> {
        let workspace_scope =
            workspace_scope_for(canonical_workspace).map_err(|error| match error {
                WorkspaceScopeError::EmptyPath => SqliteSkillStoreError::EmptyWorkspacePath,
                WorkspaceScopeError::InvalidScope => SqliteSkillStoreError::WorkspaceScope,
            })?;
        Ok(Self {
            log: Arc::new(Mutex::new(log)),
            workspace_scope,
        })
    }

    /// Returns the fixed workspace scope used by this store.
    pub fn workspace_scope(&self) -> &WorkspaceScope {
        &self.workspace_scope
    }

    /// Appends `skill` if it belongs to this store's workspace.
    ///
    /// A skill built for a different workspace is rejected with
    /// [`SkillLogError::WorkspaceMismatch`]. Duplicate handling is the same as
    /// [`SkillLog::append`].
    pub async fn append(&self, skill: NewSkill) -> Result<SkillId, SkillLogError> {
        if skill.workspace_scope() != &self.workspace_scope {
            return Err(SkillLogError::WorkspaceMismatch);
        }
        let log = Arc::clone(&self.log);
        run_blocking(log, move |log| log.append(skill)).await
    }

    /// Returns the skill with `id` if it belongs to this store's workspace.
    ///
    /// Skills in another workspace are reported as absent without revealing
    /// their existence.
    pub async fn get(&self, id: SkillId) -> Result<Option<Skill>, SkillLogError> {
        let workspace = self.workspace_scope.clone();
        let log = Arc::clone(&self.log);
        run_blocking(log, move |log| {
            Ok(log
                .get(id)?
                .filter(|skill| skill.workspace_scope() == &workspace))
        })
        .await
    }

    /// Returns up to `limit` live skills in this workspace, newest first.
    pub async fn list_recent(&self, limit: usize) -> Result<Vec<Skill>, SkillLogError> {
        let workspace = self.workspace_scope.clone();
        let log = Arc::clone(&self.log);
        run_blocking(log, move |log| log.list_recent(&workspace, limit)).await
    }

    /// Returns up to `limit` live skills with `status` in this workspace.
    pub async fn list_by_status(
        &self,
        status: SkillStatus,
        limit: usize,
    ) -> Result<Vec<Skill>, SkillLogError> {
        let workspace = self.workspace_scope.clone();
        let log = Arc::clone(&self.log);
        run_blocking(log, move |log| {
            log.list_by_status(&workspace, status, limit)
        })
        .await
    }

    /// Searches active, live skills in this workspace.
    ///
    /// The score and `min_score` semantics match [`SkillLog::search_keyword`].
    pub async fn search_keyword(
        &self,
        query: impl Into<String>,
        limit: usize,
        min_score: Option<f64>,
    ) -> Result<Vec<(Skill, f64)>, SkillLogError> {
        let workspace = self.workspace_scope.clone();
        let query = query.into();
        let log = Arc::clone(&self.log);
        run_blocking(log, move |log| {
            log.search_keyword(&workspace, &query, limit, min_score)
        })
        .await
    }

    /// Soft-deletes the skill with `id` if it belongs to this workspace.
    pub async fn soft_delete(
        &self,
        id: SkillId,
        deleted_at: Timestamp,
    ) -> Result<(), SkillLogError> {
        let workspace = self.workspace_scope.clone();
        let log = Arc::clone(&self.log);
        run_blocking(log, move |log| {
            if !belongs_to_workspace(log, id, &workspace)? {
                return Err(SkillLogError::NotFound { id: id.get() });
            }
            log.soft_delete(id, deleted_at)
        })
        .await
    }

    /// Activates the draft with `id` if it belongs to this workspace.
    ///
    /// Repeated activation is idempotent, matching [`SkillLog::activate`].
    pub async fn activate(
        &self,
        id: SkillId,
        activated_at: Timestamp,
    ) -> Result<(), SkillLogError> {
        let workspace = self.workspace_scope.clone();
        let log = Arc::clone(&self.log);
        run_blocking(log, move |log| {
            if !belongs_to_workspace(log, id, &workspace)? {
                return Err(SkillLogError::NotFound { id: id.get() });
            }
            log.activate(id, activated_at)
        })
        .await
    }

    /// Returns the live skill distilled from `session` and `turn_index` in this
    /// workspace.
    pub async fn find_by_source(
        &self,
        session: &SessionId,
        turn_index: u64,
    ) -> Result<Option<Skill>, SkillLogError> {
        let workspace = self.workspace_scope.clone();
        let session = session.clone();
        let log = Arc::clone(&self.log);
        run_blocking(log, move |log| {
            log.find_by_source(&workspace, &session, turn_index)
        })
        .await
    }
}

/// Returns whether `id` names a skill in `workspace`.
fn belongs_to_workspace(
    log: &SkillLog,
    id: SkillId,
    workspace: &WorkspaceScope,
) -> Result<bool, SkillLogError> {
    Ok(log
        .get(id)?
        .is_some_and(|skill| skill.workspace_scope() == workspace))
}

/// Runs one synchronous operation on the blocking pool.
async fn run_blocking<T, F>(log: Arc<Mutex<SkillLog>>, operation: F) -> Result<T, SkillLogError>
where
    T: Send + 'static,
    F: FnOnce(&mut SkillLog) -> Result<T, SkillLogError> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut log = lock_log(&log)?;
        operation(&mut log)
    })
    .await
    .map_err(|source| SkillLogError::TaskJoin {
        message: source.to_string(),
    })?
}

/// Maps a poisoned connection mutex to a structured error.
fn lock_log(log: &Mutex<SkillLog>) -> Result<MutexGuard<'_, SkillLog>, SkillLogError> {
    log.lock().map_err(|_| SkillLogError::ConnectionPoisoned)
}

/// Validates one text field and rejects empty or oversized values.
fn validate_text_field(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), SkillLogError> {
    if value.trim().is_empty() {
        return Err(SkillLogError::EmptyField { field });
    }
    if value.len() > max_bytes {
        return Err(SkillLogError::FieldTooLong {
            field,
            max_bytes,
            actual: value.len(),
        });
    }
    Ok(())
}

/// Trims, sorts, and de-duplicates tags, then enforces the per-tag and count
/// limits.
fn canonicalize_tags(tags: Vec<String>) -> Result<Vec<String>, SkillLogError> {
    let mut canonical = Vec::with_capacity(tags.len());
    for (index, tag) in tags.into_iter().enumerate() {
        let tag = tag.trim().to_owned();
        if tag.is_empty() {
            return Err(SkillLogError::EmptyTag { index });
        }
        if tag.len() > MAX_TAG_BYTES {
            return Err(SkillLogError::TagTooLong {
                index,
                max_bytes: MAX_TAG_BYTES,
                actual: tag.len(),
            });
        }
        canonical.push(tag);
    }
    canonical.sort();
    canonical.dedup();
    if canonical.len() > MAX_TAGS {
        return Err(SkillLogError::TooManyTags {
            max: MAX_TAGS,
            actual: canonical.len(),
        });
    }
    Ok(canonical)
}

/// Computes `sha256(name || '\0' || description || '\0' || instructions ||
/// '\0' || canonical_tags_json)`.
fn compute_content_hash(
    name: &str,
    description: &str,
    instructions: &str,
    tags_json: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.update([0]);
    hasher.update(description.as_bytes());
    hasher.update([0]);
    hasher.update(instructions.as_bytes());
    hasher.update([0]);
    hasher.update(tags_json.as_bytes());
    hex_lower(&hasher.finalize())
}

/// Encodes bytes as lowercase hexadecimal.
fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Converts a non-negative count into the signed integer SQLite expects.
fn to_sql_integer(field: &'static str, value: u64) -> Result<i64, SkillLogError> {
    i64::try_from(value).map_err(|_| SkillLogError::ValueOutOfRange { field, value })
}

/// Converts a row limit into the signed integer SQLite expects.
fn limit_to_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

/// Normalizes the optional relevance floor.
///
/// `None` and `0.0` disable the floor. A positive finite value is applied to
/// `-bm25`; negative, non-finite, and `NaN` values are rejected.
fn normalize_min_score(min_score: Option<f64>) -> Result<Option<f64>, SkillLogError> {
    let Some(value) = min_score else {
        return Ok(None);
    };
    if value == 0.0 {
        return Ok(None);
    }
    if !value.is_finite() || value < 0.0 {
        return Err(SkillLogError::InvalidMinScore { value });
    }
    Ok(Some(value))
}

/// Raw row values before JSON and status decoding.
#[derive(Debug)]
struct RawSkill {
    /// SQLite row id.
    id: i64,
    /// Workspace partition.
    workspace_scope: String,
    /// Source session id.
    source_session: String,
    /// Source turn index.
    source_turn_index: i64,
    /// Source tool-call count.
    source_tool_calls: i64,
    /// Source assistant-round count.
    source_rounds: i64,
    /// Source acceptance-failure count.
    source_acceptance_failures: i64,
    /// Human-readable name.
    name: String,
    /// Short description.
    description: String,
    /// Full instructions.
    instructions: String,
    /// Serialized canonical tags.
    tags_json: String,
    /// Content hash.
    content_hash: String,
    /// Lifecycle state.
    status: String,
    /// Caller-supplied creation time.
    created_at: i64,
    /// Soft-deletion time.
    deleted_at: Option<i64>,
    /// Activation time.
    activated_at: Option<i64>,
}

/// Reads one row in [`SELECT_COLUMNS`] order.
fn decode_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawSkill> {
    Ok(RawSkill {
        id: row.get(0)?,
        workspace_scope: row.get(1)?,
        source_session: row.get(2)?,
        source_turn_index: row.get(3)?,
        source_tool_calls: row.get(4)?,
        source_rounds: row.get(5)?,
        source_acceptance_failures: row.get(6)?,
        name: row.get(7)?,
        description: row.get(8)?,
        instructions: row.get(9)?,
        tags_json: row.get(10)?,
        content_hash: row.get(11)?,
        status: row.get(12)?,
        created_at: row.get(13)?,
        deleted_at: row.get(14)?,
        activated_at: row.get(15)?,
    })
}

/// Reads a raw row plus the score selected immediately after it.
fn decode_raw_with_score(row: &rusqlite::Row<'_>) -> rusqlite::Result<(RawSkill, f64)> {
    let raw = decode_raw(row)?;
    let score = row.get(16)?;
    Ok((raw, score))
}

/// Decodes tags, status, and counts into a public [`Skill`].
fn decode_skill(raw: RawSkill) -> Result<Skill, SkillLogError> {
    let tags = serde_json::from_str(&raw.tags_json)
        .map_err(|source| SkillLogError::Decode { id: raw.id, source })?;
    let status = SkillStatus::from_db(&raw.status, raw.id)?;
    let workspace_scope =
        WorkspaceScope::new(raw.workspace_scope).map_err(|_| SkillLogError::Corrupt {
            id: raw.id,
            message: "workspace scope is empty".to_owned(),
        })?;
    let source = SkillSource {
        session_id: SessionId::new(raw.source_session),
        turn_index: non_negative_to_u64(raw.id, "source_turn_index", raw.source_turn_index)?,
        tool_calls: non_negative_to_u64(raw.id, "source_tool_calls", raw.source_tool_calls)?,
        rounds: non_negative_to_u64(raw.id, "source_rounds", raw.source_rounds)?,
        acceptance_failures: non_negative_to_u64(
            raw.id,
            "source_acceptance_failures",
            raw.source_acceptance_failures,
        )?,
    };

    Ok(Skill {
        id: SkillId::from_raw(raw.id),
        workspace_scope,
        source,
        name: raw.name,
        description: raw.description,
        instructions: raw.instructions,
        tags,
        content_hash: raw.content_hash,
        status,
        created_at: Timestamp::from_millis(raw.created_at),
        deleted_at: raw.deleted_at.map(Timestamp::from_millis),
        activated_at: raw.activated_at.map(Timestamp::from_millis),
    })
}

/// Converts a stored count that must be non-negative into `u64`.
fn non_negative_to_u64(id: i64, field: &'static str, value: i64) -> Result<u64, SkillLogError> {
    u64::try_from(value).map_err(|_| SkillLogError::Corrupt {
        id,
        message: format!("{field} is negative"),
    })
}

/// Errors raised while opening or accessing a [`SkillLog`].
#[derive(Debug, Error)]
pub enum SkillLogError {
    /// The database file could not be opened.
    #[error("failed to open skill database `{path}`: {source}")]
    Open {
        /// Requested database path.
        path: PathBuf,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// The in-memory database could not be created.
    #[error("failed to create an in-memory skill database: {0}")]
    InMemory(#[source] rusqlite::Error),
    /// The schema, FTS5 index, or append-only triggers could not be installed.
    #[error("failed to initialize the skill schema: {0}")]
    Schema(#[source] rusqlite::Error),
    /// The database was written by a newer schema version.
    #[error(
        "skill database schema version {found} is newer than the supported version {supported}"
    )]
    UnsupportedVersion {
        /// Version found in `PRAGMA user_version`.
        found: i64,
        /// Highest version this build supports.
        supported: i64,
    },
    /// A text field was empty or whitespace-only.
    #[error("skill {field} must not be empty")]
    EmptyField {
        /// Field name.
        field: &'static str,
    },
    /// A text field exceeded its byte limit.
    #[error("skill {field} exceeds {max_bytes} bytes (got {actual})")]
    FieldTooLong {
        /// Field name.
        field: &'static str,
        /// Maximum allowed bytes.
        max_bytes: usize,
        /// Observed bytes.
        actual: usize,
    },
    /// A tag was empty after trimming.
    #[error("skill tag at index {index} must not be empty")]
    EmptyTag {
        /// Index in the supplied tag list.
        index: usize,
    },
    /// A tag exceeded its byte limit.
    #[error("skill tag at index {index} exceeds {max_bytes} bytes (got {actual})")]
    TagTooLong {
        /// Index in the supplied tag list.
        index: usize,
        /// Maximum allowed bytes.
        max_bytes: usize,
        /// Observed bytes.
        actual: usize,
    },
    /// Too many distinct tags were supplied.
    #[error("skill has {actual} tags; the maximum is {max}")]
    TooManyTags {
        /// Maximum allowed tags.
        max: usize,
        /// Observed distinct tags.
        actual: usize,
    },
    /// A source count could not be represented in SQLite.
    #[error("skill {field} value {value} is out of range")]
    ValueOutOfRange {
        /// Field name.
        field: &'static str,
        /// Rejected value.
        value: u64,
    },
    /// Tags could not be serialized to JSON.
    #[error("failed to serialize skill tags: {0}")]
    Serialize(#[from] serde_json::Error),
    /// The skill could not be inserted.
    #[error("failed to append a skill: {0}")]
    Insert(#[source] rusqlite::Error),
    /// A read query failed.
    #[error("failed to query skills: {0}")]
    Query(#[source] rusqlite::Error),
    /// A live skill already has the same source or content hash.
    #[error("skill already exists with id {id}")]
    Duplicate {
        /// Existing live skill id.
        id: SkillId,
    },
    /// An ignored insert could not be attributed to a live duplicate.
    #[error("skill insert was ignored but no live duplicate could be found")]
    DuplicateLookup,
    /// Stored tags are not a valid JSON string array.
    #[error("skill {id} contains invalid tag JSON: {source}")]
    Decode {
        /// Row id of the corrupt skill.
        id: i64,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// A stored row violates an invariant enforced by this layer.
    #[error("skill {id} is corrupt: {message}")]
    Corrupt {
        /// Row id of the corrupt skill.
        id: i64,
        /// Description of the violated invariant.
        message: String,
    },
    /// The requested skill does not exist.
    #[error("skill {id} does not exist")]
    NotFound {
        /// Requested row id.
        id: i64,
    },
    /// The requested skill was already soft-deleted.
    #[error("skill {id} is already deleted")]
    AlreadyDeleted {
        /// Requested row id.
        id: i64,
    },
    /// The soft deletion failed.
    #[error("failed to soft-delete skill {id}: {source}")]
    Delete {
        /// Requested row id.
        id: i64,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// The activation failed.
    #[error("failed to activate skill {id}: {source}")]
    Activate {
        /// Requested row id.
        id: i64,
        /// Underlying SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// The relevance floor was negative, non-finite, or `NaN`.
    #[error("skill search min_score must be a finite non-negative value (got {value})")]
    InvalidMinScore {
        /// Rejected threshold.
        value: f64,
    },
    /// A skill was submitted to an adapter bound to another workspace.
    #[error("skill does not belong to this store's workspace")]
    WorkspaceMismatch,
    /// The connection mutex was poisoned by a panicking caller.
    #[error("skill store connection mutex is poisoned")]
    ConnectionPoisoned,
    /// The blocking task failed to complete.
    #[error("skill store blocking task failed: {message}")]
    TaskJoin {
        /// Join error text.
        message: String,
    },
}

/// Errors raised while constructing a [`SqliteSkillStore`].
#[derive(Debug, Error)]
pub enum SqliteSkillStoreError {
    /// The underlying skill database could not be opened or initialized.
    #[error(transparent)]
    Open(#[from] SkillLogError),
    /// The canonical workspace path was empty.
    #[error("canonical workspace path must not be empty")]
    EmptyWorkspacePath,
    /// The derived 16-character scope was rejected unexpectedly.
    #[error("failed to derive a deterministic workspace scope")]
    WorkspaceScope,
}
