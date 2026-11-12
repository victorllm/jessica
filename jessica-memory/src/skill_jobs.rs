//! Persistent skill-job queue stored in `skills.db`.
//!
//! The queue stores one job per `(workspace_scope, session_id, turn_index)`
//! tuple. Jobs transition `pending -> processing -> done|failed`. Enqueue is
//! idempotent: a duplicate tuple returns the existing row unchanged, including
//! terminal `done`/`failed` rows. [`SkillLog::claim_next_job`] claims one row
//! with `BEGIN IMMEDIATE` and `UPDATE ... RETURNING`, increments `attempts`, and
//! records `claimed_at`. A `processing` row whose claim is older than the
//! caller-supplied stale interval can be reclaimed.
//!
//! # Plan B: dual sources of truth
//!
//! This module implements Plan B. Job state is **not** stored in
//! `jessica_core::types::AgentEvent`, and this task does not add an event
//! variant. Resume-time enqueue idempotency reads the unique index in
//! `skills.db`; the event stream and `skills.db` are two sources of truth, and
//! this module does not pretend they are one.
//!
//! Replay never opens `skills.db`, so replay determinism is unaffected by the
//! state of `skill_jobs`. Resume behavior, however, depends on the mutable
//! external state of `skills.db`.
//!
//! `skill_jobs` is not a materialized view of the event stream, cannot be
//! rebuilt from events in this phase, and `source_json` is persistent
//! `skills.db` data rather than an event-derived value.
//!
//! # TODO(reconciliation)
//!
//! Migrate to Plan A in a separate Task if either of these becomes a
//! requirement:
//!
//! - resume must produce identical events/decisions under different initial
//!   `skills.db` states; or
//! - job state must be rebuilt from the event stream.
//!
//! The migration path is:
//!
//! 1. add `AgentEvent::SkillJobEnqueued` and `AgentEvent::SkillJobCompleted`
//!    plus their payload types in `jessica-core`; core changes require a
//!    separate review;
//! 2. emit those events from enqueue/complete;
//! 3. implement `rebuild_from_events` to materialize `skill_jobs`;
//! 4. make resume read the event stream or the materialized view;
//! 5. make replay skip materialization; and
//! 6. add dual-source consistency tests.
//!
//! This phase does not implement `rebuild_from_events`.
//!
//! # Known limitations
//!
//! - Resume enqueue idempotency depends on `skills.db`, not the event stream.
//!   If `skills.db` is deleted or replaced, resume may enqueue the same tuple
//!   again. The unique index and the idempotent skill writes from the skill
//!   library are the final guard.
//! - Event replay cannot restore job state. This is an accepted trade-off for
//!   this phase, not a solved problem.
//! - Until the Plan A migration, do not use `skill_jobs` for any scenario that
//!   requires deterministic reconstruction from the event stream.
//!
//! # Single-worker assumption and concurrency
//!
//! The queue is designed for one worker per database. Claiming uses
//! `BEGIN IMMEDIATE` so two claimers cannot both select the same row. Multiple
//! processes may still process the same logical job more than once after a
//! stale-claim recovery; SQLite serializes writes and cannot corrupt the table,
//! and duplicate processing is absorbed by idempotent skill writes. WAL and a
//! 5000 ms busy timeout are enabled by [`SkillLog`].
//!
//! `last_error` is redacted for common credential forms and truncated to
//! [`MAX_LAST_ERROR_BYTES`] before it is written. Callers must still pass a
//! short error summary, never a raw provider response, secret, or full tool
//! output.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jessica_core::types::SessionId;
use rusqlite::{OptionalExtension, params};
use serde_json::Value;
use thiserror::Error;

use crate::Timestamp;
use crate::memory_log::WorkspaceScope;
use crate::skill_log::{SkillLog, SqliteSkillStore};

/// Maximum byte length of one persisted `source_json` document.
pub const MAX_SOURCE_JSON_BYTES: usize = 64 * 1024;

/// Maximum byte length of a persisted `last_error` after redaction.
pub const MAX_LAST_ERROR_BYTES: usize = 512;

/// Exact fields produced by a serialized `DistillSource` from Task 5.2.2a.
const SOURCE_FIELDS: [&str; 8] = [
    "session_id",
    "turn_index",
    "user_goal",
    "final_answer",
    "tool_summaries",
    "tool_call_count",
    "assistant_rounds",
    "acceptance_failures",
];

/// Columns selected for one [`SkillJob`].
const JOB_COLUMNS: &str = "id, workspace_scope, session_id, turn_index, source_json, \
                           status, attempts, last_error, created_at, updated_at, claimed_at";

/// Identifier of one stored skill job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SkillJobId(i64);

impl SkillJobId {
    /// Wraps a raw SQLite row id.
    ///
    /// Callers normally receive identifiers from [`SkillLog::enqueue_job`];
    /// this constructor exists for storage adapters and tests that read raw
    /// rows.
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// Returns the raw SQLite row id.
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for SkillJobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Lifecycle state of a stored skill job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkillJobStatus {
    /// Waiting to be claimed.
    Pending,
    /// Claimed by a worker; may be reclaimed after the stale threshold.
    Processing,
    /// Successfully distilled and persisted by the worker.
    Done,
    /// Permanently failed after the worker's retry budget was exhausted.
    Failed,
}

impl SkillJobStatus {
    /// Returns the database representation of this status.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    /// Decodes a status read from SQLite.
    fn from_db(id: i64, value: &str) -> Result<Self, SkillJobError> {
        match value {
            "pending" => Ok(Self::Pending),
            "processing" => Ok(Self::Processing),
            "done" => Ok(Self::Done),
            "failed" => Ok(Self::Failed),
            other => Err(SkillJobError::Corrupt {
                id,
                message: format!("unknown status `{other}`"),
            }),
        }
    }
}

/// Input for one idempotent enqueue.
///
/// The source document is the bounded, escaped JSON representation of a Task
/// 5.2.2a `DistillSource`. It must contain exactly the fields in
/// the serialized `DistillSource` fields and its `session_id`/`turn_index`
/// must match the queue tuple. Raw session transcripts and full tool outputs
/// are not valid inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSkillJob {
    /// Workspace partition for this job.
    workspace_scope: WorkspaceScope,
    /// Session that produced the source.
    session_id: SessionId,
    /// Stable turn index within the session.
    turn_index: u64,
    /// Bounded, serialized `DistillSource`.
    source_json: String,
    /// Caller-supplied enqueue time.
    created_at: Timestamp,
}

impl NewSkillJob {
    /// Builds and validates a job input.
    ///
    /// `source_json` is bounded by [`MAX_SOURCE_JSON_BYTES`] and must be a JSON
    /// object containing exactly the serialized `DistillSource` fields. Its
    /// `session_id` and `turn_index` must match the supplied tuple, which keeps
    /// the queue key aligned with the provenance embedded in the source.
    pub fn new(
        workspace_scope: WorkspaceScope,
        session_id: SessionId,
        turn_index: u64,
        source_json: impl Into<String>,
        created_at: Timestamp,
    ) -> Result<Self, SkillJobError> {
        let source_json = source_json.into();
        if source_json.len() > MAX_SOURCE_JSON_BYTES {
            return Err(SkillJobError::SourceTooLarge {
                max_bytes: MAX_SOURCE_JSON_BYTES,
                actual: source_json.len(),
            });
        }
        if i64::try_from(turn_index).is_err() {
            return Err(SkillJobError::ValueOutOfRange {
                field: "turn_index",
                value: turn_index,
            });
        }
        validate_source_json(&source_json, &session_id, turn_index)
            .map_err(|message| SkillJobError::InvalidSource { message })?;

        Ok(Self {
            workspace_scope,
            session_id,
            turn_index,
            source_json,
            created_at,
        })
    }

    /// Returns the workspace partition.
    pub fn workspace_scope(&self) -> &WorkspaceScope {
        &self.workspace_scope
    }

    /// Returns the source session identifier.
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the source turn index.
    pub fn turn_index(&self) -> u64 {
        self.turn_index
    }

    /// Returns the bounded serialized source document.
    pub fn source_json(&self) -> &str {
        &self.source_json
    }

    /// Returns the caller-supplied enqueue time.
    pub fn created_at(&self) -> Timestamp {
        self.created_at
    }
}

/// One stored skill job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillJob {
    /// SQLite row id.
    id: SkillJobId,
    /// Workspace partition.
    workspace_scope: WorkspaceScope,
    /// Source session identifier.
    session_id: SessionId,
    /// Source turn index.
    turn_index: u64,
    /// Bounded serialized `DistillSource`.
    source_json: String,
    /// Current lifecycle state.
    status: SkillJobStatus,
    /// Number of successful claims so far.
    attempts: u32,
    /// Redacted and truncated terminal error, if any.
    last_error: Option<String>,
    /// Caller-supplied enqueue time.
    created_at: Timestamp,
    /// Caller-supplied time of the latest transition.
    updated_at: Timestamp,
    /// Caller-supplied time of the latest claim.
    claimed_at: Option<Timestamp>,
}

impl SkillJob {
    /// Returns the job identifier.
    pub fn id(&self) -> SkillJobId {
        self.id
    }

    /// Returns the workspace partition.
    pub fn workspace_scope(&self) -> &WorkspaceScope {
        &self.workspace_scope
    }

    /// Returns the source session identifier.
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the source turn index.
    pub fn turn_index(&self) -> u64 {
        self.turn_index
    }

    /// Returns the bounded serialized source document.
    pub fn source_json(&self) -> &str {
        &self.source_json
    }

    /// Returns the current lifecycle state.
    pub fn status(&self) -> SkillJobStatus {
        self.status
    }

    /// Returns how many times this job has been claimed.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Returns the redacted terminal error, if the job failed.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Returns the caller-supplied enqueue time.
    pub fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Returns the caller-supplied time of the latest transition.
    pub fn updated_at(&self) -> Timestamp {
        self.updated_at
    }

    /// Returns the caller-supplied time of the latest claim.
    pub fn claimed_at(&self) -> Option<Timestamp> {
        self.claimed_at
    }
}

/// Errors raised while validating, enqueueing, claiming, or transitioning
/// skill jobs.
#[derive(Debug, Error)]
pub enum SkillJobError {
    /// The source document was not a valid serialized `DistillSource`.
    #[error("invalid skill job source: {message}")]
    InvalidSource {
        /// Description of the violated source contract.
        message: String,
    },
    /// The source document exceeded the storage bound.
    #[error("skill job source exceeds {max_bytes} bytes (got {actual})")]
    SourceTooLarge {
        /// Maximum allowed bytes.
        max_bytes: usize,
        /// Observed bytes.
        actual: usize,
    },
    /// A stored job row violates an invariant enforced by this layer.
    #[error("skill job {id} is corrupt: {message}")]
    Corrupt {
        /// Row id of the corrupt job.
        id: i64,
        /// Description of the violated invariant.
        message: String,
    },
    /// The requested job does not exist in the requested workspace.
    #[error("skill job {id} does not exist")]
    NotFound {
        /// Requested row id.
        id: i64,
    },
    /// A transition was attempted from the wrong state.
    #[error("skill job {id} cannot transition from {from:?} to {to:?}")]
    InvalidTransition {
        /// Row id.
        id: i64,
        /// State found before the transition.
        from: SkillJobStatus,
        /// Requested target state.
        to: SkillJobStatus,
    },
    /// An insert succeeded or conflicted but the row could not be read back.
    #[error("enqueued skill job could not be read back")]
    EnqueueLookup,
    /// A numeric value could not be represented in SQLite.
    #[error("skill job field {field} value {value} is out of range")]
    ValueOutOfRange {
        /// Field name.
        field: &'static str,
        /// Rejected value.
        value: u64,
    },
    /// A read or write query failed.
    #[error("failed to query skill jobs: {0}")]
    Query(#[source] rusqlite::Error),
    /// The atomic claim transaction failed.
    #[error("failed to claim a skill job: {0}")]
    Claim(#[source] rusqlite::Error),
    /// A job was submitted to an adapter bound to another workspace.
    #[error("skill job does not belong to this store's workspace")]
    WorkspaceMismatch,
    /// The connection mutex was poisoned by a panicking caller.
    #[error("skill job store connection mutex is poisoned")]
    ConnectionPoisoned,
    /// The blocking task failed to complete.
    #[error("skill job blocking task failed: {message}")]
    TaskJoin {
        /// Join error text.
        message: String,
    },
}

/// Raw row values before status and source decoding.
#[derive(Debug)]
struct RawSkillJob {
    /// SQLite row id.
    id: i64,
    /// Workspace partition.
    workspace_scope: String,
    /// Source session id.
    session_id: String,
    /// Source turn index.
    turn_index: i64,
    /// Bounded serialized source.
    source_json: String,
    /// Lifecycle state.
    status: String,
    /// Claim count.
    attempts: i64,
    /// Terminal error.
    last_error: Option<String>,
    /// Enqueue time.
    created_at: i64,
    /// Latest transition time.
    updated_at: i64,
    /// Latest claim time.
    claimed_at: Option<i64>,
}

impl SkillLog {
    /// Enqueues `job`, returning the stored job.
    ///
    /// Enqueue is idempotent on `(workspace_scope, session_id, turn_index)`.
    /// A duplicate returns the existing row unchanged; it never replaces a
    /// `pending`, `processing`, `done`, or `failed` row and never changes its
    /// `source_json` or `created_at`.
    pub fn enqueue_job(&self, job: NewSkillJob) -> Result<SkillJob, SkillJobError> {
        let turn_index =
            i64::try_from(job.turn_index).map_err(|_| SkillJobError::ValueOutOfRange {
                field: "turn_index",
                value: job.turn_index,
            })?;
        self.connection()
            .execute(
                "INSERT INTO skill_jobs \
                 (workspace_scope, session_id, turn_index, source_json, status, attempts, \
                  last_error, created_at, updated_at, claimed_at) \
                 VALUES (?1, ?2, ?3, ?4, 'pending', 0, NULL, ?5, ?5, NULL) \
                 ON CONFLICT(workspace_scope, session_id, turn_index) DO NOTHING",
                params![
                    job.workspace_scope.as_str(),
                    job.session_id.as_str(),
                    turn_index,
                    &job.source_json,
                    job.created_at.as_millis(),
                ],
            )
            .map_err(SkillJobError::Query)?;

        self.find_job_by_source(&job.workspace_scope, &job.session_id, job.turn_index)?
            .ok_or(SkillJobError::EnqueueLookup)
    }

    /// Returns the job with `id`, including terminal jobs.
    ///
    /// Returns `Ok(None)` when no row has that identifier. Workspace isolation
    /// is enforced by [`SqliteSkillStore`] at the asynchronous boundary.
    pub fn get_job(&self, id: SkillJobId) -> Result<Option<SkillJob>, SkillJobError> {
        let sql = format!("SELECT {JOB_COLUMNS} FROM skill_jobs WHERE id = ?1");
        let raw = self
            .connection()
            .query_row(&sql, [id.get()], decode_raw_job)
            .optional()
            .map_err(SkillJobError::Query)?;
        raw.map(decode_job).transpose()
    }

    /// Returns up to `limit` jobs in `workspace`, oldest first.
    pub fn list_jobs(
        &self,
        workspace: &WorkspaceScope,
        limit: usize,
    ) -> Result<Vec<SkillJob>, SkillJobError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM skill_jobs WHERE workspace_scope = ?1 \
             ORDER BY created_at ASC, id ASC LIMIT ?2"
        );
        let mut statement = self
            .connection()
            .prepare(&sql)
            .map_err(SkillJobError::Query)?;
        let rows = statement
            .query_map(params![workspace.as_str(), limit], decode_raw_job)
            .map_err(SkillJobError::Query)?;

        let mut jobs = Vec::new();
        for row in rows {
            jobs.push(decode_job(row.map_err(SkillJobError::Query)?)?);
        }
        Ok(jobs)
    }

    /// Atomically claims the next runnable job in `workspace`.
    ///
    /// A job is runnable when it is `pending`, or when it is `processing` and
    /// its `claimed_at` is at or before `now - stale_after` (a null
    /// `claimed_at` is also treated as stale). The oldest `created_at`, then
    /// the oldest id, wins. The selected row becomes `processing`, `attempts`
    /// is incremented, and `claimed_at`/`updated_at` are set to `now`.
    ///
    /// The claim runs in a `BEGIN IMMEDIATE` transaction and uses
    /// `UPDATE ... RETURNING`, so two claims cannot both receive the same row.
    /// Returns `Ok(None)` when no runnable job exists.
    pub fn claim_next_job(
        &self,
        workspace: &WorkspaceScope,
        now: Timestamp,
        stale_after: Duration,
    ) -> Result<Option<SkillJob>, SkillJobError> {
        let stale_before = stale_before(now, stale_after);
        self.connection()
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(SkillJobError::Claim)?;

        let result = self.claim_next_job_in_transaction(workspace, now, stale_before);
        match result {
            Ok(job) => {
                if let Err(source) = self.connection().execute_batch("COMMIT") {
                    let _ = self.connection().execute_batch("ROLLBACK");
                    return Err(SkillJobError::Claim(source));
                }
                Ok(job)
            }
            Err(error) => {
                let _ = self.connection().execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// Marks a `processing` job as `done`.
    ///
    /// The transition is rejected with [`SkillJobError::InvalidTransition`] if
    /// the job is not currently processing. `last_error` is cleared and
    /// `updated_at` is set to `updated_at`.
    pub fn complete_job(
        &self,
        workspace: &WorkspaceScope,
        id: SkillJobId,
        updated_at: Timestamp,
    ) -> Result<SkillJob, SkillJobError> {
        self.transition_job(workspace, id, SkillJobStatus::Done, None, updated_at)
    }

    /// Marks a `processing` job as `failed`.
    ///
    /// `last_error` is redacted for common credential forms and truncated to
    /// [`MAX_LAST_ERROR_BYTES`] before it is stored. Callers must still pass a
    /// short summary, never a raw provider response, secret, or full tool
    /// output. The transition is rejected with
    /// [`SkillJobError::InvalidTransition`] unless the job is processing.
    pub fn fail_job(
        &self,
        workspace: &WorkspaceScope,
        id: SkillJobId,
        updated_at: Timestamp,
        last_error: &str,
    ) -> Result<SkillJob, SkillJobError> {
        let last_error = redact_and_truncate_error(last_error);
        let last_error = if last_error.is_empty() {
            None
        } else {
            Some(last_error)
        };
        self.transition_job(
            workspace,
            id,
            SkillJobStatus::Failed,
            last_error,
            updated_at,
        )
    }

    /// Executes the claim while the caller holds `BEGIN IMMEDIATE`.
    fn claim_next_job_in_transaction(
        &self,
        workspace: &WorkspaceScope,
        now: Timestamp,
        stale_before: i64,
    ) -> Result<Option<SkillJob>, SkillJobError> {
        let sql = format!(
            "UPDATE skill_jobs \
             SET status = 'processing', attempts = attempts + 1, claimed_at = ?2, updated_at = ?2 \
             WHERE id = ( \
                 SELECT id FROM skill_jobs \
                 WHERE workspace_scope = ?1 \
                   AND ( \
                       status = 'pending' \
                       OR (status = 'processing' AND (claimed_at IS NULL OR claimed_at <= ?3)) \
                   ) \
                 ORDER BY created_at ASC, id ASC \
                 LIMIT 1 \
             ) \
             RETURNING {JOB_COLUMNS}"
        );
        let raw = self
            .connection()
            .query_row(
                &sql,
                params![workspace.as_str(), now.as_millis(), stale_before],
                decode_raw_job,
            )
            .optional()
            .map_err(SkillJobError::Claim)?;
        raw.map(decode_job).transpose()
    }

    /// Conditional update shared by the terminal transitions.
    fn transition_job(
        &self,
        workspace: &WorkspaceScope,
        id: SkillJobId,
        target: SkillJobStatus,
        last_error: Option<String>,
        updated_at: Timestamp,
    ) -> Result<SkillJob, SkillJobError> {
        let sql = format!(
            "UPDATE skill_jobs \
             SET status = ?3, last_error = ?4, updated_at = ?5 \
             WHERE id = ?1 AND workspace_scope = ?2 AND status = 'processing' \
             RETURNING {JOB_COLUMNS}"
        );
        let raw = self
            .connection()
            .query_row(
                &sql,
                params![
                    id.get(),
                    workspace.as_str(),
                    target.as_str(),
                    last_error,
                    updated_at.as_millis(),
                ],
                decode_raw_job,
            )
            .optional()
            .map_err(SkillJobError::Query)?;

        if let Some(raw) = raw {
            return decode_job(raw);
        }

        match self.get_job(id)? {
            Some(job) if job.workspace_scope() == workspace => {
                Err(SkillJobError::InvalidTransition {
                    id: id.get(),
                    from: job.status(),
                    to: target,
                })
            }
            _ => Err(SkillJobError::NotFound { id: id.get() }),
        }
    }

    /// Reads one job by its idempotency key.
    fn find_job_by_source(
        &self,
        workspace: &WorkspaceScope,
        session_id: &SessionId,
        turn_index: u64,
    ) -> Result<Option<SkillJob>, SkillJobError> {
        let turn_index = i64::try_from(turn_index).map_err(|_| SkillJobError::ValueOutOfRange {
            field: "turn_index",
            value: turn_index,
        })?;
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM skill_jobs \
             WHERE workspace_scope = ?1 AND session_id = ?2 AND turn_index = ?3"
        );
        let raw = self
            .connection()
            .query_row(
                &sql,
                params![workspace.as_str(), session_id.as_str(), turn_index],
                decode_raw_job,
            )
            .optional()
            .map_err(SkillJobError::Query)?;
        raw.map(decode_job).transpose()
    }
}

impl SqliteSkillStore {
    /// Enqueues `job` if it belongs to this store's workspace.
    ///
    /// Duplicate handling matches [`SkillLog::enqueue_job`].
    pub async fn enqueue_job(&self, job: NewSkillJob) -> Result<SkillJob, SkillJobError> {
        if job.workspace_scope() != self.workspace_scope() {
            return Err(SkillJobError::WorkspaceMismatch);
        }
        let log = self.shared_log();
        run_blocking_job(log, move |log| log.enqueue_job(job)).await
    }

    /// Returns the job with `id` if it belongs to this store's workspace.
    pub async fn get_job(&self, id: SkillJobId) -> Result<Option<SkillJob>, SkillJobError> {
        let workspace = self.workspace_scope().clone();
        let log = self.shared_log();
        run_blocking_job(log, move |log| {
            Ok(log
                .get_job(id)?
                .filter(|job| job.workspace_scope() == &workspace))
        })
        .await
    }

    /// Returns up to `limit` jobs in this store's workspace, oldest first.
    pub async fn list_jobs(&self, limit: usize) -> Result<Vec<SkillJob>, SkillJobError> {
        let workspace = self.workspace_scope().clone();
        let log = self.shared_log();
        run_blocking_job(log, move |log| log.list_jobs(&workspace, limit)).await
    }

    /// Claims the next runnable job in this store's workspace.
    ///
    /// Semantics match [`SkillLog::claim_next_job`].
    pub async fn claim_next_job(
        &self,
        now: Timestamp,
        stale_after: Duration,
    ) -> Result<Option<SkillJob>, SkillJobError> {
        let workspace = self.workspace_scope().clone();
        let log = self.shared_log();
        run_blocking_job(log, move |log| {
            log.claim_next_job(&workspace, now, stale_after)
        })
        .await
    }

    /// Marks a processing job in this store's workspace as `done`.
    pub async fn complete_job(
        &self,
        id: SkillJobId,
        updated_at: Timestamp,
    ) -> Result<SkillJob, SkillJobError> {
        let workspace = self.workspace_scope().clone();
        let log = self.shared_log();
        run_blocking_job(log, move |log| log.complete_job(&workspace, id, updated_at)).await
    }

    /// Marks a processing job in this store's workspace as `failed`.
    ///
    /// Error redaction and truncation match [`SkillLog::fail_job`].
    pub async fn fail_job(
        &self,
        id: SkillJobId,
        updated_at: Timestamp,
        last_error: &str,
    ) -> Result<SkillJob, SkillJobError> {
        let workspace = self.workspace_scope().clone();
        let last_error = last_error.to_owned();
        let log = self.shared_log();
        run_blocking_job(log, move |log| {
            log.fail_job(&workspace, id, updated_at, &last_error)
        })
        .await
    }
}

/// Runs one synchronous skill-job operation on the blocking pool.
async fn run_blocking_job<T, F>(log: Arc<Mutex<SkillLog>>, operation: F) -> Result<T, SkillJobError>
where
    T: Send + 'static,
    F: FnOnce(&mut SkillLog) -> Result<T, SkillJobError> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut log = log.lock().map_err(|_| SkillJobError::ConnectionPoisoned)?;
        operation(&mut log)
    })
    .await
    .map_err(|source| SkillJobError::TaskJoin {
        message: source.to_string(),
    })?
}

/// Reads one row in [`JOB_COLUMNS`] order.
fn decode_raw_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawSkillJob> {
    Ok(RawSkillJob {
        id: row.get(0)?,
        workspace_scope: row.get(1)?,
        session_id: row.get(2)?,
        turn_index: row.get(3)?,
        source_json: row.get(4)?,
        status: row.get(5)?,
        attempts: row.get(6)?,
        last_error: row.get(7)?,
        created_at: row.get(8)?,
        updated_at: row.get(9)?,
        claimed_at: row.get(10)?,
    })
}

/// Decodes one raw row into a public job, validating every stored invariant.
fn decode_job(raw: RawSkillJob) -> Result<SkillJob, SkillJobError> {
    let status = SkillJobStatus::from_db(raw.id, &raw.status)?;
    let workspace_scope =
        WorkspaceScope::new(raw.workspace_scope).map_err(|_| SkillJobError::Corrupt {
            id: raw.id,
            message: "workspace scope is empty".to_owned(),
        })?;
    let turn_index = u64::try_from(raw.turn_index).map_err(|_| SkillJobError::Corrupt {
        id: raw.id,
        message: "turn_index is negative".to_owned(),
    })?;
    let attempts = u32::try_from(raw.attempts).map_err(|_| SkillJobError::Corrupt {
        id: raw.id,
        message: "attempts is negative or too large".to_owned(),
    })?;
    let session_id = SessionId::new(raw.session_id);
    validate_source_json(&raw.source_json, &session_id, turn_index).map_err(|message| {
        SkillJobError::Corrupt {
            id: raw.id,
            message,
        }
    })?;

    Ok(SkillJob {
        id: SkillJobId::from_raw(raw.id),
        workspace_scope,
        session_id,
        turn_index,
        source_json: raw.source_json,
        status,
        attempts,
        last_error: raw.last_error,
        created_at: Timestamp::from_millis(raw.created_at),
        updated_at: Timestamp::from_millis(raw.updated_at),
        claimed_at: raw.claimed_at.map(Timestamp::from_millis),
    })
}

/// Validates the bounded serialized `DistillSource` contract.
fn validate_source_json(
    source_json: &str,
    session_id: &SessionId,
    turn_index: u64,
) -> Result<(), String> {
    if source_json.len() > MAX_SOURCE_JSON_BYTES {
        return Err(format!(
            "source JSON exceeds {MAX_SOURCE_JSON_BYTES} bytes (got {})",
            source_json.len()
        ));
    }

    let value: Value = serde_json::from_str(source_json)
        .map_err(|error| format!("source JSON is not valid JSON: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "source JSON must be an object".to_owned())?;
    if object.len() != SOURCE_FIELDS.len()
        || SOURCE_FIELDS
            .iter()
            .any(|field| !object.contains_key(*field))
    {
        return Err(format!(
            "source JSON must contain exactly these fields: {}",
            SOURCE_FIELDS.join(", ")
        ));
    }

    let document_session = object
        .get("session_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "source JSON field `session_id` must be a string".to_owned())?;
    if document_session != session_id.as_str() {
        return Err(format!(
            "source JSON session_id `{document_session}` does not match job session_id `{}`",
            session_id.as_str()
        ));
    }

    let document_turn = object
        .get("turn_index")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            "source JSON field `turn_index` must be a non-negative integer".to_owned()
        })?;
    if document_turn != turn_index {
        return Err(format!(
            "source JSON turn_index {document_turn} does not match job turn_index {turn_index}"
        ));
    }

    for field in ["user_goal", "final_answer"] {
        if object.get(field).and_then(Value::as_str).is_none() {
            return Err(format!("source JSON field `{field}` must be a string"));
        }
    }

    let summaries = object
        .get("tool_summaries")
        .and_then(Value::as_array)
        .ok_or_else(|| "source JSON field `tool_summaries` must be an array".to_owned())?;
    if summaries.iter().any(|summary| !summary.is_string()) {
        return Err("source JSON field `tool_summaries` must contain only strings".to_owned());
    }

    for field in ["tool_call_count", "assistant_rounds", "acceptance_failures"] {
        if object.get(field).and_then(Value::as_u64).is_none() {
            return Err(format!(
                "source JSON field `{field}` must be a non-negative integer"
            ));
        }
    }

    Ok(())
}

/// Computes the inclusive stale-claim boundary.
fn stale_before(now: Timestamp, stale_after: Duration) -> i64 {
    let millis = i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX);
    now.as_millis().saturating_sub(millis)
}

/// Removes common credential forms and truncates to the storage bound.
fn redact_and_truncate_error(message: &str) -> String {
    let redacted = redact_sensitive_values(message);
    truncate_on_char_boundary(&redacted, MAX_LAST_ERROR_BYTES).to_owned()
}

/// Replaces recognizable credential values with `[REDACTED]`.
fn redact_sensitive_values(input: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let mut spans = Vec::new();

    collect_token_prefix_spans(input, &lower, "sk-", &mut spans);
    collect_bearer_spans(input, &lower, &mut spans);
    for marker in [
        "authorization",
        "api_key",
        "api-key",
        "apikey",
        "access_token",
        "refresh_token",
        "secret",
        "password",
        "token",
    ] {
        collect_key_value_spans(input, &lower, marker, &mut spans);
    }

    spans.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in spans {
        if let Some((_, last_end)) = merged.last_mut()
            && start <= *last_end
        {
            *last_end = (*last_end).max(end);
            continue;
        }
        merged.push((start, end));
    }

    if merged.is_empty() {
        return input.to_owned();
    }

    let mut redacted = String::with_capacity(input.len());
    let mut cursor = 0;
    for (start, end) in merged {
        redacted.push_str(&input[cursor..start]);
        redacted.push_str("[REDACTED]");
        cursor = end;
    }
    redacted.push_str(&input[cursor..]);
    redacted
}

/// Collects spans for a bare token prefix such as `sk-`.
fn collect_token_prefix_spans(
    input: &str,
    lower: &str,
    marker: &str,
    spans: &mut Vec<(usize, usize)>,
) {
    let mut search_from = 0;
    while let Some(relative) = lower[search_from..].find(marker) {
        let start = search_from + relative;
        let end = scan_secret_value(input, start);
        if end > start {
            spans.push((start, end));
            search_from = end;
        } else {
            search_from = start + marker.len();
        }
    }
}

/// Collects spans for `Bearer <token>`.
fn collect_bearer_spans(input: &str, lower: &str, spans: &mut Vec<(usize, usize)>) {
    let marker = "bearer";
    let mut search_from = 0;
    while let Some(relative) = lower[search_from..].find(marker) {
        let start = search_from + relative;
        let after = start + marker.len();
        let Some(next) = input[after..].chars().next() else {
            break;
        };
        if next.is_whitespace() {
            let value_start = skip_whitespace(input, after);
            let end = scan_secret_value(input, value_start);
            if end > value_start {
                spans.push((start, end));
                search_from = end;
                continue;
            }
        }
        search_from = after;
    }
}

/// Collects spans for `key: value` and `key=value` credential forms.
fn collect_key_value_spans(
    input: &str,
    lower: &str,
    marker: &str,
    spans: &mut Vec<(usize, usize)>,
) {
    let mut search_from = 0;
    while let Some(relative) = lower[search_from..].find(marker) {
        let start = search_from + relative;
        let after = start + marker.len();
        let mut cursor = skip_whitespace(input, after);
        let Some(separator) = input[cursor..].chars().next() else {
            break;
        };
        if separator != ':' && separator != '=' {
            search_from = after;
            continue;
        }
        cursor += separator.len_utf8();
        cursor = skip_whitespace(input, cursor);
        cursor = skip_quote(input, cursor);
        let end = scan_secret_value(input, cursor);
        if end > cursor {
            spans.push((start, end));
            search_from = end;
        } else {
            search_from = after;
        }
    }
}

/// Skips Unicode whitespace from a character boundary.
fn skip_whitespace(input: &str, mut index: usize) -> usize {
    while let Some(character) = input[index..].chars().next() {
        if !character.is_whitespace() {
            break;
        }
        index += character.len_utf8();
    }
    index
}

/// Skips one optional opening quote.
fn skip_quote(input: &str, index: usize) -> usize {
    match input[index..].chars().next() {
        Some(character @ ('"' | '\'')) => index + character.len_utf8(),
        _ => index,
    }
}

/// Returns the end of a credential-like token starting at `start`.
fn scan_secret_value(input: &str, start: usize) -> usize {
    let mut end = start;
    for (offset, character) in input[start..].char_indices() {
        if character.is_whitespace()
            || matches!(
                character,
                ',' | ';' | '"' | '\'' | ')' | ']' | '}' | '<' | '>' | '&'
            )
        {
            break;
        }
        end = start + offset + character.len_utf8();
    }
    end
}

/// Returns the longest prefix of `text` of at most `max_bytes` bytes that ends
/// on a UTF-8 character boundary.
fn truncate_on_char_boundary(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
