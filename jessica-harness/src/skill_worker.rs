//! Single-worker orchestration for the asynchronous skill-distillation queue.
//!
//! A [`SkillWorker`] repeatedly claims one job from a [`SkillJobQueue`],
//! distills its [`DistillSource`] with a [`SkillDistiller`], writes the draft
//! through a [`SkillSink`], and records `complete`/`fail` back on the queue.
//! The queue, sink, and clock are traits so this crate stays independent of
//! `jessica-memory` and of any concrete SQLite implementation.
//!
//! # Single-worker assumption
//!
//! One worker is expected per queue database. This module does not coordinate
//! multiple consumers. A queue implementation may still be called from several
//! processes, but duplicate delivery is possible after stale-claim recovery;
//! the sink's idempotent write is the final guard against duplicate skills.
//!
//! # Retry semantics
//!
//! One attempt consists of one `distill` call followed by one `sink.write`
//! call. A retryable [`DistillError`] or [`SkillSinkError::Retryable`] restarts
//! the attempt, including the distill call, until `max_attempts` is reached.
//! Permanent failures are not retried. Exhausted retries and permanent
//! failures call [`SkillJobQueue::fail`] once with a bounded, redacted error.
//! A successful sink write is followed by exactly one
//! [`SkillJobQueue::complete`] call; the job is never completed before the
//! write succeeds. If `complete` itself fails, the error is returned to the
//! caller. The sink write stays durable and redelivery is absorbed by the
//! sink's idempotency.
//!
//! # Shutdown and drain
//!
//! [`SkillWorker::run`] stops issuing new claims once the supplied
//! [`CancellationToken`] is cancelled. A claim already in flight is allowed to
//! finish, and its job is drained: the worker waits up to `drain_timeout` for
//! it to reach a terminal transition. A timeout returns
//! [`SkillWorkerError::DrainTimeout`] without panicking; the job remains in
//! whatever state the queue recorded and can be recovered by the queue's
//! stale-claim mechanism. [`SkillWorker::run_once`] is the deterministic
//! building block used by tests and by callers that wake the worker
//! explicitly.
//!
//! # Time and side effects
//!
//! The worker never reads the host clock. Every queue timestamp comes from the
//! injected [`WorkerClock`]. It does not write the agent event stream and does
//! not expose any storage handle beyond the injected queue and sink.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::skill_distiller::{DistillError, DistilledSkill, SkillDistiller};
use crate::skill_source::DistillSource;

/// Default number of attempts, including the first one.
pub const DEFAULT_MAX_ATTEMPTS: usize = 2;

/// Default grace period for an in-flight job after shutdown.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Default idle poll interval when the queue has no runnable job.
pub const DEFAULT_IDLE_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum byte length of a redacted job error.
const MAX_JOB_ERROR_BYTES: usize = 512;

/// Opaque identifier assigned by a [`SkillJobQueue`].
///
/// The worker never interprets the value; it only passes it back to
/// `complete`/`fail`. The default SQLite adapter maps its row id into this
/// type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SkillJobId(i64);

impl SkillJobId {
    /// Wraps a backend-provided identifier.
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// Returns the backend-provided identifier.
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for SkillJobId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Unix timestamp in milliseconds supplied by a [`WorkerClock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerTimestamp(i64);

impl WorkerTimestamp {
    /// Creates a timestamp from Unix milliseconds.
    pub const fn from_millis(value: i64) -> Self {
        Self(value)
    }

    /// Returns Unix milliseconds.
    pub const fn as_millis(self) -> i64 {
        self.0
    }
}

/// Supplies queue timestamps without reading the host clock in this crate.
///
/// Production callers provide a clock backed by the system time. Tests
/// provide a deterministic implementation. The worker calls `now` exactly
/// when it records a terminal queue transition.
pub trait WorkerClock: Send + Sync {
    /// Returns the current Unix time in milliseconds.
    fn now(&self) -> WorkerTimestamp;
}

/// One claimed unit of distillation work.
///
/// The queue owns the identifier and workspace partition. The source carries
/// the session, turn, counters, and escaped content needed by the distiller;
/// the same source also gives the sink the `(workspace_scope, session_id,
/// turn_index)` provenance required for idempotent writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillJob {
    /// Backend-assigned job identifier.
    id: SkillJobId,
    /// Workspace partition for the sink.
    workspace_scope: String,
    /// Bounded source for the single distillation call.
    source: DistillSource,
}

impl SkillJob {
    /// Creates a claimed job.
    ///
    /// `workspace_scope` must be the non-empty scope used by the queue and
    /// sink; this crate does not interpret it. Callers normally receive jobs
    /// from [`SkillJobQueue::claim`].
    pub fn new(id: SkillJobId, workspace_scope: impl Into<String>, source: DistillSource) -> Self {
        Self {
            id,
            workspace_scope: workspace_scope.into(),
            source,
        }
    }

    /// Returns the backend-assigned identifier.
    pub const fn id(&self) -> SkillJobId {
        self.id
    }

    /// Returns the workspace partition.
    pub fn workspace_scope(&self) -> &str {
        &self.workspace_scope
    }

    /// Returns the bounded distillation source.
    pub fn source(&self) -> &DistillSource {
        &self.source
    }
}

/// Claim/complete/fail operations for a skill-job queue.
///
/// Each queue instance is scoped to one workspace. `complete` and `fail` must
/// persist the transition before returning; the worker never changes a job's
/// state locally. Implementations must not write the agent event stream.
#[async_trait]
pub trait SkillJobQueue: Send + Sync {
    /// Claims the next runnable job, or returns `None` when the queue is idle.
    async fn claim(&self) -> Result<Option<SkillJob>, SkillQueueError>;

    /// Marks `job` as successfully completed at `at`.
    async fn complete(&self, job: &SkillJob, at: WorkerTimestamp) -> Result<(), SkillQueueError>;

    /// Marks `job` as failed at `at` with a bounded, redacted error.
    async fn fail(
        &self,
        job: &SkillJob,
        at: WorkerTimestamp,
        last_error: &str,
    ) -> Result<(), SkillQueueError>;
}

/// Errors raised by a [`SkillJobQueue`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SkillQueueError {
    /// The queue backend could not claim or persist a transition.
    #[error("skill job queue failed: {message}")]
    Backend {
        /// Human-readable failure description.
        message: String,
    },
}

/// Writes distilled skills with backend-specific idempotency.
///
/// Implementations must ensure that the same
/// `(workspace_scope, source_session, source_turn_index)` or the same content
/// hash does not create a duplicate live skill. `Ok(())` means the draft is
/// durably written and the worker may mark the job complete. A retryable error
/// causes another attempt; a permanent error fails the job immediately.
#[async_trait]
pub trait SkillSink: Send + Sync {
    /// Persists `skill` for `job`'s provenance.
    async fn write(&self, job: &SkillJob, skill: &DistilledSkill) -> Result<(), SkillSinkError>;
}

/// Errors raised by a [`SkillSink`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SkillSinkError {
    /// The write may succeed if attempted again.
    #[error("skill sink failed temporarily: {message}")]
    Retryable {
        /// Human-readable failure description.
        message: String,
    },
    /// The write must not be retried.
    #[error("skill sink failed permanently: {message}")]
    Permanent {
        /// Human-readable failure description.
        message: String,
    },
}

/// Worker configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillWorkerConfig {
    /// Maximum number of attempts, including the first one.
    ///
    /// Must be at least one. The default is [`DEFAULT_MAX_ATTEMPTS`].
    pub max_attempts: usize,
    /// Grace period for an in-flight job after shutdown.
    ///
    /// The default is [`DEFAULT_DRAIN_TIMEOUT`].
    pub drain_timeout: Duration,
    /// How long `run` waits for a job before polling an idle queue again.
    ///
    /// The default is [`DEFAULT_IDLE_INTERVAL`]. Must be non-zero.
    pub idle_interval: Duration,
}

impl Default for SkillWorkerConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            idle_interval: DEFAULT_IDLE_INTERVAL,
        }
    }
}

/// Errors raised by [`SkillWorker`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SkillWorkerError {
    /// The configuration cannot produce a valid worker.
    #[error("invalid skill worker configuration: {message}")]
    InvalidConfig {
        /// Human-readable failure description.
        message: String,
    },
    /// A queue operation failed; the job state was not silently changed.
    #[error("skill job queue failed: {message}")]
    Queue {
        /// Human-readable failure description.
        message: String,
    },
    /// Shutdown arrived while a job was in flight and the drain deadline
    /// elapsed.
    #[error("skill worker drain timed out after {millis} ms")]
    DrainTimeout {
        /// Configured drain timeout in milliseconds.
        millis: u64,
    },
}

/// Result of one claim/process pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerOutcome {
    /// The queue had no runnable job.
    Idle,
    /// A claimed job was written and marked complete.
    Completed,
    /// A claimed job permanently failed or exhausted its retry budget.
    Failed,
}

/// A single-worker consumer for the skill-job queue.
///
/// The worker is passive: it only runs when [`SkillWorker::run`] or
/// [`SkillWorker::run_once`] is called. It has no background task, does not
/// block the turn, and holds no event-store handle.
pub struct SkillWorker {
    queue: Arc<dyn SkillJobQueue>,
    distiller: Arc<dyn SkillDistiller>,
    sink: Arc<dyn SkillSink>,
    clock: Arc<dyn WorkerClock>,
    config: SkillWorkerConfig,
}

impl SkillWorker {
    /// Creates a worker and validates its configuration.
    ///
    /// Returns [`SkillWorkerError::InvalidConfig`] when `max_attempts` is zero
    /// or `idle_interval` is zero.
    pub fn new(
        queue: Arc<dyn SkillJobQueue>,
        distiller: Arc<dyn SkillDistiller>,
        sink: Arc<dyn SkillSink>,
        clock: Arc<dyn WorkerClock>,
        config: SkillWorkerConfig,
    ) -> Result<Self, SkillWorkerError> {
        if config.max_attempts == 0 {
            return Err(SkillWorkerError::InvalidConfig {
                message: "max_attempts must be at least 1".to_owned(),
            });
        }
        if config.idle_interval.is_zero() {
            return Err(SkillWorkerError::InvalidConfig {
                message: "idle_interval must be greater than zero".to_owned(),
            });
        }
        Ok(Self {
            queue,
            distiller,
            sink,
            clock,
            config,
        })
    }

    /// Claims and processes at most one job.
    ///
    /// This is the deterministic building block for tests and for callers
    /// that wake the worker explicitly. It never sleeps and never reads the
    /// host clock.
    pub async fn run_once(&self) -> Result<WorkerOutcome, SkillWorkerError> {
        match self.queue.claim().await.map_err(queue_error)? {
            Some(job) => self.process_job(&job).await,
            None => Ok(WorkerOutcome::Idle),
        }
    }

    /// Runs until `shutdown` is cancelled.
    ///
    /// The worker checks cancellation before each claim. If cancellation
    /// arrives while a job is in flight, it stops claiming and waits up to
    /// `drain_timeout` for that job. A drain timeout returns
    /// [`SkillWorkerError::DrainTimeout`] and leaves recovery to the queue's
    /// stale-claim mechanism. An idle queue is polled every `idle_interval`.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), SkillWorkerError> {
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }

            let job = match self.queue.claim().await.map_err(queue_error)? {
                Some(job) => job,
                None => {
                    tokio::select! {
                        _ = shutdown.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(self.config.idle_interval) => continue,
                    }
                }
            };

            let processing = self.process_job(&job);
            tokio::pin!(processing);
            tokio::select! {
                biased;
                result = &mut processing => {
                    result?;
                }
                _ = shutdown.cancelled() => {
                    return match tokio::time::timeout(
                        self.config.drain_timeout,
                        &mut processing,
                    )
                    .await
                    {
                        Ok(result) => result.map(|_| ()),
                        Err(_) => Err(SkillWorkerError::DrainTimeout {
                            millis: duration_millis(self.config.drain_timeout),
                        }),
                    };
                }
            }
        }
    }

    /// Runs the retry loop for one claimed job.
    async fn process_job(&self, job: &SkillJob) -> Result<WorkerOutcome, SkillWorkerError> {
        let mut attempt = 1_usize;

        loop {
            let failure = match self.distiller.distill(job.source()).await {
                Ok(skill) => match self.sink.write(job, &skill).await {
                    Ok(()) => {
                        self.queue
                            .complete(job, self.clock.now())
                            .await
                            .map_err(queue_error)?;
                        return Ok(WorkerOutcome::Completed);
                    }
                    Err(SkillSinkError::Retryable { message }) => {
                        ClassifiedFailure::retryable(message)
                    }
                    Err(SkillSinkError::Permanent { message }) => {
                        ClassifiedFailure::permanent(message)
                    }
                },
                Err(DistillError::Retryable { message }) => ClassifiedFailure::retryable(message),
                Err(DistillError::Permanent { message }) => ClassifiedFailure::permanent(message),
            };

            if failure.retryable && attempt < self.config.max_attempts {
                attempt += 1;
                continue;
            }

            let last_error = redact_job_error(&failure.message);
            self.queue
                .fail(job, self.clock.now(), &last_error)
                .await
                .map_err(queue_error)?;
            return Ok(WorkerOutcome::Failed);
        }
    }
}

/// One classified distill or sink failure.
struct ClassifiedFailure {
    /// Whether another attempt is allowed.
    retryable: bool,
    /// Human-readable failure message.
    message: String,
}

impl ClassifiedFailure {
    /// Creates a retryable failure.
    fn retryable(message: String) -> Self {
        Self {
            retryable: true,
            message,
        }
    }

    /// Creates a permanent failure.
    fn permanent(message: String) -> Self {
        Self {
            retryable: false,
            message,
        }
    }
}

/// Maps a queue error into the worker error.
fn queue_error(error: SkillQueueError) -> SkillWorkerError {
    SkillWorkerError::Queue {
        message: error.to_string(),
    }
}

/// Returns `duration` in milliseconds, saturating at `u64::MAX`.
fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Removes common credential forms and truncates to the storage bound.
fn redact_job_error(message: &str) -> String {
    let redacted = redact_sensitive_values(message);
    truncate_on_char_boundary(&redacted, MAX_JOB_ERROR_BYTES).to_owned()
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
