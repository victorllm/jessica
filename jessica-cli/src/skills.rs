//! Assembly of skill triggering, distillation, retrieval, and review.
//!
//! The CLI owns the boundary between the `[skills]` configuration, the SQLite
//! skill library, and the backend-agnostic harness traits. Opening the store is
//! fail-open: a broken or unwritable database disables skills for the run with
//! a warning and never changes the turn's reason or exit code.
//!
//! # Determinism boundaries
//!
//! - Replay returns before this module is reached and never opens the skill
//!   database.
//! - Resume may enqueue a job after a governed `Completed` turn, but never
//!   renders skill context: the recovered checkpoint already contains the
//!   messages the first process started with.
//! - Job state lives only in `skills.db`. Enqueue idempotency relies on the
//!   queue's unique `(workspace_scope, session_id, turn_index)` index; the
//!   agent event stream is neither read nor written by the worker.
//! - Distillation runs on a background worker. A turn enqueues and returns
//!   without awaiting the model call.
//! - The worker is drained before the process exits, at most for
//!   `drain_timeout_ms`. A drain timeout only warns.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use jessica_core::llm::LlmProvider;
use jessica_core::types::{Message, Role, SessionId};
use jessica_harness::{
    DistillSource, DistillSourceInput, DistilledSkill, Guide, LlmSkillDistiller, SkillContent,
    SkillDistiller, SkillGuide, SkillJob, SkillJobId, SkillJobQueue, SkillQueueError, SkillSink,
    SkillSinkError, SkillSource as SkillSourceTrait, SkillSourceError, SkillTrigger, SkillWorker,
    SkillWorkerConfig, TurnSignals, WorkerClock, WorkerOutcome, WorkerTimestamp,
};
use jessica_memory::{
    NewSkill, NewSkillJob, Skill, SkillId, SkillJob as StoredSkillJob, SkillLogError,
    SkillSource as StoredSkillSource, SkillStatus, SqliteSkillStore, SqliteSkillStoreError,
    Timestamp,
};
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::config::SkillsConfig;

/// Maximum accepted byte length of one distillation model response.
const DISTILL_MAX_OUTPUT_BYTES: usize = 64 * 1024;

/// A processing job older than this is eligible for stale-claim recovery.
const STALE_CLAIM_AFTER: Duration = Duration::from_secs(5 * 60);

/// Worker poll interval used by [`SkillWorker::new`].
///
/// The CLI worker is notification-driven and uses `run_once`, so this value
/// only satisfies the worker's non-zero interval invariant.
const WORKER_IDLE_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum excerpt bytes taken from one tool output.
const TOOL_OUTPUT_EXCERPT_BYTES: usize = 256;

/// Default number of rows shown by `jessica skills list`.
const DEFAULT_LIST_LIMIT: usize = 50;

/// Skill components assembled for one CLI run.
pub(crate) struct SkillsAssembly {
    /// Store shared by the trigger, guide, queue, and sink adapters.
    store: Arc<SqliteSkillStore>,
    /// Guide used to render context for fresh turns.
    guide: Arc<dyn Guide>,
    /// Deterministic trigger thresholds.
    trigger: SkillTrigger,
    /// Byte budget for escaped source content.
    max_source_bytes: usize,
    /// Whether bounded tool-output excerpts enter the source.
    include_tool_outputs: bool,
    /// Wake-up signal for the background worker.
    notify: Arc<Notify>,
    /// Cooperative shutdown signal.
    shutdown: CancellationToken,
    /// Background worker task, removed when drained.
    worker_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Maximum wait for the worker before exit.
    drain_timeout: Duration,
}

impl SkillsAssembly {
    /// Opens the configured skill store and constructs every adapter.
    ///
    /// Returns `None` when skills are disabled or cannot be opened. Open
    /// failures are logged at `warn` and degrade to a run without skills; they
    /// never fail the turn. Relative database paths resolve against `root`,
    /// and missing parent directories are created for an enabled store.
    pub(crate) async fn open(
        config: &SkillsConfig,
        root: &Path,
        provider: Arc<dyn LlmProvider>,
    ) -> Option<Self> {
        if !config.enabled {
            return None;
        }

        let store = match open_store(config, root).await {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!(error = %error, "skills disabled for this run");
                return None;
            }
        };
        let trigger = SkillTrigger::new(config.min_tool_calls, config.min_rounds);
        let source: Arc<dyn SkillSourceTrait> = Arc::new(StoreSkillSource {
            store: Arc::clone(&store),
        });
        let min_score = (config.min_score > 0.0).then_some(config.min_score);
        let guide: Arc<dyn Guide> = Arc::new(SkillGuide::new(
            source,
            config.recall_limit,
            min_score,
            config.max_inject_bytes,
        ));

        let clock: Arc<dyn WorkerClock> = Arc::new(SystemWorkerClock);
        let distiller: Arc<dyn SkillDistiller> = Arc::new(LlmSkillDistiller::new(
            provider,
            Duration::from_millis(config.distill_timeout_ms),
            DISTILL_MAX_OUTPUT_BYTES,
        ));
        let queue: Arc<dyn SkillJobQueue> = Arc::new(StoreSkillQueue {
            store: Arc::clone(&store),
            clock: Arc::clone(&clock),
        });
        let sink: Arc<dyn SkillSink> = Arc::new(StoreSkillSink {
            store: Arc::clone(&store),
            clock: Arc::clone(&clock),
            auto_activate: config.auto_activate,
        });
        let worker = match SkillWorker::new(
            queue,
            distiller,
            sink,
            clock,
            SkillWorkerConfig {
                max_attempts: config.max_attempts,
                drain_timeout: Duration::from_millis(config.drain_timeout_ms),
                idle_interval: WORKER_IDLE_INTERVAL,
            },
        ) {
            Ok(worker) => Arc::new(worker),
            Err(error) => {
                tracing::warn!(error = %error, "skills disabled for this run");
                return None;
            }
        };

        let notify = Arc::new(Notify::new());
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(run_worker(worker, Arc::clone(&notify), shutdown.clone()));
        // Process jobs left pending by a previous process, then wait.
        notify.notify_one();

        Some(Self {
            store,
            guide,
            trigger,
            max_source_bytes: config.max_source_bytes,
            include_tool_outputs: config.include_tool_outputs,
            notify,
            shutdown,
            worker_task: Mutex::new(Some(worker_task)),
            drain_timeout: Duration::from_millis(config.drain_timeout_ms),
        })
    }

    /// Renders recalled skills for a fresh turn.
    ///
    /// Returns `None` when nothing is recalled or the guide fails. The guide is
    /// already fail-open; an outer failure is logged and skipped.
    pub(crate) async fn recall(&self, session: &SessionId, query: &str) -> Option<Message> {
        match self.guide.render(session, query).await {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(error = %error, "skill recall failed");
                None
            }
        }
    }

    /// Enqueues a distillation job when the turn triggers.
    ///
    /// The enqueue is idempotent on
    /// `(workspace_scope, session_id, turn_index)`. The method never writes the
    /// agent event stream, never awaits distillation, and never propagates a
    /// failure to the caller.
    pub(crate) async fn after_completed_turn(
        &self,
        session_id: &SessionId,
        turn_index: u64,
        user_goal: &str,
        final_answer: &str,
        signals: &TurnSignals,
        messages: &[Message],
    ) {
        if !self.trigger.should_trigger(signals) {
            return;
        }

        let summaries = tool_summaries(messages, self.include_tool_outputs);
        let input = DistillSourceInput::new(
            session_id.clone(),
            turn_index,
            user_goal,
            final_answer,
            signals,
        )
        .with_tool_summaries(summaries);
        let source = DistillSource::new(input, self.max_source_bytes);
        let job = match NewSkillJob::new(
            self.store.workspace_scope().clone(),
            session_id.clone(),
            turn_index,
            source.to_persisted_json(),
            now(),
        ) {
            Ok(job) => job,
            Err(error) => {
                tracing::warn!(
                    session = %session_id,
                    turn_index,
                    error = %error,
                    "failed to build a skill job"
                );
                return;
            }
        };

        match self.store.enqueue_job(job).await {
            Ok(_) => self.notify.notify_one(),
            Err(error) => {
                tracing::warn!(
                    session = %session_id,
                    turn_index,
                    error = %error,
                    "failed to enqueue a skill job"
                );
            }
        }
    }

    /// Stops the worker and waits up to the configured drain timeout.
    ///
    /// A timeout aborts the background task and logs a warning. The exit code
    /// is never changed; an interrupted job remains recoverable through the
    /// queue's stale-claim path.
    pub(crate) async fn drain(&self) {
        self.shutdown.cancel();
        let handle = self
            .worker_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(mut handle) = handle else {
            return;
        };

        match tokio::time::timeout(self.drain_timeout, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "skill worker task failed");
            }
            Err(_) => {
                handle.abort();
                tracing::warn!(
                    timeout_ms = self.drain_timeout.as_millis(),
                    "skill worker drain timed out; the in-flight job remains recoverable"
                );
            }
        }
    }
}

/// Runs the notification-driven worker until drained.
///
/// The worker processes runnable jobs until the queue is idle, then waits for
/// the next enqueue. Cancellation starts the drain: the worker keeps processing
/// runnable jobs until the queue is idle instead of dropping work that was
/// enqueued just before exit. [`SkillsAssembly::drain`] bounds that wait.
async fn run_worker(worker: Arc<SkillWorker>, notify: Arc<Notify>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                drain_queue(&worker).await;
                return;
            }
            _ = notify.notified() => {
                drain_queue(&worker).await;
                if shutdown.is_cancelled() {
                    return;
                }
            }
        }
    }
}

/// Processes runnable jobs until the queue is idle or a backend call fails.
async fn drain_queue(worker: &SkillWorker) {
    loop {
        match worker.run_once().await {
            Ok(WorkerOutcome::Idle) => return,
            Ok(WorkerOutcome::Completed | WorkerOutcome::Failed) => continue,
            Err(error) => {
                tracing::warn!(error = %error, "skill worker stopped this pass");
                return;
            }
        }
    }
}

/// Opens the skill store, creating parent directories when needed.
async fn open_store(
    config: &SkillsConfig,
    root: &Path,
) -> Result<Arc<SqliteSkillStore>, SkillOpenError> {
    let database_path = resolve_path(root, &config.path);
    let workspace_root = root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        if let Some(parent) = database_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|source| SkillOpenError::Directory {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let workspace = std::fs::canonicalize(&workspace_root).unwrap_or(workspace_root);
        SqliteSkillStore::open(&database_path, workspace)
            .map(Arc::new)
            .map_err(SkillOpenError::Store)
    })
    .await
    .map_err(SkillOpenError::Task)?
}

fn resolve_path(root: &Path, configured: &Path) -> PathBuf {
    if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        root.join(configured)
    }
}

/// Retrieval adapter from the SQLite library to [`SkillSourceTrait`].
struct StoreSkillSource {
    store: Arc<SqliteSkillStore>,
}

#[async_trait]
impl SkillSourceTrait for StoreSkillSource {
    async fn recall(
        &self,
        query: &str,
        limit: usize,
        min_score: Option<f64>,
    ) -> Result<Vec<SkillContent>, SkillSourceError> {
        let hits = self
            .store
            .search_keyword(query, limit, min_score)
            .await
            .map_err(|error| SkillSourceError::Backend {
                message: error.to_string(),
            })?;
        Ok(hits
            .into_iter()
            .map(|(skill, _score)| SkillContent {
                name: skill.name().to_owned(),
                description: skill.description().to_owned(),
                instructions: skill.instructions().to_owned(),
            })
            .collect())
    }
}

/// Queue adapter from the SQLite job table to [`SkillJobQueue`].
struct StoreSkillQueue {
    store: Arc<SqliteSkillStore>,
    clock: Arc<dyn WorkerClock>,
}

#[async_trait]
impl SkillJobQueue for StoreSkillQueue {
    async fn claim(&self) -> Result<Option<SkillJob>, SkillQueueError> {
        loop {
            let now = self.clock.now();
            let stored = self
                .store
                .claim_next_job(Timestamp::from_millis(now.as_millis()), STALE_CLAIM_AFTER)
                .await
                .map_err(queue_backend)?;
            let Some(job) = stored else {
                return Ok(None);
            };

            match rehydrate_job(&job) {
                Ok(job) => return Ok(Some(job)),
                Err(error) => {
                    tracing::warn!(
                        job = %job.id(),
                        error = %error,
                        "discarding a skill job with an invalid persisted source"
                    );
                    self.store
                        .fail_job(
                            job.id(),
                            Timestamp::from_millis(now.as_millis()),
                            "invalid persisted skill source",
                        )
                        .await
                        .map_err(queue_backend)?;
                }
            }
        }
    }

    async fn complete(&self, job: &SkillJob, at: WorkerTimestamp) -> Result<(), SkillQueueError> {
        self.store
            .complete_job(
                jessica_memory::SkillJobId::from_raw(job.id().get()),
                Timestamp::from_millis(at.as_millis()),
            )
            .await
            .map_err(queue_backend)?;
        Ok(())
    }

    async fn fail(
        &self,
        job: &SkillJob,
        at: WorkerTimestamp,
        last_error: &str,
    ) -> Result<(), SkillQueueError> {
        self.store
            .fail_job(
                jessica_memory::SkillJobId::from_raw(job.id().get()),
                Timestamp::from_millis(at.as_millis()),
                last_error,
            )
            .await
            .map_err(queue_backend)?;
        Ok(())
    }
}

/// Rehydrates one stored job into the harness representation.
fn rehydrate_job(job: &StoredSkillJob) -> Result<SkillJob, String> {
    let source =
        DistillSource::from_persisted_json(job.source_json()).map_err(|error| error.to_string())?;
    if source.session_id() != job.session_id() || source.turn_index() != job.turn_index() {
        return Err("persisted source key does not match the job key".to_owned());
    }
    Ok(SkillJob::new(
        SkillJobId::from_raw(job.id().get()),
        job.workspace_scope().as_str(),
        source,
    ))
}

fn queue_backend(error: jessica_memory::SkillJobError) -> SkillQueueError {
    SkillQueueError::Backend {
        message: error.to_string(),
    }
}

/// Sink adapter from distilled skills to the append-only library.
struct StoreSkillSink {
    store: Arc<SqliteSkillStore>,
    clock: Arc<dyn WorkerClock>,
    auto_activate: bool,
}

#[async_trait]
impl SkillSink for StoreSkillSink {
    async fn write(&self, job: &SkillJob, skill: &DistilledSkill) -> Result<(), SkillSinkError> {
        let source = job.source();
        let provenance = StoredSkillSource::new(
            source.session_id().clone(),
            source.turn_index(),
            u64::try_from(source.tool_call_count()).unwrap_or(u64::MAX),
            u64::try_from(source.assistant_rounds()).unwrap_or(u64::MAX),
            u64::try_from(source.acceptance_failures()).unwrap_or(u64::MAX),
        );
        let new_skill = NewSkill::new(
            self.store.workspace_scope().clone(),
            provenance,
            &skill.name,
            &skill.description,
            &skill.instructions,
            skill.tags.clone(),
            Timestamp::from_millis(self.clock.now().as_millis()),
        )
        .map_err(classify_sink_error)?;
        let new_skill = new_skill.with_auto_activate(self.auto_activate);

        match self.store.append(new_skill).await {
            Ok(_) | Err(SkillLogError::Duplicate { .. }) => Ok(()),
            Err(error) => Err(classify_sink_error(error)),
        }
    }
}

/// Classifies a storage failure as retryable or permanent.
fn classify_sink_error(error: SkillLogError) -> SkillSinkError {
    let retryable = matches!(
        error,
        SkillLogError::Insert(_)
            | SkillLogError::Query(_)
            | SkillLogError::ConnectionPoisoned
            | SkillLogError::TaskJoin { .. }
            | SkillLogError::DuplicateLookup
    );
    if retryable {
        SkillSinkError::Retryable {
            message: error.to_string(),
        }
    } else {
        SkillSinkError::Permanent {
            message: error.to_string(),
        }
    }
}

/// Wall-clock implementation used by the CLI worker and sink.
struct SystemWorkerClock;

impl WorkerClock for SystemWorkerClock {
    fn now(&self) -> WorkerTimestamp {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        WorkerTimestamp::from_millis(millis)
    }
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
fn now() -> Timestamp {
    Timestamp::from_millis(SystemWorkerClock.now().as_millis())
}

/// Builds bounded summaries for every tool call in `messages`.
///
/// One summary is produced per call, in call order. The summary always carries
/// the tool name and outcome. When `include_outputs` is true, a whitespace-
/// collapsed excerpt of at most [`TOOL_OUTPUT_EXCERPT_BYTES`] bytes is appended;
/// the complete output is never copied.
pub(crate) fn tool_summaries(messages: &[Message], include_outputs: bool) -> Vec<String> {
    let mut summaries = Vec::new();
    for message in messages {
        for call in &message.tool_calls {
            let result = messages.iter().find(|candidate| {
                candidate.role == Role::Tool
                    && candidate.tool_call_id.as_deref() == Some(call.id.as_str())
            });
            // The event pump renders failed tool results with this prefix;
            // messages carry no separate error flag.
            let status = match result {
                Some(result) if result.content.starts_with("tool error: ") => "error",
                Some(_) => "ok",
                None => "no result",
            };
            let mut summary = format!("{}: {status}", call.name);
            if include_outputs && let Some(result) = result {
                let excerpt = collapse_whitespace(&result.content);
                let excerpt = truncate_on_char_boundary(&excerpt, TOOL_OUTPUT_EXCERPT_BYTES);
                if !excerpt.is_empty() {
                    summary.push_str(": ");
                    summary.push_str(excerpt);
                }
            }
            summaries.push(summary);
        }
    }
    summaries
}

/// Collapses every whitespace run into one space.
fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
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

/// A manual skill-review command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SkillCommand {
    /// Lists live skills, optionally filtered by status.
    List {
        /// Status filter; `None` lists every live skill.
        status: Option<SkillStatus>,
        /// Maximum rows to print.
        limit: usize,
    },
    /// Prints one skill, including soft-deleted rows.
    Show {
        /// Skill identifier.
        id: SkillId,
    },
    /// Activates one draft skill.
    Activate {
        /// Skill identifier.
        id: SkillId,
    },
    /// Soft-deletes one skill.
    Delete {
        /// Skill identifier.
        id: SkillId,
    },
}

/// Runs one manual skill-review command without a provider or wasm tools.
pub(crate) async fn run_skill_command(
    config: &SkillsConfig,
    root: &Path,
    command: SkillCommand,
) -> Result<(), SkillCommandError> {
    if !config.enabled {
        return Err(SkillCommandError::Disabled);
    }
    let store = open_store(config, root)
        .await
        .map_err(|error| SkillCommandError::Open {
            message: error.to_string(),
        })?;

    match command {
        SkillCommand::List { status, limit } => {
            let skills = match status {
                Some(status) => store.list_by_status(status, limit).await?,
                None => store.list_recent(limit).await?,
            };
            if skills.is_empty() {
                println!("no skills");
                return Ok(());
            }
            for skill in skills {
                println!(
                    "{}\t{}\t{}\t{}",
                    skill.id().get(),
                    skill.status(),
                    skill.created_at().as_millis(),
                    skill.name()
                );
            }
        }
        SkillCommand::Show { id } => {
            let skill = store
                .get(id)
                .await?
                .ok_or(SkillCommandError::NotFound { id: id.get() })?;
            print_skill(&skill);
        }
        SkillCommand::Activate { id } => {
            store.activate(id, now()).await?;
            let skill = store
                .get(id)
                .await?
                .ok_or(SkillCommandError::NotFound { id: id.get() })?;
            println!("skill {}: {}", skill.id().get(), skill.status());
        }
        SkillCommand::Delete { id } => {
            store.soft_delete(id, now()).await?;
            println!("skill {}: deleted", id.get());
        }
    }
    Ok(())
}

/// Prints one skill without provenance JSON or raw tool outputs.
fn print_skill(skill: &Skill) {
    println!("id: {}", skill.id().get());
    println!("status: {}", skill.status());
    println!("created_at_ms: {}", skill.created_at().as_millis());
    println!(
        "activated_at_ms: {}",
        skill
            .activated_at()
            .map_or_else(|| "-".to_owned(), |at| at.as_millis().to_string())
    );
    println!(
        "deleted_at_ms: {}",
        skill
            .deleted_at()
            .map_or_else(|| "-".to_owned(), |at| at.as_millis().to_string())
    );
    println!("source_session: {}", skill.source().session_id);
    println!("source_turn_index: {}", skill.source().turn_index);
    println!("name: {}", skill.name());
    println!("description: {}", skill.description());
    println!("instructions: {}", skill.instructions());
    println!("tags: {}", skill.tags().join(", "));
}

/// Default row limit for `jessica skills list`.
pub(crate) const fn default_list_limit() -> usize {
    DEFAULT_LIST_LIMIT
}

/// Errors raised while opening or executing a skill-review command.
#[derive(Debug, Error)]
pub(crate) enum SkillCommandError {
    /// The `[skills]` section is disabled.
    #[error("skills are disabled; set `[skills] enabled = true` to review them")]
    Disabled,
    /// The skill store could not be opened.
    #[error("failed to open the skill store: {message}")]
    Open {
        /// Underlying open failure.
        message: String,
    },
    /// The requested skill does not exist.
    #[error("skill {id} does not exist")]
    NotFound {
        /// Requested identifier.
        id: i64,
    },
    /// A storage operation failed.
    #[error(transparent)]
    Store(#[from] SkillLogError),
}

/// Errors raised while opening the skill database.
#[derive(Debug, Error)]
enum SkillOpenError {
    /// The parent directory could not be created.
    #[error("failed to create skill directory `{path}`: {source}")]
    Directory {
        /// Directory that could not be created.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The skill store could not be opened.
    #[error("failed to open skill store: {0}")]
    Store(#[from] SqliteSkillStoreError),
    /// The blocking open task failed.
    #[error("skill store open task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use jessica_core::types::SessionEndReason;
    use jessica_harness::{GuideError, TurnSignals, WorkerTimestamp};
    use jessica_memory::{SkillLog, WorkspaceScope};

    use super::*;

    /// Temporary directory removed when the test finishes.
    struct TempDir {
        /// Absolute path of the directory.
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "jessica-cli-skills-{label}-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temporary directory should be created");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Test clock with a fixed timestamp.
    struct FixedClock(WorkerTimestamp);

    impl WorkerClock for FixedClock {
        fn now(&self) -> WorkerTimestamp {
            self.0
        }
    }

    fn enabled_config() -> SkillsConfig {
        SkillsConfig {
            enabled: true,
            ..SkillsConfig::default()
        }
    }

    fn scope(database: &Path, workspace: &Path) -> WorkspaceScope {
        let canonical = std::fs::canonicalize(workspace).expect("workspace must canonicalize");
        SqliteSkillStore::open(database, canonical)
            .expect("skill store should open")
            .workspace_scope()
            .clone()
    }

    #[tokio::test]
    async fn disabled_skills_never_create_the_database_or_its_directory() {
        let temp = TempDir::new("disabled");
        let config = SkillsConfig {
            enabled: false,
            ..enabled_config()
        };
        let provider = Arc::new(UnusedProvider) as Arc<dyn LlmProvider>;

        let assembly = SkillsAssembly::open(&config, &temp.path, provider).await;

        assert!(assembly.is_none());
        assert!(!temp.path.join(".jessica").exists());
    }

    #[tokio::test]
    async fn enabled_skills_resolve_relative_paths_and_create_parent_directories() {
        let temp = TempDir::new("relative");
        let config = SkillsConfig {
            path: PathBuf::from(".jessica/skills.db"),
            ..enabled_config()
        };
        let provider = Arc::new(UnusedProvider) as Arc<dyn LlmProvider>;
        let assembly = SkillsAssembly::open(&config, &temp.path, provider)
            .await
            .expect("valid skills configuration should open");
        assert!(temp.path.join(".jessica/skills.db").is_file());

        assembly.drain().await;
    }

    #[tokio::test]
    async fn open_failure_is_fail_open_and_leaves_the_invalid_path_alone() {
        let temp = TempDir::new("open-failure");
        let blocker = temp.path.join(".jessica");
        std::fs::write(&blocker, b"not a directory").expect("blocking file should be written");
        let config = SkillsConfig {
            path: PathBuf::from(".jessica/skills.db"),
            ..enabled_config()
        };
        let provider = Arc::new(UnusedProvider) as Arc<dyn LlmProvider>;

        let assembly = SkillsAssembly::open(&config, &temp.path, provider).await;

        assert!(
            assembly.is_none(),
            "open failures must degrade to no skills"
        );
        assert!(blocker.is_file());
    }

    /// Guide that always fails to render.
    struct FailingGuide;

    #[async_trait]
    impl Guide for FailingGuide {
        async fn render(
            &self,
            _session: &SessionId,
            _query: &str,
        ) -> Result<Option<Message>, GuideError> {
            Err(GuideError::Render {
                message: "skill backend unavailable".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn recall_failure_is_fail_open_and_injects_nothing() {
        let temp = TempDir::new("recall-failure");
        let config = SkillsConfig {
            path: PathBuf::from("skills.db"),
            ..enabled_config()
        };
        let provider = Arc::new(UnusedProvider) as Arc<dyn LlmProvider>;
        let mut assembly = SkillsAssembly::open(&config, &temp.path, provider)
            .await
            .expect("valid skills configuration should open");
        assembly.guide = Arc::new(FailingGuide);

        assert!(
            assembly
                .recall(&SessionId::new("recall-failure-session"), "query")
                .await
                .is_none(),
            "a failing guide must degrade to no injected skills"
        );

        assembly.drain().await;
    }

    #[tokio::test]
    async fn the_source_adapter_returns_only_active_non_deleted_skills() {
        let temp = TempDir::new("source");
        let config = SkillsConfig {
            path: PathBuf::from("skills.db"),
            ..enabled_config()
        };
        let database = temp.path.join("skills.db");
        let workspace = scope(&database, &temp.path);
        let log = SkillLog::open(&database).expect("skill log should open");
        let draft = NewSkill::new(
            workspace.clone(),
            StoredSkillSource::new(SessionId::new("s"), 0, 1, 1, 0),
            "ORANGE draft",
            "draft description",
            "draft instructions",
            Vec::new(),
            Timestamp::from_millis(1),
        )
        .expect("draft should construct");
        let active = NewSkill::new(
            workspace,
            StoredSkillSource::new(SessionId::new("s"), 1, 1, 1, 0),
            "ORANGE active",
            "active description",
            "active instructions",
            Vec::new(),
            Timestamp::from_millis(2),
        )
        .expect("active skill should construct")
        .with_auto_activate(true);
        log.append(draft).expect("draft should append");
        log.append(active).expect("active skill should append");
        drop(log);

        let store = open_store(&config, &temp.path)
            .await
            .expect("store should open");
        let source = StoreSkillSource { store };
        let hits = source
            .recall("ORANGE", 10, None)
            .await
            .expect("recall should succeed");

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "ORANGE active");
    }

    #[tokio::test]
    async fn the_sink_is_idempotent_and_duplicate_writes_keep_one_skill() {
        let temp = TempDir::new("sink");
        let config = SkillsConfig {
            path: PathBuf::from("skills.db"),
            ..enabled_config()
        };
        let store = open_store(&config, &temp.path)
            .await
            .expect("store should open");
        let clock: Arc<dyn WorkerClock> = Arc::new(FixedClock(WorkerTimestamp::from_millis(5)));
        let sink = StoreSkillSink {
            store: Arc::clone(&store),
            clock,
            auto_activate: false,
        };
        let source = DistillSource::new(
            DistillSourceInput::new(
                SessionId::new("idempotent-session"),
                0,
                "goal",
                "answer",
                &TurnSignals::new(SessionEndReason::Completed, 1, 1),
            ),
            1024,
        );
        let job = SkillJob::new(
            SkillJobId::from_raw(1),
            store.workspace_scope().as_str(),
            source,
        );
        let skill = DistilledSkill {
            name: "ORANGE skill".to_owned(),
            description: "description".to_owned(),
            instructions: "instructions".to_owned(),
            tags: vec!["tag".to_owned()],
        };

        sink.write(&job, &skill)
            .await
            .expect("first write should succeed");
        sink.write(&job, &skill)
            .await
            .expect("duplicate write must be absorbed");

        let skills = store.list_recent(10).await.expect("skills should list");
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].status(), SkillStatus::Draft);
    }

    #[tokio::test]
    async fn auto_activate_only_changes_the_initial_status_of_new_skills() {
        let temp = TempDir::new("auto-activate");
        let config = SkillsConfig {
            path: PathBuf::from("skills.db"),
            auto_activate: true,
            ..enabled_config()
        };
        let store = open_store(&config, &temp.path)
            .await
            .expect("store should open");
        let clock: Arc<dyn WorkerClock> = Arc::new(FixedClock(WorkerTimestamp::from_millis(5)));
        let sink = StoreSkillSink {
            store: Arc::clone(&store),
            clock,
            auto_activate: true,
        };
        let source = DistillSource::new(
            DistillSourceInput::new(
                SessionId::new("auto-session"),
                0,
                "goal",
                "answer",
                &TurnSignals::new(SessionEndReason::Completed, 1, 1),
            ),
            1024,
        );
        let job = SkillJob::new(
            SkillJobId::from_raw(1),
            store.workspace_scope().as_str(),
            source,
        );
        let skill = DistilledSkill {
            name: "auto skill".to_owned(),
            description: "description".to_owned(),
            instructions: "instructions".to_owned(),
            tags: Vec::new(),
        };

        sink.write(&job, &skill)
            .await
            .expect("write should succeed");

        let skills = store.list_recent(10).await.expect("skills should list");
        assert_eq!(skills[0].status(), SkillStatus::Active);
        assert!(skills[0].activated_at().is_some());
    }

    #[tokio::test]
    async fn the_queue_rehydrates_persisted_sources_without_double_escaping() {
        let temp = TempDir::new("queue");
        let config = SkillsConfig {
            path: PathBuf::from("skills.db"),
            ..enabled_config()
        };
        let database = temp.path.join("skills.db");
        let store = open_store(&config, &temp.path)
            .await
            .expect("store should open");
        let source = DistillSource::new(
            DistillSourceInput::new(
                SessionId::new("queue-session"),
                3,
                "<goal> &lt;",
                "answer",
                &TurnSignals::new(SessionEndReason::Completed, 2, 1),
            ),
            1024,
        );
        let persisted = source.to_persisted_json();
        let log = SkillLog::open(&database).expect("skill log should open");
        let job = NewSkillJob::new(
            store.workspace_scope().clone(),
            SessionId::new("queue-session"),
            3,
            persisted.clone(),
            Timestamp::from_millis(1),
        )
        .expect("job should construct");
        log.enqueue_job(job).expect("job should enqueue");
        drop(log);

        let clock: Arc<dyn WorkerClock> = Arc::new(FixedClock(WorkerTimestamp::from_millis(2)));
        let queue = StoreSkillQueue {
            store: Arc::clone(&store),
            clock,
        };
        let claimed = queue
            .claim()
            .await
            .expect("claim should succeed")
            .expect("one job should be runnable");

        assert_eq!(claimed.source().user_goal(), source.user_goal());
        assert_eq!(claimed.source().to_persisted_json(), persisted);
    }

    #[test]
    fn tool_summaries_are_bounded_and_never_copy_full_outputs() {
        let output = "x".repeat(4096);
        let messages = vec![
            Message::assistant_with_tool_calls(
                "",
                vec![jessica_core::types::ToolCall::new(
                    "call-1",
                    "read_file",
                    serde_json::json!({"path":"big.txt"}),
                )],
            ),
            Message::tool("call-1", output),
        ];

        let summaries = tool_summaries(&messages, true);

        assert_eq!(summaries.len(), 1);
        assert!(summaries[0].starts_with("read_file: ok: "));
        assert!(summaries[0].len() <= "read_file: ok: ".len() + TOOL_OUTPUT_EXCERPT_BYTES);
        assert!(!summaries[0].contains(&"x".repeat(TOOL_OUTPUT_EXCERPT_BYTES + 1)));
    }

    #[test]
    fn tool_summaries_omit_outputs_by_default_but_keep_names_and_statuses() {
        let messages = vec![
            Message::assistant_with_tool_calls(
                "",
                vec![jessica_core::types::ToolCall::new(
                    "call-1",
                    "bash",
                    serde_json::json!({"command":"false"}),
                )],
            ),
            Message::tool("call-1", "tool error: exit 1 SECRET-OUTPUT"),
        ];

        let summaries = tool_summaries(&messages, false);

        assert_eq!(summaries, vec!["bash: error"]);
        assert!(!summaries[0].contains("SECRET-OUTPUT"));
    }

    #[tokio::test]
    async fn skills_list_show_activate_and_delete_are_non_destructive() {
        let temp = TempDir::new("commands");
        let config = SkillsConfig {
            path: PathBuf::from("skills.db"),
            ..enabled_config()
        };
        let database = temp.path.join("skills.db");
        let workspace = scope(&database, &temp.path);
        let log = SkillLog::open(&database).expect("skill log should open");
        let id = log
            .append(
                NewSkill::new(
                    workspace,
                    StoredSkillSource::new(SessionId::new("command-session"), 0, 1, 1, 0),
                    "command skill",
                    "description",
                    "instructions",
                    vec!["tag".to_owned()],
                    Timestamp::from_millis(1),
                )
                .expect("skill should construct"),
            )
            .expect("skill should append");
        drop(log);

        run_skill_command(
            &config,
            &temp.path,
            SkillCommand::List {
                status: Some(SkillStatus::Draft),
                limit: default_list_limit(),
            },
        )
        .await
        .expect("list should succeed");
        run_skill_command(&config, &temp.path, SkillCommand::Show { id })
            .await
            .expect("show should succeed");
        run_skill_command(&config, &temp.path, SkillCommand::Activate { id })
            .await
            .expect("activate should succeed");
        run_skill_command(&config, &temp.path, SkillCommand::Delete { id })
            .await
            .expect("delete should succeed");

        let store = open_store(&config, &temp.path)
            .await
            .expect("store should reopen");
        let skill = store
            .get(id)
            .await
            .expect("lookup should succeed")
            .expect("soft-deleted skill should still exist");
        assert!(skill.deleted_at().is_some());
        assert_eq!(skill.status(), SkillStatus::Active);
        assert!(
            store
                .list_recent(10)
                .await
                .expect("list should succeed")
                .is_empty(),
            "soft-deleted skills must not be listed"
        );
    }

    #[tokio::test]
    async fn disabled_skills_reject_manual_commands_without_creating_a_database() {
        let temp = TempDir::new("commands-disabled");
        let config = SkillsConfig {
            enabled: false,
            path: PathBuf::from(".jessica/skills.db"),
            ..SkillsConfig::default()
        };

        let error = run_skill_command(
            &config,
            &temp.path,
            SkillCommand::List {
                status: None,
                limit: 10,
            },
        )
        .await
        .expect_err("disabled skills must reject review commands");

        assert!(matches!(error, SkillCommandError::Disabled));
        assert!(!temp.path.join(".jessica").exists());
    }

    #[tokio::test]
    async fn missing_skills_report_a_structured_not_found_error() {
        let temp = TempDir::new("missing");
        let config = SkillsConfig {
            path: PathBuf::from("skills.db"),
            ..enabled_config()
        };
        let error = run_skill_command(
            &config,
            &temp.path,
            SkillCommand::Show {
                id: SkillId::from_raw(999),
            },
        )
        .await
        .expect_err("missing skills must fail");

        assert!(matches!(error, SkillCommandError::NotFound { id: 999 }));
    }

    /// Provider that must never be called by these tests.
    struct UnusedProvider;

    #[async_trait]
    impl jessica_core::llm::LlmProvider for UnusedProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[jessica_core::tool::ToolSchema],
        ) -> Result<jessica_core::llm::LlmStream, jessica_core::llm::LlmError> {
            Err(jessica_core::llm::LlmError::Request {
                message: "unused provider".to_owned(),
            })
        }
    }
}
