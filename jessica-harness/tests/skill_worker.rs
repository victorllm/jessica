//! Behaviour tests for the single-worker skill-job consumer.

use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::types::{SessionEndReason, SessionId};
use jessica_harness::{
    DEFAULT_MAX_ATTEMPTS, DistillError, DistillSource, DistillSourceInput, DistilledSkill,
    SkillDistiller, SkillJob, SkillJobId, SkillJobQueue, SkillQueueError, SkillSink,
    SkillSinkError, SkillWorker, SkillWorkerConfig, SkillWorkerError, TurnSignals, WorkerClock,
    WorkerOutcome, WorkerTimestamp,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Timestamp returned by every test clock.
const NOW: WorkerTimestamp = WorkerTimestamp::from_millis(1_700_000_000_123);

/// One observable side effect in the fake queue, distiller, or sink.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TraceEvent {
    /// The queue handed out a job.
    Claim(SkillJobId),
    /// The distiller was called.
    Distill,
    /// The sink was called.
    Write(SkillJobId),
    /// The queue accepted a completion.
    Complete(SkillJobId, WorkerTimestamp),
    /// The queue accepted a failure with a bounded error.
    Fail(SkillJobId, WorkerTimestamp, String),
}

/// Shared side-effect trace.
type Trace = Arc<Mutex<Vec<TraceEvent>>>;

/// Creates an empty trace.
fn trace() -> Trace {
    Arc::new(Mutex::new(Vec::new()))
}

/// Appends one event to the trace.
fn push(trace: &Trace, event: TraceEvent) {
    trace
        .lock()
        .expect("trace mutex should not be poisoned")
        .push(event);
}

/// Returns a snapshot of the trace.
fn events(trace: &Trace) -> Vec<TraceEvent> {
    trace
        .lock()
        .expect("trace mutex should not be poisoned")
        .clone()
}

/// Builds a job identifier.
fn id(value: i64) -> SkillJobId {
    SkillJobId::from_raw(value)
}

/// Builds a bounded source for `session` and `turn`.
fn source(session: &str, turn: u64) -> DistillSource {
    let signals = TurnSignals::new(SessionEndReason::Completed, 3, 2);
    DistillSource::new(
        DistillSourceInput::new(SessionId::new(session), turn, "goal", "answer", &signals),
        1024,
    )
}

/// Builds a claimed job in the fixed test workspace.
fn job(job_id: i64, session: &str, turn: u64) -> SkillJob {
    SkillJob::new(id(job_id), "workspace-a", source(session, turn))
}

/// Builds a valid distilled skill draft.
fn skill(name: &str) -> DistilledSkill {
    DistilledSkill {
        name: name.to_owned(),
        description: "Deploys a service safely.".to_owned(),
        instructions: "1. Build\n2. Deploy".to_owned(),
        tags: vec!["deploy".to_owned(), "ops".to_owned()],
    }
}

/// A deterministic clock that counts reads.
struct FixedClock {
    /// Value returned to every caller.
    value: WorkerTimestamp,
    /// Number of `now` calls.
    reads: AtomicUsize,
}

impl FixedClock {
    /// Creates a clock fixed at `value`.
    fn fixed(value: WorkerTimestamp) -> Self {
        Self {
            value,
            reads: AtomicUsize::new(0),
        }
    }

    /// Returns how many timestamps were requested.
    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl WorkerClock for FixedClock {
    fn now(&self) -> WorkerTimestamp {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.value
    }
}

/// Queue mock that records every claim and terminal transition.
struct TestQueue {
    /// Jobs returned by successive claims.
    jobs: Mutex<VecDeque<SkillJob>>,
    /// Shared side-effect trace.
    trace: Trace,
    /// Number of claim calls.
    claim_calls: AtomicUsize,
    /// Number of complete calls.
    complete_calls: AtomicUsize,
    /// Number of fail calls.
    fail_calls: AtomicUsize,
    /// When set, `complete` returns this backend error.
    complete_failure: Mutex<Option<String>>,
    /// Wakes tests waiting for the first claim.
    claim_notify: Notify,
}

impl TestQueue {
    /// Creates a queue over `jobs`.
    fn with_jobs(jobs: Vec<SkillJob>, trace: Trace) -> Self {
        Self {
            jobs: Mutex::new(jobs.into()),
            trace,
            claim_calls: AtomicUsize::new(0),
            complete_calls: AtomicUsize::new(0),
            fail_calls: AtomicUsize::new(0),
            complete_failure: Mutex::new(None),
            claim_notify: Notify::new(),
        }
    }

    /// Makes every subsequent `complete` fail with `message`.
    fn fail_complete_with(&self, message: &str) {
        *self
            .complete_failure
            .lock()
            .expect("complete failure mutex should not be poisoned") = Some(message.to_owned());
    }

    /// Returns the number of claim calls.
    fn claim_calls(&self) -> usize {
        self.claim_calls.load(Ordering::SeqCst)
    }

    /// Returns the number of complete calls.
    fn complete_calls(&self) -> usize {
        self.complete_calls.load(Ordering::SeqCst)
    }

    /// Returns the number of fail calls.
    fn fail_calls(&self) -> usize {
        self.fail_calls.load(Ordering::SeqCst)
    }

    /// Waits until at least one claim has been attempted.
    async fn wait_until_claimed(&self) {
        self.claim_notify.notified().await;
    }
}

#[async_trait]
impl SkillJobQueue for TestQueue {
    async fn claim(&self) -> Result<Option<SkillJob>, SkillQueueError> {
        self.claim_calls.fetch_add(1, Ordering::SeqCst);
        self.claim_notify.notify_one();
        let job = self
            .jobs
            .lock()
            .expect("job queue mutex should not be poisoned")
            .pop_front();
        if let Some(claimed) = &job {
            push(&self.trace, TraceEvent::Claim(claimed.id()));
        }
        Ok(job)
    }

    async fn complete(&self, job: &SkillJob, at: WorkerTimestamp) -> Result<(), SkillQueueError> {
        self.complete_calls.fetch_add(1, Ordering::SeqCst);
        push(&self.trace, TraceEvent::Complete(job.id(), at));
        match self
            .complete_failure
            .lock()
            .expect("complete failure mutex should not be poisoned")
            .as_deref()
        {
            Some(message) => Err(SkillQueueError::Backend {
                message: message.to_owned(),
            }),
            None => Ok(()),
        }
    }

    async fn fail(
        &self,
        job: &SkillJob,
        at: WorkerTimestamp,
        last_error: &str,
    ) -> Result<(), SkillQueueError> {
        self.fail_calls.fetch_add(1, Ordering::SeqCst);
        push(
            &self.trace,
            TraceEvent::Fail(job.id(), at, last_error.to_owned()),
        );
        Ok(())
    }
}

/// One scripted distillation response.
enum DistillStep {
    /// Return a valid draft.
    Skill(DistilledSkill),
    /// Return a retryable error.
    Retryable(String),
    /// Return a permanent error.
    Permanent(String),
}

/// Distiller mock that serves scripted responses in order.
struct ScriptedDistiller {
    /// Remaining responses; each call consumes one.
    steps: Mutex<VecDeque<DistillStep>>,
    /// Shared side-effect trace.
    trace: Trace,
    /// Number of distill calls.
    calls: AtomicUsize,
}

impl ScriptedDistiller {
    /// Creates a distiller over `steps`.
    fn with_steps(steps: Vec<DistillStep>, trace: Trace) -> Self {
        Self {
            steps: Mutex::new(steps.into()),
            trace,
            calls: AtomicUsize::new(0),
        }
    }

    /// Returns the number of distill calls.
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SkillDistiller for ScriptedDistiller {
    async fn distill(&self, _source: &DistillSource) -> Result<DistilledSkill, DistillError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        push(&self.trace, TraceEvent::Distill);
        let step = self
            .steps
            .lock()
            .expect("distill step mutex should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                DistillStep::Permanent("no scripted distillation response".to_owned())
            });
        match step {
            DistillStep::Skill(skill) => Ok(skill),
            DistillStep::Retryable(message) => Err(DistillError::Retryable { message }),
            DistillStep::Permanent(message) => Err(DistillError::Permanent { message }),
        }
    }
}

/// Distiller mock that blocks until the test releases it.
struct ControlledDistiller {
    /// Notified once the call has started.
    entered: Notify,
    /// Releases the call.
    release: Notify,
    /// Shared side-effect trace.
    trace: Trace,
    /// Draft returned after release.
    skill: DistilledSkill,
}

impl ControlledDistiller {
    /// Creates a distiller that returns `skill` after release.
    fn blocking(skill: DistilledSkill, trace: Trace) -> Self {
        Self {
            entered: Notify::new(),
            release: Notify::new(),
            trace,
            skill,
        }
    }

    /// Waits until a distillation call has started.
    async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    /// Allows the in-flight call to finish.
    fn release(&self) {
        self.release.notify_one();
    }
}

#[async_trait]
impl SkillDistiller for ControlledDistiller {
    async fn distill(&self, _source: &DistillSource) -> Result<DistilledSkill, DistillError> {
        push(&self.trace, TraceEvent::Distill);
        self.entered.notify_one();
        self.release.notified().await;
        Ok(self.skill.clone())
    }
}

/// One stored skill with its provenance.
struct StoredSkill {
    /// Workspace partition.
    workspace_scope: String,
    /// Source session.
    session_id: String,
    /// Source turn.
    turn_index: u64,
    /// Persisted draft.
    skill: DistilledSkill,
}

/// Sink mock with the same idempotency contract the worker relies on.
struct IdempotentSink {
    /// Shared side-effect trace.
    trace: Trace,
    /// Uniquely stored skills.
    stored: Mutex<Vec<StoredSkill>>,
    /// Scripted write outcomes; each call consumes one and defaults to `Ok`.
    steps: Mutex<VecDeque<Result<(), SkillSinkError>>>,
    /// Number of write calls.
    calls: AtomicUsize,
}

impl IdempotentSink {
    /// Creates a sink over `steps`.
    fn with_steps(steps: Vec<Result<(), SkillSinkError>>, trace: Trace) -> Self {
        Self {
            trace,
            stored: Mutex::new(Vec::new()),
            steps: Mutex::new(steps.into()),
            calls: AtomicUsize::new(0),
        }
    }

    /// Returns the number of write calls.
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// Returns the uniquely stored drafts.
    fn skills(&self) -> Vec<DistilledSkill> {
        self.stored
            .lock()
            .expect("stored skill mutex should not be poisoned")
            .iter()
            .map(|entry| entry.skill.clone())
            .collect()
    }
}

#[async_trait]
impl SkillSink for IdempotentSink {
    async fn write(&self, job: &SkillJob, skill: &DistilledSkill) -> Result<(), SkillSinkError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        push(&self.trace, TraceEvent::Write(job.id()));

        let step = self
            .steps
            .lock()
            .expect("sink step mutex should not be poisoned")
            .pop_front()
            .unwrap_or(Ok(()));
        step?;

        let mut stored = self
            .stored
            .lock()
            .expect("stored skill mutex should not be poisoned");
        let provenance_exists = stored.iter().any(|entry| {
            entry.workspace_scope == job.workspace_scope()
                && entry.session_id == job.source().session_id().as_str()
                && entry.turn_index == job.source().turn_index()
        });
        let incoming_hash = content_hash(skill);
        let content_exists = stored
            .iter()
            .any(|entry| content_hash(&entry.skill) == incoming_hash);

        if !provenance_exists && !content_exists {
            stored.push(StoredSkill {
                workspace_scope: job.workspace_scope().to_owned(),
                session_id: job.source().session_id().as_str().to_owned(),
                turn_index: job.source().turn_index(),
                skill: skill.clone(),
            });
        }
        Ok(())
    }
}

/// Hashes the reusable content of a draft.
fn content_hash(skill: &DistilledSkill) -> u64 {
    let mut hasher = DefaultHasher::new();
    skill.name.hash(&mut hasher);
    skill.description.hash(&mut hasher);
    skill.instructions.hash(&mut hasher);
    skill.tags.hash(&mut hasher);
    hasher.finish()
}

/// Test configuration with a long idle poll so shutdown races stay observable.
fn config(max_attempts: usize) -> SkillWorkerConfig {
    SkillWorkerConfig {
        max_attempts,
        drain_timeout: Duration::from_secs(1),
        idle_interval: Duration::from_secs(10),
    }
}

/// Fully wired worker plus its observable collaborators.
struct Rig {
    /// Queue mock.
    queue: Arc<TestQueue>,
    /// Distiller mock.
    distiller: Arc<ScriptedDistiller>,
    /// Sink mock.
    sink: Arc<IdempotentSink>,
    /// Injected clock.
    clock: Arc<FixedClock>,
    /// Shared side-effect trace.
    trace: Trace,
    /// Worker under test.
    worker: Arc<SkillWorker>,
}

/// Builds a rig with scripted responses.
fn rig(
    jobs: Vec<SkillJob>,
    distill_steps: Vec<DistillStep>,
    sink_steps: Vec<Result<(), SkillSinkError>>,
    max_attempts: usize,
) -> Rig {
    let trace = trace();
    let queue = Arc::new(TestQueue::with_jobs(jobs, Arc::clone(&trace)));
    let distiller = Arc::new(ScriptedDistiller::with_steps(
        distill_steps,
        Arc::clone(&trace),
    ));
    let sink = Arc::new(IdempotentSink::with_steps(sink_steps, Arc::clone(&trace)));
    let clock = Arc::new(FixedClock::fixed(NOW));
    let worker = SkillWorker::new(
        queue.clone(),
        distiller.clone(),
        sink.clone(),
        clock.clone(),
        config(max_attempts),
    )
    .expect("test worker configuration should be valid");

    Rig {
        queue,
        distiller,
        sink,
        clock,
        trace,
        worker: Arc::new(worker),
    }
}

#[tokio::test]
async fn success_claims_distills_writes_then_completes() {
    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![DistillStep::Skill(skill("deploy"))],
        Vec::new(),
        2,
    );

    let outcome = rig
        .worker
        .run_once()
        .await
        .expect("successful processing should return Ok");

    assert_eq!(outcome, WorkerOutcome::Completed);
    assert_eq!(
        events(&rig.trace),
        vec![
            TraceEvent::Claim(id(1)),
            TraceEvent::Distill,
            TraceEvent::Write(id(1)),
            TraceEvent::Complete(id(1), NOW),
        ],
        "write must happen before the queue records completion"
    );
    assert_eq!(rig.distiller.calls(), 1);
    assert_eq!(rig.sink.calls(), 1);
    assert_eq!(rig.sink.skills(), vec![skill("deploy")]);
    assert_eq!(rig.queue.complete_calls(), 1);
    assert_eq!(rig.queue.fail_calls(), 0);
    assert_eq!(
        rig.clock.reads(),
        1,
        "only the terminal transition should ask for a timestamp"
    );
}

#[tokio::test]
async fn an_empty_queue_is_idle_without_side_effects() {
    let rig = rig(Vec::new(), Vec::new(), Vec::new(), 2);

    let outcome = rig
        .worker
        .run_once()
        .await
        .expect("an empty queue should return Ok");

    assert_eq!(outcome, WorkerOutcome::Idle);
    assert_eq!(rig.queue.claim_calls(), 1);
    assert_eq!(rig.distiller.calls(), 0);
    assert_eq!(rig.sink.calls(), 0);
    assert_eq!(rig.clock.reads(), 0);
    assert!(events(&rig.trace).is_empty());
}

#[tokio::test]
async fn a_retryable_failure_is_retried_and_then_completes() {
    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![
            DistillStep::Retryable("connection reset".to_owned()),
            DistillStep::Skill(skill("deploy")),
        ],
        Vec::new(),
        2,
    );

    let outcome = rig
        .worker
        .run_once()
        .await
        .expect("a transient failure should not surface as an error");

    assert_eq!(outcome, WorkerOutcome::Completed);
    assert_eq!(
        events(&rig.trace),
        vec![
            TraceEvent::Claim(id(1)),
            TraceEvent::Distill,
            TraceEvent::Distill,
            TraceEvent::Write(id(1)),
            TraceEvent::Complete(id(1), NOW),
        ],
        "a retry must restart distillation before retrying the sink"
    );
    assert_eq!(rig.distiller.calls(), 2);
    assert_eq!(rig.sink.calls(), 1);
    assert_eq!(rig.queue.complete_calls(), 1);
    assert_eq!(rig.queue.fail_calls(), 0);
}

#[tokio::test]
async fn retries_are_bounded_by_default_max_attempts() {
    assert_eq!(DEFAULT_MAX_ATTEMPTS, 2);

    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![
            DistillStep::Retryable("first failure".to_owned()),
            DistillStep::Retryable("second failure".to_owned()),
            DistillStep::Skill(skill("never reached")),
        ],
        Vec::new(),
        DEFAULT_MAX_ATTEMPTS,
    );

    let outcome = rig
        .worker
        .run_once()
        .await
        .expect("exhausted retries are a job failure, not a worker error");

    assert_eq!(outcome, WorkerOutcome::Failed);
    assert_eq!(
        events(&rig.trace),
        vec![
            TraceEvent::Claim(id(1)),
            TraceEvent::Distill,
            TraceEvent::Distill,
            TraceEvent::Fail(id(1), NOW, "second failure".to_owned()),
        ],
        "exactly max_attempts distill calls and one terminal failure"
    );
    assert_eq!(rig.distiller.calls(), 2);
    assert_eq!(rig.sink.calls(), 0);
    assert_eq!(rig.queue.complete_calls(), 0);
    assert_eq!(rig.queue.fail_calls(), 1);
}

#[tokio::test]
async fn a_permanent_distill_failure_is_not_retried() {
    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![
            DistillStep::Permanent("invalid JSON".to_owned()),
            DistillStep::Skill(skill("never reached")),
        ],
        Vec::new(),
        5,
    );

    let outcome = rig
        .worker
        .run_once()
        .await
        .expect("a permanent job failure is not a worker error");

    assert_eq!(outcome, WorkerOutcome::Failed);
    assert_eq!(
        events(&rig.trace),
        vec![
            TraceEvent::Claim(id(1)),
            TraceEvent::Distill,
            TraceEvent::Fail(id(1), NOW, "invalid JSON".to_owned()),
        ]
    );
    assert_eq!(rig.distiller.calls(), 1);
    assert_eq!(rig.sink.calls(), 0);
    assert_eq!(rig.queue.fail_calls(), 1);
    assert_eq!(rig.queue.complete_calls(), 0);
}

#[tokio::test]
async fn a_retryable_sink_failure_restarts_the_attempt() {
    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![
            DistillStep::Skill(skill("deploy")),
            DistillStep::Skill(skill("deploy")),
        ],
        vec![Err(SkillSinkError::Retryable {
            message: "database is locked".to_owned(),
        })],
        2,
    );

    let outcome = rig
        .worker
        .run_once()
        .await
        .expect("a transient sink failure should be retried");

    assert_eq!(outcome, WorkerOutcome::Completed);
    assert_eq!(
        events(&rig.trace),
        vec![
            TraceEvent::Claim(id(1)),
            TraceEvent::Distill,
            TraceEvent::Write(id(1)),
            TraceEvent::Distill,
            TraceEvent::Write(id(1)),
            TraceEvent::Complete(id(1), NOW),
        ]
    );
    assert_eq!(rig.distiller.calls(), 2);
    assert_eq!(rig.sink.calls(), 2);
    assert_eq!(rig.sink.skills(), vec![skill("deploy")]);
    assert_eq!(rig.queue.fail_calls(), 0);
}

#[tokio::test]
async fn a_permanent_sink_failure_is_not_retried() {
    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![
            DistillStep::Skill(skill("deploy")),
            DistillStep::Skill(skill("never reached")),
        ],
        vec![Err(SkillSinkError::Permanent {
            message: "constraint violation".to_owned(),
        })],
        5,
    );

    let outcome = rig
        .worker
        .run_once()
        .await
        .expect("a permanent sink failure is not a worker error");

    assert_eq!(outcome, WorkerOutcome::Failed);
    assert_eq!(
        events(&rig.trace),
        vec![
            TraceEvent::Claim(id(1)),
            TraceEvent::Distill,
            TraceEvent::Write(id(1)),
            TraceEvent::Fail(id(1), NOW, "constraint violation".to_owned()),
        ]
    );
    assert_eq!(rig.distiller.calls(), 1);
    assert_eq!(rig.sink.calls(), 1);
    assert_eq!(rig.queue.complete_calls(), 0);
    assert_eq!(rig.queue.fail_calls(), 1);
}

#[tokio::test]
async fn exhausted_retryable_sink_failures_fail_the_job_with_the_last_error() {
    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![
            DistillStep::Skill(skill("deploy")),
            DistillStep::Skill(skill("deploy")),
            DistillStep::Skill(skill("never reached")),
        ],
        vec![
            Err(SkillSinkError::Retryable {
                message: "first sink failure".to_owned(),
            }),
            Err(SkillSinkError::Retryable {
                message: "last sink failure".to_owned(),
            }),
        ],
        2,
    );

    let outcome = rig
        .worker
        .run_once()
        .await
        .expect("exhausted sink retries are a job failure");

    assert_eq!(outcome, WorkerOutcome::Failed);
    assert_eq!(
        events(&rig.trace),
        vec![
            TraceEvent::Claim(id(1)),
            TraceEvent::Distill,
            TraceEvent::Write(id(1)),
            TraceEvent::Distill,
            TraceEvent::Write(id(1)),
            TraceEvent::Fail(id(1), NOW, "last sink failure".to_owned()),
        ]
    );
    assert_eq!(rig.distiller.calls(), 2);
    assert_eq!(rig.sink.calls(), 2);
    assert_eq!(rig.queue.complete_calls(), 0);
}

#[tokio::test]
async fn duplicate_delivery_of_the_same_job_stores_one_skill() {
    let rig = rig(
        vec![job(1, "session-1", 7), job(1, "session-1", 7)],
        vec![
            DistillStep::Skill(skill("deploy")),
            DistillStep::Skill(skill("deploy")),
        ],
        Vec::new(),
        2,
    );

    assert_eq!(
        rig.worker
            .run_once()
            .await
            .expect("first delivery should succeed"),
        WorkerOutcome::Completed
    );
    assert_eq!(
        rig.worker
            .run_once()
            .await
            .expect("redelivery should also succeed"),
        WorkerOutcome::Completed
    );

    assert_eq!(rig.sink.calls(), 2, "the worker does not dedupe deliveries");
    assert_eq!(
        rig.sink.skills(),
        vec![skill("deploy")],
        "sink provenance idempotency must absorb redelivery"
    );
    assert_eq!(rig.queue.complete_calls(), 2);
    assert_eq!(rig.queue.fail_calls(), 0);
}

#[tokio::test]
async fn identical_content_from_another_turn_stores_one_skill() {
    let rig = rig(
        vec![job(1, "session-1", 7), job(2, "session-2", 9)],
        vec![
            DistillStep::Skill(skill("deploy")),
            DistillStep::Skill(skill("deploy")),
        ],
        Vec::new(),
        2,
    );

    rig.worker
        .run_once()
        .await
        .expect("first job should succeed");
    rig.worker
        .run_once()
        .await
        .expect("second job should succeed");

    assert_eq!(rig.sink.calls(), 2);
    assert_eq!(
        rig.sink.skills(),
        vec![skill("deploy")],
        "content-hash idempotency must absorb an identical draft"
    );
    assert_eq!(rig.queue.complete_calls(), 2);
}

#[tokio::test]
async fn a_complete_failure_is_surfaced_without_a_fail_transition() {
    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![DistillStep::Skill(skill("deploy"))],
        Vec::new(),
        2,
    );
    rig.queue.fail_complete_with("disk full");

    let error = rig
        .worker
        .run_once()
        .await
        .expect_err("a queue failure must not be swallowed");

    match error {
        SkillWorkerError::Queue { message } => {
            assert!(
                message.contains("disk full"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected a queue error, got {other:?}"),
    }
    assert_eq!(rig.sink.calls(), 1);
    assert_eq!(rig.queue.complete_calls(), 1);
    assert_eq!(
        rig.queue.fail_calls(),
        0,
        "a failed completion must not be silently turned into a fail"
    );
}

#[tokio::test]
async fn shutdown_before_the_first_claim_never_claims() {
    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![DistillStep::Skill(skill("never reached"))],
        Vec::new(),
        2,
    );
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    rig.worker
        .run(shutdown)
        .await
        .expect("a pre-cancelled worker should exit cleanly");

    assert_eq!(rig.queue.claim_calls(), 0);
    assert_eq!(rig.distiller.calls(), 0);
    assert!(events(&rig.trace).is_empty());
}

#[tokio::test]
async fn shutdown_while_idle_stops_the_claim_loop() {
    let rig = rig(Vec::new(), Vec::new(), Vec::new(), 2);
    let shutdown = CancellationToken::new();
    let worker = Arc::clone(&rig.worker);
    let token = shutdown.clone();
    let handle = tokio::spawn(async move { worker.run(token).await });

    rig.queue.wait_until_claimed().await;
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("an idle worker should stop promptly")
        .expect("the worker task should not panic");
    assert!(result.is_ok());
    assert_eq!(
        rig.queue.claim_calls(),
        1,
        "no claim may happen after cancellation"
    );
}

#[tokio::test]
async fn shutdown_drains_an_in_flight_job_before_returning() {
    let trace = trace();
    let queue = Arc::new(TestQueue::with_jobs(
        vec![job(1, "session-1", 7)],
        Arc::clone(&trace),
    ));
    let distiller = Arc::new(ControlledDistiller::blocking(
        skill("deploy"),
        Arc::clone(&trace),
    ));
    let sink = Arc::new(IdempotentSink::with_steps(Vec::new(), Arc::clone(&trace)));
    let clock = Arc::new(FixedClock::fixed(NOW));
    let worker = Arc::new(
        SkillWorker::new(
            queue.clone(),
            distiller.clone(),
            sink.clone(),
            clock,
            config(2),
        )
        .expect("test worker configuration should be valid"),
    );

    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let running = Arc::clone(&worker);
    let handle = tokio::spawn(async move { running.run(token).await });

    distiller.wait_until_entered().await;
    shutdown.cancel();
    distiller.release();

    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("the worker should drain promptly")
        .expect("the worker task should not panic");
    result.expect("draining should succeed");

    assert_eq!(queue.claim_calls(), 1, "no new claim after cancellation");
    assert_eq!(queue.complete_calls(), 1);
    assert_eq!(queue.fail_calls(), 0);
    assert_eq!(sink.calls(), 1);
    assert_eq!(
        events(&trace).last(),
        Some(&TraceEvent::Complete(id(1), NOW))
    );
}

#[tokio::test]
async fn a_drain_timeout_is_a_structured_error_without_panic() {
    let trace = trace();
    let queue = Arc::new(TestQueue::with_jobs(
        vec![job(1, "session-1", 7)],
        Arc::clone(&trace),
    ));
    let distiller = Arc::new(ControlledDistiller::blocking(
        skill("deploy"),
        Arc::clone(&trace),
    ));
    let sink = Arc::new(IdempotentSink::with_steps(Vec::new(), Arc::clone(&trace)));
    let clock = Arc::new(FixedClock::fixed(NOW));
    let worker = Arc::new(
        SkillWorker::new(
            queue.clone(),
            distiller.clone(),
            sink.clone(),
            clock,
            SkillWorkerConfig {
                max_attempts: 2,
                drain_timeout: Duration::from_millis(20),
                idle_interval: Duration::from_secs(10),
            },
        )
        .expect("test worker configuration should be valid"),
    );

    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let running = Arc::clone(&worker);
    let handle = tokio::spawn(async move { running.run(token).await });

    distiller.wait_until_entered().await;
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("the drain timeout must fire promptly")
        .expect("the worker task should not panic");

    assert_eq!(result, Err(SkillWorkerError::DrainTimeout { millis: 20 }));
    assert_eq!(queue.claim_calls(), 1);
    assert_eq!(
        queue.complete_calls(),
        0,
        "an unfinished job must not be marked complete"
    );
    assert_eq!(
        queue.fail_calls(),
        0,
        "the worker must not record a failure for a drained job"
    );
    assert_eq!(
        sink.calls(),
        0,
        "the interrupted attempt must not write a partial skill"
    );
}

#[tokio::test]
async fn invalid_configuration_is_rejected_before_any_claim() {
    let trace = trace();
    let queue = Arc::new(TestQueue::with_jobs(Vec::new(), Arc::clone(&trace)));
    let distiller = Arc::new(ScriptedDistiller::with_steps(
        Vec::new(),
        Arc::clone(&trace),
    ));
    let sink = Arc::new(IdempotentSink::with_steps(Vec::new(), Arc::clone(&trace)));
    let clock = Arc::new(FixedClock::fixed(NOW));

    let zero_attempts = SkillWorker::new(
        queue.clone(),
        distiller.clone(),
        sink.clone(),
        clock.clone(),
        SkillWorkerConfig {
            max_attempts: 0,
            ..config(2)
        },
    );
    assert!(matches!(
        zero_attempts,
        Err(SkillWorkerError::InvalidConfig { .. })
    ));

    let zero_idle = SkillWorker::new(
        queue.clone(),
        distiller.clone(),
        sink.clone(),
        clock,
        SkillWorkerConfig {
            idle_interval: Duration::ZERO,
            ..config(2)
        },
    );
    assert!(matches!(
        zero_idle,
        Err(SkillWorkerError::InvalidConfig { .. })
    ));

    assert_eq!(queue.claim_calls(), 0);
}

#[tokio::test]
async fn failed_jobs_record_a_redacted_and_bounded_error() {
    let secret = format!("sk-live-{}", "A".repeat(40));
    let raw_error = format!(
        "Authorization: Bearer {secret} password=hunter2 {}",
        "é".repeat(400)
    );
    let rig = rig(
        vec![job(1, "session-1", 7)],
        vec![DistillStep::Permanent(raw_error)],
        Vec::new(),
        1,
    );

    let outcome = rig
        .worker
        .run_once()
        .await
        .expect("a permanent job failure is not a worker error");
    assert_eq!(outcome, WorkerOutcome::Failed);

    let recorded = match events(&rig.trace).pop() {
        Some(TraceEvent::Fail(job_id, at, error)) => {
            assert_eq!(job_id, id(1));
            assert_eq!(at, NOW);
            error
        }
        other => panic!("expected a terminal failure transition, got {other:?}"),
    };

    assert!(!recorded.contains("sk-live-"), "secret leaked: {recorded}");
    assert!(!recorded.contains("hunter2"), "password leaked: {recorded}");
    assert!(
        !recorded.to_ascii_lowercase().contains("bearer"),
        "bearer value leaked: {recorded}"
    );
    assert!(recorded.contains("[REDACTED]"));
    assert!(
        recorded.len() <= 512,
        "recorded error exceeds 512 bytes: {}",
        recorded.len()
    );
}

#[test]
fn the_worker_has_no_event_store_or_host_clock_access() {
    let source = include_str!("../src/skill_worker.rs");
    for forbidden in [
        "EventStore",
        "AgentEvent",
        "SystemTime",
        "Instant::now",
        "Utc::now",
        "Local::now",
        "jessica_memory",
    ] {
        assert!(
            !source.contains(forbidden),
            "skill_worker.rs must not reference {forbidden}"
        );
    }

    let manifest = include_str!("../Cargo.toml");
    assert!(!manifest.contains("jessica-memory"));
    assert!(!manifest.contains("jessica_memory"));
}
