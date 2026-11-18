//! Producer-side turn journal.
//!
//! [`TurnJournal`] sits on the event producer path rather than behind the
//! bounded broadcast channel. Producers await [`jessica_core::events::EventSink::emit`]
//! before broadcasting, and a single writer task appends the optional legacy
//! event and its trace entry in one SQLite transaction.
//!
//! The queue is bounded: a full queue applies backpressure instead of dropping
//! events. A successful turn is only reported after the writer has committed
//! every queued event and the terminal `turn_ended` marker.
//!
//! When persistence is disabled the journal is inert. The runtime still uses
//! its ordinary broadcast channel, and a pump lag keeps the pre-existing
//! warn-and-continue or silent-failure behaviour.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::events::{EventSink, EventSinkError};
use jessica_core::trace::{TraceEvent, TracePayload, TraceTurnMode, TurnId};
use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::events::{EventJournal, RecorderError};

/// Maximum number of journal commands waiting for the writer.
const JOURNAL_QUEUE_CAPACITY: usize = 1024;

/// Maximum time to wait for a writer acknowledgement.
const JOURNAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Result channel used by commands that must be durable before continuing.
type Ack = oneshot::Sender<Result<(), RecorderError>>;

/// Per-turn attribution and persistence state.
pub(crate) struct TurnJournal {
    /// Whether a real backing journal was configured.
    enabled: bool,
    /// Bounded command queue to the single writer, absent when disabled.
    sender: Option<mpsc::Sender<JournalCommand>>,
    /// State shared with the writer task.
    shared: Arc<JournalShared>,
    /// Cancels the agent when recording cannot continue.
    cancel: CancellationToken,
    /// Writer task, aborted when the turn is torn down.
    writer: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

/// State shared by the journal facade and its writer.
struct JournalShared {
    /// Turn lifecycle, attribution, and first-failure state.
    state: StdMutex<JournalState>,
    /// Publishes the first failure to tasks waiting on the journal.
    failure: watch::Sender<Option<String>>,
}

impl JournalShared {
    /// Creates empty shared state with no failure signal.
    fn new() -> Self {
        let (failure, _) = watch::channel(None);
        Self {
            state: StdMutex::new(JournalState::default()),
            failure,
        }
    }
}

/// Mutable state protected by the journal mutex.
#[derive(Default)]
struct JournalState {
    /// Active turn identifier, absent before `begin_turn`.
    turn_id: Option<TurnId>,
    /// Attempt that owns events without an explicit session.
    current: Option<CurrentAttempt>,
    /// One-based attempt to session attribution.
    attempts: BTreeMap<u32, SessionId>,
    /// First integrity failure.
    failure: Option<String>,
    /// Whether writes must stop because the trace is already broken.
    writes_stopped: bool,
    /// Whether `turn_ended` has been committed.
    closed: bool,
}

/// One attributed agent event ready for the writer.
struct PreparedEvent {
    /// Trace envelope carrying the event and its attribution.
    trace: TraceEvent,
    /// Legacy mirror for session-scoped events.
    legacy: Option<(SessionId, AgentEvent)>,
    /// Whether the event ends the current attempt.
    clear_current: bool,
}

/// Attempt currently receiving session-scoped events.
#[derive(Clone)]
struct CurrentAttempt {
    /// One-based attempt number.
    attempt: u32,
    /// Session created for this attempt.
    session_id: SessionId,
}

/// One operation handled by the journal writer.
enum JournalCommand {
    /// Records immutable session bootstrap messages.
    SessionStart {
        /// Session whose metadata is being recorded.
        session_id: SessionId,
        /// Bootstrap messages for the session.
        messages: Vec<Message>,
        /// Acknowledges the write.
        ack: Ack,
    },
    /// Records one trace entry and its optional legacy mirror.
    Trace {
        /// Trace entry to append.
        ///
        /// Boxed because trace payloads carry full conversations and the
        /// bounded queue stores many of these commands.
        trace: Box<TraceEvent>,
        /// Session-scoped legacy event, when the entry has one.
        legacy: Option<(SessionId, AgentEvent)>,
        /// Acknowledges the write when the caller must wait for it.
        ack: Option<Ack>,
    },
    /// Closes the turn trace.
    Finish {
        /// Turn being closed.
        turn_id: TurnId,
        /// Final governed reason.
        reason: SessionEndReason,
        /// Acknowledges the terminal write.
        ack: Ack,
    },
    /// Waits until every earlier command has been processed.
    Flush {
        /// Acknowledges the barrier.
        ack: Ack,
    },
    /// Stops the writer.
    Shutdown,
}

impl TurnJournal {
    /// Creates a journal for one turn.
    ///
    /// When `journal` is `None` the returned journal is inert and
    /// [`TurnJournal::sink`] returns `None`, preserving the no-persistence
    /// behaviour of the CLI.
    pub(crate) fn new(
        journal: Option<Arc<dyn EventJournal>>,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        Self::with_capacity(journal, cancel, JOURNAL_QUEUE_CAPACITY)
    }

    /// Creates a journal with an explicit queue capacity.
    ///
    /// Small capacities are used by behavior tests to force backpressure.
    pub(crate) fn with_capacity(
        journal: Option<Arc<dyn EventJournal>>,
        cancel: CancellationToken,
        capacity: usize,
    ) -> Arc<Self> {
        let enabled = journal.is_some();
        let shared = Arc::new(JournalShared::new());
        let (sender, writer) = match journal {
            Some(journal) => {
                let (sender, receiver) = mpsc::channel(capacity.max(1));
                let handle = tokio::spawn(writer_loop(
                    journal,
                    receiver,
                    Arc::clone(&shared),
                    cancel.clone(),
                ));
                (Some(sender), Some(handle))
            }
            None => (None, None),
        };
        Arc::new(Self {
            enabled,
            sender,
            shared,
            cancel,
            writer: StdMutex::new(writer),
        })
    }

    /// Returns whether persistence is enabled for this turn.
    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the producer-side sink, when persistence is enabled.
    pub(crate) fn sink(self: &Arc<Self>) -> Option<Arc<dyn EventSink>> {
        self.enabled.then(|| Arc::clone(self) as Arc<dyn EventSink>)
    }

    /// Returns the first integrity failure, if any.
    pub(crate) fn integrity_failure(&self) -> Option<String> {
        self.lock_state().failure.clone()
    }

    /// Completes when the journal has recorded its first failure.
    ///
    /// Returns immediately when a failure already exists. The future is used by
    /// attempt orchestration to stop waiting for a broadcast terminal event
    /// that the sink deliberately suppressed.
    pub(crate) async fn failure_signal(&self) {
        let mut receiver = self.shared.failure.subscribe();
        if receiver.borrow().is_some() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if receiver.borrow().is_some() {
                return;
            }
        }
    }

    /// Marks the turn trace as failed and cancels the agent.
    ///
    /// `stop_writes` is true for storage and attribution failures, and false
    /// for an observer gap: after a gap the produced events are still recorded,
    /// but the turn can never close successfully.
    pub(crate) fn fail_integrity(&self, cause: String, stop_writes: bool) {
        if !self.enabled {
            return;
        }
        record_failure(&self.shared, &self.cancel, cause, stop_writes);
    }

    /// Starts the turn trace with its `turn_started` entry.
    ///
    /// The acknowledgement is the ordering barrier: no attempt may start until
    /// `turn_started` is durable.
    pub(crate) async fn begin_turn(
        &self,
        turn_id: TurnId,
        mode: TraceTurnMode,
        messages: Vec<Message>,
        first_session: SessionId,
    ) -> Result<(), RecorderError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_writable()?;
        {
            let state = self.lock_state();
            if state.turn_id.is_some() {
                return Err(integrity("the turn trace has already started"));
            }
        }

        let trace = TraceEvent::new(
            turn_id.clone(),
            None,
            Some(first_session),
            TracePayload::TurnStarted { mode, messages },
        );
        self.send_trace_ack(trace, None).await?;

        self.lock_state().turn_id = Some(turn_id);
        Ok(())
    }

    /// Records immutable bootstrap messages for one attempt or outer session.
    pub(crate) async fn record_session_start(
        &self,
        session_id: &SessionId,
        messages: &[Message],
    ) -> Result<(), RecorderError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_writable()?;

        let (ack, receiver) = oneshot::channel();
        self.send(JournalCommand::SessionStart {
            session_id: session_id.clone(),
            messages: messages.to_vec(),
            ack,
        })
        .await?;
        self.await_ack(receiver).await
    }

    /// Attributes an attempt number to its session before the attempt starts.
    ///
    /// The mapping survives the attempt's terminal event so a later acceptance
    /// failure can be attributed to the attempt it judged.
    pub(crate) fn begin_attempt(
        &self,
        attempt: u32,
        session_id: &SessionId,
    ) -> Result<(), RecorderError> {
        if attempt == 0 {
            return Err(integrity("attempt numbers start at one"));
        }

        let mut state = self.lock_state();
        if let Some(cause) = &state.failure {
            return Err(integrity(cause));
        }
        if state.writes_stopped {
            return Err(integrity("trace writes have stopped"));
        }
        if state.closed {
            return Err(integrity("the turn trace is already closed"));
        }
        if self.enabled && state.turn_id.is_none() {
            return Err(integrity("an attempt started before the turn trace"));
        }
        if let Some(existing) = state.attempts.get(&attempt)
            && existing != session_id
        {
            return Err(integrity(format!(
                "attempt {attempt} was already attributed to session `{existing}`"
            )));
        }
        state.attempts.insert(attempt, session_id.clone());
        state.current = Some(CurrentAttempt {
            attempt,
            session_id: session_id.clone(),
        });
        Ok(())
    }

    /// Records the outer session's metadata and terminal pair.
    ///
    /// Both entries are committed before the caller broadcasts them. The outer
    /// session has no attempt: it carries the governed turn's final reason,
    /// while each attempt keeps its own terminal event.
    pub(crate) async fn record_outer_session(
        &self,
        session_id: &SessionId,
        reason: SessionEndReason,
    ) -> Result<(), RecorderError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_writable()?;
        self.record_session_start(session_id, &[]).await?;

        let turn_id = self
            .lock_state()
            .turn_id
            .clone()
            .ok_or_else(|| integrity("the turn trace was never started"))?;
        let started = AgentEvent::SessionStarted {
            session_id: session_id.clone(),
            messages: Vec::new(),
        };
        let ended = AgentEvent::SessionEnded {
            session_id: session_id.clone(),
            reason,
        };
        let started_trace = TraceEvent::new(
            turn_id.clone(),
            None,
            Some(session_id.clone()),
            TracePayload::Agent {
                event: started.clone(),
            },
        );
        let ended_trace = TraceEvent::new(
            turn_id,
            None,
            Some(session_id.clone()),
            TracePayload::Agent {
                event: ended.clone(),
            },
        );

        self.send_trace_ack(started_trace, Some((session_id.clone(), started)))
            .await?;
        self.send_trace_ack(ended_trace, Some((session_id.clone(), ended)))
            .await
    }

    /// Commits every queued event and the terminal `turn_ended` marker.
    ///
    /// Only after the writer acknowledges this command is the turn considered
    /// successful. Any earlier write failure or a pre-existing integrity
    /// failure refuses the close.
    pub(crate) async fn finish_turn(&self, reason: SessionEndReason) -> Result<(), RecorderError> {
        if !self.enabled {
            return Ok(());
        }
        self.ensure_writable()?;
        let turn_id = self
            .lock_state()
            .turn_id
            .clone()
            .ok_or_else(|| integrity("the turn trace was never started"))?;

        let (ack, receiver) = oneshot::channel();
        self.send(JournalCommand::Finish {
            turn_id,
            reason,
            ack,
        })
        .await?;
        self.await_ack(receiver).await?;

        let mut state = self.lock_state();
        state.closed = true;
        state.writes_stopped = true;
        Ok(())
    }

    /// Waits until the writer has processed every earlier command.
    ///
    /// Unlike [`TurnJournal::finish_turn`], this is allowed after an integrity
    /// failure: it persists the events already captured for a turn that will
    /// remain unclosed.
    pub(crate) async fn flush(&self) -> Result<(), RecorderError> {
        if !self.enabled {
            return Ok(());
        }
        let (ack, receiver) = oneshot::channel();
        self.send(JournalCommand::Flush { ack }).await?;
        self.await_ack(receiver).await
    }

    /// Stops the writer task.
    ///
    /// Successful turns have already received their terminal acknowledgement.
    /// Failure paths flush first when they want to preserve captured events.
    pub(crate) fn shutdown(&self) {
        if let Some(sender) = &self.sender {
            let _ = sender.try_send(JournalCommand::Shutdown);
        }
        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(handle) = writer.take() {
            handle.abort();
        }
    }

    /// Attributes and enqueues one agent event.
    async fn record_agent_event(&self, event: AgentEvent) -> Result<(), RecorderError> {
        if !self.enabled {
            return Ok(());
        }

        let PreparedEvent {
            trace,
            legacy,
            clear_current,
        } = match self.prepare_event(&event) {
            Ok(prepared) => prepared,
            Err(cause) => {
                // A completed turn rejects late events without retroactively
                // marking the committed trace as failed.
                if !self.lock_state().closed {
                    self.fail_integrity(cause.clone(), true);
                }
                return Err(integrity(cause));
            }
        };

        self.send_agent_command(JournalCommand::Trace {
            trace: Box::new(trace),
            legacy,
            ack: None,
        })
        .await?;

        if clear_current {
            self.lock_state().current = None;
        }
        Ok(())
    }

    /// Builds the trace envelope and legacy mirror for one event.
    fn prepare_event(&self, event: &AgentEvent) -> Result<PreparedEvent, String> {
        let mut state = self.lock_state();
        if state.closed {
            return Err("the turn trace is already closed".to_owned());
        }
        if state.writes_stopped {
            // A storage failure takes precedence over the generic marker so
            // the producer sees the original cause.
            return Err(state
                .failure
                .clone()
                .unwrap_or_else(|| "trace writes have stopped".to_owned()));
        }
        // A non-stopping failure (an observer gap) still permits recording:
        // the producer stream is the authoritative trace, and the turn will
        // fail later because it can never close.
        let turn_id = state
            .turn_id
            .clone()
            .ok_or_else(|| "an event arrived before the turn trace started".to_owned())?;

        match event {
            AgentEvent::AcceptanceFailed { attempt, .. } => {
                if *attempt == 0 {
                    return Err("an acceptance failure reported attempt zero".to_owned());
                }
                let session_id = state.attempts.get(attempt).cloned().ok_or_else(|| {
                    format!("acceptance failure for attempt {attempt} has no attributed session")
                })?;
                Ok(PreparedEvent {
                    trace: TraceEvent::new(
                        turn_id,
                        Some(*attempt),
                        Some(session_id),
                        TracePayload::Agent {
                            event: event.clone(),
                        },
                    ),
                    legacy: None,
                    clear_current: false,
                })
            }
            AgentEvent::SessionStarted { session_id, .. } => {
                let attempt = attempt_for_session(&state, session_id).ok_or_else(|| {
                    format!("session `{session_id}` started without an attributed attempt")
                })?;
                if let Some(current) = &state.current
                    && current.session_id != *session_id
                {
                    return Err(format!(
                        "session `{session_id}` started while `{}` was active",
                        current.session_id
                    ));
                }
                state.current = Some(CurrentAttempt {
                    attempt,
                    session_id: session_id.clone(),
                });
                Ok(PreparedEvent {
                    trace: TraceEvent::new(
                        turn_id,
                        Some(attempt),
                        Some(session_id.clone()),
                        TracePayload::Agent {
                            event: event.clone(),
                        },
                    ),
                    legacy: Some((session_id.clone(), event.clone())),
                    clear_current: false,
                })
            }
            AgentEvent::SessionEnded { session_id, .. } => {
                let attempt = attempt_for_session(&state, session_id).ok_or_else(|| {
                    format!("session `{session_id}` ended without an attributed attempt")
                })?;
                if let Some(current) = &state.current
                    && current.session_id != *session_id
                {
                    return Err(format!(
                        "session `{session_id}` ended while `{}` was active",
                        current.session_id
                    ));
                }
                Ok(PreparedEvent {
                    trace: TraceEvent::new(
                        turn_id,
                        Some(attempt),
                        Some(session_id.clone()),
                        TracePayload::Agent {
                            event: event.clone(),
                        },
                    ),
                    legacy: Some((session_id.clone(), event.clone())),
                    clear_current: true,
                })
            }
            // Audit facts describe work whose observable effects already reach
            // the legacy log through the surrounding tool events. Replay does
            // not re-run permission gates or child sessions, so mirroring these
            // would make the legacy stream impossible to reproduce exactly.
            AgentEvent::PermissionDecided { .. }
            | AgentEvent::ChildSessionStarted { .. }
            | AgentEvent::ChildSessionEnded { .. } => {
                let current = state
                    .current
                    .clone()
                    .ok_or_else(|| "an event arrived with no active session".to_owned())?;
                Ok(PreparedEvent {
                    trace: TraceEvent::new(
                        turn_id,
                        Some(current.attempt),
                        Some(current.session_id),
                        TracePayload::Agent {
                            event: event.clone(),
                        },
                    ),
                    legacy: None,
                    clear_current: false,
                })
            }
            _ => {
                let current = state
                    .current
                    .clone()
                    .ok_or_else(|| "an event arrived with no active session".to_owned())?;
                Ok(PreparedEvent {
                    trace: TraceEvent::new(
                        turn_id,
                        Some(current.attempt),
                        Some(current.session_id.clone()),
                        TracePayload::Agent {
                            event: event.clone(),
                        },
                    ),
                    legacy: Some((current.session_id, event.clone())),
                    clear_current: false,
                })
            }
        }
    }

    /// Sends a trace command and waits for its acknowledgement.
    async fn send_trace_ack(
        &self,
        trace: TraceEvent,
        legacy: Option<(SessionId, AgentEvent)>,
    ) -> Result<(), RecorderError> {
        let (ack, receiver) = oneshot::channel();
        self.send(JournalCommand::Trace {
            trace: Box::new(trace),
            legacy,
            ack: Some(ack),
        })
        .await?;
        self.await_ack(receiver).await
    }

    /// Sends a command to the writer.
    async fn send(&self, command: JournalCommand) -> Result<(), RecorderError> {
        let Some(sender) = &self.sender else {
            return Ok(());
        };
        sender.send(command).await.map_err(|_| {
            let cause = "the journal writer is not running".to_owned();
            self.fail_integrity(cause.clone(), true);
            integrity(cause)
        })
    }

    /// Enqueues a high-volume agent event, waiting for capacity when needed.
    ///
    /// The wait ignores the cancellation token: a cancelled turn may still
    /// emit its terminal event, and an observer-gap failure still records the
    /// producer tail. Waiting is safe because the independent writer keeps
    /// draining the queue; if it has stopped, the closed channel reports that
    /// immediately instead of dropping the event.
    async fn send_agent_command(&self, command: JournalCommand) -> Result<(), RecorderError> {
        let Some(sender) = &self.sender else {
            return Ok(());
        };
        match sender.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(command)) => {
                sender.send(command).await.map_err(|_| {
                    let cause = "the journal writer is not running".to_owned();
                    self.fail_integrity(cause.clone(), true);
                    integrity(cause)
                })
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                let cause = "the journal writer is not running".to_owned();
                self.fail_integrity(cause.clone(), true);
                Err(integrity(cause))
            }
        }
    }

    /// Waits for a writer acknowledgement with a bounded timeout.
    async fn await_ack(
        &self,
        receiver: oneshot::Receiver<Result<(), RecorderError>>,
    ) -> Result<(), RecorderError> {
        match tokio::time::timeout(JOURNAL_TIMEOUT, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                let cause = "the journal writer stopped before acknowledging a command".to_owned();
                self.fail_integrity(cause.clone(), true);
                Err(integrity(cause))
            }
            Err(_) => {
                let cause = "the journal writer did not acknowledge a command in time".to_owned();
                self.fail_integrity(cause.clone(), true);
                Err(integrity(cause))
            }
        }
    }

    /// Rejects commands after the trace has failed or closed.
    fn ensure_writable(&self) -> Result<(), RecorderError> {
        let state = self.lock_state();
        if let Some(cause) = &state.failure {
            return Err(integrity(cause));
        }
        if state.writes_stopped {
            return Err(integrity("trace writes have stopped"));
        }
        if state.closed {
            return Err(integrity("the turn trace is already closed"));
        }
        Ok(())
    }

    /// Locks the shared state, recovering from a poisoned mutex.
    fn lock_state(&self) -> MutexGuard<'_, JournalState> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[async_trait]
impl EventSink for TurnJournal {
    async fn emit(&self, event: AgentEvent) -> Result<(), EventSinkError> {
        self.record_agent_event(event)
            .await
            .map_err(|error| EventSinkError::new(error.to_string()))
    }
}

/// Single writer that turns commands into trace/legacy transactions.
async fn writer_loop(
    journal: Arc<dyn EventJournal>,
    mut receiver: mpsc::Receiver<JournalCommand>,
    shared: Arc<JournalShared>,
    cancel: CancellationToken,
) {
    while let Some(command) = receiver.recv().await {
        match command {
            JournalCommand::SessionStart {
                session_id,
                messages,
                ack,
            } => {
                let result = journal.record_session_start(&session_id, &messages).await;
                if let Err(error) = &result {
                    record_failure(
                        &shared,
                        &cancel,
                        format!("failed to record session metadata: {error}"),
                        true,
                    );
                    let _ = ack.send(result);
                    break;
                }
                let _ = ack.send(Ok(()));
            }
            JournalCommand::Trace { trace, legacy, ack } => {
                let result = journal.record_traced(*trace, legacy).await;
                match result {
                    Ok(()) => {
                        if let Some(ack) = ack {
                            let _ = ack.send(Ok(()));
                        }
                    }
                    Err(error) => {
                        record_failure(
                            &shared,
                            &cancel,
                            format!("failed to record an event for the turn trace: {error}"),
                            true,
                        );
                        if let Some(ack) = ack {
                            let _ = ack.send(Err(error));
                        }
                        break;
                    }
                }
            }
            JournalCommand::Finish {
                turn_id,
                reason,
                ack,
            } => {
                let trace =
                    TraceEvent::new(turn_id, None, None, TracePayload::TurnEnded { reason });
                match journal.record_traced(trace, None).await {
                    Ok(()) => {
                        let _ = ack.send(Ok(()));
                    }
                    Err(error) => {
                        record_failure(
                            &shared,
                            &cancel,
                            format!("failed to close the turn trace: {error}"),
                            true,
                        );
                        let _ = ack.send(Err(error));
                        break;
                    }
                }
            }
            JournalCommand::Flush { ack } => {
                let _ = ack.send(Ok(()));
            }
            JournalCommand::Shutdown => break,
        }
    }
}

/// Publishes the first journal failure and cancels the agent.
fn record_failure(
    shared: &JournalShared,
    cancel: &CancellationToken,
    cause: String,
    stop_writes: bool,
) {
    let (message, first) = {
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if stop_writes {
            state.writes_stopped = true;
        }
        if state.failure.is_none() {
            state.failure = Some(cause);
            (state.failure.clone().unwrap_or_default(), true)
        } else {
            (state.failure.clone().unwrap_or_default(), false)
        }
    };
    if first {
        eprintln!("[error] trace integrity failure: {message}");
    }
    cancel.cancel();
    let _ = shared.failure.send_replace(Some(message));
}

/// Finds the attempt attributed to `session_id`.
fn attempt_for_session(state: &JournalState, session_id: &SessionId) -> Option<u32> {
    state
        .attempts
        .iter()
        .find_map(|(attempt, candidate)| (candidate == session_id).then_some(*attempt))
}

/// Builds a trace integrity error.
fn integrity(message: impl Into<String>) -> RecorderError {
    RecorderError::Integrity {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use jessica_core::types::{PermissionDecision, ToolCall};
    use jessica_harness::Harness;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    use super::*;
    use crate::pump::{EventPump, PumpOutput};

    /// One recorded trace entry and its optional legacy mirror.
    type RecordedEntry = (TraceEvent, Option<(SessionId, AgentEvent)>);

    /// Journal double that records writes, can fail one trace write, and can
    /// block agent entries to force producer backpressure.
    #[derive(Default)]
    struct RecordingJournal {
        sessions: StdMutex<Vec<(SessionId, Vec<Message>)>>,
        records: StdMutex<Vec<RecordedEntry>>,
        attempts: AtomicUsize,
        fail_at: StdMutex<Option<usize>>,
        block_agents: AtomicBool,
        entered: Notify,
        gate: Notify,
    }

    impl RecordingJournal {
        fn records(&self) -> Vec<RecordedEntry> {
            self.records.lock().expect("records mutex poisoned").clone()
        }

        fn sessions(&self) -> Vec<(SessionId, Vec<Message>)> {
            self.sessions
                .lock()
                .expect("sessions mutex poisoned")
                .clone()
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }

        fn fail_at(&self, attempt: usize) {
            *self.fail_at.lock().expect("fail_at mutex poisoned") = Some(attempt);
        }

        fn block_agents(&self) {
            self.block_agents.store(true, Ordering::SeqCst);
        }

        fn release_agents(&self) {
            self.block_agents.store(false, Ordering::SeqCst);
            self.gate.notify_waiters();
        }
    }

    #[async_trait]
    impl EventJournal for RecordingJournal {
        async fn record_session_start(
            &self,
            session_id: &SessionId,
            messages: &[Message],
        ) -> Result<(), RecorderError> {
            self.sessions
                .lock()
                .expect("sessions mutex poisoned")
                .push((session_id.clone(), messages.to_vec()));
            Ok(())
        }

        async fn record_traced(
            &self,
            trace: TraceEvent,
            legacy: Option<(SessionId, AgentEvent)>,
        ) -> Result<(), RecorderError> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if *self.fail_at.lock().expect("fail_at mutex poisoned") == Some(attempt) {
                return Err(RecorderError::Integrity {
                    message: "injected storage failure".to_owned(),
                });
            }
            if self.block_agents.load(Ordering::SeqCst)
                && matches!(trace.payload, TracePayload::Agent { .. })
            {
                self.entered.notify_one();
                self.gate.notified().await;
            }
            self.records
                .lock()
                .expect("records mutex poisoned")
                .push((trace, legacy));
            Ok(())
        }
    }

    /// Builds a real turn journal over the recording double.
    fn active_journal(
        journal: &Arc<RecordingJournal>,
        capacity: usize,
    ) -> (Arc<TurnJournal>, CancellationToken) {
        let cancel = CancellationToken::new();
        let journal = TurnJournal::with_capacity(
            Some(Arc::clone(journal) as Arc<dyn EventJournal>),
            cancel.clone(),
            capacity,
        );
        (journal, cancel)
    }

    fn delta(text: &str) -> AgentEvent {
        AgentEvent::ModelDelta {
            text: text.to_owned(),
        }
    }

    #[tokio::test]
    async fn a_successful_turn_records_attributed_events_and_closes_last() {
        let recorder = Arc::new(RecordingJournal::default());
        let (journal, _cancel) = active_journal(&recorder, 8);
        let turn = TurnId::new("turn-lifecycle");
        let first = SessionId::new("attempt-1");
        let second = SessionId::new("attempt-2");
        let outer = SessionId::new("outer");
        let messages = vec![Message::system("system"), Message::user("go")];

        journal
            .begin_turn(
                turn.clone(),
                TraceTurnMode::Fresh,
                messages.clone(),
                first.clone(),
            )
            .await
            .expect("turn must start");
        journal.begin_attempt(1, &first).expect("attempt one");
        journal
            .record_session_start(&first, &messages)
            .await
            .expect("first metadata");
        let sink = journal.sink().expect("persistence must expose a sink");

        sink.emit(AgentEvent::SessionStarted {
            session_id: first.clone(),
            messages: Vec::new(),
        })
        .await
        .expect("first start");
        sink.emit(delta("first answer")).await.expect("first delta");
        sink.emit(AgentEvent::SessionEnded {
            session_id: first.clone(),
            reason: SessionEndReason::Completed,
        })
        .await
        .expect("first end");
        sink.emit(AgentEvent::AcceptanceFailed {
            attempt: 1,
            feedback: "retry".to_owned(),
        })
        .await
        .expect("first verdict");

        journal.begin_attempt(2, &second).expect("attempt two");
        journal
            .record_session_start(&second, &messages)
            .await
            .expect("second metadata");
        sink.emit(AgentEvent::SessionStarted {
            session_id: second.clone(),
            messages: Vec::new(),
        })
        .await
        .expect("second start");
        sink.emit(AgentEvent::SessionEnded {
            session_id: second.clone(),
            reason: SessionEndReason::Completed,
        })
        .await
        .expect("second end");
        sink.emit(AgentEvent::AcceptanceFailed {
            attempt: 2,
            feedback: "still failing".to_owned(),
        })
        .await
        .expect("second verdict");

        journal
            .record_outer_session(&outer, SessionEndReason::AcceptanceFailed)
            .await
            .expect("outer session");
        journal
            .finish_turn(SessionEndReason::AcceptanceFailed)
            .await
            .expect("turn must close");
        assert!(
            sink.emit(delta("after close")).await.is_err(),
            "no event may be recorded after turn_ended"
        );
        journal.shutdown();

        assert_eq!(
            recorder.sessions(),
            vec![
                (first.clone(), messages.clone()),
                (second.clone(), messages),
                (outer.clone(), Vec::new()),
            ],
            "metadata must be durable before each session's first event"
        );
        let records = recorder.records();
        let payloads: Vec<&TracePayload> =
            records.iter().map(|(trace, _)| &trace.payload).collect();
        assert!(matches!(payloads[0], TracePayload::TurnStarted { .. }));
        assert!(matches!(payloads[1], TracePayload::Agent { .. }));
        assert!(matches!(payloads[5], TracePayload::Agent { .. }));
        assert_eq!(
            records.len(),
            11,
            "turn start, two attempts, outer pair, turn end: {records:?}"
        );
        assert!(matches!(
            payloads.last(),
            Some(TracePayload::TurnEnded {
                reason: SessionEndReason::AcceptanceFailed
            })
        ));

        assert_eq!(records[1].0.attempt, Some(1));
        assert_eq!(records[1].0.session_id.as_ref(), Some(&first));
        assert!(records[1].1.is_some(), "attempt events need legacy rows");
        assert_eq!(records[3].0.attempt, Some(1));
        assert!(records[3].1.is_some(), "session end needs its legacy row");
        assert_eq!(records[4].0.attempt, Some(1));
        assert!(records[4].1.is_none(), "verdicts are trace-only");
        assert_eq!(records[5].0.attempt, Some(2));
        assert_eq!(records[5].0.session_id.as_ref(), Some(&second));
        assert_eq!(records[7].0.attempt, Some(2));
        assert!(records[7].1.is_none());
        assert_eq!(records[8].0.attempt, None);
        assert_eq!(records[8].0.session_id.as_ref(), Some(&outer));
        assert!(records[8].1.is_some(), "outer events need legacy rows");
        assert_eq!(records[9].0.attempt, None);
        assert!(records[9].1.is_some());
        assert_eq!(records[10].0.attempt, None);
        assert_eq!(records[10].0.session_id, None);
        assert!(records[10].1.is_none());

        assert_eq!(recorder.attempts(), records.len());
        assert_eq!(journal.integrity_failure(), None);
        assert!(journal.lock_state().closed);
    }

    #[tokio::test]
    async fn a_writer_failure_is_observable_and_permanently_prevents_closure() {
        let recorder = Arc::new(RecordingJournal::default());
        let (journal, cancel) = active_journal(&recorder, 8);
        let session = SessionId::new("writer-failure");
        recorder.fail_at(2);
        let sink = journal.sink().expect("persistence must expose a sink");

        journal
            .begin_turn(
                TurnId::new("turn-writer-failure"),
                TraceTurnMode::Fresh,
                vec![Message::user("go")],
                session.clone(),
            )
            .await
            .expect("turn_started must be recorded");
        journal.begin_attempt(1, &session).expect("attempt");
        journal
            .record_session_start(&session, &[Message::user("go")])
            .await
            .expect("metadata");
        sink.emit(AgentEvent::SessionStarted {
            session_id: session.clone(),
            messages: Vec::new(),
        })
        .await
        .expect("the failing write is enqueued asynchronously");

        timeout(Duration::from_secs(2), journal.failure_signal())
            .await
            .expect("the writer must publish its failure");
        assert!(
            cancel.is_cancelled(),
            "a write failure must cancel the agent"
        );
        assert!(
            journal
                .integrity_failure()
                .expect("failure must be observable")
                .contains("injected storage failure")
        );
        assert!(
            sink.emit(delta("must not be accepted")).await.is_err(),
            "no event may be accepted after the writer stopped"
        );
        assert!(
            journal
                .finish_turn(SessionEndReason::Completed)
                .await
                .is_err(),
            "a failed trace can never close"
        );
        assert!(journal.flush().await.is_err());
        journal.shutdown();

        assert_eq!(recorder.attempts(), 2);
        assert_eq!(
            recorder.records().len(),
            1,
            "only turn_started was committed: {:?}",
            recorder.records()
        );
        assert!(!journal.lock_state().closed);
    }

    #[tokio::test]
    async fn a_full_queue_waits_for_capacity_and_loses_no_event() {
        let recorder = Arc::new(RecordingJournal::default());
        let (journal, _cancel) = active_journal(&recorder, 1);
        let session = SessionId::new("backpressure");
        journal
            .begin_turn(
                TurnId::new("turn-backpressure"),
                TraceTurnMode::Fresh,
                vec![Message::user("go")],
                session.clone(),
            )
            .await
            .expect("turn must start");
        journal.begin_attempt(1, &session).expect("attempt");
        let sink = journal.sink().expect("sink");
        sink.emit(AgentEvent::SessionStarted {
            session_id: session.clone(),
            messages: Vec::new(),
        })
        .await
        .expect("start must be recorded");

        recorder.block_agents();
        let producer = tokio::spawn({
            let sink = Arc::clone(&sink);
            async move {
                let mut results = Vec::new();
                for index in 0..6 {
                    results.push(sink.emit(delta(&format!("delta {index}"))).await);
                }
                results
            }
        });
        timeout(Duration::from_secs(2), recorder.entered.notified())
            .await
            .expect("the writer must enter the blocked write");
        assert!(
            timeout(Duration::from_millis(50), async {
                while !producer.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "a full queue must block the producer instead of dropping events"
        );

        recorder.release_agents();
        let results = timeout(Duration::from_secs(2), producer)
            .await
            .expect("the producer must finish after capacity returns")
            .expect("the producer task must not panic");
        assert!(
            results.iter().all(Result::is_ok),
            "no event may be rejected by backpressure: {results:?}"
        );
        journal
            .finish_turn(SessionEndReason::Completed)
            .await
            .expect("turn must close");
        journal.shutdown();

        let records = recorder.records();
        assert_eq!(
            records.len(),
            9,
            "turn start, session start, six deltas, turn end"
        );
        let deltas = records
            .iter()
            .filter(|(trace, _)| {
                matches!(
                    trace.payload,
                    TracePayload::Agent {
                        event: AgentEvent::ModelDelta { .. }
                    }
                )
            })
            .count();
        assert_eq!(deltas, 6, "every queued delta must reach the writer");
        assert!(matches!(
            records.last().map(|(trace, _)| &trace.payload),
            Some(TracePayload::TurnEnded {
                reason: SessionEndReason::Completed
            })
        ));
    }

    #[tokio::test]
    async fn an_observer_gap_keeps_recording_the_producer_tail_but_never_closes() {
        let recorder = Arc::new(RecordingJournal::default());
        let (journal, cancel) = active_journal(&recorder, 8);
        let session = SessionId::new("lag-tail");
        journal
            .begin_turn(
                TurnId::new("turn-lag-tail"),
                TraceTurnMode::Fresh,
                vec![Message::user("go")],
                session.clone(),
            )
            .await
            .expect("turn must start");
        journal.begin_attempt(1, &session).expect("attempt");
        let sink = journal.sink().expect("sink");
        sink.emit(AgentEvent::SessionStarted {
            session_id: session.clone(),
            messages: Vec::new(),
        })
        .await
        .expect("start");

        journal.fail_integrity("the pump lagged".to_owned(), false);
        assert!(cancel.is_cancelled());
        sink.emit(delta("recorded after the gap"))
            .await
            .expect("a non-stopping failure must keep recording the producer tail");
        sink.emit(AgentEvent::SessionEnded {
            session_id: session,
            reason: SessionEndReason::Cancelled,
        })
        .await
        .expect("the cancelled tail must still be recorded");
        journal
            .flush()
            .await
            .expect("the live writer must flush the tail");
        assert!(
            journal
                .finish_turn(SessionEndReason::Cancelled)
                .await
                .is_err()
        );
        journal.shutdown();

        let records = recorder.records();
        assert_eq!(records.len(), 4, "start, session start, delta, end");
        assert!(
            !records
                .iter()
                .any(|(trace, _)| matches!(trace.payload, TracePayload::Gap { .. })),
            "the journal never invents a gap entry"
        );
        assert!(
            !records
                .iter()
                .any(|(trace, _)| matches!(trace.payload, TracePayload::TurnEnded { .. })),
            "an incomplete turn must not be closed"
        );
    }

    #[tokio::test]
    async fn a_pump_gap_fails_the_journal_without_writing_a_gap_entry() {
        let recorder = Arc::new(RecordingJournal::default());
        let (journal, cancel) = active_journal(&recorder, 8);
        let (sender, receiver) = tokio::sync::broadcast::channel(1);
        let harness = Arc::new(Harness::with_event_capacity(Vec::new(), 8));
        let governance = Arc::new(crate::governance::GovernanceState::new(
            harness.events(),
            cancel.clone(),
        ));
        let session = SessionId::new("pump-gap");
        journal
            .begin_turn(
                TurnId::new("turn-pump-gap"),
                TraceTurnMode::Fresh,
                vec![Message::user("go")],
                session.clone(),
            )
            .await
            .expect("turn must start");
        journal.begin_attempt(1, &session).expect("attempt");
        let pump = Arc::new(EventPump::new(
            harness,
            governance,
            Arc::clone(&journal),
            PumpOutput::Silent,
            None,
        ));
        pump.register(session, Vec::new());
        let pump_task = tokio::spawn(Arc::clone(&pump).run(receiver));

        for _ in 0..3 {
            sender
                .send(delta("lost"))
                .expect("the pump receiver must stay open");
        }
        timeout(Duration::from_secs(2), journal.failure_signal())
            .await
            .expect("the pump gap must fail the journal");
        assert!(pump.has_lagged());
        assert!(
            journal
                .integrity_failure()
                .expect("gap failure")
                .contains("lagged")
        );
        assert!(cancel.is_cancelled());
        assert!(
            !recorder
                .records()
                .iter()
                .any(|(trace, _)| matches!(trace.payload, TracePayload::Gap { .. })),
            "the CLI no longer emits broadcast-gap markers"
        );
        assert!(
            journal
                .finish_turn(SessionEndReason::Completed)
                .await
                .is_err()
        );
        pump_task.abort();
        journal.shutdown();
    }

    #[tokio::test]
    async fn a_non_stopping_failure_waits_for_capacity_without_dropping_the_tail() {
        let recorder = Arc::new(RecordingJournal::default());
        let (journal, cancel) = active_journal(&recorder, 1);
        let session = SessionId::new("lag-backpressure");
        journal
            .begin_turn(
                TurnId::new("turn-lag-backpressure"),
                TraceTurnMode::Fresh,
                vec![Message::user("go")],
                session.clone(),
            )
            .await
            .expect("turn must start");
        journal.begin_attempt(1, &session).expect("attempt");
        let sink = journal.sink().expect("sink");
        sink.emit(AgentEvent::SessionStarted {
            session_id: session.clone(),
            messages: Vec::new(),
        })
        .await
        .expect("start");

        recorder.block_agents();
        let producer = tokio::spawn({
            let sink = Arc::clone(&sink);
            async move {
                let mut results = Vec::new();
                for index in 0..3 {
                    results.push(sink.emit(delta(&format!("tail {index}"))).await);
                }
                results
            }
        });
        timeout(Duration::from_secs(2), recorder.entered.notified())
            .await
            .expect("the writer must enter the blocked write");
        journal.fail_integrity("the pump lagged".to_owned(), false);
        assert!(cancel.is_cancelled());
        assert!(!journal.lock_state().writes_stopped);
        assert!(
            timeout(Duration::from_millis(50), async {
                while !producer.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "a non-stopping failure must keep waiting for capacity, not reject the tail"
        );

        recorder.release_agents();
        let results = timeout(Duration::from_secs(2), producer)
            .await
            .expect("the producer must finish after capacity returns")
            .expect("the producer task must not panic");
        assert!(
            results.iter().all(Result::is_ok),
            "the post-gap tail must be accepted: {results:?}"
        );
        journal.flush().await.expect("the tail must flush");
        assert!(
            journal
                .finish_turn(SessionEndReason::Cancelled)
                .await
                .is_err()
        );
        journal.shutdown();

        let records = recorder.records();
        let tails = records
            .iter()
            .filter(|(trace, _)| {
                matches!(
                    trace.payload,
                    TracePayload::Agent {
                        event: AgentEvent::ModelDelta { .. }
                    }
                )
            })
            .count();
        assert_eq!(tails, 3, "all three post-gap deltas must be recorded");
        assert!(
            !records
                .iter()
                .any(|(trace, _)| matches!(trace.payload, TracePayload::TurnEnded { .. }))
        );
    }

    #[tokio::test]
    async fn attempts_require_an_open_turn_and_stable_attribution() {
        let recorder = Arc::new(RecordingJournal::default());
        let (journal, _cancel) = active_journal(&recorder, 8);
        let first = SessionId::new("first");
        let other = SessionId::new("other");

        assert!(
            journal.begin_attempt(1, &first).is_err(),
            "an attempt cannot precede the turn"
        );
        journal
            .begin_turn(
                TurnId::new("turn-attempts"),
                TraceTurnMode::Fresh,
                vec![Message::user("go")],
                first.clone(),
            )
            .await
            .expect("turn must start");
        assert!(
            journal.begin_attempt(0, &first).is_err(),
            "attempt numbers are one-based"
        );
        journal.begin_attempt(1, &first).expect("first attribution");
        assert!(
            journal.begin_attempt(1, &other).is_err(),
            "an attempt number cannot be rebound to another session"
        );
        journal.begin_attempt(1, &first).expect("idempotent");
        journal.shutdown();
        assert_eq!(recorder.records().len(), 1, "only turn_started was written");
    }

    #[tokio::test]
    async fn audit_only_events_are_trace_only_and_attributed_to_the_active_session() {
        let recorder = Arc::new(RecordingJournal::default());
        let (journal, _cancel) = active_journal(&recorder, 16);
        let session = SessionId::new("attempt-audit");
        let turn = TurnId::new("turn-audit");
        let messages = vec![Message::system("system"), Message::user("go")];

        journal
            .begin_turn(
                turn,
                TraceTurnMode::Fresh,
                messages.clone(),
                session.clone(),
            )
            .await
            .expect("turn must start");
        journal.begin_attempt(1, &session).expect("attempt one");
        journal
            .record_session_start(&session, &messages)
            .await
            .expect("metadata");
        let sink = journal.sink().expect("persistence must expose a sink");
        sink.emit(AgentEvent::SessionStarted {
            session_id: session.clone(),
            messages,
        })
        .await
        .expect("session start");

        let call = ToolCall::new("call-1", "bash", serde_json::json!({"command": "true"}));
        let child = SessionId::new("child-audit");
        sink.emit(AgentEvent::PermissionDecided {
            call: call.clone(),
            decision: PermissionDecision::Deny,
        })
        .await
        .expect("permission decision");
        sink.emit(AgentEvent::ChildSessionStarted {
            child_session_id: child.clone(),
            call_id: "call-parent".to_owned(),
        })
        .await
        .expect("child start");
        sink.emit(AgentEvent::ChildSessionEnded {
            child_session_id: child,
            call_id: "call-parent".to_owned(),
            reason: SessionEndReason::Completed,
            rounds: 1,
            tool_calls: 0,
            tool_errors: 0,
        })
        .await
        .expect("child end");
        sink.emit(AgentEvent::SessionEnded {
            session_id: session.clone(),
            reason: SessionEndReason::Completed,
        })
        .await
        .expect("session end");
        journal
            .finish_turn(SessionEndReason::Completed)
            .await
            .expect("turn must close");
        journal.shutdown();

        let records = recorder.records();
        assert_eq!(records.len(), 7, "start, five events, end: {records:?}");
        for record in &records[1..6] {
            assert_eq!(record.0.attempt, Some(1));
            assert_eq!(record.0.session_id.as_ref(), Some(&session));
        }
        assert!(
            records[1].1.is_some(),
            "session_started keeps its legacy mirror"
        );
        let audit_payloads = [
            &records[2].0.payload,
            &records[3].0.payload,
            &records[4].0.payload,
        ];
        assert!(matches!(
            audit_payloads[0],
            TracePayload::Agent {
                event: AgentEvent::PermissionDecided { .. }
            }
        ));
        assert!(matches!(
            audit_payloads[1],
            TracePayload::Agent {
                event: AgentEvent::ChildSessionStarted { .. }
            }
        ));
        assert!(matches!(
            audit_payloads[2],
            TracePayload::Agent {
                event: AgentEvent::ChildSessionEnded { .. }
            }
        ));
        for record in &records[2..5] {
            assert!(
                record.1.is_none(),
                "permission and child lifecycle facts must be trace-only"
            );
        }
        assert!(
            records[5].1.is_some(),
            "session_ended keeps its legacy mirror"
        );
    }
}
