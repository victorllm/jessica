//! Event pump for the CLI: live rendering, transcript recovery, sensors, and
//! guard checkpoints.
//!
//! The runtime loop returns only a [`SessionEndReason`](jessica_core::types::SessionEndReason).
//! Acceptance retries need the full conversation, so this pump rebuilds it from
//! the observable event stream. The same pump runs the post-tool sensor pipeline
//! and evaluates guards at the first tool call of each model round.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use jessica_core::guard::GuardContext;
use jessica_core::trace::{TraceEvent, TraceGapReason, TracePayload, TraceTurnMode, TurnId};
use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId, ToolCall, ToolResult};
use jessica_harness::{Harness, SensorReport};
use tokio::sync::{Notify, broadcast};

use crate::events::{EventJournal, RecorderError};
use crate::governance::GovernanceState;

/// Conversation and final text recovered from one attempt's event stream.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SessionSnapshot {
    /// Reconstructed conversation fed to the acceptance check and retries.
    pub(crate) messages: Vec<Message>,
    /// Final assistant text from the attempt.
    pub(crate) last_output: String,
}

/// Human-readable output policy for the event pump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpOutput {
    /// Render events to the terminal exactly as the CLI always has.
    HumanConsole,
    /// Never render. Events reach a programmatic client only through its
    /// [`UpdateSink`], so stdout stays available for a protocol stream.
    Silent,
}

/// Programmatic consumer for every event observed by the pump.
///
/// Events are published in the same order as persistence and rendering. The
/// sink is called synchronously; a client that needs backpressure should
/// enqueue non-blockingly and provide its own acknowledgement boundary.
pub(crate) trait UpdateSink: Send + Sync {
    /// Observes one event of the current turn.
    fn publish(&self, event: &AgentEvent);
}

/// Destination for human-readable pump rendering.
pub(crate) trait PumpConsole: Send + Sync {
    /// Writes one complete line.
    fn line(&self, text: &str);
    /// Writes inline text without a trailing newline and flushes it.
    fn inline(&self, text: &str);
    /// Writes a diagnostic that belongs on stderr.
    fn diagnostic(&self, text: &str);
}

/// Terminal renderer preserving the original CLI output.
struct StdoutConsole;

impl PumpConsole for StdoutConsole {
    fn line(&self, text: &str) {
        println!("{text}");
    }

    fn inline(&self, text: &str) {
        print!("{text}");
        let _ = io::stdout().flush();
    }

    fn diagnostic(&self, text: &str) {
        eprintln!("{text}");
    }
}

/// One tool call waiting for its result.
#[derive(Debug)]
struct PendingCall {
    call: ToolCall,
    result: Option<ToolResult>,
}

/// Incrementally rebuilt conversation for one session.
#[derive(Debug)]
struct Transcript {
    messages: Vec<Message>,
    round: u32,
    round_open: bool,
    round_output: String,
    pending: Vec<PendingCall>,
}

impl Transcript {
    fn new(messages: Vec<Message>) -> Self {
        Self {
            messages,
            round: 0,
            round_open: false,
            round_output: String::new(),
            pending: Vec::new(),
        }
    }

    /// Starts a model round, flushing the previous tool batch first.
    fn start_round(&mut self) {
        self.flush_pending();
        self.round = self.round.saturating_add(1);
        self.round_output.clear();
        self.round_open = true;
    }

    /// Appends model text to the current round.
    ///
    /// A delta arriving after tool results belongs to the next model turn; the
    /// runtime never emits text after starting tools within the same turn.
    fn on_model_delta(&mut self, text: &str) {
        if !self.round_open || self.pending.iter().any(|pending| pending.result.is_some()) {
            self.start_round();
        }
        self.round_output.push_str(text);
    }

    /// Records a tool call and returns the current round/output for guards.
    ///
    /// The event stream has no explicit model-turn boundary. A tool call starts
    /// a round when no round is open; multiple calls emitted for one assistant
    /// message remain in the same round.
    fn on_tool_started(&mut self, call: &ToolCall) -> (u32, String) {
        if !self.round_open {
            self.start_round();
        }
        self.pending.push(PendingCall {
            call: call.clone(),
            result: None,
        });
        (self.round, self.round_output.clone())
    }

    /// Pairs a result with its pending call, returning the pair for sensors.
    fn on_tool_finished(&mut self, result: &ToolResult) -> Option<(ToolCall, ToolResult)> {
        let pending = self
            .pending
            .iter_mut()
            .find(|pending| pending.call.id == result.tool_call_id)?;
        let call = pending.call.clone();
        pending.result = Some(result.clone());
        Some((call, result.clone()))
    }

    /// Converts pending calls into assistant and tool messages.
    fn flush_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }

        let calls = self
            .pending
            .iter()
            .map(|pending| pending.call.clone())
            .collect();
        self.messages.push(Message::assistant_with_tool_calls(
            std::mem::take(&mut self.round_output),
            calls,
        ));

        for pending in self.pending.drain(..) {
            let content = match pending.result {
                Some(result) if result.is_error => format!("tool error: {}", result.content),
                Some(result) => result.content,
                None => "tool error: tool execution was interrupted".to_owned(),
            };
            self.messages.push(Message::tool(pending.call.id, content));
        }
    }

    /// Finalizes the transcript and returns its messages plus final output.
    fn finish(&mut self) -> (Vec<Message>, String) {
        let last_output = self.round_output.clone();
        if self.pending.is_empty() {
            if self.round_open && !last_output.is_empty() {
                self.messages.push(Message::assistant(last_output.clone()));
            }
        } else {
            self.flush_pending();
        }
        self.round_open = false;
        (self.messages.clone(), last_output)
    }
}

/// Context attached to one persisted event.
#[derive(Debug)]
struct Attribution {
    /// One-based acceptance attempt, when the event belongs to one.
    attempt: Option<u32>,
    /// Session the event belongs to, when one exists.
    session_id: Option<SessionId>,
    /// Whether the event must also be appended to the legacy event stream.
    legacy: bool,
}

/// Shared pump state guarded by a standard mutex.
#[derive(Debug, Default)]
struct PumpState {
    current: Option<SessionId>,
    active: BTreeMap<SessionId, Transcript>,
    ended: BTreeMap<SessionId, SessionSnapshot>,
    /// Sessions whose transcript lost events to broadcast lag.
    failed: BTreeSet<SessionId>,
    /// Turn whose trace is currently open, when persistence is enabled.
    turn: Option<TurnId>,
    /// Attempt number to session mapping for the whole turn.
    ///
    /// Acceptance failures are broadcast after an attempt's terminal event, so
    /// attribution must use the failure's own attempt number and this map, not
    /// whichever attempt happens to be current when the pump processes it.
    attempt_sessions: BTreeMap<u32, SessionId>,
    /// First trace integrity failure, if any.
    integrity_failed: Option<String>,
    /// Whether trace writes must stop because the trace is already broken.
    writes_stopped: bool,
}

/// Reconstructs sessions, runs sensors, and evaluates guards from events.
pub(crate) struct EventPump {
    harness: Arc<Harness>,
    governance: Arc<GovernanceState>,
    recorder: Option<Arc<dyn EventJournal>>,
    output: PumpOutput,
    console: Option<Arc<dyn PumpConsole>>,
    sink: Option<Arc<dyn UpdateSink>>,
    state: Mutex<PumpState>,
    notify: Notify,
    lagged: AtomicBool,
}

impl EventPump {
    /// Creates a pump around the harness and governance of one CLI turn.
    ///
    /// When `recorder` is present, every event is appended to the session's
    /// event stream and the turn's trace before it is rendered. A store
    /// failure or a broadcast gap marks the turn's trace as incomplete, cancels
    /// the agent, and makes [`EventPump::finish_turn`] fail instead of writing
    /// the terminal marker. `output` selects terminal rendering; `sink`
    /// receives every event in pump order.
    pub(crate) fn new(
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        recorder: Option<Arc<dyn EventJournal>>,
        output: PumpOutput,
        sink: Option<Arc<dyn UpdateSink>>,
    ) -> Self {
        let console = match output {
            PumpOutput::HumanConsole => Some(Arc::new(StdoutConsole) as Arc<dyn PumpConsole>),
            PumpOutput::Silent => None,
        };
        Self::assemble(harness, governance, recorder, output, console, sink)
    }

    /// Assembles a pump with an explicit console, used by behavior tests.
    fn assemble(
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        recorder: Option<Arc<dyn EventJournal>>,
        output: PumpOutput,
        console: Option<Arc<dyn PumpConsole>>,
        sink: Option<Arc<dyn UpdateSink>>,
    ) -> Self {
        Self {
            harness,
            governance,
            recorder,
            output,
            console,
            sink,
            state: Mutex::new(PumpState::default()),
            notify: Notify::new(),
            lagged: AtomicBool::new(false),
        }
    }

    /// Returns whether the pump lost events to broadcast lag.
    ///
    /// Without a recorder, silent mode fails the current session and the
    /// human-console path keeps its warn-and-continue behavior. With a
    /// recorder, lag always writes a gap marker and fails the turn through
    /// [`EventPump::integrity_failure`].
    pub(crate) fn has_lagged(&self) -> bool {
        self.lagged.load(Ordering::SeqCst)
    }

    /// Starts the turn trace with its `turn_started` entry.
    ///
    /// Must be awaited before any attempt begins. With persistence disabled
    /// this is a no-op. A store failure is returned to the caller, which must
    /// abort the turn instead of starting an untraceable attempt.
    pub(crate) async fn begin_turn(
        &self,
        turn_id: TurnId,
        mode: TraceTurnMode,
        messages: Vec<Message>,
        first_session: SessionId,
    ) -> Result<(), RecorderError> {
        let Some(journal) = &self.recorder else {
            return Ok(());
        };
        {
            let state = self.lock_state();
            if let Some(cause) = &state.integrity_failed {
                return Err(RecorderError::Integrity {
                    message: cause.clone(),
                });
            }
            if state.turn.is_some() {
                return Err(RecorderError::Integrity {
                    message: "the turn trace has already started".to_owned(),
                });
            }
        }

        let trace = TraceEvent::new(
            turn_id.clone(),
            None,
            Some(first_session),
            TracePayload::TurnStarted { mode, messages },
        );
        journal.record_traced(trace, None).await?;

        let mut state = self.lock_state();
        state.turn = Some(turn_id);
        Ok(())
    }

    /// Attributes an attempt number to its session before the attempt starts.
    ///
    /// The mapping survives the attempt's terminal event so a later
    /// acceptance failure can be attributed to the attempt it judged. With
    /// persistence disabled this is a no-op.
    pub(crate) fn begin_attempt(
        &self,
        attempt: u32,
        session_id: &SessionId,
    ) -> Result<(), RecorderError> {
        if attempt == 0 {
            return Err(RecorderError::Integrity {
                message: "attempt numbers start at one".to_owned(),
            });
        }
        let mut state = self.lock_state();
        if self.recorder.is_some() && state.turn.is_none() {
            return Err(RecorderError::Integrity {
                message: "an attempt started before the turn trace".to_owned(),
            });
        }
        if let Some(existing) = state.attempt_sessions.get(&attempt)
            && existing != session_id
        {
            return Err(RecorderError::Integrity {
                message: format!(
                    "attempt {attempt} was already attributed to session `{existing}`"
                ),
            });
        }
        state.attempt_sessions.insert(attempt, session_id.clone());
        Ok(())
    }

    /// Returns the first trace integrity failure, if any.
    pub(crate) fn integrity_failure(&self) -> Option<String> {
        self.lock_state().integrity_failed.clone()
    }

    /// Marks the turn trace as broken and cancels the agent.
    ///
    /// `stop_writes` is true for storage failures and false for a recorded gap:
    /// after a gap the remaining events are still recorded, but the turn can
    /// never be closed successfully.
    pub(crate) fn fail_integrity(&self, cause: String, stop_writes: bool) {
        let message = {
            let mut state = self.lock_state();
            if stop_writes {
                state.writes_stopped = true;
            }
            if state.integrity_failed.is_none() {
                state.integrity_failed = Some(cause);
            }
            state.integrity_failed.clone()
        };
        if let Some(message) = message {
            eprintln!("[error] trace integrity failure: {message}");
        }
        self.governance.cancel_for_integrity_failure();
    }

    /// Closes the turn trace with its `turn_ended` entry.
    ///
    /// Returns an error, without writing the marker, when the trace already
    /// failed. With persistence disabled this is a no-op. After a successful
    /// close, later events are ignored rather than appended after the terminal
    /// marker.
    pub(crate) async fn finish_turn(&self, reason: SessionEndReason) -> Result<(), RecorderError> {
        let Some(journal) = &self.recorder else {
            return Ok(());
        };
        let turn_id = {
            let state = self.lock_state();
            if let Some(cause) = &state.integrity_failed {
                return Err(RecorderError::Integrity {
                    message: cause.clone(),
                });
            }
            state.turn.clone().ok_or_else(|| RecorderError::Integrity {
                message: "the turn trace was never started".to_owned(),
            })?
        };

        let trace = TraceEvent::new(turn_id, None, None, TracePayload::TurnEnded { reason });
        if let Err(error) = journal.record_traced(trace, None).await {
            self.fail_integrity(format!("failed to close the turn trace: {error}"), true);
            return Err(error);
        }
        let mut state = self.lock_state();
        state.turn = None;
        state.writes_stopped = true;
        Ok(())
    }

    /// Registers the initial conversation before the agent starts.
    ///
    /// The session becomes the current attribution target so a resumed stream,
    /// which has no `SessionStarted` event, still reaches the transcript and
    /// the recorder.
    pub(crate) fn register(&self, session_id: SessionId, messages: Vec<Message>) {
        let mut state = self.lock_state();
        state.ended.remove(&session_id);
        state
            .active
            .insert(session_id.clone(), Transcript::new(messages));
        state.current = Some(session_id);
    }

    /// Consumes events until the channel closes.
    pub(crate) async fn run(self: Arc<Self>, mut receiver: broadcast::Receiver<AgentEvent>) {
        loop {
            match receiver.recv().await {
                Ok(event) => self.handle(event).await,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    if self.recorder.is_some() {
                        // With persistence enabled a gap is never hidden: the
                        // trace records how many events were lost, the turn is
                        // cancelled, and it cannot receive `turn_ended`.
                        self.record_gap(skipped).await;
                    } else {
                        match self.output {
                            // A protocol client cannot be told that its update
                            // stream is incomplete, so silent mode treats the
                            // lag as a failed turn instead of continuing with
                            // a corrupt transcript.
                            PumpOutput::Silent => self.mark_lagged(),
                            PumpOutput::HumanConsole => {
                                eprintln!("[warn] skipped {skipped} events");
                            }
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    /// Waits until the pump has processed `session_id`'s terminal event.
    ///
    /// Returns `None` when the session was never registered or when silent
    /// mode lost events for it to broadcast lag, which prevents a typo or a
    /// torn-down pump from waiting forever.
    pub(crate) async fn wait_for(&self, session_id: &SessionId) -> Option<SessionSnapshot> {
        loop {
            let notified = self.notify.notified();
            {
                let state = self.lock_state();
                if state.failed.contains(session_id) {
                    return None;
                }
                if let Some(snapshot) = state.ended.get(session_id) {
                    return Some(snapshot.clone());
                }
                if !state.active.contains_key(session_id) {
                    return None;
                }
            }
            notified.await;
        }
    }

    /// Records that the current silent session lost events to lag.
    fn mark_lagged(&self) {
        self.lagged.store(true, Ordering::SeqCst);
        {
            let mut state = self.lock_state();
            if let Some(current) = state.current.clone() {
                state.failed.insert(current);
            }
        }
        self.notify.notify_waiters();
    }

    /// Applies one event to the observable, sensor, and guard paths.
    async fn handle(&self, event: AgentEvent) {
        self.record_event(&event).await;
        if let Some(sink) = &self.sink {
            sink.publish(&event);
        }

        // When a CLI budget trip cuts the attempt short, the runtime still
        // reports Completed or Cancelled because it does not know about the
        // trip. The outer session event that follows carries BudgetExceeded,
        // so suppress the contradictory inner terminal line.
        let overridden = matches!(
            &event,
            AgentEvent::SessionEnded {
                reason: SessionEndReason::Completed | SessionEndReason::Cancelled,
                ..
            }
        ) && self.governance.tripped();
        if !overridden {
            self.render_event(&event);
        }

        match event {
            AgentEvent::SessionStarted { session_id } => {
                let mut state = self.lock_state();
                state
                    .active
                    .entry(session_id.clone())
                    .or_insert_with(|| Transcript::new(Vec::new()));
                state.current = Some(session_id);
            }
            AgentEvent::ModelDelta { text } => {
                let mut state = self.lock_state();
                if let Some(transcript) = state.current_transcript_mut() {
                    transcript.on_model_delta(&text);
                }
            }
            AgentEvent::ToolCallStarted { call } => {
                let guard_snapshot = {
                    let mut state = self.lock_state();
                    state
                        .current_transcript_mut()
                        .map(|transcript| transcript.on_tool_started(&call))
                };

                if let Some((round, output)) = guard_snapshot {
                    let context = GuardContext::new(round, self.governance.tokens_used(), &output)
                        .with_tool_call(&call);
                    if let Some(event) = self.harness.check_guards(&context) {
                        self.governance.trip(event);
                    }
                }
            }
            AgentEvent::ToolCallFinished { result } => {
                let sensor_job = {
                    let mut state = self.lock_state();
                    state
                        .current_transcript_mut()
                        .and_then(|transcript| transcript.on_tool_finished(&result))
                };

                if let Some((call, result)) = sensor_job {
                    let report = self.harness.run_sensors(&call, &result).await;
                    self.render_sensor_report(&report);
                }
            }
            AgentEvent::SessionEnded { session_id, .. } => {
                {
                    let mut state = self.lock_state();
                    if state.current.as_ref() == Some(&session_id) {
                        state.current = None;
                    }
                    if let Some(mut transcript) = state.active.remove(&session_id) {
                        let (messages, last_output) = transcript.finish();
                        state.ended.insert(
                            session_id,
                            SessionSnapshot {
                                messages,
                                last_output,
                            },
                        );
                    }
                }
                self.notify.notify_waiters();
            }
            AgentEvent::PolicyDenied { .. }
            | AgentEvent::BudgetExceeded { .. }
            | AgentEvent::AcceptanceFailed { .. }
            | AgentEvent::Error { .. } => {}
        }
    }

    /// Appends one event to the turn trace and its legacy mirror.
    ///
    /// Events are processed sequentially by [`EventPump::run`], so the store
    /// observes the same order as every other subscriber. An acceptance
    /// failure is trace-only and is attributed through its own attempt number;
    /// every other session-scoped agent event is also appended to the legacy
    /// stream in the same transaction.
    async fn record_event(&self, event: &AgentEvent) {
        let Some(journal) = &self.recorder else {
            return;
        };

        let prepared = {
            let state = self.lock_state();
            if state.writes_stopped {
                return;
            }
            match &state.turn {
                Some(turn_id) => state
                    .attribution(event)
                    .map(|attribution| (turn_id.clone(), attribution)),
                None => Err("an event arrived before the turn trace started".to_owned()),
            }
        };

        let (turn_id, attribution) = match prepared {
            Ok(prepared) => prepared,
            Err(cause) => {
                self.fail_integrity(cause, true);
                return;
            }
        };
        let trace = TraceEvent::new(
            turn_id,
            attribution.attempt,
            attribution.session_id.clone(),
            TracePayload::Agent {
                event: event.clone(),
            },
        );
        let legacy = if attribution.legacy {
            attribution
                .session_id
                .map(|session_id| (session_id, event.clone()))
        } else {
            None
        };
        if let Err(error) = journal.record_traced(trace, legacy).await {
            self.fail_integrity(
                format!("failed to record an event for the turn trace: {error}"),
                true,
            );
        }
    }

    /// Records a broadcast gap and cancels the turn.
    ///
    /// A gap leaves the trace permanently incomplete: the skipped events
    /// cannot be reconstructed. Remaining events are still recorded so the
    /// tail of the turn is visible, but [`EventPump::finish_turn`] refuses to
    /// write the terminal marker.
    async fn record_gap(&self, skipped: u64) {
        self.lagged.store(true, Ordering::SeqCst);
        let Some(journal) = &self.recorder else {
            return;
        };

        let prepared = {
            let state = self.lock_state();
            if state.writes_stopped {
                return;
            }
            match &state.turn {
                Some(turn_id) => {
                    let (attempt, session_id) = state
                        .attempt_sessions
                        .last_key_value()
                        .map(|(attempt, session_id)| (Some(*attempt), Some(session_id.clone())))
                        .unwrap_or((None, None));
                    Ok((turn_id.clone(), attempt, session_id))
                }
                None => Err("broadcast lag occurred before the turn trace started".to_owned()),
            }
        };

        let (turn_id, attempt, session_id) = match prepared {
            Ok(prepared) => prepared,
            Err(cause) => {
                self.fail_integrity(cause, true);
                return;
            }
        };
        let trace = TraceEvent::new(
            turn_id,
            attempt,
            session_id,
            TracePayload::Gap {
                skipped,
                reason: TraceGapReason::BroadcastLagged,
            },
        );
        match journal.record_traced(trace, None).await {
            Ok(()) => self.fail_integrity(
                format!("the broadcast receiver lagged and skipped {skipped} events"),
                false,
            ),
            Err(error) => {
                self.fail_integrity(format!("failed to record a broadcast gap: {error}"), true)
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, PumpState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Renders one event when human-console output is enabled.
    ///
    /// The strings and stream choices match the original CLI renderer exactly.
    fn render_event(&self, event: &AgentEvent) {
        if self.output != PumpOutput::HumanConsole {
            return;
        }
        let Some(console) = &self.console else {
            return;
        };
        match event {
            AgentEvent::SessionStarted { session_id } => {
                console.line(&format!("[session] started {session_id}"));
            }
            AgentEvent::ModelDelta { text } => {
                console.inline(text);
            }
            AgentEvent::ToolCallStarted { call } => {
                console.line(&format!(
                    "\n[tool] {} {}",
                    call.name,
                    summarize(&call.arguments.to_string(), 120)
                ));
            }
            AgentEvent::ToolCallFinished { result } => {
                let status = if result.is_error { "error" } else { "ok" };
                console.line(&format!(
                    "[tool:{status}] {}",
                    summarize(&result.content, 200)
                ));
            }
            AgentEvent::PolicyDenied { call, reason } => {
                console.line(&format!("[policy] denied {}: {reason}", call.name));
            }
            AgentEvent::BudgetExceeded { kind, message } => {
                console.line(&format!("\n[budget] {kind:?}: {message}"));
            }
            AgentEvent::AcceptanceFailed { attempt, feedback } => {
                console.line(&format!(
                    "[acceptance] attempt {attempt} failed: {}",
                    summarize(feedback, 200)
                ));
            }
            AgentEvent::Error { message } => {
                console.diagnostic(&format!("\n[error] {message}"));
            }
            AgentEvent::SessionEnded { session_id, reason } => {
                console.line(&format!("\n[session] ended {session_id}: {reason:?}"));
            }
        }
    }

    /// Renders one sensor report when human-console output is enabled.
    fn render_sensor_report(&self, report: &SensorReport) {
        if self.output != PumpOutput::HumanConsole {
            return;
        }
        let Some(console) = &self.console else {
            return;
        };
        for observation in report.observations() {
            let status = if observation.result.passed {
                "pass"
            } else {
                "fail"
            };
            console.line(&format!(
                "[sensor:{status}] {}: {}",
                observation.sensor,
                summarize(&observation.result.output, 200)
            ));
        }
    }
}

impl PumpState {
    fn current_transcript_mut(&mut self) -> Option<&mut Transcript> {
        let session_id = self.current.clone()?;
        self.active.get_mut(&session_id)
    }

    /// Computes the turn/attempt/session context for `event`.
    fn attribution(&self, event: &AgentEvent) -> Result<Attribution, String> {
        match event {
            AgentEvent::SessionStarted { session_id }
            | AgentEvent::SessionEnded { session_id, .. } => Ok(Attribution {
                attempt: self.attempt_for_session(session_id),
                session_id: Some(session_id.clone()),
                legacy: true,
            }),
            AgentEvent::AcceptanceFailed { attempt, .. } => {
                if *attempt == 0 {
                    return Err("an acceptance failure reported attempt zero".to_owned());
                }
                let session_id = self.attempt_sessions.get(attempt).cloned().ok_or_else(|| {
                    format!("acceptance failure for attempt {attempt} has no attributed session")
                })?;
                Ok(Attribution {
                    attempt: Some(*attempt),
                    session_id: Some(session_id),
                    legacy: false,
                })
            }
            _ => {
                let session_id = self
                    .current
                    .clone()
                    .ok_or_else(|| "an event arrived with no active session".to_owned())?;
                let attempt = self.attempt_for_session(&session_id);
                Ok(Attribution {
                    attempt,
                    session_id: Some(session_id),
                    legacy: true,
                })
            }
        }
    }

    /// Returns the attempt number attributed to `session_id`, if any.
    fn attempt_for_session(&self, session_id: &SessionId) -> Option<u32> {
        self.attempt_sessions
            .iter()
            .find_map(|(attempt, candidate)| (candidate == session_id).then_some(*attempt))
    }
}

/// Flattens a value to one line and truncates it to `limit` characters.
fn summarize(text: &str, limit: usize) -> String {
    let flattened = text.replace('\n', " ");
    if flattened.chars().count() <= limit {
        flattened
    } else {
        let mut truncated: String = flattened.chars().take(limit).collect();
        truncated.push('…');
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use jessica_core::types::Role;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;
    use tokio_util::sync::CancellationToken;

    fn base_messages() -> Vec<Message> {
        vec![Message::system("system"), Message::user("fix it")]
    }

    /// One recorded write: its trace entry and its optional legacy mirror.
    type RecordedEntry = (TraceEvent, Option<(SessionId, AgentEvent)>);

    /// Journal double that records writes and can inject a storage failure.
    #[derive(Default)]
    struct RecordingJournal {
        records: Mutex<Vec<RecordedEntry>>,
        attempts: AtomicUsize,
        fail_traced: AtomicBool,
    }

    impl RecordingJournal {
        /// Returns every successfully recorded trace entry.
        fn records(&self) -> Vec<RecordedEntry> {
            self.records.lock().expect("records mutex poisoned").clone()
        }

        /// Returns how many trace writes were attempted, including failures.
        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }

        /// Makes every subsequent trace write fail.
        fn fail_traced_writes(&self) {
            self.fail_traced.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl EventJournal for RecordingJournal {
        async fn record_session_start(
            &self,
            _session_id: &SessionId,
            _messages: &[Message],
        ) -> Result<(), RecorderError> {
            Ok(())
        }

        async fn record_traced(
            &self,
            trace: TraceEvent,
            legacy: Option<(SessionId, AgentEvent)>,
        ) -> Result<(), RecorderError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail_traced.load(Ordering::SeqCst) {
                return Err(RecorderError::Integrity {
                    message: "injected storage failure".to_owned(),
                });
            }
            self.records
                .lock()
                .expect("records mutex poisoned")
                .push((trace, legacy));
            Ok(())
        }
    }

    /// Builds a pump with an injected journal and returns its cancel token.
    fn traced_fixture(
        journal: Arc<RecordingJournal>,
    ) -> (
        Arc<EventPump>,
        Arc<Harness>,
        broadcast::Sender<AgentEvent>,
        CancellationToken,
    ) {
        let harness = Arc::new(Harness::with_event_capacity(Vec::new(), 64));
        let events = harness.events();
        let cancel = CancellationToken::new();
        let governance = Arc::new(GovernanceState::new(events.clone(), cancel.clone()));
        let pump = Arc::new(EventPump::assemble(
            Arc::clone(&harness),
            governance,
            Some(journal as Arc<dyn EventJournal>),
            PumpOutput::Silent,
            None,
            None,
        ));
        (pump, harness, events, cancel)
    }

    /// Console double that records every rendering call.
    #[derive(Default)]
    struct RecordingConsole {
        lines: Mutex<Vec<String>>,
        inline: Mutex<Vec<String>>,
        diagnostics: Mutex<Vec<String>>,
    }

    impl RecordingConsole {
        fn lines(&self) -> Vec<String> {
            self.lines.lock().expect("lines mutex poisoned").clone()
        }

        fn inline(&self) -> Vec<String> {
            self.inline.lock().expect("inline mutex poisoned").clone()
        }
    }

    impl PumpConsole for RecordingConsole {
        fn line(&self, text: &str) {
            self.lines
                .lock()
                .expect("lines mutex poisoned")
                .push(text.to_owned());
        }

        fn inline(&self, text: &str) {
            self.inline
                .lock()
                .expect("inline mutex poisoned")
                .push(text.to_owned());
        }

        fn diagnostic(&self, text: &str) {
            self.diagnostics
                .lock()
                .expect("diagnostics mutex poisoned")
                .push(text.to_owned());
        }
    }

    /// Sink double that records every published event.
    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<AgentEvent>>,
    }

    impl RecordingSink {
        fn events(&self) -> Vec<AgentEvent> {
            self.events.lock().expect("events mutex poisoned").clone()
        }
    }

    impl UpdateSink for RecordingSink {
        fn publish(&self, event: &AgentEvent) {
            self.events
                .lock()
                .expect("events mutex poisoned")
                .push(event.clone());
        }
    }

    /// Builds a pump plus its event sender for rendering and sink tests.
    fn fixture(
        output: PumpOutput,
        console: Option<Arc<RecordingConsole>>,
        sink: Option<Arc<RecordingSink>>,
    ) -> (Arc<EventPump>, Arc<Harness>, broadcast::Sender<AgentEvent>) {
        let harness = Arc::new(Harness::with_event_capacity(Vec::new(), 64));
        let events = harness.events();
        let governance = Arc::new(GovernanceState::new(
            events.clone(),
            CancellationToken::new(),
        ));
        let console = console.map(|console| console as Arc<dyn PumpConsole>);
        let sink = sink.map(|sink| sink as Arc<dyn UpdateSink>);
        let pump = Arc::new(EventPump::assemble(
            Arc::clone(&harness),
            governance,
            None,
            output,
            console,
            sink,
        ));
        (pump, harness, events)
    }

    #[test]
    fn transcript_rebuilds_text_tool_results_and_final_output() {
        let mut transcript = Transcript::new(base_messages());
        transcript.on_model_delta("I will ");
        transcript.on_model_delta("read the file.");

        let call = ToolCall::new("call-1", "read_file", json!({"path": "README.md"}));
        let (round, output) = transcript.on_tool_started(&call);
        assert_eq!(round, 1);
        assert_eq!(output, "I will read the file.");

        let finished = transcript
            .on_tool_finished(&ToolResult::success("call-1", "contents"))
            .expect("started call must pair with its result");
        assert_eq!(finished.0, call);

        transcript.on_model_delta("Fixed.");
        let (messages, last_output) = transcript.finish();

        assert_eq!(last_output, "Fixed.");
        assert_eq!(messages.len(), 5);
        assert_eq!(messages[0], Message::system("system"));
        assert_eq!(messages[1], Message::user("fix it"));
        assert_eq!(messages[2].role, Role::Assistant);
        assert_eq!(messages[2].content, "I will read the file.");
        assert_eq!(messages[2].tool_calls, vec![call]);
        assert_eq!(messages[3], Message::tool("call-1", "contents"));
        assert_eq!(messages[4], Message::assistant("Fixed."));
    }

    #[test]
    fn interrupted_tools_get_a_synthetic_tool_error_message() {
        let mut transcript = Transcript::new(base_messages());
        let call = ToolCall::new("call-9", "bash", json!({"command": "sleep 100"}));
        transcript.on_tool_started(&call);

        let (messages, _last_output) = transcript.finish();

        assert_eq!(messages.len(), 4);
        assert_eq!(messages[2].tool_calls, vec![call]);
        assert_eq!(messages[3].role, Role::Tool);
        assert_eq!(messages[3].tool_call_id.as_deref(), Some("call-9"));
        assert!(
            messages[3]
                .content
                .contains("tool execution was interrupted")
        );
    }

    #[test]
    fn failed_tool_results_use_the_same_prefix_the_runtime_sends_to_models() {
        let mut transcript = Transcript::new(base_messages());
        let call = ToolCall::new("call-2", "bash", json!({"command": "false"}));
        transcript.on_tool_started(&call);
        transcript.on_tool_finished(&ToolResult::error("call-2", "exit 1"));

        let (messages, _last_output) = transcript.finish();

        assert_eq!(messages[3], Message::tool("call-2", "tool error: exit 1"));
    }

    #[test]
    fn a_text_only_round_is_preserved_as_the_final_assistant_message() {
        let mut transcript = Transcript::new(base_messages());
        transcript.on_model_delta("done");

        let (messages, last_output) = transcript.finish();

        assert_eq!(last_output, "done");
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[2], Message::assistant("done"));
    }

    #[test]
    fn a_delta_after_tool_results_starts_a_new_round() {
        let mut transcript = Transcript::new(base_messages());
        let first = ToolCall::new("call-1", "read_file", json!({}));
        transcript.on_tool_started(&first);
        transcript.on_tool_finished(&ToolResult::success("call-1", "a"));

        transcript.on_model_delta("next round");
        let second = ToolCall::new("call-2", "write_file", json!({}));
        let (round, output) = transcript.on_tool_started(&second);

        assert_eq!(round, 2);
        assert_eq!(output, "next round");
    }

    #[test]
    fn multiple_calls_without_new_output_share_one_round() {
        let mut transcript = Transcript::new(base_messages());
        let first = ToolCall::new("call-1", "read_file", json!({}));
        let second = ToolCall::new("call-2", "bash", json!({}));

        let (first_round, first_output) = transcript.on_tool_started(&first);
        transcript.on_tool_finished(&ToolResult::success("call-1", "a"));
        let (second_round, second_output) = transcript.on_tool_started(&second);

        assert_eq!(first_round, 1);
        assert_eq!(first_output, "");
        assert_eq!(second_round, 1);
        assert_eq!(second_output, "");
    }

    #[tokio::test]
    async fn silent_mode_forwards_events_to_the_sink_and_never_renders() {
        let console = Arc::new(RecordingConsole::default());
        let sink = Arc::new(RecordingSink::default());
        let (pump, harness, events) = fixture(
            PumpOutput::Silent,
            Some(Arc::clone(&console)),
            Some(Arc::clone(&sink)),
        );
        let session = SessionId::new("silent");
        pump.register(session.clone(), base_messages());
        let pump_task = tokio::spawn(Arc::clone(&pump).run(harness.subscribe()));

        events
            .send(AgentEvent::SessionStarted {
                session_id: session.clone(),
            })
            .expect("the pump receiver must stay open");
        events
            .send(AgentEvent::ModelDelta {
                text: "hello".to_owned(),
            })
            .expect("the pump receiver must stay open");
        events
            .send(AgentEvent::SessionEnded {
                session_id: session.clone(),
                reason: SessionEndReason::Completed,
            })
            .expect("the pump receiver must stay open");

        let snapshot = pump
            .wait_for(&session)
            .await
            .expect("a silent pump must still complete the session");
        assert_eq!(snapshot.last_output, "hello");
        assert_eq!(snapshot.messages.last(), Some(&Message::assistant("hello")));

        let observed = sink.events();
        assert_eq!(
            observed.len(),
            3,
            "every event must reach the sink in order"
        );
        assert!(matches!(observed[0], AgentEvent::SessionStarted { .. }));
        assert!(matches!(observed[1], AgentEvent::ModelDelta { .. }));
        assert!(matches!(observed[2], AgentEvent::SessionEnded { .. }));

        assert!(
            console.lines().is_empty() && console.inline().is_empty(),
            "silent mode must not invoke the human console"
        );
        pump_task.abort();
    }

    #[tokio::test]
    async fn human_console_mode_renders_the_original_event_lines() {
        let console = Arc::new(RecordingConsole::default());
        let (pump, harness, events) =
            fixture(PumpOutput::HumanConsole, Some(Arc::clone(&console)), None);
        let session = SessionId::new("human");
        pump.register(session.clone(), base_messages());
        let pump_task = tokio::spawn(Arc::clone(&pump).run(harness.subscribe()));

        let call = ToolCall::new("call-1", "read_file", json!({"path": "x"}));
        events
            .send(AgentEvent::SessionStarted {
                session_id: session.clone(),
            })
            .expect("the pump receiver must stay open");
        events
            .send(AgentEvent::ModelDelta {
                text: "hello".to_owned(),
            })
            .expect("the pump receiver must stay open");
        events
            .send(AgentEvent::ToolCallStarted { call })
            .expect("the pump receiver must stay open");
        events
            .send(AgentEvent::ToolCallFinished {
                result: ToolResult::success("call-1", "contents"),
            })
            .expect("the pump receiver must stay open");
        events
            .send(AgentEvent::SessionEnded {
                session_id: session.clone(),
                reason: SessionEndReason::Completed,
            })
            .expect("the pump receiver must stay open");

        pump.wait_for(&session)
            .await
            .expect("the human-console pump must complete the session");

        assert_eq!(
            console.lines(),
            vec![
                format!("[session] started {session}"),
                "\n[tool] read_file {\"path\":\"x\"}".to_owned(),
                "[tool:ok] contents".to_owned(),
                format!("\n[session] ended {session}: Completed"),
            ]
        );
        assert_eq!(console.inline(), vec!["hello".to_owned()]);
        pump_task.abort();
    }

    #[tokio::test]
    async fn silent_lag_fails_the_current_session() {
        let (sender, receiver) = broadcast::channel(1);
        let harness = Arc::new(Harness::with_event_capacity(Vec::new(), 8));
        let governance = Arc::new(GovernanceState::new(
            harness.events(),
            CancellationToken::new(),
        ));
        let pump = Arc::new(EventPump::assemble(
            harness,
            governance,
            None,
            PumpOutput::Silent,
            None,
            None,
        ));
        let session = SessionId::new("lagged");
        pump.register(session.clone(), base_messages());
        let pump_task = tokio::spawn(Arc::clone(&pump).run(receiver));

        // All three sends happen before the spawned pump polls the receiver,
        // so a capacity-one channel deterministically reports a lag.
        for _ in 0..3 {
            sender
                .send(AgentEvent::ModelDelta {
                    text: "lost".to_owned(),
                })
                .expect("the pump receiver must stay open");
        }

        assert!(
            pump.wait_for(&session).await.is_none(),
            "silent lag must be observable as a failed session, not a partial transcript"
        );
        assert!(pump.has_lagged(), "the pump must record the lag state");
        pump_task.abort();
    }

    #[tokio::test]
    async fn human_console_lag_keeps_the_original_warn_and_continue_behavior() {
        let (sender, receiver) = broadcast::channel(1);
        let harness = Arc::new(Harness::with_event_capacity(Vec::new(), 8));
        let governance = Arc::new(GovernanceState::new(
            harness.events(),
            CancellationToken::new(),
        ));
        let console = Arc::new(RecordingConsole::default());
        let pump = Arc::new(EventPump::assemble(
            harness,
            governance,
            None,
            PumpOutput::HumanConsole,
            Some(Arc::clone(&console) as Arc<dyn PumpConsole>),
            None,
        ));
        let session = SessionId::new("human-lagged");
        pump.register(session.clone(), base_messages());
        let pump_task = tokio::spawn(Arc::clone(&pump).run(receiver));

        sender
            .send(AgentEvent::SessionStarted {
                session_id: session.clone(),
            })
            .expect("the pump receiver must stay open");
        sender
            .send(AgentEvent::ModelDelta {
                text: "lost".to_owned(),
            })
            .expect("the pump receiver must stay open");
        sender
            .send(AgentEvent::SessionEnded {
                session_id: session.clone(),
                reason: SessionEndReason::Completed,
            })
            .expect("the pump receiver must stay open");

        let snapshot = pump
            .wait_for(&session)
            .await
            .expect("the human-console path must continue after a lag");
        assert_eq!(
            snapshot.messages,
            base_messages(),
            "the dropped delta must not fabricate transcript content"
        );
        assert!(!pump.has_lagged());
        pump_task.abort();
    }

    #[tokio::test]
    async fn a_turn_trace_attributes_acceptance_failures_to_the_judged_attempt() {
        let journal = Arc::new(RecordingJournal::default());
        let (pump, harness, events, _cancel) = traced_fixture(Arc::clone(&journal));
        let turn = TurnId::new("turn-1");
        let first = SessionId::new("attempt-1");
        let second = SessionId::new("attempt-2");

        pump.begin_turn(turn, TraceTurnMode::Fresh, base_messages(), first.clone())
            .await
            .expect("turn trace must start");
        let pump_task = tokio::spawn(Arc::clone(&pump).run(harness.subscribe()));

        pump.begin_attempt(1, &first)
            .expect("first attempt must be attributed");
        pump.register(first.clone(), base_messages());
        events
            .send(AgentEvent::SessionStarted {
                session_id: first.clone(),
            })
            .expect("pump receiver must stay open");
        events
            .send(AgentEvent::ModelDelta {
                text: "first answer".to_owned(),
            })
            .expect("pump receiver must stay open");
        events
            .send(AgentEvent::SessionEnded {
                session_id: first.clone(),
                reason: SessionEndReason::Completed,
            })
            .expect("pump receiver must stay open");
        pump.wait_for(&first)
            .await
            .expect("first attempt must finish");

        events
            .send(AgentEvent::AcceptanceFailed {
                attempt: 1,
                feedback: "retry".to_owned(),
            })
            .expect("pump receiver must stay open");

        // The second attempt may start before the pump processes the
        // acceptance failure; attribution must still use attempt one.
        pump.begin_attempt(2, &second)
            .expect("second attempt must be attributed");
        pump.register(second.clone(), base_messages());
        events
            .send(AgentEvent::SessionStarted {
                session_id: second.clone(),
            })
            .expect("pump receiver must stay open");
        events
            .send(AgentEvent::ModelDelta {
                text: "second answer".to_owned(),
            })
            .expect("pump receiver must stay open");
        events
            .send(AgentEvent::SessionEnded {
                session_id: second.clone(),
                reason: SessionEndReason::Completed,
            })
            .expect("pump receiver must stay open");
        pump.wait_for(&second)
            .await
            .expect("second attempt must finish");
        pump.finish_turn(SessionEndReason::Completed)
            .await
            .expect("turn trace must close");
        pump_task.abort();

        let records = journal.records();
        assert_eq!(records.len(), 9, "start + 3 + 1 + 3 + end");
        assert!(matches!(
            records[0].0.payload,
            TracePayload::TurnStarted { .. }
        ));
        assert_eq!(records[0].0.attempt, None);
        assert_eq!(
            records[0].0.session_id.as_ref(),
            Some(&first),
            "the turn must name its first session"
        );

        assert_eq!(records[1].0.attempt, Some(1));
        assert_eq!(records[1].0.session_id.as_ref(), Some(&first));
        assert!(records[1].1.is_some(), "agent events need a legacy mirror");

        let (failure, failure_legacy) = &records[4];
        assert!(
            failure_legacy.is_none(),
            "acceptance failures must be trace-only"
        );
        assert_eq!(failure.attempt, Some(1));
        assert_eq!(
            failure.session_id.as_ref(),
            Some(&first),
            "the failure must name the attempt it judged, not the next attempt"
        );
        assert!(matches!(
            failure.payload,
            TracePayload::Agent {
                event: AgentEvent::AcceptanceFailed {
                    attempt: 1,
                    ref feedback,
                }
            } if feedback == "retry"
        ));

        assert_eq!(records[5].0.attempt, Some(2));
        assert_eq!(records[5].0.session_id.as_ref(), Some(&second));

        let (end, end_legacy) = records.last().expect("turn must have an end");
        assert!(matches!(
            end.payload,
            TracePayload::TurnEnded {
                reason: SessionEndReason::Completed
            }
        ));
        assert_eq!(end.attempt, None);
        assert_eq!(end.session_id, None);
        assert!(end_legacy.is_none());
        assert_eq!(journal.attempts(), records.len());
    }

    #[tokio::test]
    async fn a_store_failure_stops_writes_cancels_the_agent_and_never_closes_the_trace() {
        let journal = Arc::new(RecordingJournal::default());
        let (pump, harness, events, cancel) = traced_fixture(Arc::clone(&journal));
        let session = SessionId::new("store-failure");

        pump.begin_turn(
            TurnId::new("turn-failure"),
            TraceTurnMode::Fresh,
            base_messages(),
            session.clone(),
        )
        .await
        .expect("turn trace must start");
        journal.fail_traced_writes();
        pump.begin_attempt(1, &session)
            .expect("attempt must be attributed");
        pump.register(session.clone(), base_messages());
        let pump_task = tokio::spawn(Arc::clone(&pump).run(harness.subscribe()));

        events
            .send(AgentEvent::SessionStarted {
                session_id: session.clone(),
            })
            .expect("pump receiver must stay open");
        events
            .send(AgentEvent::ModelDelta {
                text: "not persisted".to_owned(),
            })
            .expect("pump receiver must stay open");
        events
            .send(AgentEvent::SessionEnded {
                session_id: session.clone(),
                reason: SessionEndReason::Completed,
            })
            .expect("pump receiver must stay open");

        let snapshot = pump
            .wait_for(&session)
            .await
            .expect("the transcript must still complete");
        assert_eq!(snapshot.last_output, "not persisted");
        assert!(
            pump.integrity_failure()
                .expect("the failure must be observable")
                .contains("injected storage failure")
        );
        assert!(
            cancel.is_cancelled(),
            "a trace failure must cancel the agent"
        );
        assert_eq!(
            journal.attempts(),
            2,
            "only turn_started and the first failing event may be attempted"
        );
        assert_eq!(
            journal.records().len(),
            1,
            "no event after the failure may be written"
        );

        let error = pump
            .finish_turn(SessionEndReason::Completed)
            .await
            .expect_err("a failed trace can never close");
        assert!(matches!(error, RecorderError::Integrity { .. }));
        assert!(
            !journal
                .records()
                .iter()
                .any(|(trace, _)| matches!(trace.payload, TracePayload::TurnEnded { .. })),
            "a storage failure must not produce turn_ended"
        );
        pump_task.abort();
    }

    #[tokio::test]
    async fn a_broadcast_gap_with_a_recorder_is_persisted_and_prevents_closure() {
        let journal = Arc::new(RecordingJournal::default());
        let (sender, receiver) = broadcast::channel(1);
        let harness = Arc::new(Harness::with_event_capacity(Vec::new(), 8));
        let cancel = CancellationToken::new();
        let governance = Arc::new(GovernanceState::new(harness.events(), cancel.clone()));
        let pump = Arc::new(EventPump::assemble(
            harness,
            governance,
            Some(Arc::clone(&journal) as Arc<dyn EventJournal>),
            PumpOutput::Silent,
            None,
            None,
        ));
        let session = SessionId::new("lagged-with-recorder");
        pump.begin_turn(
            TurnId::new("turn-gap"),
            TraceTurnMode::Fresh,
            base_messages(),
            session.clone(),
        )
        .await
        .expect("turn trace must start");
        pump.begin_attempt(1, &session)
            .expect("attempt must be attributed");
        pump.register(session, base_messages());
        let pump_task = tokio::spawn(Arc::clone(&pump).run(receiver));

        // All sends happen before the spawned pump polls, so a capacity-one
        // channel deterministically reports a gap.
        for _ in 0..3 {
            sender
                .send(AgentEvent::ModelDelta {
                    text: "lost".to_owned(),
                })
                .expect("pump receiver must stay open");
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if journal
                    .records()
                    .iter()
                    .any(|(trace, _)| matches!(trace.payload, TracePayload::Gap { .. }))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the gap must be persisted");

        assert!(pump.has_lagged());
        assert!(
            pump.integrity_failure()
                .expect("a gap must fail the turn")
                .contains("lagged")
        );
        assert!(cancel.is_cancelled(), "a gap must cancel the agent");
        let gap = journal
            .records()
            .into_iter()
            .find_map(|(trace, legacy)| match trace.payload {
                TracePayload::Gap { skipped, .. } => Some((skipped, legacy)),
                _ => None,
            })
            .expect("the gap entry must exist");
        assert!(gap.0 >= 1, "the gap must report the lost events");
        assert!(gap.1.is_none());

        let error = pump
            .finish_turn(SessionEndReason::Completed)
            .await
            .expect_err("a gap can never be closed");
        assert!(matches!(error, RecorderError::Integrity { .. }));
        assert!(
            !journal
                .records()
                .iter()
                .any(|(trace, _)| matches!(trace.payload, TracePayload::TurnEnded { .. })),
            "a gap must not produce turn_ended"
        );
        pump_task.abort();
    }

    #[tokio::test]
    async fn an_attempt_cannot_start_before_its_turn_trace() {
        let journal = Arc::new(RecordingJournal::default());
        let (pump, _harness, _events, _cancel) = traced_fixture(Arc::clone(&journal));

        let error = pump
            .begin_attempt(1, &SessionId::new("orphan"))
            .expect_err("an attempt without a turn trace must be rejected");
        assert!(matches!(error, RecorderError::Integrity { .. }));
        let error = pump
            .begin_attempt(0, &SessionId::new("zero"))
            .expect_err("attempt numbers are one-based");
        assert!(matches!(error, RecorderError::Integrity { .. }));
        assert!(
            journal.records().is_empty(),
            "a rejected attempt must write nothing"
        );
    }
}
