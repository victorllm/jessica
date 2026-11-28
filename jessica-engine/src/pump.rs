//! Event pump: live rendering, transcript recovery, sensors, and guard
//! checkpoints.
//!
//! The runtime loop returns only a [`SessionEndReason`](jessica_core::types::SessionEndReason).
//! Acceptance retries need the full conversation, so this pump rebuilds it from
//! the observable event stream. The same pump runs the post-tool sensor pipeline
//! and evaluates guards at the first tool call of each model round.
//!
//! The pump is an observer. Durable recording happens on the producer side
//! before an event is broadcast, so a slow pump cannot lose events from the
//! trace. A pump gap still means the reconstructed transcript and sensor path
//! are incomplete; [`PumpIntegrity`] decides whether that fails the session,
//! keeps a human-console warning, or is merely reported in the outcome.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use jessica_core::guard::GuardContext;
use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId, ToolCall, ToolResult};
use jessica_harness::{Harness, SensorReport};
use tokio::sync::{Notify, broadcast};

use crate::governance::GovernanceState;
use crate::ports::{PumpConsole, TurnObserver as UpdateSink};

/// Conversation and final text recovered from one attempt's event stream.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionSnapshot {
    /// Reconstructed conversation fed to the acceptance check and retries.
    pub messages: Vec<Message>,
    /// Final assistant text from the attempt.
    pub last_output: String,
}

/// Human-readable output policy for the event pump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpOutput {
    /// Render events to the terminal exactly as the CLI always has.
    HumanConsole,
    /// Never render. Events reach a programmatic client only through its
    /// [`UpdateSink`], so stdout stays available for a protocol stream.
    Silent,
}

/// What the pump should do when its broadcast receiver reports a gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagHandling {
    /// Keep running, mark the pump as lagged, and expose the gap in the
    /// caller's outcome. The producer-side trace is unaffected.
    Continue,
    /// Mark the current session as failed, so waiting callers observe that its
    /// reconstructed transcript is incomplete.
    FailSession,
    /// Keep running without marking the session, for human-console output that
    /// already warns about the gap.
    Warn,
}

/// Policy consulted when the pump loses events to broadcast lag.
///
/// The engine's own turns continue and report the gap in
/// [`TurnOutcome::lagged`](crate::TurnOutcome). The CLI's journal fails an
/// active trace while continuing to record the producer tail, and keeps its
/// original no-persistence behavior.
pub trait PumpIntegrity: Send + Sync {
    /// Decides how to handle `skipped` events lost by the pump's receiver.
    fn broadcast_lagged(&self, skipped: u64, output: PumpOutput) -> LagHandling;
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

    /// Starts the explicitly numbered model round.
    ///
    /// The event stream carries the runtime's round number, which may continue
    /// past one when a checkpoint is resumed. Trusting it keeps guard context
    /// aligned without double-counting tool batches.
    fn on_round_started(&mut self, round: u32) {
        self.flush_pending();
        self.round = round;
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

/// Shared pump state guarded by a standard mutex.
#[derive(Debug, Default)]
struct PumpState {
    current: Option<SessionId>,
    active: BTreeMap<SessionId, Transcript>,
    ended: BTreeMap<SessionId, SessionSnapshot>,
    /// Sessions whose transcript lost events to broadcast lag.
    failed: BTreeSet<SessionId>,
}

/// Reconstructs sessions, runs sensors, and evaluates guards from events.
///
/// The pump is an observer. Durable recording happens on the producer side
/// before an event is broadcast, so a slow pump cannot lose events from the
/// trace. A pump gap still means the reconstructed transcript and sensor path
/// are incomplete; [`PumpIntegrity`] decides how the caller treats that gap.
pub struct EventPump {
    harness: Arc<Harness>,
    governance: Arc<GovernanceState>,
    integrity: Arc<dyn PumpIntegrity>,
    output: PumpOutput,
    console: Option<Arc<dyn PumpConsole>>,
    sink: Option<Arc<dyn UpdateSink>>,
    state: Mutex<PumpState>,
    notify: Notify,
    lagged: AtomicBool,
}

impl EventPump {
    /// Creates a pump around the harness, governance, and integrity policy of
    /// one turn.
    ///
    /// The pump never writes durable state itself. It shares `integrity` only
    /// to report a broadcast gap; `output` selects terminal rendering; `sink`
    /// receives every event in pump order.
    pub fn new(
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        integrity: Arc<dyn PumpIntegrity>,
        output: PumpOutput,
        sink: Option<Arc<dyn UpdateSink>>,
    ) -> Self {
        Self::new_with_console(harness, governance, integrity, output, None, sink)
    }

    /// Creates a pump with a host-supplied human-console renderer.
    ///
    /// The console is used only when `output` is [`PumpOutput::HumanConsole`];
    /// silent output never invokes it. A `None` console keeps the built-in
    /// terminal renderer, so an embedder can override rendering without
    /// changing the pump's event or guard behavior.
    pub fn new_with_console(
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        integrity: Arc<dyn PumpIntegrity>,
        output: PumpOutput,
        console: Option<Arc<dyn PumpConsole>>,
        sink: Option<Arc<dyn UpdateSink>>,
    ) -> Self {
        let console = match output {
            PumpOutput::HumanConsole => {
                Some(console.unwrap_or_else(|| Arc::new(StdoutConsole) as Arc<dyn PumpConsole>))
            }
            PumpOutput::Silent => None,
        };
        Self::assemble(harness, governance, integrity, output, console, sink)
    }

    /// Assembles a pump from already-resolved rendering state.
    fn assemble(
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        integrity: Arc<dyn PumpIntegrity>,
        output: PumpOutput,
        console: Option<Arc<dyn PumpConsole>>,
        sink: Option<Arc<dyn UpdateSink>>,
    ) -> Self {
        Self {
            harness,
            governance,
            integrity,
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
    /// [`LagHandling::Continue`] and [`LagHandling::FailSession`] set this
    /// flag; [`LagHandling::Warn`] leaves it unset so human-console output
    /// keeps its original behavior.
    pub fn has_lagged(&self) -> bool {
        self.lagged.load(Ordering::SeqCst)
    }

    /// Registers the initial conversation before the agent starts.
    ///
    /// The session becomes the current attribution target so a resumed stream,
    /// which has no `SessionStarted` event, still reaches the transcript.
    pub fn register(&self, session_id: SessionId, messages: Vec<Message>) {
        let mut state = self.lock_state();
        state.ended.remove(&session_id);
        state
            .active
            .insert(session_id.clone(), Transcript::new(messages));
        state.current = Some(session_id);
    }

    /// Consumes events until the channel closes.
    pub async fn run(self: Arc<Self>, mut receiver: broadcast::Receiver<AgentEvent>) {
        loop {
            match receiver.recv().await {
                Ok(event) => self.handle(event).await,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    match self.integrity.broadcast_lagged(skipped, self.output) {
                        LagHandling::Continue => {
                            // The producer recorded every event before
                            // broadcasting, so the trace is complete even
                            // though the pump's transcript is not.
                            self.lagged.store(true, Ordering::SeqCst);
                        }
                        // A protocol client cannot be told that its update
                        // stream is incomplete, so silent mode treats the lag
                        // as a failed turn instead of continuing with a
                        // corrupt transcript.
                        LagHandling::FailSession => self.mark_lagged(),
                        LagHandling::Warn => {
                            eprintln!("[warn] skipped {skipped} events");
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    /// Waits until the pump has processed `session_id`'s terminal event.
    ///
    /// Returns `None` when the session was never registered or when the
    /// integrity policy failed the session after a broadcast gap, which
    /// prevents a typo or a torn-down pump from waiting forever.
    pub async fn wait_for(&self, session_id: &SessionId) -> Option<SessionSnapshot> {
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
            AgentEvent::SessionStarted {
                session_id,
                messages,
            } => {
                let mut state = self.lock_state();
                state
                    .active
                    .entry(session_id.clone())
                    .or_insert_with(|| Transcript::new(messages));
                state.current = Some(session_id);
            }
            AgentEvent::RoundStarted { round } => {
                let mut state = self.lock_state();
                if let Some(transcript) = state.current_transcript_mut() {
                    transcript.on_round_started(round);
                }
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
                        self.governance.trip(event).await;
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
            AgentEvent::RoundFinished { .. }
            | AgentEvent::PermissionDecided { .. }
            | AgentEvent::ChildSessionStarted { .. }
            | AgentEvent::ChildSessionEnded { .. }
            | AgentEvent::PolicyDenied { .. }
            | AgentEvent::BudgetExceeded { .. }
            | AgentEvent::AcceptanceFailed { .. }
            | AgentEvent::Error { .. } => {}
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
            AgentEvent::SessionStarted { session_id, .. } => {
                console.line(&format!("[session] started {session_id}"));
            }
            // Round boundaries, permission decisions, and child lifecycle are
            // structured trace facts; the existing delta/tool/session lines
            // already render the user-observable progress.
            AgentEvent::RoundStarted { .. }
            | AgentEvent::RoundFinished { .. }
            | AgentEvent::PermissionDecided { .. }
            | AgentEvent::ChildSessionStarted { .. }
            | AgentEvent::ChildSessionEnded { .. } => {}
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
    use jessica_core::types::Role;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    fn base_messages() -> Vec<Message> {
        vec![Message::system("system"), Message::user("fix it")]
    }

    /// Integrity policy that keeps running after a gap.
    struct ContinueIntegrity;

    impl PumpIntegrity for ContinueIntegrity {
        fn broadcast_lagged(&self, _skipped: u64, _output: PumpOutput) -> LagHandling {
            LagHandling::Continue
        }
    }

    /// Integrity policy that applies one fixed decision.
    struct FixedIntegrity(LagHandling);

    impl PumpIntegrity for FixedIntegrity {
        fn broadcast_lagged(&self, _skipped: u64, _output: PumpOutput) -> LagHandling {
            self.0
        }
    }

    fn continue_integrity() -> Arc<dyn PumpIntegrity> {
        Arc::new(ContinueIntegrity)
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
        let pump = Arc::new(EventPump::new_with_console(
            Arc::clone(&harness),
            governance,
            continue_integrity(),
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
                messages: Vec::new(),
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
                messages: Vec::new(),
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
    async fn a_failing_integrity_policy_marks_the_current_session() {
        let (sender, receiver) = broadcast::channel(1);
        let harness = Arc::new(Harness::with_event_capacity(Vec::new(), 8));
        let governance = Arc::new(GovernanceState::new(
            harness.events(),
            CancellationToken::new(),
        ));
        let pump = Arc::new(EventPump::new_with_console(
            harness,
            governance,
            Arc::new(FixedIntegrity(LagHandling::FailSession)),
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
            "a failed session must be observable instead of a partial transcript"
        );
        assert!(pump.has_lagged(), "the pump must record the lag state");
        pump_task.abort();
    }

    #[tokio::test]
    async fn a_warn_policy_keeps_the_original_console_behavior() {
        let (sender, receiver) = broadcast::channel(1);
        let harness = Arc::new(Harness::with_event_capacity(Vec::new(), 8));
        let governance = Arc::new(GovernanceState::new(
            harness.events(),
            CancellationToken::new(),
        ));
        let console = Arc::new(RecordingConsole::default());
        let pump = Arc::new(EventPump::new_with_console(
            harness,
            governance,
            Arc::new(FixedIntegrity(LagHandling::Warn)),
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
                messages: Vec::new(),
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
    async fn a_continue_policy_exposes_the_lag_and_still_reaches_the_terminal_event() {
        let (sender, receiver) = broadcast::channel(1);
        let harness = Arc::new(Harness::with_event_capacity(Vec::new(), 8));
        let governance = Arc::new(GovernanceState::new(
            harness.events(),
            CancellationToken::new(),
        ));
        let pump = Arc::new(EventPump::new_with_console(
            harness,
            governance,
            continue_integrity(),
            PumpOutput::Silent,
            None,
            None,
        ));
        let session = SessionId::new("continue-lagged");
        pump.register(session.clone(), base_messages());
        let pump_task = tokio::spawn(Arc::clone(&pump).run(receiver));

        for event in [
            AgentEvent::SessionStarted {
                session_id: session.clone(),
                messages: Vec::new(),
            },
            AgentEvent::ModelDelta {
                text: "lost".to_owned(),
            },
            AgentEvent::SessionEnded {
                session_id: session.clone(),
                reason: SessionEndReason::Completed,
            },
        ] {
            sender
                .send(event)
                .expect("the pump receiver must stay open");
        }

        let snapshot = pump
            .wait_for(&session)
            .await
            .expect("a continue policy must still observe the terminal event");
        assert_eq!(
            snapshot.messages,
            base_messages(),
            "the dropped event must not fabricate transcript content"
        );
        assert!(
            pump.has_lagged(),
            "a continue policy must expose the gap to its caller"
        );
        pump_task.abort();
    }
}
