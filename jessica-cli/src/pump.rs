//! Event pump for the CLI: live rendering, transcript recovery, sensors, and
//! guard checkpoints.
//!
//! The runtime loop returns only a [`SessionEndReason`](jessica_core::types::SessionEndReason).
//! Acceptance retries need the full conversation, so this pump rebuilds it from
//! the observable event stream. The same pump runs the post-tool sensor pipeline
//! and evaluates guards at the first tool call of each model round.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, MutexGuard};

use jessica_core::guard::GuardContext;
use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId, ToolCall, ToolResult};
use jessica_harness::{Harness, SensorReport};
use tokio::sync::{Notify, broadcast};

use crate::events::EventRecorder;
use crate::governance::GovernanceState;

/// Conversation and final text recovered from one attempt's event stream.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SessionSnapshot {
    /// Reconstructed conversation fed to the acceptance check and retries.
    pub(crate) messages: Vec<Message>,
    /// Final assistant text from the attempt.
    pub(crate) last_output: String,
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

/// Shared pump state guarded by a standard mutex.
#[derive(Debug, Default)]
struct PumpState {
    current: Option<SessionId>,
    active: BTreeMap<SessionId, Transcript>,
    ended: BTreeMap<SessionId, SessionSnapshot>,
}

/// Reconstructs sessions, runs sensors, and evaluates guards from events.
pub(crate) struct EventPump {
    harness: Arc<Harness>,
    governance: Arc<GovernanceState>,
    recorder: Option<Arc<EventRecorder>>,
    state: Mutex<PumpState>,
    notify: Notify,
}

impl EventPump {
    /// Creates a pump around the harness and governance of one CLI turn.
    ///
    /// When `recorder` is present, every event is appended to the session's
    /// event stream before it is rendered. Persistence is best-effort: a store
    /// failure is warned about and does not stop the agent.
    pub(crate) fn new(
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        recorder: Option<Arc<EventRecorder>>,
    ) -> Self {
        Self {
            harness,
            governance,
            recorder,
            state: Mutex::new(PumpState::default()),
            notify: Notify::new(),
        }
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
                    eprintln!("[warn] skipped {skipped} events");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    /// Waits until the pump has processed `session_id`'s terminal event.
    ///
    /// Returns `None` when the session was never registered, which prevents a
    /// typo or a torn-down pump from waiting forever.
    pub(crate) async fn wait_for(&self, session_id: &SessionId) -> Option<SessionSnapshot> {
        loop {
            let notified = self.notify.notified();
            {
                let state = self.lock_state();
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

    /// Applies one event to the observable, sensor, and guard paths.
    async fn handle(&self, event: AgentEvent) {
        self.persist(&event).await;

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
            print_event(&event);
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
                    print_sensor_report(&report);
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

    /// Appends one event to the store when recording is enabled.
    ///
    /// Events are processed sequentially by [`EventPump::run`], so the store
    /// observes the same order as every other subscriber. Events with no
    /// attributed session (for example an acceptance failure emitted between
    /// attempts) are not persisted.
    async fn persist(&self, event: &AgentEvent) {
        let Some(recorder) = &self.recorder else {
            return;
        };
        let session_id = {
            let state = self.lock_state();
            match event {
                AgentEvent::SessionStarted { session_id }
                | AgentEvent::SessionEnded { session_id, .. } => Some(session_id.clone()),
                _ => state.current.clone(),
            }
        };
        let Some(session_id) = session_id else {
            return;
        };
        if let Err(error) = recorder.append(&session_id, event).await {
            eprintln!("[warn] failed to persist an event for session {session_id}: {error}");
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, PumpState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl PumpState {
    fn current_transcript_mut(&mut self) -> Option<&mut Transcript> {
        let session_id = self.current.clone()?;
        self.active.get_mut(&session_id)
    }
}

/// Renders one structured event for the terminal.
fn print_event(event: &AgentEvent) {
    match event {
        AgentEvent::SessionStarted { session_id } => {
            println!("[session] started {session_id}");
        }
        AgentEvent::ModelDelta { text } => {
            print!("{text}");
            let _ = io::stdout().flush();
        }
        AgentEvent::ToolCallStarted { call } => {
            println!(
                "\n[tool] {} {}",
                call.name,
                summarize(&call.arguments.to_string(), 120)
            );
        }
        AgentEvent::ToolCallFinished { result } => {
            let status = if result.is_error { "error" } else { "ok" };
            println!("[tool:{status}] {}", summarize(&result.content, 200));
        }
        AgentEvent::PolicyDenied { call, reason } => {
            println!("[policy] denied {}: {reason}", call.name);
        }
        AgentEvent::BudgetExceeded { kind, message } => {
            println!("\n[budget] {kind:?}: {message}");
        }
        AgentEvent::AcceptanceFailed { attempt, feedback } => {
            println!(
                "[acceptance] attempt {attempt} failed: {}",
                summarize(feedback, 200)
            );
        }
        AgentEvent::Error { message } => {
            eprintln!("\n[error] {message}");
        }
        AgentEvent::SessionEnded { session_id, reason } => {
            println!("\n[session] ended {session_id}: {reason:?}");
        }
    }
}

/// Prints one line per sensor observation.
fn print_sensor_report(report: &SensorReport) {
    for observation in report.observations() {
        let status = if observation.result.passed {
            "pass"
        } else {
            "fail"
        };
        println!(
            "[sensor:{status}] {}: {}",
            observation.sensor,
            summarize(&observation.result.output, 200)
        );
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

    fn base_messages() -> Vec<Message> {
        vec![Message::system("system"), Message::user("fix it")]
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
}
