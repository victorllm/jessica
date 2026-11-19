//! Behaviour tests for session recovery and continuation.
//!
//! The central test runs a session to completion, recovers a checkpoint from
//! the recorded events of its first round, and resumes from that checkpoint
//! with a brand-new provider and tool. The resumed event sequence must equal the
//! uninterrupted tail exactly, and neither the model nor the tool may observe
//! work that was already recorded.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
use jessica_core::tool::{Tool, ToolRegistry, ToolSchema};
use jessica_core::types::{
    AgentEvent, Budget, Message, SessionEndReason, SessionId, TokenUsage, ToolCall, ToolResult,
};
use jessica_runtime::{
    AgentLoop, RecordedEvent, RecoveryError, SessionCheckpoint, recover_session,
};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::TryRecvError;
use tokio_util::sync::CancellationToken;

const MAX_ROUNDS: u32 = 3;

fn session_id() -> SessionId {
    SessionId::new("session-recovery")
}

fn test_budget() -> Budget {
    Budget::new(1_000_000, Duration::from_secs(5), MAX_ROUNDS)
}

/// One scripted provider response.
enum Response {
    /// A stream that ends after these events.
    Events(Vec<LlmEvent>),
}

/// Request captured by [`MockProvider`].
#[derive(Clone)]
struct RecordedCall {
    messages: Vec<Message>,
}

/// Provider that returns scripted responses and records every request.
struct MockProvider {
    responses: Mutex<VecDeque<Response>>,
    calls: Mutex<Vec<RecordedCall>>,
}

impl MockProvider {
    fn new(responses: Vec<Response>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<RecordedCall> {
        self.calls
            .lock()
            .expect("provider calls mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl LlmProvider for MockProvider {
    async fn stream_chat(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        self.calls
            .lock()
            .expect("provider calls mutex poisoned")
            .push(RecordedCall {
                messages: messages.to_vec(),
            });

        let response = self
            .responses
            .lock()
            .expect("provider responses mutex poisoned")
            .pop_front()
            .expect("the resumed loop must not call the provider without a scripted response");

        let Response::Events(events) = response;
        Ok(Box::pin(futures::stream::iter(events)))
    }
}

/// Tool that echoes its arguments and records every execution.
struct EchoTool {
    executions: Arc<Mutex<Vec<Value>>>,
}

#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "echo",
                "parameters": {"type": "object"}
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        self.executions
            .lock()
            .expect("executions mutex poisoned")
            .push(args.clone());
        ToolResult::success("ignored", format!("echo {args}"))
    }
}

fn echo_tool() -> (Arc<ToolRegistry>, Arc<Mutex<Vec<Value>>>) {
    let executions = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool {
            executions: Arc::clone(&executions),
        }))
        .expect("tool registration should succeed");
    (Arc::new(registry), executions)
}

fn tool_call_response(id: &str, step: u32) -> Response {
    Response::Events(vec![
        LlmEvent::ToolCallRequested {
            call: ToolCall::new(id, "echo", json!({"step": step})),
        },
        LlmEvent::Done { usage: None },
    ])
}

fn text_response(text: &str) -> Response {
    Response::Events(vec![
        LlmEvent::TextDelta {
            text: text.to_owned(),
        },
        LlmEvent::Done { usage: None },
    ])
}

fn drain(receiver: &mut broadcast::Receiver<AgentEvent>) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    loop {
        match receiver.try_recv() {
            Ok(event) => events.push(event),
            Err(TryRecvError::Empty | TryRecvError::Closed) => break,
            Err(TryRecvError::Lagged(skipped)) => {
                panic!("event receiver lagged by {skipped} events");
            }
        }
    }
    events
}

/// Runs the full scripted session and returns everything needed to compare it.
async fn run_uninterrupted() -> (
    SessionEndReason,
    Vec<AgentEvent>,
    Arc<MockProvider>,
    Arc<Mutex<Vec<Value>>>,
) {
    let provider = Arc::new(MockProvider::new(vec![
        tool_call_response("call-1", 1),
        tool_call_response("call-2", 2),
        text_response("finished"),
    ]));
    let (tools, executions) = echo_tool();
    let (events, mut receiver) = broadcast::channel(128);
    let agent = AgentLoop::new(
        Arc::clone(&provider) as Arc<dyn LlmProvider>,
        tools,
        test_budget(),
        events,
    );

    let reason = agent
        .run(
            session_id(),
            vec![Message::user("go")],
            CancellationToken::new(),
        )
        .await;
    (reason, drain(&mut receiver), provider, executions)
}

/// Runs a resumed loop and returns its events, provider calls, and executions.
async fn resume(
    checkpoint: jessica_runtime::SessionCheckpoint,
    responses: Vec<Response>,
) -> (
    SessionEndReason,
    Vec<AgentEvent>,
    Arc<MockProvider>,
    Arc<Mutex<Vec<Value>>>,
) {
    let provider = Arc::new(MockProvider::new(responses));
    let (tools, executions) = echo_tool();
    let (events, mut receiver) = broadcast::channel(128);
    let agent = AgentLoop::new(
        Arc::clone(&provider) as Arc<dyn LlmProvider>,
        tools,
        test_budget(),
        events,
    );

    let reason = agent.resume(checkpoint, CancellationToken::new()).await;
    (reason, drain(&mut receiver), provider, executions)
}

/// Assigns one-based sequence numbers to an event sequence.
fn recorded(events: &[AgentEvent]) -> Vec<RecordedEvent> {
    events
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, event)| RecordedEvent::new(index as u64 + 1, event))
        .collect()
}

#[tokio::test]
async fn resumed_execution_matches_the_uninterrupted_tail_without_replaying_work() {
    let (reason, events, provider, uninterrupted_executions) = run_uninterrupted().await;
    assert_eq!(reason, SessionEndReason::Completed);
    assert_eq!(
        *uninterrupted_executions
            .lock()
            .expect("executions mutex poisoned"),
        vec![json!({"step": 1}), json!({"step": 2})]
    );

    // The checkpoint is the first complete tool batch: assistant message plus
    // the tool result are all observable by then.
    let checkpoint_seq = events
        .iter()
        .position(|event| matches!(event, AgentEvent::ToolCallFinished { result } if result.tool_call_id == "call-1"))
        .expect("the first tool must finish")
        as u64
        + 1;
    assert_eq!(
        events[checkpoint_seq as usize - 1],
        AgentEvent::ToolCallFinished {
            result: ToolResult::success("call-1", "echo {\"step\":1}"),
        }
    );

    let checkpoint = recover_session(
        &session_id(),
        vec![Message::user("go")],
        &recorded(&events),
        Some(checkpoint_seq),
    )
    .expect("a complete tool batch is a stable checkpoint");

    assert_eq!(checkpoint.session_id(), &session_id());
    assert_eq!(checkpoint.last_seq(), checkpoint_seq);
    assert_eq!(checkpoint.rounds_completed(), 1);
    assert_eq!(checkpoint.end_reason(), None);
    let uninterrupted_calls = provider.calls();
    assert_eq!(
        checkpoint.messages(),
        uninterrupted_calls[1].messages.as_slice(),
        "the checkpoint must see exactly what the uninterrupted second model call saw"
    );

    // Resume with a fresh provider and tool that only contain round two and
    // three. If recovery replayed round one, the provider would run out of
    // scripted responses and the tool would record a second step-1 execution.
    let (resumed_reason, resumed_events, resumed_provider, resumed_executions) = resume(
        checkpoint,
        vec![tool_call_response("call-2", 2), text_response("finished")],
    )
    .await;

    assert_eq!(resumed_reason, SessionEndReason::Completed);
    assert_eq!(
        resumed_events,
        events[checkpoint_seq as usize..].to_vec(),
        "the resumed event sequence must equal the uninterrupted tail exactly"
    );
    assert_eq!(
        *resumed_executions
            .lock()
            .expect("executions mutex poisoned"),
        vec![json!({"step": 2})],
        "only the unfinished round's tool may run"
    );

    let resumed_calls = resumed_provider.calls();
    assert_eq!(resumed_calls.len(), 2);
    assert_eq!(resumed_calls[0].messages, uninterrupted_calls[1].messages);
    assert_eq!(resumed_calls[1].messages, uninterrupted_calls[2].messages);
    assert_eq!(
        provider.calls().len(),
        3,
        "the original provider saw the same three rounds as an uninterrupted run"
    );
}

#[tokio::test]
async fn checkpoints_in_the_middle_of_a_turn_are_rejected() {
    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::ToolCallStarted {
            call: ToolCall::new("call-1", "echo", json!({"step": 1})),
        },
    ]);
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::IncompleteRound { seq, tool_call_id }) => {
            assert_eq!(seq, 2);
            assert_eq!(tool_call_id, "call-1");
        }
        other => panic!("expected IncompleteRound, got {other:?}"),
    }

    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::ToolCallStarted {
            call: ToolCall::new("call-1", "echo", json!({})),
        },
        AgentEvent::ToolCallFinished {
            result: ToolResult::success("call-1", "done"),
        },
        AgentEvent::ModelDelta {
            text: "partial".to_owned(),
        },
    ]);
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::IncompleteModelTurn { seq }) => assert_eq!(seq, 4),
        other => panic!("expected IncompleteModelTurn, got {other:?}"),
    }
}

#[tokio::test]
async fn an_ended_checkpoint_returns_the_recorded_reason_without_calling_anything() {
    let (reason, events, provider, executions) = run_uninterrupted().await;
    assert_eq!(reason, SessionEndReason::Completed);

    let checkpoint = recover_session(
        &session_id(),
        vec![Message::user("go")],
        &recorded(&events),
        None,
    )
    .expect("the complete stream must recover");
    assert_eq!(checkpoint.end_reason(), Some(SessionEndReason::Completed));
    // Explicit round boundaries separate the two tool-only model calls from
    // the final text round even though the text-only heuristic could not.
    assert_eq!(checkpoint.rounds_completed(), 3);
    assert_eq!(
        checkpoint.messages().last(),
        Some(&Message::assistant("finished"))
    );

    // No scripted responses: resume must not call the provider at all.
    let (resumed_reason, resumed_events, resumed_provider, resumed_executions) =
        resume(checkpoint, Vec::new()).await;

    assert_eq!(resumed_reason, SessionEndReason::Completed);
    assert!(resumed_events.is_empty(), "resume must emit no events");
    assert!(
        resumed_provider.calls().is_empty(),
        "resume must not call the model"
    );
    assert!(
        resumed_executions
            .lock()
            .expect("executions mutex poisoned")
            .is_empty(),
        "resume must not execute tools"
    );
    assert_eq!(provider.calls().len(), MAX_ROUNDS as usize);
    assert_eq!(
        *executions.lock().expect("executions mutex poisoned"),
        vec![json!({"step": 1}), json!({"step": 2})]
    );
}

#[tokio::test]
async fn cancelled_rounds_keep_their_interrupted_tool_and_stop_resuming() {
    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::ToolCallStarted {
            call: ToolCall::new("call-1", "echo", json!({"step": 1})),
        },
        AgentEvent::SessionEnded {
            session_id: session_id(),
            reason: SessionEndReason::Cancelled,
        },
    ]);

    let checkpoint = recover_session(&session_id(), vec![Message::user("go")], &events, None)
        .expect("a terminal event makes even an unfinished batch recoverable");
    assert_eq!(checkpoint.end_reason(), Some(SessionEndReason::Cancelled));
    assert_eq!(
        checkpoint.messages(),
        &[
            Message::user("go"),
            Message::assistant_with_tool_calls(
                "",
                vec![ToolCall::new("call-1", "echo", json!({"step": 1}))]
            ),
            Message::tool("call-1", "tool error: tool execution was interrupted"),
        ]
    );
}

#[tokio::test]
async fn policy_and_budget_events_are_informational_during_replay() {
    let call = ToolCall::new("call-1", "echo", json!({}));
    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::ToolCallStarted { call: call.clone() },
        AgentEvent::PolicyDenied {
            call: call.clone(),
            reason: "denied".to_owned(),
        },
        AgentEvent::BudgetExceeded {
            kind: jessica_core::types::BudgetKind::TokenLimit,
            message: "token budget".to_owned(),
        },
        AgentEvent::ToolCallFinished {
            result: ToolResult::error("call-1", "denied by policy"),
        },
        AgentEvent::ModelDelta {
            text: "adapted".to_owned(),
        },
        AgentEvent::SessionEnded {
            session_id: session_id(),
            reason: SessionEndReason::Completed,
        },
    ]);

    let checkpoint = recover_session(&session_id(), vec![Message::user("go")], &events, None)
        .expect("governance events must not break tool pairing");
    assert_eq!(
        checkpoint.messages(),
        &[
            Message::user("go"),
            Message::assistant_with_tool_calls("", vec![call]),
            Message::tool("call-1", "tool error: denied by policy"),
            Message::assistant("adapted"),
        ]
    );
}

#[tokio::test]
async fn inconsistent_streams_are_rejected() {
    let other = SessionId::new("other-session");
    let events = recorded(&[AgentEvent::SessionStarted {
        session_id: other.clone(),
        messages: Vec::new(),
    }]);
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::SessionMismatch { expected, found }) => {
            assert_eq!(expected, "session-recovery");
            assert_eq!(found, "other-session");
        }
        other => panic!("expected SessionMismatch, got {other:?}"),
    }

    let events = vec![
        RecordedEvent::new(
            2,
            AgentEvent::SessionStarted {
                session_id: session_id(),
                messages: Vec::new(),
            },
        ),
        RecordedEvent::new(
            2,
            AgentEvent::ModelDelta {
                text: "duplicate".to_owned(),
            },
        ),
    ];
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::NonIncreasingSequence { previous, next }) => {
            assert_eq!(previous, 2);
            assert_eq!(next, 2);
        }
        other => panic!("expected NonIncreasingSequence, got {other:?}"),
    }

    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::SessionEnded {
            session_id: session_id(),
            reason: SessionEndReason::Completed,
        },
        AgentEvent::ModelDelta {
            text: "late".to_owned(),
        },
    ]);
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::EventAfterSessionEnd { seq }) => assert_eq!(seq, 3),
        other => panic!("expected EventAfterSessionEnd, got {other:?}"),
    }

    let events = recorded(&[AgentEvent::ModelDelta {
        text: "no start".to_owned(),
    }]);
    assert_eq!(
        recover_session(&session_id(), Vec::new(), &events, None),
        Err(RecoveryError::MissingSessionStart)
    );
}

/// Builds a complete explicit-round transcript with the given round usage.
fn explicit_round_events(usage: Option<TokenUsage>) -> Vec<AgentEvent> {
    let messages = vec![Message::system("system"), Message::user("go")];
    let call = ToolCall::new("call-1", "echo", json!({"step": 1}));
    vec![
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages,
        },
        AgentEvent::RoundStarted { round: 1 },
        AgentEvent::ModelDelta {
            text: "thinking".to_owned(),
        },
        AgentEvent::RoundFinished { round: 1, usage },
        AgentEvent::RoundStarted { round: 2 },
        AgentEvent::RoundFinished { round: 2, usage },
        AgentEvent::ToolCallStarted { call: call.clone() },
        AgentEvent::ToolCallFinished {
            result: ToolResult::success("call-1", "tool output"),
        },
        AgentEvent::RoundStarted { round: 3 },
        AgentEvent::ModelDelta {
            text: "done".to_owned(),
        },
        AgentEvent::RoundFinished { round: 3, usage },
        AgentEvent::SessionEnded {
            session_id: session_id(),
            reason: SessionEndReason::Completed,
        },
    ]
}

#[test]
fn explicit_rounds_rebuild_the_transcript_without_using_usage() {
    let with_usage = explicit_round_events(Some(TokenUsage::new(11, 7)));
    let checkpoint = recover_session(&session_id(), Vec::new(), &recorded(&with_usage), None)
        .expect("explicit rounds must recover");
    assert_eq!(checkpoint.rounds_completed(), 3);
    assert_eq!(checkpoint.end_reason(), Some(SessionEndReason::Completed));
    assert_eq!(
        checkpoint.messages(),
        &[
            Message::system("system"),
            Message::user("go"),
            Message::assistant("thinking"),
            Message::assistant_with_tool_calls(
                "",
                vec![ToolCall::new("call-1", "echo", json!({"step": 1}))]
            ),
            Message::tool("call-1", "tool output"),
            Message::assistant("done"),
        ]
    );

    let without_usage = explicit_round_events(None);
    let usage_free = recover_session(&session_id(), Vec::new(), &recorded(&without_usage), None)
        .expect("usage-free explicit rounds must recover identically");
    assert_eq!(
        usage_free, checkpoint,
        "token usage is informational and must not change recovery"
    );
}

#[test]
fn session_started_messages_are_authoritative_and_must_match_the_caller() {
    let messages = vec![Message::system("event system"), Message::user("event user")];
    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: messages.clone(),
        },
        AgentEvent::RoundStarted { round: 1 },
        AgentEvent::RoundFinished {
            round: 1,
            usage: None,
        },
        AgentEvent::SessionEnded {
            session_id: session_id(),
            reason: SessionEndReason::Completed,
        },
    ]);

    let checkpoint = recover_session(&session_id(), Vec::new(), &events, None)
        .expect("the event must supply bootstrap messages when the caller has none");
    assert_eq!(&checkpoint.messages()[..2], messages.as_slice());

    let checkpoint = recover_session(&session_id(), messages.clone(), &events, None)
        .expect("agreeing caller and event messages must recover");
    assert_eq!(&checkpoint.messages()[..2], messages.as_slice());

    let conflicting = vec![Message::system("metadata"), Message::user("metadata")];
    match recover_session(&session_id(), conflicting, &events, None) {
        Err(RecoveryError::InputMismatch { seq }) => assert_eq!(seq, 1),
        other => panic!("expected InputMismatch, got {other:?}"),
    }
}

#[test]
fn malformed_explicit_rounds_are_rejected() {
    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::RoundStarted { round: 2 },
    ]);
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::RoundSequence {
            seq,
            expected,
            found,
        }) => {
            assert_eq!((seq, expected, found), (2, 1, 2));
        }
        other => panic!("expected RoundSequence, got {other:?}"),
    }

    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::RoundFinished {
            round: 1,
            usage: None,
        },
    ]);
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::UnexpectedRoundFinish { seq, round }) => {
            assert_eq!((seq, round), (2, 1));
        }
        other => panic!("expected UnexpectedRoundFinish, got {other:?}"),
    }

    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::RoundStarted { round: 1 },
        AgentEvent::RoundStarted { round: 2 },
    ]);
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::UnexpectedRoundStart { seq, round }) => {
            assert_eq!((seq, round), (3, 2));
        }
        other => panic!("expected UnexpectedRoundStart, got {other:?}"),
    }

    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::RoundStarted { round: 1 },
        AgentEvent::ToolCallStarted {
            call: ToolCall::new("call-1", "echo", json!({})),
        },
    ]);
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::MissingRoundFinish { seq }) => assert_eq!(seq, 3),
        other => panic!("expected MissingRoundFinish, got {other:?}"),
    }

    let events = recorded(&[
        AgentEvent::SessionStarted {
            session_id: session_id(),
            messages: Vec::new(),
        },
        AgentEvent::RoundStarted { round: 1 },
        AgentEvent::ModelDelta {
            text: "unfinished".to_owned(),
        },
        AgentEvent::SessionEnded {
            session_id: session_id(),
            reason: SessionEndReason::Completed,
        },
    ]);
    match recover_session(&session_id(), Vec::new(), &events, None) {
        Err(RecoveryError::IncompleteModelTurn { seq }) => assert_eq!(seq, 4),
        other => panic!("expected IncompleteModelTurn, got {other:?}"),
    }
}

#[test]
fn a_checkpoint_can_be_built_from_recorded_state_without_a_terminal_event() {
    let messages = vec![Message::user("continue"), Message::assistant("ready")];
    let checkpoint = SessionCheckpoint::from_recorded_state(session_id(), messages.clone(), 4);

    assert_eq!(checkpoint.session_id(), &session_id());
    assert_eq!(checkpoint.messages(), messages.as_slice());
    assert_eq!(checkpoint.rounds_completed(), 4);
    assert_eq!(checkpoint.last_seq(), 0);
    assert_eq!(checkpoint.end_reason(), None);
}
