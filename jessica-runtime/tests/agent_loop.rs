//! Behaviour tests for the agent loop.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
use jessica_core::tool::{Tool, ToolRegistry, ToolSchema};
use jessica_core::types::{
    AgentEvent, Budget, BudgetKind, Message, SessionEndReason, SessionId, ToolCall, ToolResult,
};
use jessica_runtime::AgentLoop;
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::TryRecvError;
use tokio_util::sync::CancellationToken;

fn session_id() -> SessionId {
    SessionId::new("session-1")
}

fn started() -> AgentEvent {
    AgentEvent::SessionStarted {
        session_id: session_id(),
    }
}

fn ended(reason: SessionEndReason) -> AgentEvent {
    AgentEvent::SessionEnded {
        session_id: session_id(),
        reason,
    }
}

fn test_budget(max_rounds: u32) -> Budget {
    Budget::new(1_000_000, Duration::from_secs(5), max_rounds)
}

/// One scripted provider response.
enum Response {
    /// A stream that ends after these events.
    Events(Vec<LlmEvent>),
    /// Events followed by a stream that never produces another item.
    EventsThenPending(Vec<LlmEvent>),
    /// The request itself fails.
    Error(LlmError),
}

/// Request captured by [`MockProvider`].
#[derive(Clone)]
struct RecordedCall {
    messages: Vec<Message>,
    tools: Vec<ToolSchema>,
}

/// Provider that returns scripted responses and records how it was called.
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
        tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        self.calls
            .lock()
            .expect("provider calls mutex poisoned")
            .push(RecordedCall {
                messages: messages.to_vec(),
                tools: tools.to_vec(),
            });

        let response = self
            .responses
            .lock()
            .expect("provider responses mutex poisoned")
            .pop_front()
            .expect("mock provider has no queued response");

        match response {
            Response::Events(events) => Ok(Box::pin(futures::stream::iter(events))),
            Response::EventsThenPending(events) => Ok(Box::pin(
                futures::stream::iter(events).chain(futures::stream::pending::<LlmEvent>()),
            )),
            Response::Error(error) => Err(error),
        }
    }
}

/// Tool that records every argument object it receives.
struct RecordingTool {
    name: &'static str,
    executions: Arc<Mutex<Vec<Value>>>,
    result: ToolResult,
}

impl RecordingTool {
    fn new(name: &'static str, executions: Arc<Mutex<Vec<Value>>>, result: ToolResult) -> Self {
        Self {
            name,
            executions,
            result,
        }
    }
}

#[async_trait]
impl Tool for RecordingTool {
    fn name(&self) -> &str {
        self.name
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": self.name,
                "parameters": {"type": "object"}
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        self.executions
            .lock()
            .expect("executions mutex poisoned")
            .push(args);
        self.result.clone()
    }
}

/// Tool that records full call metadata and rejects calls through `execute`.
struct CallAwareTool {
    calls: Arc<Mutex<Vec<ToolCall>>>,
}

impl CallAwareTool {
    fn new(calls: Arc<Mutex<Vec<ToolCall>>>) -> Self {
        Self { calls }
    }
}

#[async_trait]
impl Tool for CallAwareTool {
    fn name(&self) -> &str {
        "call_aware"
    }

    fn schema(&self) -> ToolSchema {
        json!({"type": "function", "function": {"name": "call_aware"}})
    }

    async fn execute(&self, _args: Value) -> ToolResult {
        ToolResult::error("unused", "execute must not be called")
    }

    async fn execute_with_call(&self, call: &ToolCall) -> ToolResult {
        self.calls
            .lock()
            .expect("calls mutex poisoned")
            .push(call.clone());
        ToolResult::success(call.id.clone(), "observed")
    }
}

/// Runs the loop with a fresh event channel and returns every emitted event.
async fn run(
    provider: Arc<MockProvider>,
    tools: Arc<ToolRegistry>,
    max_rounds: u32,
    messages: Vec<Message>,
    cancel: CancellationToken,
) -> (SessionEndReason, Vec<AgentEvent>) {
    let (events, mut receiver) = broadcast::channel(128);
    let agent = AgentLoop::new(provider, tools, test_budget(max_rounds), events);
    let reason = agent.run(session_id(), messages, cancel).await;
    (reason, drain(&mut receiver))
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

fn tool_call_response(id: &str) -> Response {
    Response::Events(vec![
        LlmEvent::ToolCallRequested {
            call: ToolCall::new(id, "echo", json!({"call": id})),
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

#[tokio::test]
async fn text_only_response_completes() {
    let provider = Arc::new(MockProvider::new(vec![Response::Events(vec![
        LlmEvent::TextDelta {
            text: "Hello".to_owned(),
        },
        LlmEvent::TextDelta {
            text: " world".to_owned(),
        },
        LlmEvent::Done { usage: None },
    ])]));

    let (reason, events) = run(
        Arc::clone(&provider),
        Arc::new(ToolRegistry::new()),
        4,
        vec![Message::user("hi")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::Completed);
    assert_eq!(
        events,
        vec![
            started(),
            AgentEvent::ModelDelta {
                text: "Hello".to_owned(),
            },
            AgentEvent::ModelDelta {
                text: " world".to_owned(),
            },
            ended(SessionEndReason::Completed),
        ]
    );

    let calls = provider.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].messages, vec![Message::user("hi")]);
    assert!(calls[0].tools.is_empty());
}

#[tokio::test]
async fn single_tool_call_is_executed_and_result_is_fed_back() {
    let executions = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(RecordingTool::new(
            "echo",
            Arc::clone(&executions),
            // The loop, not the tool, must stamp the originating call id.
            ToolResult::success("tool-provided-id", "tool output"),
        )))
        .expect("tool registration should succeed");
    let tools = Arc::new(registry);

    let call = ToolCall::new("call-1", "echo", json!({"value": 7}));
    let provider = Arc::new(MockProvider::new(vec![
        Response::Events(vec![
            LlmEvent::ToolCallRequested { call: call.clone() },
            LlmEvent::Done { usage: None },
        ]),
        text_response("done"),
    ]));

    let (reason, events) = run(
        Arc::clone(&provider),
        tools,
        4,
        vec![Message::user("go")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::Completed);
    assert_eq!(
        events,
        vec![
            started(),
            AgentEvent::ToolCallStarted { call: call.clone() },
            AgentEvent::ToolCallFinished {
                result: ToolResult::success("call-1", "tool output"),
            },
            AgentEvent::ModelDelta {
                text: "done".to_owned(),
            },
            ended(SessionEndReason::Completed),
        ]
    );

    assert_eq!(
        *executions.lock().expect("executions mutex poisoned"),
        vec![json!({"value": 7})]
    );

    let calls = provider.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].messages, vec![Message::user("go")]);
    assert_eq!(
        calls[1].messages,
        vec![
            Message::user("go"),
            Message::assistant_with_tool_calls("", vec![call]),
            Message::tool("call-1", "tool output"),
        ]
    );
}

#[tokio::test]
async fn agent_loop_passes_the_full_call_to_execute_with_call() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(CallAwareTool::new(Arc::clone(&calls))))
        .expect("tool registration should succeed");
    let tools = Arc::new(registry);

    let call = ToolCall::new("call-ctx", "call_aware", json!({"value": 9}));
    let provider = Arc::new(MockProvider::new(vec![
        Response::Events(vec![
            LlmEvent::ToolCallRequested { call: call.clone() },
            LlmEvent::Done { usage: None },
        ]),
        text_response("done"),
    ]));

    let (reason, events) = run(
        Arc::clone(&provider),
        tools,
        4,
        vec![Message::user("go")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::Completed);
    assert_eq!(
        *calls.lock().expect("calls mutex poisoned"),
        vec![call.clone()]
    );
    assert!(
        events.contains(&AgentEvent::ToolCallFinished {
            result: ToolResult::success("call-ctx", "observed"),
        }),
        "the call-aware result must be fed back with the provider call id"
    );
}

#[tokio::test]
async fn multiple_rounds_and_multiple_tool_calls_run_in_order() {
    let executions = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(RecordingTool::new(
            "echo",
            Arc::clone(&executions),
            ToolResult::success("ignored", "ok"),
        )))
        .expect("tool registration should succeed");
    let tools = Arc::new(registry);

    let provider = Arc::new(MockProvider::new(vec![
        Response::Events(vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new("call-1", "echo", json!({"step": 1})),
            },
            LlmEvent::Done { usage: None },
        ]),
        Response::Events(vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new("call-2", "echo", json!({"step": 2})),
            },
            LlmEvent::ToolCallRequested {
                call: ToolCall::new("call-3", "echo", json!({"step": 3})),
            },
            LlmEvent::Done { usage: None },
        ]),
        text_response("finished"),
    ]));

    let (reason, events) = run(
        Arc::clone(&provider),
        tools,
        4,
        vec![Message::user("go")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::Completed);
    assert_eq!(
        *executions.lock().expect("executions mutex poisoned"),
        vec![json!({"step": 1}), json!({"step": 2}), json!({"step": 3})]
    );

    let started_ids: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolCallStarted { call } => Some(call.id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(started_ids, vec!["call-1", "call-2", "call-3"]);

    let finished_ids: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolCallFinished { result } => Some(result.tool_call_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(finished_ids, vec!["call-1", "call-2", "call-3"]);

    assert_eq!(provider.calls().len(), 3);
    assert_eq!(events.last(), Some(&ended(SessionEndReason::Completed)));
}

#[tokio::test]
async fn max_rounds_terminates_with_budget_exceeded() {
    let executions = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(RecordingTool::new(
            "echo",
            Arc::clone(&executions),
            ToolResult::success("ignored", "ok"),
        )))
        .expect("tool registration should succeed");
    let tools = Arc::new(registry);

    let provider = Arc::new(MockProvider::new(vec![
        tool_call_response("call-1"),
        tool_call_response("call-2"),
        // Must remain unused: the loop stops after max_rounds calls.
        tool_call_response("call-3"),
    ]));

    let (reason, events) = run(
        Arc::clone(&provider),
        tools,
        2,
        vec![Message::user("go")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::BudgetExceeded);
    assert_eq!(provider.calls().len(), 2);
    assert_eq!(
        executions.lock().expect("executions mutex poisoned").len(),
        2
    );

    let budget_exceeded = events
        .iter()
        .find_map(|event| match event {
            AgentEvent::BudgetExceeded { kind, message } => Some((*kind, message)),
            _ => None,
        })
        .expect("a budget event should be emitted");
    assert_eq!(budget_exceeded.0, BudgetKind::MaxRounds);
    assert!(budget_exceeded.1.contains('2'));

    assert!(matches!(
        events[events.len() - 2],
        AgentEvent::BudgetExceeded {
            kind: BudgetKind::MaxRounds,
            ..
        }
    ));
    assert_eq!(
        events.last(),
        Some(&ended(SessionEndReason::BudgetExceeded))
    );
}

#[tokio::test]
async fn pre_cancelled_token_stops_before_any_model_call() {
    let provider = Arc::new(MockProvider::new(vec![text_response("unused")]));
    let cancel = CancellationToken::new();
    cancel.cancel();

    let (reason, events) = run(
        Arc::clone(&provider),
        Arc::new(ToolRegistry::new()),
        4,
        vec![Message::user("go")],
        cancel,
    )
    .await;

    assert_eq!(reason, SessionEndReason::Cancelled);
    assert_eq!(events, vec![started(), ended(SessionEndReason::Cancelled)]);
    assert!(provider.calls().is_empty());
}

#[tokio::test]
async fn cancellation_interrupts_an_in_flight_model_stream() {
    let provider = Arc::new(MockProvider::new(vec![Response::EventsThenPending(vec![
        LlmEvent::TextDelta {
            text: "partial".to_owned(),
        },
    ])]));
    let (events, mut receiver) = broadcast::channel(128);
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let agent = AgentLoop::new(
        provider,
        Arc::new(ToolRegistry::new()),
        test_budget(4),
        events,
    );

    let handle = tokio::spawn(async move {
        agent
            .run(session_id(), vec![Message::user("go")], task_cancel)
            .await
    });

    let mut observed = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("the agent should emit events promptly")
            .expect("the event channel should remain open");
        let saw_delta = matches!(event, AgentEvent::ModelDelta { .. });
        observed.push(event);
        if saw_delta {
            break;
        }
    }

    cancel.cancel();

    let reason = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("the agent should stop after cancellation")
        .expect("the agent task should not panic");
    observed.extend(drain(&mut receiver));

    assert_eq!(reason, SessionEndReason::Cancelled);
    assert_eq!(
        observed,
        vec![
            started(),
            AgentEvent::ModelDelta {
                text: "partial".to_owned(),
            },
            ended(SessionEndReason::Cancelled),
        ]
    );
}

#[tokio::test]
async fn provider_request_error_becomes_an_error_event_and_ends() {
    let provider = Arc::new(MockProvider::new(vec![Response::Error(
        LlmError::Request {
            message: "network down".to_owned(),
        },
    )]));

    let (reason, events) = run(
        Arc::clone(&provider),
        Arc::new(ToolRegistry::new()),
        4,
        vec![Message::user("go")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::Error);
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], started());
    match &events[1] {
        AgentEvent::Error { message } => {
            assert!(
                message.contains("network down"),
                "unexpected error: {message}"
            );
        }
        other => panic!("expected an error event, got {other:?}"),
    }
    assert_eq!(events[2], ended(SessionEndReason::Error));
    assert_eq!(provider.calls().len(), 1);
}

#[tokio::test]
async fn stream_error_event_becomes_an_error_event_and_ends() {
    let provider = Arc::new(MockProvider::new(vec![Response::Events(vec![
        LlmEvent::Error {
            message: "stream broke".to_owned(),
        },
    ])]));

    let (reason, events) = run(
        Arc::clone(&provider),
        Arc::new(ToolRegistry::new()),
        4,
        vec![Message::user("go")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::Error);
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], started());
    match &events[1] {
        AgentEvent::Error { message } => {
            assert!(
                message.contains("stream broke"),
                "unexpected error: {message}"
            );
        }
        other => panic!("expected an error event, got {other:?}"),
    }
    assert_eq!(events[2], ended(SessionEndReason::Error));
}

#[tokio::test]
async fn stream_ending_without_done_is_an_error() {
    let provider = Arc::new(MockProvider::new(vec![Response::Events(vec![
        LlmEvent::TextDelta {
            text: "partial".to_owned(),
        },
    ])]));

    let (reason, events) = run(
        Arc::clone(&provider),
        Arc::new(ToolRegistry::new()),
        4,
        vec![Message::user("go")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::Error);
    assert_eq!(events.len(), 4);
    assert_eq!(events[0], started());
    assert_eq!(
        events[1],
        AgentEvent::ModelDelta {
            text: "partial".to_owned(),
        }
    );
    match &events[2] {
        AgentEvent::Error { message } => {
            assert!(
                message.contains("without a Done event"),
                "unexpected error: {message}"
            );
        }
        other => panic!("expected an error event, got {other:?}"),
    }
    assert_eq!(events[3], ended(SessionEndReason::Error));
}

#[tokio::test]
async fn unknown_tool_is_reported_to_the_model() {
    let provider = Arc::new(MockProvider::new(vec![
        Response::Events(vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new("call-x", "missing", json!({})),
            },
            LlmEvent::Done { usage: None },
        ]),
        text_response("recovered"),
    ]));

    let (reason, events) = run(
        Arc::clone(&provider),
        Arc::new(ToolRegistry::new()),
        4,
        vec![Message::user("go")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::Completed);
    let finished = events
        .iter()
        .find_map(|event| match event {
            AgentEvent::ToolCallFinished { result } => Some(result),
            _ => None,
        })
        .expect("the unknown tool call should finish with an error result");
    assert!(finished.is_error);
    assert_eq!(finished.tool_call_id, "call-x");
    assert!(finished.content.contains("unknown tool"));

    let calls = provider.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[1].messages[2],
        Message::tool("call-x", "tool error: unknown tool `missing`")
    );
}

#[tokio::test]
async fn failed_tool_results_are_prefixed_and_fed_back() {
    let executions = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(RecordingTool::new(
            "echo",
            executions,
            ToolResult::error("ignored", "boom"),
        )))
        .expect("tool registration should succeed");
    let tools = Arc::new(registry);

    let call = ToolCall::new("call-1", "echo", json!({}));
    let provider = Arc::new(MockProvider::new(vec![
        Response::Events(vec![
            LlmEvent::ToolCallRequested { call: call.clone() },
            LlmEvent::Done { usage: None },
        ]),
        text_response("done"),
    ]));

    let (reason, events) = run(
        Arc::clone(&provider),
        tools,
        4,
        vec![Message::user("go")],
        CancellationToken::new(),
    )
    .await;

    assert_eq!(reason, SessionEndReason::Completed);
    let finished = events
        .iter()
        .find_map(|event| match event {
            AgentEvent::ToolCallFinished { result } => Some(result),
            _ => None,
        })
        .expect("a tool result should be emitted");
    assert_eq!(finished, &ToolResult::error("call-1", "boom"));

    let calls = provider.calls();
    assert_eq!(
        calls[1].messages,
        vec![
            Message::user("go"),
            Message::assistant_with_tool_calls("", vec![call]),
            Message::tool("call-1", "tool error: boom"),
        ]
    );
}
