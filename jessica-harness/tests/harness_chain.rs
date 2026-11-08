//! Behaviour tests for harness middleware assembly.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use jessica_core::hook::Hook;
use jessica_core::types::{AgentEvent, SessionEndReason, SessionId, ToolCall, ToolResult};
use jessica_harness::{Harness, LoggingHook};
use serde_json::json;
use tokio::sync::broadcast;

/// Hook that records callbacks and events without changing execution.
struct RecordingHook {
    label: &'static str,
    events: Mutex<Vec<AgentEvent>>,
    calls: Option<Arc<Mutex<Vec<String>>>>,
}

impl RecordingHook {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            events: Mutex::new(Vec::new()),
            calls: None,
        }
    }

    fn with_calls(label: &'static str, calls: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            label,
            events: Mutex::new(Vec::new()),
            calls: Some(calls),
        }
    }

    fn record_call(&self, callback: String) {
        if let Some(calls) = &self.calls {
            calls
                .lock()
                .expect("test mutex should not be poisoned")
                .push(format!("{}:{callback}", self.label));
        }
    }

    fn events(&self) -> Vec<AgentEvent> {
        self.events
            .lock()
            .expect("test mutex should not be poisoned")
            .clone()
    }
}

#[async_trait]
impl Hook for RecordingHook {
    fn name(&self) -> &str {
        self.label
    }

    async fn on_session_start(&self, session_id: &SessionId) {
        self.record_call(format!("start:{session_id}"));
    }

    async fn on_event(&self, event: &AgentEvent) {
        self.events
            .lock()
            .expect("test mutex should not be poisoned")
            .push(event.clone());
        self.record_call(format!("event:{}", event_name(event)));
    }

    async fn on_session_end(&self, session_id: &SessionId) {
        self.record_call(format!("end:{session_id}"));
    }
}

/// Representative event sequence emitted by an agent round.
fn sample_events(session_id: &SessionId) -> Vec<AgentEvent> {
    let call = ToolCall::new("call-1", "echo", json!({"text": "hi"}));
    let result = ToolResult::success("call-1", "hi");
    vec![
        AgentEvent::SessionStarted {
            session_id: session_id.clone(),
        },
        AgentEvent::ModelDelta {
            text: "hel".to_owned(),
        },
        AgentEvent::ModelDelta {
            text: "lo".to_owned(),
        },
        AgentEvent::ToolCallStarted { call: call.clone() },
        AgentEvent::ToolCallFinished { result },
        AgentEvent::ModelDelta {
            text: "done".to_owned(),
        },
        AgentEvent::SessionEnded {
            session_id: session_id.clone(),
            reason: SessionEndReason::Completed,
        },
    ]
}

/// Agent stand-in that broadcasts `sequence`, then returns `reason`.
///
/// It never yields between sends, which exercises the harness's post-completion
/// drain: events can still be in the channel when the agent future resolves.
async fn emitting_agent(
    events: broadcast::Sender<AgentEvent>,
    sequence: Vec<AgentEvent>,
    reason: SessionEndReason,
) -> SessionEndReason {
    for event in sequence {
        let _ = events.send(event);
    }
    reason
}

fn drain(receiver: &mut broadcast::Receiver<AgentEvent>) -> Vec<AgentEvent> {
    let mut collected = Vec::new();
    loop {
        match receiver.try_recv() {
            Ok(event) => collected.push(event),
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Closed) => break,
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                panic!("test receiver lagged by {skipped} events")
            }
        }
    }
    collected
}

/// Runs the mock agent without the harness, for a baseline comparison.
async fn run_unwrapped(
    session_id: &SessionId,
    reason: SessionEndReason,
) -> (SessionEndReason, Vec<AgentEvent>) {
    let (events, mut receiver) = broadcast::channel(1024);
    let sequence = sample_events(session_id);
    let returned = emitting_agent(events, sequence, reason).await;
    (returned, drain(&mut receiver))
}

#[tokio::test]
async fn empty_middleware_preserves_event_sequence_and_reason() {
    let session_id = SessionId::new("session-empty");
    let (expected_reason, expected_events) =
        run_unwrapped(&session_id, SessionEndReason::Completed).await;

    let harness = Harness::new(Vec::new());
    let mut observer = harness.subscribe();
    let reason = harness
        .run(
            &session_id,
            emitting_agent(
                harness.events(),
                sample_events(&session_id),
                SessionEndReason::Completed,
            ),
        )
        .await;

    assert_eq!(reason, expected_reason);
    assert_eq!(drain(&mut observer), expected_events);
}

#[tokio::test]
async fn observation_hook_sees_the_same_sequence_without_altering_it() {
    let session_id = SessionId::new("session-observed");
    let (expected_reason, expected_events) =
        run_unwrapped(&session_id, SessionEndReason::Completed).await;

    let recorder = Arc::new(RecordingHook::new("recorder"));
    let harness = Harness::new(vec![recorder.clone()]);
    let mut observer = harness.subscribe();
    let reason = harness
        .run(
            &session_id,
            emitting_agent(
                harness.events(),
                sample_events(&session_id),
                SessionEndReason::Completed,
            ),
        )
        .await;

    assert_eq!(reason, expected_reason);
    assert_eq!(drain(&mut observer), expected_events);
    assert_eq!(recorder.events(), expected_events);
}

#[tokio::test]
async fn hooks_run_in_registration_order_at_each_lifecycle_point() {
    let session_id = SessionId::new("session-ordered");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let first = Arc::new(RecordingHook::with_calls("first", calls.clone()));
    let second = Arc::new(RecordingHook::with_calls("second", calls.clone()));
    let harness = Harness::new(vec![first, second]);

    let sequence = vec![AgentEvent::ModelDelta {
        text: "one delta".to_owned(),
    }];
    let reason = harness
        .run(
            &session_id,
            emitting_agent(harness.events(), sequence, SessionEndReason::Completed),
        )
        .await;

    assert_eq!(reason, SessionEndReason::Completed);
    assert_eq!(
        *calls.lock().expect("test mutex should not be poisoned"),
        vec![
            "first:start:session-ordered",
            "second:start:session-ordered",
            "first:event:model_delta",
            "second:event:model_delta",
            "first:end:session-ordered",
            "second:end:session-ordered",
        ]
    );
}

#[tokio::test]
async fn hooks_do_not_change_non_completed_return_reasons() {
    let session_id = SessionId::new("session-reasons");

    for reason in [
        SessionEndReason::Cancelled,
        SessionEndReason::BudgetExceeded,
        SessionEndReason::AcceptanceFailed,
        SessionEndReason::Error,
    ] {
        let harness = Harness::new(vec![Arc::new(RecordingHook::new("recorder"))]);
        let sequence = vec![AgentEvent::SessionEnded {
            session_id: session_id.clone(),
            reason,
        }];
        let returned = harness
            .run(
                &session_id,
                emitting_agent(harness.events(), sequence, reason),
            )
            .await;
        assert_eq!(returned, reason);
    }
}

/// Minimal subscriber that counts emitted tracing events.
///
/// The test runtime is single-threaded, so the thread-local default dispatcher
/// covers every hook callback.
#[derive(Clone, Default)]
struct CountingSubscriber {
    events: Arc<AtomicUsize>,
}

impl tracing::Subscriber for CountingSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, _event: &tracing::Event<'_>) {
        self.events.fetch_add(1, Ordering::Relaxed);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

#[tokio::test]
async fn logging_hook_emits_one_tracing_event_per_callback() {
    let subscriber = CountingSubscriber::default();
    let count = subscriber.events.clone();
    let _guard = tracing::subscriber::set_default(subscriber);

    let hook = LoggingHook::new();
    assert_eq!(hook.name(), "logging");

    let session_id = SessionId::new("session-logged");
    let sequence = sample_events(&session_id);
    let expected_events = sequence.len();
    let harness = Harness::new(vec![Arc::new(hook)]);
    let reason = harness
        .run(
            &session_id,
            emitting_agent(harness.events(), sequence, SessionEndReason::Completed),
        )
        .await;

    assert_eq!(reason, SessionEndReason::Completed);
    assert_eq!(count.load(Ordering::Relaxed), expected_events + 2);
}

/// Returns the stable name of an event for order assertions.
fn event_name(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::SessionStarted { .. } => "session_started",
        AgentEvent::ModelDelta { .. } => "model_delta",
        AgentEvent::ToolCallStarted { .. } => "tool_call_started",
        AgentEvent::ToolCallFinished { .. } => "tool_call_finished",
        AgentEvent::PolicyDenied { .. } => "policy_denied",
        AgentEvent::BudgetExceeded { .. } => "budget_exceeded",
        AgentEvent::AcceptanceFailed { .. } => "acceptance_failed",
        AgentEvent::Error { .. } => "error",
        AgentEvent::SessionEnded { .. } => "session_ended",
    }
}
