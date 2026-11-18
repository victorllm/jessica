//! Behaviour tests for the sensor pipeline and acceptance orchestration.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use jessica_core::acceptance::{AcceptanceCheck, AcceptanceContext, AcceptanceResult};
use jessica_core::events::{EventSink, EventSinkError};
use jessica_core::sensor::{Sensor, SensorContext, SensorResult};
use jessica_core::types::{AgentEvent, Message, Role, SessionEndReason, ToolCall, ToolResult};
use jessica_harness::{AcceptanceRunner, CommandSensor, Harness, SensorPipeline, SessionAttempt};
use serde_json::json;
use tokio::sync::broadcast;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct MockSensor {
    name: &'static str,
    result: SensorResult,
    calls: AtomicU32,
}

impl MockSensor {
    fn new(name: &'static str, result: SensorResult) -> Self {
        Self {
            name,
            result,
            calls: AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl Sensor for MockSensor {
    fn name(&self) -> &str {
        self.name
    }

    async fn observe(&self, _context: &SensorContext<'_>) -> SensorResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.result.clone()
    }
}

#[tokio::test]
async fn sensor_pipeline_passes_when_every_sensor_passes() {
    let first = std::sync::Arc::new(MockSensor::new("first", SensorResult::pass("first ok")));
    let second = std::sync::Arc::new(MockSensor::new("second", SensorResult::pass("second ok")));
    let pipeline = SensorPipeline::new(vec![
        std::sync::Arc::clone(&first) as std::sync::Arc<dyn Sensor>,
        std::sync::Arc::clone(&second) as std::sync::Arc<dyn Sensor>,
    ]);
    let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));
    let result = ToolResult::success("call-1", "ok");

    let report = pipeline
        .run(&SensorContext {
            tool_call: &call,
            tool_result: &result,
        })
        .await;

    assert!(report.passed());
    assert_eq!(report.failures().count(), 0);
    let observations = report.observations();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].sensor, "first");
    assert_eq!(observations[0].result.output, "first ok");
    assert_eq!(observations[1].sensor, "second");
    assert_eq!(observations[1].result.output, "second ok");
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn harness_sensor_pipeline_reports_failures_and_still_runs_later_sensors() {
    let passing = std::sync::Arc::new(MockSensor::new("passing", SensorResult::pass("ok")));
    let failing = std::sync::Arc::new(MockSensor::new("failing", SensorResult::fail("bad state")));
    let harness = Harness::new(Vec::new()).with_sensors(vec![
        std::sync::Arc::clone(&passing) as std::sync::Arc<dyn Sensor>,
        std::sync::Arc::clone(&failing) as std::sync::Arc<dyn Sensor>,
    ]);
    let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));
    let result = ToolResult::success("call-1", "ok");

    let report = harness.run_sensors(&call, &result).await;

    assert!(!report.passed());
    let failures: Vec<_> = report.failures().collect();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].sensor, "failing");
    assert_eq!(failures[0].result.output, "bad state");
    assert_eq!(passing.calls.load(Ordering::SeqCst), 1);
    assert_eq!(failing.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn command_sensor_passes_on_zero_exit_and_reports_output() {
    let sensor = CommandSensor::new("echo", "sh").args(["-c", "printf hello"]);
    let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));
    let result = ToolResult::success("call-1", "ok");

    let observed = sensor
        .observe(&SensorContext {
            tool_call: &call,
            tool_result: &result,
        })
        .await;

    assert_eq!(sensor.name(), "echo");
    assert!(observed.passed);
    assert!(observed.output.contains("stdout:"));
    assert!(observed.output.contains("hello"));
}

#[tokio::test]
async fn command_sensor_fails_on_nonzero_exit_and_reports_stderr() {
    let sensor = CommandSensor::new("failing", "sh").args(["-c", "printf boom >&2; exit 7"]);
    let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));
    let result = ToolResult::success("call-1", "ok");

    let observed = sensor
        .observe(&SensorContext {
            tool_call: &call,
            tool_result: &result,
        })
        .await;

    assert!(!observed.passed);
    assert!(observed.output.contains("failed with"));
    assert!(observed.output.contains("stderr:"));
    assert!(observed.output.contains("boom"));
    assert!(observed.output.contains('7'));
}

#[tokio::test]
async fn command_sensor_reports_spawn_failure_without_panicking() {
    let sensor = CommandSensor::new("missing", "/definitely/not/a/jessica-command");
    let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));
    let result = ToolResult::success("call-1", "ok");

    let observed = sensor
        .observe(&SensorContext {
            tool_call: &call,
            tool_result: &result,
        })
        .await;

    assert!(!observed.passed);
    assert!(observed.output.contains("could not start"));
    assert!(
        observed
            .output
            .contains("/definitely/not/a/jessica-command")
    );
}

struct ScriptedCheck {
    results: Mutex<VecDeque<AcceptanceResult>>,
    seen: Mutex<Vec<(Vec<Message>, String)>>,
}

impl ScriptedCheck {
    fn new(results: Vec<AcceptanceResult>) -> Self {
        Self {
            results: Mutex::new(results.into()),
            seen: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> usize {
        lock(&self.seen).len()
    }

    fn seen(&self) -> Vec<(Vec<Message>, String)> {
        lock(&self.seen).clone()
    }
}

#[async_trait]
impl AcceptanceCheck for ScriptedCheck {
    fn name(&self) -> &str {
        "scripted"
    }

    async fn check(&self, context: &AcceptanceContext<'_>) -> AcceptanceResult {
        lock(&self.seen).push((context.messages.to_vec(), context.last_output.to_owned()));
        lock(&self.results)
            .pop_front()
            .unwrap_or_else(|| AcceptanceResult::fail("scripted check exhausted"))
    }
}

#[tokio::test]
async fn acceptance_passes_on_the_first_attempt_without_a_failure_event() {
    let (events, mut receiver) = broadcast::channel(8);
    let check = std::sync::Arc::new(ScriptedCheck::new(vec![AcceptanceResult::pass("accepted")]));
    let runner = AcceptanceRunner::new(
        std::sync::Arc::clone(&check) as std::sync::Arc<dyn AcceptanceCheck>,
        2,
        events,
    );
    let inputs = Mutex::new(Vec::new());
    let attempts = AtomicU32::new(0);

    let outcome = runner
        .run(vec![Message::user("fix it")], |messages| {
            attempts.fetch_add(1, Ordering::SeqCst);
            lock(&inputs).push(messages.clone());
            async {
                SessionAttempt {
                    messages: vec![Message::user("fix it"), Message::assistant("done")],
                    last_output: "done".to_owned(),
                    reason: SessionEndReason::Completed,
                }
            }
        })
        .await;

    assert_eq!(outcome.reason, SessionEndReason::Completed);
    assert_eq!(outcome.attempts, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(check.calls(), 1);
    let seen = check.seen();
    assert_eq!(seen[0].1, "done");
    assert!(seen[0].0.iter().any(|message| message.content == "done"));
    assert_eq!(lock(&inputs).len(), 1);
    assert!(matches!(
        receiver.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn acceptance_failure_injects_feedback_and_reenters_once() {
    let (events, mut receiver) = broadcast::channel(8);
    let check = std::sync::Arc::new(ScriptedCheck::new(vec![
        AcceptanceResult::fail("missing test"),
        AcceptanceResult::pass("accepted"),
    ]));
    let runner = AcceptanceRunner::new(
        std::sync::Arc::clone(&check) as std::sync::Arc<dyn AcceptanceCheck>,
        2,
        events,
    );
    let inputs = Mutex::new(Vec::new());
    let attempts = AtomicU32::new(0);

    let outcome = runner
        .run(vec![Message::user("fix it")], |messages| {
            lock(&inputs).push(messages.clone());
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                let mut messages = messages;
                messages.push(Message::assistant(format!("attempt {attempt}")));
                SessionAttempt {
                    messages,
                    last_output: format!("attempt {attempt}"),
                    reason: SessionEndReason::Completed,
                }
            }
        })
        .await;

    assert_eq!(outcome.reason, SessionEndReason::Completed);
    assert_eq!(outcome.attempts, 2);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(check.calls(), 2);

    let inputs = lock(&inputs);
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0], vec![Message::user("fix it")]);
    assert_eq!(inputs[1].len(), 3);
    assert_eq!(inputs[1][0], Message::user("fix it"));
    assert_eq!(inputs[1][1], Message::assistant("attempt 1"));
    assert_eq!(inputs[1][2].role, Role::User);
    assert_eq!(inputs[1][2].content, "missing test");
    drop(inputs);

    let seen = check.seen();
    assert_eq!(seen[0].1, "attempt 1");
    assert_eq!(seen[1].1, "attempt 2");

    match receiver.try_recv() {
        Ok(AgentEvent::AcceptanceFailed { attempt, feedback }) => {
            assert_eq!(attempt, 1);
            assert_eq!(feedback, "missing test");
        }
        other => panic!("expected one AcceptanceFailed event, got {other:?}"),
    }
    assert!(matches!(
        receiver.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn acceptance_exhaustion_stops_after_max_retries_and_emits_every_failure() {
    let (events, mut receiver) = broadcast::channel(8);
    let check = std::sync::Arc::new(ScriptedCheck::new(vec![
        AcceptanceResult::fail("fail 1"),
        AcceptanceResult::fail("fail 2"),
        AcceptanceResult::fail("fail 3"),
    ]));
    let runner = AcceptanceRunner::new(
        std::sync::Arc::clone(&check) as std::sync::Arc<dyn AcceptanceCheck>,
        2,
        events,
    );
    let inputs = Mutex::new(Vec::new());
    let attempts = AtomicU32::new(0);

    let outcome = runner
        .run(vec![Message::user("fix it")], |messages| {
            lock(&inputs).push(messages.clone());
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                let mut messages = messages;
                messages.push(Message::assistant(format!("attempt {attempt}")));
                SessionAttempt {
                    messages,
                    last_output: format!("attempt {attempt}"),
                    reason: SessionEndReason::Completed,
                }
            }
        })
        .await;

    assert_eq!(outcome.reason, SessionEndReason::AcceptanceFailed);
    assert_eq!(outcome.attempts, 3);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(check.calls(), 3);

    let inputs = lock(&inputs);
    assert_eq!(inputs.len(), 3);
    assert!(
        inputs[1]
            .iter()
            .any(|message| message.role == Role::User && message.content == "fail 1")
    );
    assert!(
        inputs[2]
            .iter()
            .any(|message| message.role == Role::User && message.content == "fail 2")
    );
    drop(inputs);

    let mut failures = Vec::new();
    loop {
        match receiver.try_recv() {
            Ok(AgentEvent::AcceptanceFailed { attempt, feedback }) => {
                failures.push((attempt, feedback));
            }
            Ok(other) => panic!("unexpected event {other:?}"),
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Closed) => break,
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                panic!("receiver lagged by {skipped} events");
            }
        }
    }
    assert_eq!(
        failures,
        vec![
            (1, "fail 1".to_owned()),
            (2, "fail 2".to_owned()),
            (3, "fail 3".to_owned()),
        ]
    );
}

#[tokio::test]
async fn acceptance_is_skipped_when_the_attempt_does_not_complete() {
    let (events, mut receiver) = broadcast::channel(8);
    let check = std::sync::Arc::new(ScriptedCheck::new(Vec::new()));
    let runner = AcceptanceRunner::new(
        std::sync::Arc::clone(&check) as std::sync::Arc<dyn AcceptanceCheck>,
        2,
        events,
    );

    let outcome = runner
        .run(vec![Message::user("fix it")], |messages| async move {
            SessionAttempt {
                messages,
                last_output: "cancelled".to_owned(),
                reason: SessionEndReason::Cancelled,
            }
        })
        .await;

    assert_eq!(outcome.reason, SessionEndReason::Cancelled);
    assert_eq!(outcome.attempts, 1);
    assert_eq!(check.calls(), 0);
    assert!(matches!(
        receiver.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
}

/// Sink double that rejects every acceptance failure.
struct RejectingAcceptanceSink;

#[async_trait]
impl EventSink for RejectingAcceptanceSink {
    async fn emit(&self, _event: AgentEvent) -> Result<(), EventSinkError> {
        Err(EventSinkError::new("injected acceptance sink failure"))
    }
}

#[tokio::test]
async fn a_rejected_acceptance_failure_stops_without_retrying() {
    let (events, mut receiver) = broadcast::channel(8);
    let check = std::sync::Arc::new(ScriptedCheck::new(vec![AcceptanceResult::fail("bad")]));
    let runner = AcceptanceRunner::new(
        std::sync::Arc::clone(&check) as std::sync::Arc<dyn AcceptanceCheck>,
        3,
        events,
    )
    .with_event_sink(std::sync::Arc::new(RejectingAcceptanceSink));
    let attempts = AtomicU32::new(0);

    let outcome = runner
        .run(vec![Message::user("fix it")], |messages| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                SessionAttempt {
                    messages,
                    last_output: "answer".to_owned(),
                    reason: SessionEndReason::Completed,
                }
            }
        })
        .await;

    assert_eq!(outcome.reason, SessionEndReason::Error);
    assert_eq!(outcome.attempts, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(check.calls(), 1);
    assert!(
        matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ),
        "a rejected acceptance failure must not be broadcast"
    );
}

/// Sink double that verifies no acceptance failure has been broadcast yet.
struct BeforeBroadcastSink {
    observer: Mutex<broadcast::Receiver<AgentEvent>>,
    saw_broadcast: AtomicU32,
}

#[async_trait]
impl EventSink for BeforeBroadcastSink {
    async fn emit(&self, _event: AgentEvent) -> Result<(), EventSinkError> {
        if !matches!(
            lock(&self.observer).try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ) {
            self.saw_broadcast.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[tokio::test]
async fn an_acceptance_failure_reaches_the_sink_before_the_broadcast() {
    let (events, _receiver) = broadcast::channel(8);
    let sink = std::sync::Arc::new(BeforeBroadcastSink {
        observer: Mutex::new(events.subscribe()),
        saw_broadcast: AtomicU32::new(0),
    });
    let check = std::sync::Arc::new(ScriptedCheck::new(vec![AcceptanceResult::fail("bad")]));
    let runner = AcceptanceRunner::new(
        std::sync::Arc::clone(&check) as std::sync::Arc<dyn AcceptanceCheck>,
        0,
        events,
    )
    .with_event_sink(std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>);

    let outcome = runner
        .run(vec![Message::user("fix it")], |messages| async move {
            SessionAttempt {
                messages,
                last_output: "answer".to_owned(),
                reason: SessionEndReason::Completed,
            }
        })
        .await;

    assert_eq!(outcome.reason, SessionEndReason::AcceptanceFailed);
    assert_eq!(sink.saw_broadcast.load(Ordering::SeqCst), 0);
}
