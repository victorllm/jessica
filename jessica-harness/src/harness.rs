//! Middleware assembly around one agent run.

use std::future::Future;
use std::sync::Arc;

use jessica_core::guard::{Guard, GuardContext, GuardDecision};
use jessica_core::hook::Hook;
use jessica_core::policy::{DenyAll, Policy, PolicyDecision};
use jessica_core::sensor::{Sensor, SensorContext};
use jessica_core::types::{AgentEvent, SessionEndReason, SessionId, ToolCall, ToolResult};
use tokio::sync::broadcast;
use tracing::warn;

use crate::sensors::{SensorPipeline, SensorReport};

/// Default number of events retained by the harness broadcast channel.
pub const DEFAULT_EVENT_CAPACITY: usize = 1024;

/// Holds middleware hooks, guards, a tool policy, and sensors around a single
/// agent run.
///
/// The harness owns the event channel the agent loop must use. It subscribes
/// before polling the agent, so hooks observe the same event sequence as every
/// other subscriber. Hooks are awaited in vector order at each lifecycle point;
/// an empty hook list is therefore a transparent wrapper.
///
/// Guards and the policy are evaluated explicitly at caller-supplied
/// checkpoints: [`Harness::check_guards`] and [`Harness::authorize_tool_call`]
/// return the structured event that must stop the session. Sensors run through
/// [`Harness::run_sensors`] after a tool result is available. [`Harness::run`]
/// itself does not poll any of them, because the event stream does not carry
/// token usage, round boundaries, measured tool durations, or tool results in a
/// form that can be replayed through a sensor.
pub struct Harness {
    hooks: Vec<Arc<dyn Hook>>,
    guards: Vec<Arc<dyn Guard>>,
    policy: Arc<dyn Policy>,
    sensors: SensorPipeline,
    events: broadcast::Sender<AgentEvent>,
}

impl Harness {
    /// Creates a harness with the default event capacity.
    pub fn new(hooks: Vec<Arc<dyn Hook>>) -> Self {
        Self::with_event_capacity(hooks, DEFAULT_EVENT_CAPACITY)
    }

    /// Creates a harness with an explicit broadcast capacity.
    ///
    /// A capacity of zero is treated as one, the minimum accepted by
    /// [`broadcast::channel`].
    pub fn with_event_capacity(hooks: Vec<Arc<dyn Hook>>, event_capacity: usize) -> Self {
        let (events, _receiver) = broadcast::channel(event_capacity.max(1));
        Self {
            hooks,
            guards: Vec::new(),
            policy: Arc::new(DenyAll),
            sensors: SensorPipeline::default(),
            events,
        }
    }

    /// Replaces the guard list.
    ///
    /// Guards are evaluated in this order at every checkpoint.
    pub fn with_guards(mut self, guards: Vec<Arc<dyn Guard>>) -> Self {
        self.guards = guards;
        self
    }

    /// Replaces the tool policy.
    ///
    /// The default is [`DenyAll`]: no tool call is permitted until an explicit
    /// allowlist is installed.
    pub fn with_policy(mut self, policy: Arc<dyn Policy>) -> Self {
        self.policy = policy;
        self
    }

    /// Replaces the sensor list.
    ///
    /// Sensors run sequentially in registration order after a tool result is
    /// available.
    pub fn with_sensors(mut self, sensors: Vec<Arc<dyn Sensor>>) -> Self {
        self.sensors = SensorPipeline::new(sensors);
        self
    }

    /// Runs every configured sensor against a completed tool execution.
    ///
    /// Sensors run in registration order and all of them run even when one
    /// fails. The returned report preserves that order.
    pub async fn run_sensors(&self, call: &ToolCall, result: &ToolResult) -> SensorReport {
        self.sensors
            .run(&SensorContext {
                tool_call: call,
                tool_result: result,
            })
            .await
    }

    /// Evaluates guards in registration order at a caller-supplied checkpoint.
    ///
    /// Returns `None` when every guard allows execution. Otherwise it returns
    /// the [`AgentEvent::BudgetExceeded`] for the first exceeded guard; the
    /// caller must stop the loop and emit that event. Deterministic guard order
    /// means the first configured limit wins when several would trip.
    pub fn check_guards(&self, context: &GuardContext<'_>) -> Option<AgentEvent> {
        self.guards
            .iter()
            .find_map(|guard| match guard.check(context) {
                GuardDecision::Allow => None,
                GuardDecision::Exceeded { kind, reason } => Some(AgentEvent::BudgetExceeded {
                    kind,
                    message: reason,
                }),
            })
    }

    /// Evaluates the tool policy for `call`.
    ///
    /// Returns `None` when the call is allowed, or the
    /// [`AgentEvent::PolicyDenied`] describing the denial. The caller must emit
    /// the event and must not execute the tool. The default policy is
    /// [`DenyAll`].
    pub fn authorize_tool_call(&self, call: &ToolCall) -> Option<AgentEvent> {
        match self.policy.check(call) {
            PolicyDecision::Allow => None,
            PolicyDecision::Deny(reason) => Some(AgentEvent::PolicyDenied {
                call: call.clone(),
                reason,
            }),
        }
    }

    /// Returns a sender the agent loop must use for its events.
    ///
    /// The agent loop must be the only producer on this channel; the harness
    /// treats every message as agent output.
    pub fn events(&self) -> broadcast::Sender<AgentEvent> {
        self.events.clone()
    }

    /// Subscribes an additional observer, such as the CLI or an event store.
    ///
    /// Additional receivers must be polled concurrently. A receiver that falls
    /// behind the channel capacity will lag and skip events.
    pub fn subscribe(&self) -> broadcast::Receiver<AgentEvent> {
        self.events.subscribe()
    }

    /// Runs `agent` while invoking hooks at fixed lifecycle points.
    ///
    /// The invocation order is:
    ///
    /// 1. `on_session_start` for every hook, in registration order;
    /// 2. every event broadcast by the agent, delivered to `on_event` in
    ///    arrival order, each event to every hook in registration order;
    /// 3. events still buffered when the agent future resolves are drained;
    /// 4. `on_session_end` for every hook, in registration order.
    ///
    /// The agent's return value is propagated unchanged. Hooks must not panic or
    /// block indefinitely; the harness does not contain either condition. The
    /// agent must finish emitting before its future resolves; events sent by
    /// detached tasks afterwards are outside this run.
    ///
    /// The broadcast channel is bounded. If a hook blocks long enough for the
    /// receiver to fall behind, the receiver lags, missed events are reported
    /// through `tracing`, and the remaining sequence continues. Keep observation
    /// hooks lightweight.
    pub async fn run<F>(&self, session_id: &SessionId, agent: F) -> SessionEndReason
    where
        F: Future<Output = SessionEndReason>,
    {
        let mut receiver = self.events.subscribe();

        for hook in &self.hooks {
            hook.on_session_start(session_id).await;
        }

        tokio::pin!(agent);

        let reason = loop {
            tokio::select! {
                reason = &mut agent => break reason,
                event = receiver.recv() => match event {
                    Ok(event) => self.dispatch_event(&event).await,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "harness event receiver lagged; hooks missed events");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // `self.events` keeps the channel open for the whole run.
                        warn!("harness event channel closed unexpectedly");
                    }
                },
            }
        };

        loop {
            match receiver.try_recv() {
                Ok(event) => self.dispatch_event(&event).await,
                Err(
                    broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed,
                ) => break,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    warn!(
                        skipped,
                        "harness event receiver lagged; hooks missed events"
                    );
                }
            }
        }

        for hook in &self.hooks {
            hook.on_session_end(session_id).await;
        }

        reason
    }

    /// Delivers one event to every hook in registration order.
    async fn dispatch_event(&self, event: &AgentEvent) {
        for hook in &self.hooks {
            hook.on_event(event).await;
        }
    }
}
