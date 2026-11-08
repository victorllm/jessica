//! Middleware assembly around one agent run.

use std::future::Future;
use std::sync::Arc;

use jessica_core::hook::Hook;
use jessica_core::types::{AgentEvent, SessionEndReason, SessionId};
use tokio::sync::broadcast;
use tracing::warn;

/// Default number of events retained by the harness broadcast channel.
pub const DEFAULT_EVENT_CAPACITY: usize = 1024;

/// Holds middleware hooks and wraps a single agent run.
///
/// The harness owns the event channel the agent loop must use. It subscribes
/// before polling the agent, so hooks observe the same event sequence as every
/// other subscriber. Hooks are awaited in vector order at each lifecycle point;
/// an empty hook list is therefore a transparent wrapper.
///
/// This type only observes. Guards, policies, and sensors are added by later
/// tasks, once they can influence execution rather than just record it.
pub struct Harness {
    hooks: Vec<Arc<dyn Hook>>,
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
        Self { hooks, events }
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
