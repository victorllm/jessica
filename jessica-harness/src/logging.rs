//! Logging hook backed by `tracing`.

use async_trait::async_trait;
use jessica_core::hook::Hook;
use jessica_core::types::{AgentEvent, SessionId};

/// Emits one structured `tracing` event per hook callback.
///
/// Events are logged at `DEBUG` level. Model deltas are recorded by event type
/// only, so prompt and completion text never enter the logs.
#[derive(Debug, Default, Clone, Copy)]
pub struct LoggingHook;

impl LoggingHook {
    /// Creates a logging hook.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Hook for LoggingHook {
    fn name(&self) -> &str {
        "logging"
    }

    async fn on_session_start(&self, session_id: &SessionId) {
        tracing::debug!(session_id = %session_id, "session started");
    }

    async fn on_event(&self, event: &AgentEvent) {
        tracing::debug!(event = event_name(event), "agent event");
    }

    async fn on_session_end(&self, session_id: &SessionId) {
        tracing::debug!(session_id = %session_id, "session ended");
    }
}

/// Returns the stable name of an event, matching its serde tag.
fn event_name(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::SessionStarted { .. } => "session_started",
        AgentEvent::RoundStarted { .. } => "round_started",
        AgentEvent::RoundFinished { .. } => "round_finished",
        AgentEvent::ModelDelta { .. } => "model_delta",
        AgentEvent::ToolCallStarted { .. } => "tool_call_started",
        AgentEvent::ToolCallFinished { .. } => "tool_call_finished",
        AgentEvent::PermissionDecided { .. } => "permission_decided",
        AgentEvent::ChildSessionStarted { .. } => "child_session_started",
        AgentEvent::ChildSessionEnded { .. } => "child_session_ended",
        AgentEvent::PolicyDenied { .. } => "policy_denied",
        AgentEvent::BudgetExceeded { .. } => "budget_exceeded",
        AgentEvent::AcceptanceFailed { .. } => "acceptance_failed",
        AgentEvent::Error { .. } => "error",
        AgentEvent::SessionEnded { .. } => "session_ended",
    }
}
