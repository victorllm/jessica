//! Ports implemented by adapters and consumed by the engine.
//!
//! The engine dictates when each port is called; an adapter supplies the
//! concrete provider, terminal, ACP, storage, or memory implementation. Ports
//! deliberately use only core types so terminal and protocol concepts do not
//! leak into the engine API.

use std::path::Path;

use async_trait::async_trait;
use jessica_core::trace::TraceTurnMode;
use jessica_core::types::{
    AgentEvent, Message, PermissionDecision, SessionId, ToolCall, ToolResult,
};
use tokio_util::sync::CancellationToken;

use crate::api::{TurnOutcome, TurnRequest};
use crate::error::EngineError;
use crate::policy::{PolicyError, PolicyVerdict};
use crate::tooling::{ResultFormat, ToolCatalog};

/// Turn-specific inputs available while a host builds the tool catalog.
#[derive(Debug)]
pub struct ToolContext<'a> {
    /// Canonical workspace root for this turn.
    pub workspace_root: &'a Path,
    /// Cancellation signal for the turn.
    pub cancellation: &'a CancellationToken,
    /// The immutable request that started the turn.
    pub request: &'a TurnRequest,
}

/// Supplies the tools visible to one turn.
///
/// Implementations construct fresh turn-scoped tools where necessary and
/// return host-owned descriptors for every executable tool. The engine applies
/// the governance chain around the returned tools; it never accepts a
/// pre-assembled registry or a tool that bypasses the catalog.
#[async_trait]
pub trait ToolProvider: Send + Sync {
    /// Builds the catalog for `context`.
    async fn provide(&self, context: &ToolContext<'_>) -> Result<ToolCatalog, EngineError>;
}

/// Decision produced by a [`ToolPermissionGate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ToolPermissionDecision {
    /// The user approved this one execution.
    Allow,
    /// The user rejected this one execution.
    Reject,
    /// The prompt or permission request was cancelled.
    Cancelled,
    /// No decision arrived before the gate's deadline.
    TimedOut,
    /// The request could not be delivered or answered.
    TransportFailed,
}

impl ToolPermissionDecision {
    /// Maps the host decision to the persisted core permission outcome.
    pub fn as_core(self) -> PermissionDecision {
        match self {
            Self::Allow => PermissionDecision::Allow,
            Self::Reject => PermissionDecision::Deny,
            Self::Cancelled => PermissionDecision::Cancelled,
            Self::TimedOut => PermissionDecision::TimedOut,
            Self::TransportFailed => PermissionDecision::Failed,
        }
    }
}

/// Asks a client for approval before a sensitive tool runs.
///
/// The gate runs after policy authorization and before the tool timeout
/// starts. A gate never executes the tool itself. Only an
/// [`ToolPermissionDecision::Allow`] lets execution proceed.
#[async_trait]
pub trait ToolPermissionGate: Send + Sync {
    /// Requests permission for `call`.
    ///
    /// `call.id` is the provider-assigned id and must not be synthesized by
    /// the adapter.
    async fn request_permission(&self, call: &ToolCall) -> ToolPermissionDecision;
}

/// Host-side policy evaluator consulted after the allowlist and before any
/// permission gate.
///
/// The engine owns the ordering; an adapter supplies the host's concrete rule
/// language and approval configuration. Returning an error means the call must
/// fail closed, exactly like a denial.
#[async_trait]
pub trait ToolPolicy: Send + Sync {
    /// Evaluates `call` against the host's policy rules.
    async fn evaluate(&self, call: &ToolCall) -> Result<PolicyVerdict, PolicyError>;
}

/// Host-side manager for oversized tool results.
///
/// The engine calls this after a tool returns and before the result-size guard,
/// transcript, trace, or model observes the result. Implementations may store
/// content outside the transcript and return a bounded preview.
#[async_trait]
pub trait ToolResultManager: Send + Sync {
    /// Transforms `result` before it enters the governed transcript.
    async fn manage(&self, tool: &str, format: ResultFormat, result: ToolResult) -> ToolResult;
}

/// Stable identity and attribution of one trace turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnTrace {
    /// Stable identifier for the turn.
    pub turn_id: String,
    /// Session whose events belong to the turn.
    ///
    /// A trace turn can contain several sessions when acceptance retries are
    /// used. The engine supplies the session that owns the events currently
    /// being recorded.
    pub session_id: SessionId,
    /// One-based attempt that owns the events, when the host attributes them.
    ///
    /// Events that carry their own attempt attribution, such as an acceptance
    /// failure, override this value.
    pub attempt: Option<u32>,
}

/// Persists one governed turn.
///
/// `begin_turn` must complete successfully before any provider call and
/// records the turn's initial conversation. `finish_turn` is the durability
/// acknowledgement boundary: a turn whose trace could not be closed is never
/// reported as a successful [`TurnOutcome`]. Implementations map storage
/// failures to [`EngineError::TraceIncomplete`] on behalf of the engine.
#[async_trait]
pub trait TraceStore: Send + Sync {
    /// Opens a trace turn with its immutable initial messages.
    async fn begin_turn(
        &self,
        trace: &TurnTrace,
        mode: TraceTurnMode,
        messages: &[Message],
    ) -> Result<(), EngineError>;

    /// Records one event in an open trace turn.
    async fn record(&self, trace: &TurnTrace, event: AgentEvent) -> Result<(), EngineError>;

    /// Closes a trace turn after the outcome is known.
    ///
    /// Returning `Ok` acknowledges that the terminal marker is durable.
    /// Returning an error leaves the trace incomplete; the engine maps it to
    /// [`EngineError::TraceIncomplete`].
    async fn finish_turn(
        &self,
        trace: &TurnTrace,
        outcome: &TurnOutcome,
    ) -> Result<(), EngineError>;
}

/// Recalls long-term memory for one turn.
///
/// The engine calls this at most once per turn, before the user message is
/// appended. Implementations should return `Ok(None)` when nothing matches;
/// the engine treats an error as fail-open and logs it without changing the
/// turn reason.
#[async_trait]
pub trait MemoryPort: Send + Sync {
    /// Returns one memory message for `user_input`, if any.
    async fn recall(&self, user_input: &str) -> Result<Option<Message>, EngineError>;
}

/// Recalls relevant skills for one turn.
///
/// The engine calls this at most once per turn, after memory recall. Errors
/// are fail-open, matching [`MemoryPort`].
#[async_trait]
pub trait SkillPort: Send + Sync {
    /// Returns one skill guidance message for `user_input`, if any.
    async fn recall(&self, user_input: &str) -> Result<Option<Message>, EngineError>;
}

/// Receives every event observed by the engine's event pump.
///
/// The observer runs after durable recording and before or alongside human
/// rendering; it must not block the turn indefinitely.
pub trait TurnObserver: Send + Sync {
    /// Publishes one turn event.
    fn publish(&self, event: &AgentEvent);
}

/// Destination for human-readable event rendering.
pub trait PumpConsole: Send + Sync {
    /// Writes one complete line.
    fn line(&self, text: &str);
    /// Writes inline text without a trailing newline.
    fn inline(&self, text: &str);
    /// Writes a diagnostic line.
    fn diagnostic(&self, text: &str);
}

/// Supplies the system prompt for a turn.
pub trait SystemPromptSource: Send + Sync {
    /// Returns the system prompt text.
    fn system_prompt(&self) -> String;
}

/// Supplies stable identities for turn and session records.
pub trait IdSource: Send + Sync {
    /// Returns the identifier for the next turn.
    fn next_turn_id(&self) -> String;
    /// Returns the identifier for the next session.
    fn next_session_id(&self) -> SessionId;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_host_permission_decision_maps_to_a_distinct_core_outcome() {
        let cases = [
            (ToolPermissionDecision::Allow, PermissionDecision::Allow),
            (ToolPermissionDecision::Reject, PermissionDecision::Deny),
            (
                ToolPermissionDecision::Cancelled,
                PermissionDecision::Cancelled,
            ),
            (
                ToolPermissionDecision::TimedOut,
                PermissionDecision::TimedOut,
            ),
            (
                ToolPermissionDecision::TransportFailed,
                PermissionDecision::Failed,
            ),
        ];

        for (host, core) in cases {
            assert_eq!(host.as_core(), core);
        }
    }
}
