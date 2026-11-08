//! Guard contract: deterministic budget enforcement at loop checkpoints.

use std::time::Duration;

use crate::types::{BudgetKind, ToolCall, ToolResult};

/// Decision produced by a [`Guard`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardDecision {
    /// Execution may continue.
    Allow,
    /// A budget boundary was crossed and execution must stop.
    Exceeded {
        /// Kind of budget that was exceeded.
        kind: BudgetKind,
        /// Human-readable explanation.
        reason: String,
    },
}

/// Snapshot of loop state presented to guards.
#[derive(Debug, Clone)]
pub struct GuardContext<'a> {
    /// One-based model round number.
    pub round: u32,
    /// Total tokens consumed so far (prompt plus completion).
    pub tokens_used: u64,
    /// Most recent model output.
    pub last_output: &'a str,
    /// Tool call currently being processed, if any.
    pub tool_call: Option<&'a ToolCall>,
    /// Tool result currently being processed, if any.
    pub tool_result: Option<&'a ToolResult>,
    /// Wall-clock time spent executing the current tool, if known.
    pub tool_elapsed: Option<Duration>,
}

impl<'a> GuardContext<'a> {
    /// Creates a context for a model-round checkpoint.
    pub fn new(round: u32, tokens_used: u64, last_output: &'a str) -> Self {
        Self {
            round,
            tokens_used,
            last_output,
            tool_call: None,
            tool_result: None,
            tool_elapsed: None,
        }
    }

    /// Attaches the tool call being processed.
    pub fn with_tool_call(mut self, call: &'a ToolCall) -> Self {
        self.tool_call = Some(call);
        self
    }

    /// Attaches the tool result being processed and its elapsed execution time.
    pub fn with_tool_result(mut self, result: &'a ToolResult, elapsed: Option<Duration>) -> Self {
        self.tool_result = Some(result);
        self.tool_elapsed = elapsed;
        self
    }
}

/// Deterministic guard evaluated at loop checkpoints.
///
/// Guards must decide from the supplied context alone; they may not call a
/// model, perform I/O, or consult wall-clock time beyond the values already in
/// the context.
pub trait Guard: Send + Sync {
    /// Human-readable name used in diagnostics.
    fn name(&self) -> &str;

    /// Inspects `context` and decides whether execution may continue.
    fn check(&self, context: &GuardContext<'_>) -> GuardDecision;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct RoundLimitGuard {
        limit: u32,
    }

    impl Guard for RoundLimitGuard {
        fn name(&self) -> &str {
            "round_limit"
        }

        fn check(&self, context: &GuardContext<'_>) -> GuardDecision {
            if context.round > self.limit {
                GuardDecision::Exceeded {
                    kind: BudgetKind::MaxRounds,
                    reason: format!("round {} exceeds limit {}", context.round, self.limit),
                }
            } else {
                GuardDecision::Allow
            }
        }
    }

    #[test]
    fn decisions_construct_with_expected_fields() {
        assert_eq!(GuardDecision::Allow, GuardDecision::Allow);
        let exceeded = GuardDecision::Exceeded {
            kind: BudgetKind::TokenLimit,
            reason: "over budget".into(),
        };
        match exceeded {
            GuardDecision::Exceeded { kind, reason } => {
                assert_eq!(kind, BudgetKind::TokenLimit);
                assert_eq!(reason, "over budget");
            }
            GuardDecision::Allow => panic!("expected an exceeded decision"),
        }
    }

    #[test]
    fn context_builder_attaches_tool_data() {
        let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));
        let result = ToolResult::success("call-1", "ok");
        let elapsed = Duration::from_millis(12);

        let context = GuardContext::new(3, 900, "latest output")
            .with_tool_call(&call)
            .with_tool_result(&result, Some(elapsed));

        assert_eq!(context.round, 3);
        assert_eq!(context.tokens_used, 900);
        assert_eq!(context.last_output, "latest output");
        assert_eq!(context.tool_call, Some(&call));
        assert_eq!(context.tool_result, Some(&result));
        assert_eq!(context.tool_elapsed, Some(elapsed));
    }

    #[test]
    fn mock_guard_allows_and_trips_at_its_limit() {
        let guard = RoundLimitGuard { limit: 2 };
        assert_eq!(guard.name(), "round_limit");

        let allowed = GuardContext::new(2, 10, "");
        assert_eq!(guard.check(&allowed), GuardDecision::Allow);

        let tripped = GuardContext::new(3, 10, "");
        match guard.check(&tripped) {
            GuardDecision::Exceeded { kind, reason } => {
                assert_eq!(kind, BudgetKind::MaxRounds);
                assert!(reason.contains('3'));
            }
            GuardDecision::Allow => panic!("guard should have tripped"),
        }
    }
}
