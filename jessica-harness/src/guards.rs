//! Deterministic guard implementations.
//!
//! Guards inspect a [`GuardContext`] and return a decision; they never call a
//! model, perform I/O, or read the wall clock. A stateful guard (currently only
//! [`StuckDetectionGuard`]) keeps one session's history, so assemble a fresh
//! instance per session.

use std::sync::Mutex;
use std::time::Duration;

use jessica_core::guard::{Guard, GuardContext, GuardDecision};
use jessica_core::types::BudgetKind;

/// Stops a session once token usage exceeds a fixed limit.
#[derive(Debug, Clone, Copy)]
pub struct TokenBudgetGuard {
    max_tokens: u64,
}

impl TokenBudgetGuard {
    /// Creates a guard allowing at most `max_tokens` prompt plus completion
    /// tokens.
    pub fn new(max_tokens: u64) -> Self {
        Self { max_tokens }
    }
}

impl Guard for TokenBudgetGuard {
    fn name(&self) -> &str {
        "token_budget"
    }

    fn check(&self, context: &GuardContext<'_>) -> GuardDecision {
        if context.tokens_used > self.max_tokens {
            GuardDecision::Exceeded {
                kind: BudgetKind::TokenLimit,
                reason: format!(
                    "token usage {} exceeds limit {}",
                    context.tokens_used, self.max_tokens
                ),
            }
        } else {
            GuardDecision::Allow
        }
    }
}

/// Stops a session when a tool runs longer than a fixed wall-clock limit.
///
/// A context without [`GuardContext::tool_elapsed`] is allowed: the guard only
/// acts on measured durations supplied by the caller.
#[derive(Debug, Clone, Copy)]
pub struct ToolTimeoutGuard {
    timeout: Duration,
}

impl ToolTimeoutGuard {
    /// Creates a guard allowing tool executions up to `timeout`.
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl Guard for ToolTimeoutGuard {
    fn name(&self) -> &str {
        "tool_timeout"
    }

    fn check(&self, context: &GuardContext<'_>) -> GuardDecision {
        let Some(elapsed) = context.tool_elapsed else {
            return GuardDecision::Allow;
        };
        if elapsed > self.timeout {
            let tool_name = context
                .tool_call
                .map_or("<unknown>", |call| call.name.as_str());
            GuardDecision::Exceeded {
                kind: BudgetKind::ToolTimeout,
                reason: format!(
                    "tool `{tool_name}` took {elapsed:?}, exceeding limit {:?}",
                    self.timeout
                ),
            }
        } else {
            GuardDecision::Allow
        }
    }
}

/// State carried by one [`StuckDetectionGuard`] across rounds.
#[derive(Debug, Default)]
struct StuckState {
    /// Last observed one-based round, if any.
    last_round: Option<u32>,
    /// Output observed in that round.
    last_output: String,
    /// Number of consecutive rounds without progress.
    stalled_rounds: u32,
}

/// Stops a session after consecutive rounds make no observable progress.
///
/// A round makes progress when it carries a tool call or tool result, or when
/// its output differs from the previous round's output. A round is stalled only
/// when none of those happen. The guard trips when the number of consecutive
/// stalled rounds reaches the configured limit.
///
/// The first observed round establishes the baseline and is never counted as
/// stalled. Repeated checks for the same round are idempotent. Rounds that move
/// backwards are treated as the start of a new session and reset the state; a
/// new session that starts at the same round number must use a fresh guard
/// instance.
#[derive(Debug)]
pub struct StuckDetectionGuard {
    max_stalled_rounds: u32,
    state: Mutex<StuckState>,
}

impl StuckDetectionGuard {
    /// Creates a guard that trips after `max_stalled_rounds` consecutive
    /// no-progress rounds.
    ///
    /// Values below one are treated as one.
    pub fn new(max_stalled_rounds: u32) -> Self {
        Self {
            max_stalled_rounds: max_stalled_rounds.max(1),
            state: Mutex::new(StuckState::default()),
        }
    }

    /// Forgets all observed rounds, preparing the guard for a new session.
    pub fn reset(&self) {
        let mut state = self.lock_state();
        *state = StuckState::default();
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, StuckState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn decision(&self, stalled_rounds: u32) -> GuardDecision {
        if stalled_rounds >= self.max_stalled_rounds {
            GuardDecision::Exceeded {
                kind: BudgetKind::Stuck,
                reason: format!(
                    "no progress for {stalled_rounds} consecutive rounds (limit {})",
                    self.max_stalled_rounds
                ),
            }
        } else {
            GuardDecision::Allow
        }
    }
}

impl Guard for StuckDetectionGuard {
    fn name(&self) -> &str {
        "stuck_detection"
    }

    fn check(&self, context: &GuardContext<'_>) -> GuardDecision {
        let mut state = self.lock_state();

        if let Some(last_round) = state.last_round {
            if context.round < last_round {
                *state = StuckState::default();
            } else if context.round == last_round {
                return self.decision(state.stalled_rounds);
            }
        }

        let progressed = context.tool_call.is_some()
            || context.tool_result.is_some()
            || context.last_output != state.last_output;

        state.stalled_rounds = if state.last_round.is_some() && !progressed {
            state.stalled_rounds.saturating_add(1)
        } else {
            0
        };
        state.last_round = Some(context.round);
        state.last_output = context.last_output.to_owned();

        self.decision(state.stalled_rounds)
    }
}

/// Stops a session when a tool result exceeds a fixed byte size.
#[derive(Debug, Clone, Copy)]
pub struct ResultSizeGuard {
    max_bytes: usize,
}

impl ResultSizeGuard {
    /// Creates a guard allowing tool result contents up to `max_bytes` bytes.
    pub fn new(max_bytes: usize) -> Self {
        Self { max_bytes }
    }
}

impl Guard for ResultSizeGuard {
    fn name(&self) -> &str {
        "result_size"
    }

    fn check(&self, context: &GuardContext<'_>) -> GuardDecision {
        let Some(result) = context.tool_result else {
            return GuardDecision::Allow;
        };
        let size = result.content.len();
        if size > self.max_bytes {
            GuardDecision::Exceeded {
                kind: BudgetKind::ResultSize,
                reason: format!(
                    "tool result size {size} bytes exceeds limit {} bytes",
                    self.max_bytes
                ),
            }
        } else {
            GuardDecision::Allow
        }
    }
}
