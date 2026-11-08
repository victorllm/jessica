//! Harness middleware assembly for Jessica sessions.
//!
//! The harness observes the same [`AgentEvent`](jessica_core::types::AgentEvent)
//! stream as the agent loop and invokes [`Hook`]s in deterministic registration
//! order. It also assembles deterministic guards and a tool policy, producing
//! structured termination events for checkpoints supplied by the caller.

#![deny(missing_docs)]

mod guards;
mod harness;
mod logging;
mod policy;

pub use guards::{ResultSizeGuard, StuckDetectionGuard, TokenBudgetGuard, ToolTimeoutGuard};
pub use harness::{DEFAULT_EVENT_CAPACITY, Harness};
pub use logging::LoggingHook;
pub use policy::AllowlistPolicy;
