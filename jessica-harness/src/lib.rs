//! Harness middleware assembly for Jessica sessions.
//!
//! The harness observes the same [`AgentEvent`](jessica_core::types::AgentEvent)
//! stream as the agent loop and invokes [`Hook`]s in deterministic registration
//! order. This phase is observation-only: guards, policies, and sensors are
//! added by later tasks.

#![deny(missing_docs)]

mod harness;
mod logging;

pub use harness::{DEFAULT_EVENT_CAPACITY, Harness};
pub use logging::LoggingHook;
