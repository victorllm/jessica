//! Deterministic agent loop for Jessica.
//!
//! The runtime depends only on the frozen contracts in `jessica-core`; concrete
//! providers and tools are injected as trait objects by the caller. Every state
//! transition is broadcast as an [`jessica_core::types::AgentEvent`], which is
//! the trace consumed by the CLI, the harness, and (in Phase 4) the event store.

#![deny(missing_docs)]

mod agent;

pub use agent::AgentLoop;
