//! Deterministic agent loop for Jessica.
//!
//! The runtime depends only on the frozen contracts in `jessica-core`; concrete
//! providers and tools are injected as trait objects by the caller. Every state
//! transition is broadcast as an [`jessica_core::types::AgentEvent`], which is
//! the trace consumed by the CLI, the harness, and (in Phase 4) the event store.

#![deny(missing_docs)]

mod agent;
mod delegation;
mod recovery;

pub use agent::AgentLoop;
pub use delegation::{
    CHILD_TOOL_TIMEOUT, DELEGATE_TASK_TOOL_NAME, DelegationError, DelegationLimits,
    MIN_RESULT_BYTES, NarrowedTools, ParentCapabilities, narrow_tools,
};
pub use recovery::{RecordedEvent, RecoveryError, SessionCheckpoint, recover_session};
