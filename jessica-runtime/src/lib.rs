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

pub use agent::{AgentLoop, TranscriptRun};
pub use delegation::{
    AgentChildExecutor, CHILD_AGENT_SYSTEM_PROMPT, CHILD_TOOL_TIMEOUT, ChildEventSink,
    ChildToolFactory, DELEGATE_TASK_TOOL_NAME, DelegateTaskArgs, DelegateTaskTool, DelegationError,
    DelegationLimits, MIN_RESULT_BYTES, NarrowedTools, ParentCapabilities, child_session_hash,
    delegate_task_schema, narrow_tools, parse_delegate_task_args,
};
pub use recovery::{RecordedEvent, RecoveryError, SessionCheckpoint, recover_session};
