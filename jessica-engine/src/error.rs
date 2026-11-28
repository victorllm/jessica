//! Structured errors returned by the engine API.

use thiserror::Error;

use crate::policy::PolicyError;

/// Failure while configuring or assembling the governed engine.
///
/// The enum is non-exhaustive: later Phase 11 tasks add lifecycle and trace
/// failures without breaking downstream matches.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EngineError {
    /// The builder was not given an LLM provider.
    #[error("engine requires an LLM provider")]
    MissingProvider,
    /// The builder was not given a tool provider.
    #[error("engine requires a tool provider")]
    MissingToolProvider,
    /// The builder was not given an engine configuration.
    #[error("engine requires an engine configuration")]
    MissingConfig,
    /// The builder was not given a result-management configuration.
    #[error("engine requires a result configuration")]
    MissingResultConfig,
    /// The policy can require one-time approval, but no gate was installed.
    #[error("engine policy requires approval but no permission gate was installed")]
    MissingApprovalGate,
    /// A required trace policy was configured without a trace store.
    #[error("engine requires a trace store when `TracePolicy::Required` is configured")]
    MissingTraceStore,
    /// The turn trace could not be opened, recorded, or closed.
    ///
    /// A turn whose trace is incomplete is never returned as a normal outcome:
    /// the missing terminal marker means the run cannot be proven complete.
    #[error("trace for turn `{turn_id}` is incomplete: {cause}")]
    TraceIncomplete {
        /// Turn whose trace could not be completed.
        turn_id: String,
        /// Underlying storage, attribution, or acknowledgement failure.
        cause: String,
    },
    /// A catalog entry's descriptor names a different tool.
    #[error("tool descriptor names `{descriptor}`, but the tool reports `{tool}`")]
    ToolNameMismatch {
        /// Name reported by the descriptor.
        descriptor: String,
        /// Name reported by the tool implementation.
        tool: String,
    },
    /// Two catalog entries have the same model-visible name.
    #[error("tool `{name}` is registered in the catalog more than once")]
    DuplicateTool {
        /// Duplicated tool name.
        name: String,
    },
    /// The turn's workspace root could not be canonicalized.
    #[error("engine workspace root `{root}` could not be canonicalized: {source}")]
    Workspace {
        /// Root supplied by the caller.
        root: String,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Policy evaluation could not be initialized for the turn.
    #[error("engine policy could not be initialized: {0}")]
    Policy(#[from] PolicyError),
}
