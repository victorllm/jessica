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
    /// The policy can require one-time approval, but no gate was installed.
    #[error("engine policy requires approval but no permission gate was installed")]
    MissingApprovalGate,
    /// Delegation limits were configured without a child tool factory.
    #[error("engine delegation is configured but no child tool factory was installed")]
    MissingChildToolFactory,
    /// A host-supplied catalog entry has no executable implementation.
    #[error("tool `{name}` is exposed by the host catalog but has no executable implementation")]
    MissingToolImplementation {
        /// Tool whose implementation is missing.
        name: String,
    },
    /// The exposed catalog exceeds the configured tool-count budget.
    #[error("tool catalog exposes {exposed} tools but the engine limit is {limit}")]
    CatalogTooLarge {
        /// Number of exposed tools.
        exposed: usize,
        /// Configured limit.
        limit: usize,
    },
    /// The exposed catalog exceeds the configured schema-byte budget.
    #[error("exposed tool schemas total {bytes} bytes but the engine limit is {limit}")]
    SchemaBudgetExceeded {
        /// Total canonical schema bytes.
        bytes: usize,
        /// Configured limit.
        limit: usize,
    },
    /// A host port failed in a way the engine cannot interpret structurally.
    #[error("host port `{component}` failed: {message}")]
    HostPort {
        /// Port or host component that failed.
        component: String,
        /// Underlying failure text.
        message: String,
    },
    /// A required trace policy was configured without a trace store.
    #[error("engine requires a trace store when `TracePolicy::Required` is configured")]
    MissingTraceStore,
    /// The turn orchestration was built without an event journal.
    ///
    /// [`Engine::run_turn_request`](crate::Engine::run_turn_request) and
    /// [`Engine::resume_turn`](crate::Engine::resume_turn) require the journal
    /// seam because they record per-attempt sessions, outer sessions, and the
    /// turn lifecycle through [`TurnJournal`](crate::TurnJournal).
    #[error("engine requires an event journal for full turn orchestration")]
    MissingEventJournal,
    /// The configuration enables acceptance but no check was installed.
    #[error("engine acceptance is configured but no acceptance check was installed")]
    MissingAcceptanceCheck,
    /// Resume was requested without a session store.
    #[error("engine requires a session store to resume a recorded session")]
    MissingSessionStore,
    /// Resume was requested for a session the store does not know.
    #[error("session `{session_id}` has no recorded evidence to resume")]
    UnknownSession {
        /// Session requested by the caller.
        session_id: String,
    },
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
    /// A recorded stream could not be loaded.
    #[error("session evidence could not be loaded: {0}")]
    Recorder(#[from] crate::recorder::RecorderError),
    /// A recorded session could not be rebuilt into a checkpoint.
    #[error("session recovery failed: {0}")]
    Recovery(#[from] jessica_runtime::RecoveryError),
    /// Delegation limits, parent capabilities, or a child factory failed.
    #[error("delegation failed: {0}")]
    Delegation(#[from] jessica_runtime::DelegationError),
}
