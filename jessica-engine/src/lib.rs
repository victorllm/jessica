//! Governed turn runtime for Jessica.
//!
//! `jessica-engine` owns the ordering and invariants of one governed turn. The
//! runtime crate stays a bare model/tool loop, the harness stays a collection
//! of reusable middleware, and this crate is the only assembly layer an
//! embedder needs in order to run a turn without bypassing governance.
//!
//! # Status
//!
//! Tasks E1–E5 built this crate. It contains the public API, ports,
//! configuration, the governed provider-to-tool ordering, the producer
//! journal, event pump, SQLite recorder, offline replay, acceptance retries,
//! memory/skill post-processing, delegation parent assembly, and resume.
//! [`TracePolicy::Required`] is the default: a required turn trace is opened
//! before any provider call and only succeeds after `turn_ended` is durable.
//! The configured [`CatalogProfile`] is applied to the host catalog before
//! exposure budgets, delegation parent capabilities, and the model-visible
//! registry are computed.
//!
//! Artifact storage, bounded result management, and the [`ReadArtifactTool`]
//! are owned here. [`ArtifactStore`] writes the CLI-compatible v1 sidecar and
//! validates it on every read; [`ResultManager`] runs cleanup at most once per
//! turn and lets `read_artifact` results bypass management. The CLI supplies
//! [`ResultConfig`] and uses the same manager and tool.
//!
//! [`Engine::run_turn_request`] and [`Engine::resume_turn`] are the complete
//! turn entry points: fixed initial message ordering, one memory and one skill
//! recall per turn, acceptance retries with per-attempt sessions, the outer
//! session recorded on final failure or a governance trip, budget-reason
//! overrides, fail-open post-turn memory/skill hooks, delegation parent
//! assembly, and resume from recorded evidence without replaying completed
//! work. The CLI and ACP are adapters over these entry points; [`replay`]
//! provides the deterministic offline replay used by trace verification.
//!
//! # API stability
//!
//! The public API was frozen by Task E5 on 2026-11-28. Changing any public
//! type, trait, method, variant, or field requires a separate Task. While the
//! crate is pre-1.0, minor releases may include breaking changes and patch
//! releases carry fixes and documentation only; from 1.0 the crate follows
//! standard semantic versioning.
//!
//! Governed entry points are never hidden behind a feature or `doc(hidden)`.
//! [`Engine::run_turn`] is the trace-store variant for embedders that do not
//! need the full lifecycle; [`Engine::run_turn_request`] and
//! [`Engine::resume_turn`] are the complete paths used by the CLI and ACP.
//!
//! Verify the dependency boundary with:
//!
//! ```text
//! cargo tree -p jessica-engine --edges normal
//! ```

#![deny(missing_docs)]

mod api;
mod artifacts;
mod config;
mod error;
mod governance;
mod journal;
mod policy;
mod ports;
mod pump;
mod recorder;
pub mod replay;
mod tooling;

pub use api::{Engine, EngineBuilder, TurnOutcome, TurnRequest};
pub use artifacts::{
    ArtifactError, ArtifactPage, ArtifactStore, CleanupReport, READ_ARTIFACT_TOOL,
    ReadArtifactTool, ResultManager, StoredArtifact,
};
pub use config::{
    AcceptanceConfig, ApprovalConfig, ApprovalMode, CatalogLimits, EngineConfig, PolicyConfig,
    PolicyRule, ResultConfig, RuleAction, ToolExecutionConfig, ToolSelector, TracePolicy,
};
pub use error::EngineError;
pub use governance::{GovernanceState, GovernedTool, TokenCountingProvider};
pub use journal::TurnJournal;
pub use policy::{PolicyError, PolicyTool, PolicyV2, PolicyVerdict, RuleMatch, glob_matches};
pub use ports::{
    IdSource, MemoryPort, PumpConsole, SessionStore, SkillPort, SystemPromptSource, ToolContext,
    ToolPermissionDecision, ToolPermissionGate, ToolPolicy, ToolProvider, ToolResultManager,
    TraceStore, TurnObserver, TurnObserver as UpdateSink, TurnTrace,
};
pub use pump::{EventPump, LagHandling, PumpIntegrity, PumpOutput, SessionSnapshot};
pub use recorder::{EventJournal, EventRecorder, RecorderError, SqliteTraceStore, StoredSession};
pub use tooling::{
    ApprovalPolicy, Capabilities, CatalogProfile, PolicyHints, ResultFormat, ToolAnnotations,
    ToolCatalog, ToolDescriptor, ToolEntry, ToolLimits, ToolMetadata, ToolSource, digest_schema,
    policy_hints_for, result_format_for,
};

pub use jessica_core::acceptance::AcceptanceCheck;
pub use jessica_runtime::{ChildToolFactory, DelegationLimits};
