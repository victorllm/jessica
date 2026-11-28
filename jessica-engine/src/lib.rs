//! Governed turn runtime for Jessica.
//!
//! `jessica-engine` owns the ordering and invariants of one governed turn. The
//! runtime crate stays a bare model/tool loop, the harness stays a collection
//! of reusable middleware, and this crate is the only assembly layer an
//! embedder needs in order to run a turn without bypassing governance.
//!
//! # E3a status
//!
//! This crate contains the public API, ports, configuration, and the governed
//! provider-to-tool vertical slice introduced by Tasks E1a and E1b, the
//! producer-side event sink and host policy/result-manager ports from E1c, the
//! descriptor/patch/artifact alignment from E1d, and the producer journal,
//! event pump, and SQLite recorder from E2. [`TracePolicy::Required`] is the
//! default: a required turn trace is opened before any provider call and only
//! succeeds after `turn_ended` is durable.
//!
//! Task E3a added the engine-owned complete-turn entry points
//! [`Engine::run_turn_request`] and [`Engine::resume_turn`]: fixed initial
//! message ordering, one memory and one skill recall per turn, acceptance
//! retries with per-attempt sessions, the outer session recorded on final
//! failure or a governance trip, budget-reason overrides, fail-open post-turn
//! memory/skill hooks, and resume from recorded evidence without replaying
//! completed work. Delegation parent assembly, CLI adoption of these entry
//! points, and the `HarnessConfig`/`EngineConfig` split remain in E3b; the CLI
//! still owns its parallel turn assembly until then.

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
mod tooling;

pub use api::{Engine, EngineBuilder, TurnOutcome, TurnRequest};
pub use artifacts::{
    ArtifactError, ArtifactPage, ArtifactStore, CleanupReport, ResultManager, StoredArtifact,
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
    ApprovalPolicy, Capabilities, PolicyHints, ResultFormat, ToolAnnotations, ToolCatalog,
    ToolDescriptor, ToolEntry, ToolLimits, ToolMetadata, ToolSource, digest_schema,
    policy_hints_for, result_format_for,
};

pub use jessica_core::acceptance::AcceptanceCheck;
pub use jessica_runtime::{ChildToolFactory, DelegationLimits};
