//! Governed turn runtime for Jessica.
//!
//! `jessica-engine` owns the ordering and invariants of one governed turn. The
//! runtime crate stays a bare model/tool loop, the harness stays a collection
//! of reusable middleware, and this crate is the only assembly layer an
//! embedder needs in order to run a turn without bypassing governance.
//!
//! # E1c status
//!
//! This crate contains the public API, ports, configuration, and the
//! provider-to-tool governance vertical slice introduced by Tasks E1a and E1b.
//! Task E1c added the producer-side event sink, host policy and result-manager
//! ports, and the CLI adapter that delegates its governed-tool ordering here.
//! Descriptor/catalog/artifact ownership is still CLI-side, and journal, pump,
//! acceptance, memory, skill, delegation and resume orchestration remain
//! absent until the corresponding Phase 11 tasks migrate them.
//! [`TracePolicy::Required`] is rejected rather than silently downgraded until
//! E2 implements durable trace persistence.

#![deny(missing_docs)]

mod api;
mod artifacts;
mod config;
mod error;
mod governance;
mod policy;
mod ports;
mod tooling;

pub use api::{Engine, EngineBuilder, TurnOutcome, TurnRequest};
pub use artifacts::{ArtifactError, ArtifactPage, ArtifactStore, ResultManager, StoredArtifact};
pub use config::{
    ApprovalConfig, ApprovalMode, EngineConfig, PolicyConfig, PolicyRule, ResultConfig, RuleAction,
    ToolExecutionConfig, ToolSelector, TracePolicy,
};
pub use error::EngineError;
pub use governance::{GovernanceState, GovernedTool, TokenCountingProvider};
pub use policy::{PolicyError, PolicyTool, PolicyV2, PolicyVerdict, RuleMatch, glob_matches};
pub use ports::{
    IdSource, MemoryPort, PumpConsole, SkillPort, SystemPromptSource, ToolContext,
    ToolPermissionDecision, ToolPermissionGate, ToolPolicy, ToolProvider, ToolResultManager,
    TraceStore, TurnObserver, TurnTrace,
};
pub use tooling::{
    ApprovalPolicy, Capabilities, ResultFormat, ToolCatalog, ToolDescriptor, ToolEntry, ToolLimits,
    ToolSource,
};

pub use jessica_core::acceptance::AcceptanceCheck;
pub use jessica_runtime::ChildToolFactory;
