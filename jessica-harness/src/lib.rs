//! Harness middleware assembly for Jessica sessions.
//!
//! The harness observes the same [`AgentEvent`](jessica_core::types::AgentEvent)
//! stream as the agent loop and invokes [`Hook`]s in deterministic registration
//! order. It also assembles deterministic guards, a tool policy, and a sensor
//! pipeline. Acceptance orchestration is provided separately because it must
//! re-enter the agent session after a failed check.
//!
//! [`Guide`] and [`MemoryGuide`] provide backend-agnostic context injection:
//! a guide renders recalled memory into one bounded system message without
//! coupling the harness to a concrete storage implementation. [`SkillDistiller`]
//! and [`LlmSkillDistiller`] turn an eligible, bounded turn source into a
//! strictly parsed skill draft without writing storage or the event stream.
//! [`SkillWorker`] is the single-worker consumer that claims a queued job,
//! distills it, writes it through an injected sink, and records the terminal
//! transition through the injected queue. [`SkillGuide`] renders skills from a
//! [`SkillSource`] as one bounded, fail-open system message.

#![deny(missing_docs)]

mod acceptance;
mod guards;
mod guide;
mod harness;
mod logging;
mod policy;
mod sensors;
mod skill_distiller;
mod skill_guide;
mod skill_source;
mod skill_worker;

pub use acceptance::{AcceptanceOutcome, AcceptanceRunner, SessionAttempt};
pub use guards::{ResultSizeGuard, StuckDetectionGuard, TokenBudgetGuard, ToolTimeoutGuard};
pub use guide::{Guide, GuideError, MemoryGuide};
pub use harness::{DEFAULT_EVENT_CAPACITY, Harness};
pub use logging::LoggingHook;
pub use policy::AllowlistPolicy;
pub use sensors::{CommandSensor, SensorObservation, SensorPipeline, SensorReport};
pub use skill_distiller::{
    DistillError, DistilledSkill, LlmSkillDistiller, MAX_SKILL_DESCRIPTION_BYTES,
    MAX_SKILL_INSTRUCTIONS_BYTES, MAX_SKILL_NAME_BYTES, MAX_SKILL_TAG_BYTES, MAX_SKILL_TAGS,
    SKILL_DISTILL_SYSTEM_PROMPT, SkillDistiller,
};
pub use skill_guide::{
    DEFAULT_SKILL_INJECT_BYTES, SkillContent, SkillGuide, SkillSource, SkillSourceError,
};
pub use skill_source::{
    DEFAULT_MIN_ROUNDS, DEFAULT_MIN_TOOL_CALLS, DistillSource, DistillSourceInput,
    DistillSourcePersistError, MAX_TOOL_SUMMARY_BYTES, SkillTrigger, TurnSignals, escape_untrusted,
};
pub use skill_worker::{
    DEFAULT_DRAIN_TIMEOUT, DEFAULT_IDLE_INTERVAL, DEFAULT_MAX_ATTEMPTS, SkillJob, SkillJobId,
    SkillJobQueue, SkillQueueError, SkillSink, SkillSinkError, SkillWorker, SkillWorkerConfig,
    SkillWorkerError, WorkerClock, WorkerOutcome, WorkerTimestamp,
};
