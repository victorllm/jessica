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
//! coupling the harness to a concrete storage implementation.

#![deny(missing_docs)]

mod acceptance;
mod guards;
mod guide;
mod harness;
mod logging;
mod policy;
mod sensors;

pub use acceptance::{AcceptanceOutcome, AcceptanceRunner, SessionAttempt};
pub use guards::{ResultSizeGuard, StuckDetectionGuard, TokenBudgetGuard, ToolTimeoutGuard};
pub use guide::{Guide, GuideError, MemoryGuide};
pub use harness::{DEFAULT_EVENT_CAPACITY, Harness};
pub use logging::LoggingHook;
pub use policy::AllowlistPolicy;
pub use sensors::{CommandSensor, SensorObservation, SensorPipeline, SensorReport};
