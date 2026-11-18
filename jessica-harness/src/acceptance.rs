//! Acceptance-check orchestration across agent attempts.

use std::future::Future;
use std::sync::Arc;

use jessica_core::acceptance::{AcceptanceCheck, AcceptanceContext};
use jessica_core::events::EventSink;
use jessica_core::types::{AgentEvent, Message, SessionEndReason};
use tokio::sync::broadcast;

/// One completed agent attempt handed to an acceptance check.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionAttempt {
    /// Full conversation produced by the attempt.
    pub messages: Vec<Message>,
    /// Final text produced by the model.
    pub last_output: String,
    /// Why the attempt stopped.
    pub reason: SessionEndReason,
}

/// Final result of acceptance orchestration.
#[derive(Debug, Clone, PartialEq)]
pub struct AcceptanceOutcome {
    /// Terminal reason for the outer session.
    ///
    /// This is [`SessionEndReason::Completed`] when a check passed, the original
    /// non-completed reason when the agent did not complete, or
    /// [`SessionEndReason::AcceptanceFailed`] when retries were exhausted.
    pub reason: SessionEndReason,
    /// Conversation from the final attempt.
    pub messages: Vec<Message>,
    /// Final model output from the final attempt.
    pub last_output: String,
    /// Number of agent attempts made, starting at one.
    pub attempts: u32,
}

/// Runs an acceptance check after each completed agent attempt.
///
/// A failed check emits [`AgentEvent::AcceptanceFailed`] and injects the
/// feedback as a new user [`Message`] before invoking the next attempt. The
/// check runs only when the attempt stopped with
/// [`SessionEndReason::Completed`]; cancellation, budget exhaustion, and errors
/// pass through unchanged.
///
/// `max_retries` is the number of additional attempts allowed after the first
/// failed check. With `max_retries == 0`, one failed check ends the session with
/// [`SessionEndReason::AcceptanceFailed`].
pub struct AcceptanceRunner {
    check: Arc<dyn AcceptanceCheck>,
    max_retries: u32,
    events: broadcast::Sender<AgentEvent>,
    sink: Option<Arc<dyn EventSink>>,
}

impl AcceptanceRunner {
    /// Creates a runner that emits acceptance events on `events`.
    pub fn new(
        check: Arc<dyn AcceptanceCheck>,
        max_retries: u32,
        events: broadcast::Sender<AgentEvent>,
    ) -> Self {
        Self {
            check,
            max_retries,
            events,
            sink: None,
        }
    }

    /// Attaches a producer-side event sink for acceptance failures.
    ///
    /// The sink receives [`AgentEvent::AcceptanceFailed`] before it is
    /// broadcast. If the sink rejects the event, the runner returns
    /// [`SessionEndReason::Error`] and does not start another attempt: the
    /// failed check would otherwise be missing from the turn trace.
    pub fn with_event_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Runs attempts until acceptance passes, the agent stops for another
    /// reason, or retries are exhausted.
    ///
    /// `attempt` receives the conversation for the next agent attempt and must
    /// return the full conversation, final output, and terminal reason when it
    /// stops. The first call receives `initial_messages`; later calls receive
    /// the previous attempt's conversation plus a user message carrying the
    /// failed check's feedback.
    ///
    /// Each failed check emits [`AgentEvent::AcceptanceFailed`] with the
    /// one-based attempt number whose output failed. The event is sent before
    /// the next attempt starts.
    pub async fn run<F, Fut>(
        &self,
        initial_messages: Vec<Message>,
        mut attempt: F,
    ) -> AcceptanceOutcome
    where
        F: FnMut(Vec<Message>) -> Fut,
        Fut: Future<Output = SessionAttempt>,
    {
        let mut messages = initial_messages;
        let mut attempt_number = 0_u32;

        loop {
            attempt_number = attempt_number.saturating_add(1);
            let outcome = attempt(messages).await;

            if outcome.reason != SessionEndReason::Completed {
                return AcceptanceOutcome {
                    reason: outcome.reason,
                    messages: outcome.messages,
                    last_output: outcome.last_output,
                    attempts: attempt_number,
                };
            }

            let result = self
                .check
                .check(&AcceptanceContext {
                    messages: &outcome.messages,
                    last_output: &outcome.last_output,
                })
                .await;

            if result.passed {
                return AcceptanceOutcome {
                    reason: SessionEndReason::Completed,
                    messages: outcome.messages,
                    last_output: outcome.last_output,
                    attempts: attempt_number,
                };
            }

            let event = AgentEvent::AcceptanceFailed {
                attempt: attempt_number,
                feedback: result.feedback.clone(),
            };
            if let Some(sink) = &self.sink
                && sink.emit(event.clone()).await.is_err()
            {
                return AcceptanceOutcome {
                    reason: SessionEndReason::Error,
                    messages: outcome.messages,
                    last_output: outcome.last_output,
                    attempts: attempt_number,
                };
            }
            let _ = self.events.send(event);

            if attempt_number > self.max_retries || attempt_number == u32::MAX {
                return AcceptanceOutcome {
                    reason: SessionEndReason::AcceptanceFailed,
                    messages: outcome.messages,
                    last_output: outcome.last_output,
                    attempts: attempt_number,
                };
            }

            messages = outcome.messages;
            messages.push(Message::user(result.feedback));
        }
    }
}
