//! Producer-side event delivery contracts.
//!
//! The runtime, harness, and CLI governance emit [`AgentEvent`]s. A
//! [`broadcast`] channel is deliberately lossy for slow subscribers: it
//! overwrites old entries instead of applying backpressure. An [`EventSink`]
//! gives the host a place to capture every event before it is broadcast, so
//! persistence is not a race against a bounded observer queue.
//!
//! Implementations must either accept the event or return an error. They must
//! not silently drop an event because an internal queue is full; waiting for
//! capacity is the intended backpressure mechanism.
//!
//! [`broadcast`]: https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html

use async_trait::async_trait;
use thiserror::Error;

use crate::types::AgentEvent;

/// Receives agent events on the producer path, before they are broadcast.
///
/// Every producer that supports a sink must call it before publishing the
/// event to observers. A successful call means the sink accepted ownership of
/// the event; an error means the caller must stop the run and fail the turn.
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Accepts one event for durable recording.
    ///
    /// Implementations may wait for internal capacity, but must not drop the
    /// event. Returning an error tells the producer to stop emitting.
    async fn emit(&self, event: AgentEvent) -> Result<(), EventSinkError>;
}

/// An [`EventSink`] rejected an event.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("event sink failed: {message}")]
pub struct EventSinkError {
    /// Human-readable failure description.
    pub message: String,
}

impl EventSinkError {
    /// Creates a sink error from a human-readable description.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::executor::block_on;

    use super::*;

    /// Sink double that records each accepted event.
    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<AgentEvent>>,
    }

    #[async_trait]
    impl EventSink for RecordingSink {
        async fn emit(&self, event: AgentEvent) -> Result<(), EventSinkError> {
            self.events
                .lock()
                .expect("events mutex poisoned")
                .push(event);
            Ok(())
        }
    }

    #[test]
    fn a_sink_accepts_events_through_a_trait_object() {
        let sink = Arc::new(RecordingSink::default());
        let event = AgentEvent::ModelDelta {
            text: "captured".to_owned(),
        };

        block_on((Arc::clone(&sink) as Arc<dyn EventSink>).emit(event.clone()))
            .expect("the recording sink must accept the event");

        assert_eq!(
            sink.events
                .lock()
                .expect("events mutex poisoned")
                .as_slice(),
            &[event]
        );
    }

    #[test]
    fn sink_errors_keep_the_failure_message() {
        let error = EventSinkError::new("journal closed");

        assert_eq!(error.to_string(), "event sink failed: journal closed");
        assert_eq!(error.message, "journal closed");
    }
}
