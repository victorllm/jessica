//! Hook contract: observability and lifecycle middleware.

use async_trait::async_trait;

use crate::types::{AgentEvent, SessionId};

/// Middleware invoked at fixed points of the agent loop.
///
/// Every method defaults to a no-op so hooks only override what they need.
#[async_trait]
pub trait Hook: Send + Sync {
    /// Human-readable name used in diagnostics.
    fn name(&self) -> &str;

    /// Called once before the loop starts.
    async fn on_session_start(&self, _session_id: &SessionId) {}

    /// Called for every event emitted by the loop.
    async fn on_event(&self, _event: &AgentEvent) {}

    /// Called once after the loop ends.
    async fn on_session_end(&self, _session_id: &SessionId) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingHook {
        events: Mutex<Vec<AgentEvent>>,
    }

    #[async_trait]
    impl Hook for RecordingHook {
        fn name(&self) -> &str {
            "recording"
        }

        async fn on_event(&self, event: &AgentEvent) {
            self.events
                .lock()
                .expect("test mutex should not be poisoned")
                .push(event.clone());
        }
    }

    #[test]
    fn default_lifecycle_methods_are_no_ops_and_event_hook_records() {
        let hook: RecordingHook = RecordingHook {
            events: Mutex::new(Vec::new()),
        };
        let dynamic: &dyn Hook = &hook;
        assert_eq!(dynamic.name(), "recording");

        let session = SessionId::new("session-1");
        let event = AgentEvent::ModelDelta {
            text: "hello".into(),
        };

        futures::executor::block_on(async {
            dynamic.on_session_start(&session).await;
            dynamic.on_event(&event).await;
            dynamic.on_session_end(&session).await;
        });

        let recorded = hook
            .events
            .lock()
            .expect("test mutex should not be poisoned");
        assert_eq!(recorded.as_slice(), std::slice::from_ref(&event));
    }
}
