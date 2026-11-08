//! LLM provider contract.

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::tool::ToolSchema;
use crate::types::{Message, TokenUsage, ToolCall};

/// Owned stream of events produced by an [`LlmProvider`].
pub type LlmStream = BoxStream<'static, LlmEvent>;

/// Event emitted by a streaming model provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LlmEvent {
    /// A fragment of model-generated text.
    TextDelta {
        /// Text fragment.
        text: String,
    },
    /// The model requested a complete tool call.
    ToolCallRequested {
        /// The assembled tool call.
        call: ToolCall,
    },
    /// The stream completed successfully.
    Done {
        /// Token usage reported by the provider, when available.
        usage: Option<TokenUsage>,
    },
    /// The provider reported an error while streaming.
    Error {
        /// Human-readable error description.
        message: String,
    },
}

/// Errors raised while requesting or streaming a model response.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LlmError {
    /// The request could not be issued.
    #[error("LLM request failed: {message}")]
    Request {
        /// Human-readable error description.
        message: String,
    },
    /// The response stream failed or ended unexpectedly.
    #[error("LLM stream failed: {message}")]
    Stream {
        /// Human-readable error description.
        message: String,
    },
    /// The provider returned a response that could not be interpreted.
    #[error("LLM response was invalid: {message}")]
    InvalidResponse {
        /// Human-readable error description.
        message: String,
    },
}

/// Streaming chat-completion provider.
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// Starts a streaming chat completion for `messages`, exposing `tools` to
    /// the model.
    ///
    /// The returned stream owns all of its data so it can outlive the borrow of
    /// this provider invocation.
    async fn stream_chat(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;

    fn assert_round_trip<T>(value: &T)
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_string(value).expect("value should serialize");
        let decoded: T = serde_json::from_str(&encoded).expect("value should deserialize");
        assert_eq!(&decoded, value);
    }

    struct MockProvider {
        events: Vec<LlmEvent>,
    }

    #[async_trait]
    impl LlmProvider for MockProvider {
        async fn stream_chat(
            &self,
            messages: &[Message],
            tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].content, "hello");
            assert!(tools.is_empty());
            Ok(Box::pin(futures::stream::iter(self.events.clone())))
        }
    }

    #[test]
    fn every_llm_event_round_trips_with_type_tag() {
        let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));
        let events = vec![
            (LlmEvent::TextDelta { text: "hi".into() }, "text_delta"),
            (LlmEvent::ToolCallRequested { call }, "tool_call_requested"),
            (
                LlmEvent::Done {
                    usage: Some(TokenUsage::new(10, 5)),
                },
                "done",
            ),
            (
                LlmEvent::Error {
                    message: "boom".into(),
                },
                "error",
            ),
        ];

        for (event, tag) in events {
            let encoded = serde_json::to_value(&event).unwrap();
            assert_eq!(encoded.get("type").and_then(|v| v.as_str()), Some(tag));
            assert_round_trip(&event);
        }
    }

    #[test]
    fn provider_streams_events_through_object_safe_trait() {
        let provider: Arc<dyn LlmProvider> = Arc::new(MockProvider {
            events: vec![
                LlmEvent::TextDelta {
                    text: "hello".into(),
                },
                LlmEvent::Done { usage: None },
            ],
        });

        let events = futures::executor::block_on(async {
            let stream = provider
                .stream_chat(&[Message::user("hello")], &[])
                .await
                .expect("mock provider should succeed");
            stream.collect::<Vec<_>>().await
        });

        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "hello".into()
                },
                LlmEvent::Done { usage: None }
            ]
        );
    }

    #[test]
    fn provider_errors_are_structured_and_readable() {
        let error = LlmError::InvalidResponse {
            message: "missing choices".into(),
        };
        assert!(error.to_string().contains("missing choices"));
        assert_eq!(
            error,
            LlmError::InvalidResponse {
                message: "missing choices".into()
            }
        );
    }
}
