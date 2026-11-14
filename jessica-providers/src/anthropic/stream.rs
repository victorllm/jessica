//! Anthropic SSE aggregation into provider-neutral [`LlmEvent`]s.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use futures::StreamExt;
use futures::stream::Stream;
use jessica_core::llm::{LlmError, LlmEvent, LlmStream};
use jessica_core::types::{TokenUsage, ToolCall};
use serde_json::Value;

use super::sse::{
    AnthropicErrorBody, AnthropicSseDecoder, AnthropicSseEvent, AnthropicUsage, ContentBlock,
    ContentBlockDelta,
};
use crate::reliability::{ClassifiedStream, ProviderFailure};

/// Turns raw Anthropic SSE byte chunks into a legacy stream of [`LlmEvent`]s.
///
/// The chunk and error types are generic so tests can drive the production
/// pipeline with an in-memory SSE stream.
pub(crate) fn anthropic_event_stream<S, B, E>(chunks: S) -> LlmStream
where
    S: Stream<Item = Result<B, E>> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    Box::pin(
        classified_anthropic_event_stream(chunks).map(|item| match item {
            Ok(event) => event,
            Err(failure) => LlmEvent::Error {
                message: failure.message().to_owned(),
            },
        }),
    )
}

/// Turns raw Anthropic SSE byte chunks into classified [`LlmEvent`]s.
pub(crate) fn classified_anthropic_event_stream<S, B, E>(chunks: S) -> ClassifiedStream
where
    S: Stream<Item = Result<B, E>> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    let chunks = Box::pin(chunks);
    let state = AnthropicStreamState::default();
    Box::pin(futures::stream::unfold(
        (chunks, state),
        |(mut chunks, mut state)| async move {
            loop {
                if let Some(item) = state.pending.pop_front() {
                    return Some((item, (chunks, state)));
                }
                if state.done {
                    return None;
                }
                match chunks.next().await {
                    Some(Ok(chunk)) => state.push_bytes(chunk.as_ref()),
                    Some(Err(error)) => {
                        state.fail(ProviderFailure::transport(error.to_string()));
                    }
                    None => state.finish_stream(),
                }
            }
        },
    ))
}

/// One classified item produced by the Anthropic stream assembler.
#[derive(Debug)]
pub(crate) enum AnthropicStreamItem {
    /// A provider-neutral event.
    Event(LlmEvent),
    /// A classified stream failure.
    Failure(ProviderFailure),
}

#[derive(Debug, Default)]
struct AnthropicStreamState {
    decoder: AnthropicSseDecoder,
    assembler: AnthropicStreamAssembler,
    pending: VecDeque<Result<LlmEvent, ProviderFailure>>,
    done: bool,
}

impl AnthropicStreamState {
    fn push_bytes(&mut self, bytes: &[u8]) {
        match self.decoder.push(bytes) {
            Ok(events) => {
                for event in events {
                    self.process_event(event);
                    if self.done {
                        return;
                    }
                }
            }
            Err(error) => self.fail(ProviderFailure::invalid_response(error.to_string())),
        }
    }

    fn process_event(&mut self, event: AnthropicSseEvent) {
        match self.assembler.push(event) {
            Ok(items) => {
                for item in items {
                    match item {
                        AnthropicStreamItem::Event(event) => {
                            self.pending.push_back(Ok(event));
                        }
                        AnthropicStreamItem::Failure(failure) => {
                            self.pending.push_back(Err(failure));
                            self.done = true;
                            return;
                        }
                    }
                }
                if self.assembler.is_done() {
                    self.done = true;
                }
            }
            Err(error) => self.fail(ProviderFailure::invalid_response(error.to_string())),
        }
    }

    fn finish_stream(&mut self) {
        if self.done {
            return;
        }
        match self.decoder.finish() {
            Ok(events) => {
                for event in events {
                    self.process_event(event);
                    if self.done {
                        return;
                    }
                }
            }
            Err(error) => {
                self.fail(ProviderFailure::invalid_response(error.to_string()));
                return;
            }
        }

        if !self.done
            && let Err(error) = self.assembler.finish()
        {
            self.fail(ProviderFailure::stream_interrupted(error.to_string()));
        }
    }

    fn fail(&mut self, failure: ProviderFailure) {
        if self.done {
            return;
        }
        self.pending.push_back(Err(failure));
        self.done = true;
    }
}

/// State machine that assembles Anthropic content blocks into LLM events.
#[derive(Debug, Default)]
pub(crate) struct AnthropicStreamAssembler {
    blocks: BTreeMap<usize, BlockState>,
    usage: UsageAccumulator,
    done: bool,
}

impl AnthropicStreamAssembler {
    /// Applies one SSE event, returning any completed provider-neutral items.
    pub(crate) fn push(
        &mut self,
        event: AnthropicSseEvent,
    ) -> Result<Vec<AnthropicStreamItem>, LlmError> {
        if self.done {
            return Ok(Vec::new());
        }

        let mut items = Vec::new();
        match event {
            AnthropicSseEvent::MessageStart { message } => {
                self.usage.merge(message.usage);
            }
            AnthropicSseEvent::ContentBlockStart {
                index,
                content_block,
            } => {
                let state = match content_block {
                    ContentBlock::Text { text } => {
                        if !text.is_empty() {
                            items.push(AnthropicStreamItem::Event(LlmEvent::TextDelta {
                                text: text.clone(),
                            }));
                        }
                        BlockState::Text { text }
                    }
                    ContentBlock::ToolUse { id, name, input } => BlockState::ToolUse {
                        id,
                        name,
                        initial_input: input,
                        partial_json: String::new(),
                    },
                    ContentBlock::Other => BlockState::Other,
                };
                self.blocks.insert(index, state);
            }
            AnthropicSseEvent::ContentBlockDelta { index, delta } => {
                let state =
                    self.blocks
                        .get_mut(&index)
                        .ok_or_else(|| LlmError::InvalidResponse {
                            message: format!("received a delta for unknown content block {index}"),
                        })?;
                match delta {
                    ContentBlockDelta::TextDelta { text } => match state {
                        BlockState::Text { text: buffer } => {
                            buffer.push_str(&text);
                            if !text.is_empty() {
                                items
                                    .push(AnthropicStreamItem::Event(LlmEvent::TextDelta { text }));
                            }
                        }
                        BlockState::Other => {}
                        BlockState::ToolUse { .. } => {
                            return Err(LlmError::InvalidResponse {
                                message: format!("received a text delta for tool block {index}"),
                            });
                        }
                    },
                    ContentBlockDelta::InputJsonDelta {
                        partial_json: fragment,
                    } => match state {
                        BlockState::ToolUse { partial_json, .. } => {
                            partial_json.push_str(&fragment);
                        }
                        BlockState::Other => {}
                        BlockState::Text { .. } => {
                            return Err(LlmError::InvalidResponse {
                                message: format!("received a tool delta for text block {index}"),
                            });
                        }
                    },
                    ContentBlockDelta::Other => {}
                }
            }
            AnthropicSseEvent::ContentBlockStop { index } => {
                if let Some(BlockState::ToolUse {
                    id,
                    name,
                    initial_input,
                    partial_json,
                }) = self.blocks.remove(&index)
                {
                    let arguments = if partial_json.trim().is_empty() {
                        initial_input
                    } else {
                        serde_json::from_str(&partial_json).map_err(|error| {
                            LlmError::InvalidResponse {
                                message: format!(
                                    "tool call `{id}` returned invalid JSON arguments: {error}"
                                ),
                            }
                        })?
                    };
                    items.push(AnthropicStreamItem::Event(LlmEvent::ToolCallRequested {
                        call: ToolCall::new(id, name, arguments),
                    }));
                }
            }
            AnthropicSseEvent::MessageDelta { usage } => {
                if let Some(usage) = usage {
                    self.usage.merge(usage);
                }
            }
            AnthropicSseEvent::MessageStop => {
                self.done = true;
                items.push(AnthropicStreamItem::Event(LlmEvent::Done {
                    usage: self.usage.token_usage(),
                }));
            }
            AnthropicSseEvent::Ping => {}
            AnthropicSseEvent::Error { error } => {
                self.done = true;
                items.push(AnthropicStreamItem::Failure(
                    classify_anthropic_stream_error(&error),
                ));
            }
        }

        Ok(items)
    }

    /// Returns whether a terminal event has been observed.
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    /// Verifies that the stream reached `message_stop`.
    pub(crate) fn finish(&self) -> Result<(), LlmError> {
        if self.done {
            Ok(())
        } else {
            Err(LlmError::Stream {
                message: "Anthropic SSE stream ended before message_stop".to_owned(),
            })
        }
    }
}

/// Classifies an Anthropic `error` SSE event.
fn classify_anthropic_stream_error(error: &AnthropicErrorBody) -> ProviderFailure {
    let message = if error.error_type.is_empty() {
        format!("Anthropic stream error: {}", error.message)
    } else {
        format!(
            "Anthropic stream error ({}): {}",
            error.error_type, error.message
        )
    };
    match error.error_type.as_str() {
        "rate_limit_error" => ProviderFailure::rate_limited(message),
        "authentication_error"
        | "permission_error"
        | "invalid_request_error"
        | "not_found_error" => ProviderFailure::client(message),
        _ => ProviderFailure::server(message),
    }
}

/// Content block state keyed by Anthropic's block index.
#[derive(Debug)]
enum BlockState {
    /// Text block whose accumulated text is used for delta emission.
    Text { text: String },
    /// Tool block accumulating JSON argument fragments.
    ToolUse {
        id: String,
        name: String,
        initial_input: Value,
        partial_json: String,
    },
    /// A block type ignored by this provider.
    Other,
}

/// Accumulates token counters reported at different points in the stream.
#[derive(Debug, Default)]
struct UsageAccumulator {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
}

impl UsageAccumulator {
    fn merge(&mut self, usage: AnthropicUsage) {
        if let Some(tokens) = usage.input_tokens {
            self.prompt_tokens = Some(tokens);
        }
        if let Some(tokens) = usage.output_tokens {
            self.completion_tokens = Some(tokens);
        }
    }

    fn token_usage(&self) -> Option<TokenUsage> {
        match (self.prompt_tokens, self.completion_tokens) {
            (None, None) => None,
            (prompt_tokens, completion_tokens) => Some(TokenUsage::new(
                prompt_tokens.unwrap_or_default(),
                completion_tokens.unwrap_or_default(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    fn fixture(name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
    }

    fn frame(event: &str, data: Value) -> String {
        format!(
            "event: {event}\ndata: {}\n\n",
            serde_json::to_string(&data).expect("data should serialize")
        )
    }

    fn collect(sse: &str, chunk_size: usize) -> Vec<LlmEvent> {
        let chunks: Vec<Vec<u8>> = sse
            .as_bytes()
            .chunks(chunk_size)
            .map(<[u8]>::to_vec)
            .collect();
        let stream = anthropic_event_stream(futures::stream::iter(
            chunks.into_iter().map(Ok::<_, String>),
        ));
        futures::executor::block_on(stream.collect())
    }

    fn collect_classified(sse: &str, chunk_size: usize) -> Vec<Result<LlmEvent, ProviderFailure>> {
        let chunks: Vec<Vec<u8>> = sse
            .as_bytes()
            .chunks(chunk_size)
            .map(<[u8]>::to_vec)
            .collect();
        let stream = classified_anthropic_event_stream(futures::stream::iter(
            chunks.into_iter().map(Ok::<_, String>),
        ));
        futures::executor::block_on(stream.collect())
    }

    fn fixture_events() -> Vec<LlmEvent> {
        vec![
            LlmEvent::TextDelta {
                text: "Let me ".to_owned(),
            },
            LlmEvent::TextDelta {
                text: "check.".to_owned(),
            },
            LlmEvent::ToolCallRequested {
                call: ToolCall::new("toolu_1", "read_file", json!({"path": "文档.txt"})),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(12, 25)),
            },
        ]
    }

    #[test]
    fn anthropic_fixture_streams_text_and_tool_calls() {
        let sse = fixture("anthropic_sse_text_and_tool.txt");
        assert_eq!(collect(&sse, 4096), fixture_events());
    }

    #[test]
    fn anthropic_stream_survives_one_byte_chunks_including_unicode() {
        let sse = fixture("anthropic_sse_text_and_tool.txt");
        assert_eq!(
            collect(&sse, 1),
            fixture_events(),
            "one-byte chunks must preserve UTF-8 and JSON assembly"
        );
    }

    #[test]
    fn anthropic_single_text_block_is_forwarded() {
        let sse = [
            frame(
                "message_start",
                json!({
                    "type": "message_start",
                    "message": {"usage": {"input_tokens": 3, "output_tokens": 1}},
                }),
            ),
            frame(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""},
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "Hel"},
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "lo"},
                }),
            ),
            frame(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 0}),
            ),
            frame(
                "message_delta",
                json!({
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn"},
                    "usage": {"output_tokens": 7},
                }),
            ),
            frame("message_stop", json!({"type": "message_stop"})),
        ]
        .concat();

        assert_eq!(
            collect(&sse, 3),
            vec![
                LlmEvent::TextDelta {
                    text: "Hel".to_owned(),
                },
                LlmEvent::TextDelta {
                    text: "lo".to_owned(),
                },
                LlmEvent::Done {
                    usage: Some(TokenUsage::new(3, 7)),
                },
            ]
        );
    }

    #[test]
    fn anthropic_text_and_tool_deltas_can_interleave() {
        let sse = [
            frame(
                "message_start",
                json!({
                    "type": "message_start",
                    "message": {"usage": {"input_tokens": 5, "output_tokens": 1}},
                }),
            ),
            frame(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""},
                }),
            ),
            frame(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": 1,
                    "content_block": {
                        "type": "tool_use",
                        "id": "toolu_9",
                        "name": "echo",
                        "input": {},
                    },
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "A"},
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 1,
                    "delta": {"type": "input_json_delta", "partial_json": "{\"value\":"},
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "B"},
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 1,
                    "delta": {"type": "input_json_delta", "partial_json": "42}"},
                }),
            ),
            frame(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 0}),
            ),
            frame(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 1}),
            ),
            frame(
                "message_delta",
                json!({
                    "type": "message_delta",
                    "delta": {"stop_reason": "tool_use"},
                    "usage": {"output_tokens": 8},
                }),
            ),
            frame("message_stop", json!({"type": "message_stop"})),
        ]
        .concat();

        assert_eq!(
            collect(&sse, 4),
            vec![
                LlmEvent::TextDelta {
                    text: "A".to_owned(),
                },
                LlmEvent::TextDelta {
                    text: "B".to_owned(),
                },
                LlmEvent::ToolCallRequested {
                    call: ToolCall::new("toolu_9", "echo", json!({"value": 42})),
                },
                LlmEvent::Done {
                    usage: Some(TokenUsage::new(5, 8)),
                },
            ]
        );
    }

    #[test]
    fn anthropic_parallel_tool_blocks_are_emitted_on_stop() {
        let sse = [
            frame(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {
                        "type": "tool_use", "id": "toolu_a", "name": "a", "input": {},
                    },
                }),
            ),
            frame(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": 1,
                    "content_block": {
                        "type": "tool_use", "id": "toolu_b", "name": "b", "input": {},
                    },
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 1,
                    "delta": {"type": "input_json_delta", "partial_json": "{\"b\":2}"},
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "input_json_delta", "partial_json": "{\"a\":1}"},
                }),
            ),
            frame(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 0}),
            ),
            frame(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 1}),
            ),
            frame("message_stop", json!({"type": "message_stop"})),
        ]
        .concat();

        assert_eq!(
            collect(&sse, 2),
            vec![
                LlmEvent::ToolCallRequested {
                    call: ToolCall::new("toolu_a", "a", json!({"a": 1})),
                },
                LlmEvent::ToolCallRequested {
                    call: ToolCall::new("toolu_b", "b", json!({"b": 2})),
                },
                LlmEvent::Done { usage: None },
            ]
        );
    }

    #[test]
    fn anthropic_error_event_becomes_llm_error_event() {
        let sse = fixture("anthropic_sse_error.txt");
        let events = collect(&sse, 1);

        assert_eq!(events.len(), 1);
        match &events[0] {
            LlmEvent::Error { message } => {
                assert!(message.contains("overloaded_error"));
                assert!(message.contains("Overloaded"));
            }
            other => panic!("expected an error event, got {other:?}"),
        }
    }

    #[test]
    fn anthropic_malformed_tool_arguments_become_llm_error_event() {
        let sse = [
            frame(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {
                        "type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {},
                    },
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "input_json_delta", "partial_json": "{\"path\":"},
                }),
            ),
            frame(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 0}),
            ),
            frame("message_stop", json!({"type": "message_stop"})),
        ]
        .concat();

        let events = collect(&sse, 4096);
        assert_eq!(events.len(), 1);
        match &events[0] {
            LlmEvent::Error { message } => {
                assert!(message.contains("invalid JSON arguments"));
            }
            other => panic!("expected an error event, got {other:?}"),
        }
    }

    #[test]
    fn anthropic_stream_without_message_stop_is_an_error() {
        let sse = frame(
            "message_start",
            json!({"type": "message_start", "message": {"usage": {}}}),
        );
        let events = collect(&sse, 4096);

        assert_eq!(events.len(), 1);
        match &events[0] {
            LlmEvent::Error { message } => {
                assert!(message.contains("message_stop"));
            }
            other => panic!("expected an error event, got {other:?}"),
        }
    }

    #[test]
    fn anthropic_ping_is_ignored() {
        let sse = [
            frame("ping", json!({"type": "ping"})),
            frame("message_stop", json!({"type": "message_stop"})),
        ]
        .concat();

        assert_eq!(collect(&sse, 4096), vec![LlmEvent::Done { usage: None }]);
    }

    #[test]
    fn classified_overloaded_error_is_a_retryable_server_failure() {
        let sse = fixture("anthropic_sse_error.txt");
        let items = collect_classified(&sse, 1);

        assert_eq!(items.len(), 1);
        match &items[0] {
            Err(failure) => {
                assert_eq!(
                    failure.kind(),
                    crate::reliability::ProviderFailureKind::Server
                );
                assert!(failure.is_retryable());
                assert!(failure.message().contains("overloaded_error"));
            }
            other => panic!("expected a server failure, got {other:?}"),
        }
    }

    #[test]
    fn classified_rate_limit_error_is_retryable() {
        let sse = frame(
            "error",
            json!({
                "type": "error",
                "error": {"type": "rate_limit_error", "message": "slow down"},
            }),
        );
        let items = collect_classified(&sse, 4096);

        assert_eq!(items.len(), 1);
        match &items[0] {
            Err(failure) => {
                assert_eq!(
                    failure.kind(),
                    crate::reliability::ProviderFailureKind::RateLimited
                );
                assert!(failure.is_retryable());
                assert!(failure.message().contains("slow down"));
            }
            other => panic!("expected a rate-limit failure, got {other:?}"),
        }
    }

    #[test]
    fn classified_authentication_error_is_non_retryable() {
        let sse = frame(
            "error",
            json!({
                "type": "error",
                "error": {"type": "authentication_error", "message": "bad key"},
            }),
        );
        let items = collect_classified(&sse, 4096);

        assert_eq!(items.len(), 1);
        match &items[0] {
            Err(failure) => {
                assert_eq!(
                    failure.kind(),
                    crate::reliability::ProviderFailureKind::Client
                );
                assert!(!failure.is_retryable());
            }
            other => panic!("expected a client failure, got {other:?}"),
        }
    }

    #[test]
    fn classified_missing_message_stop_is_stream_interrupted() {
        let sse = frame(
            "message_start",
            json!({"type": "message_start", "message": {"usage": {}}}),
        );
        let items = collect_classified(&sse, 4096);

        assert_eq!(items.len(), 1);
        match &items[0] {
            Err(failure) => {
                assert_eq!(
                    failure.kind(),
                    crate::reliability::ProviderFailureKind::StreamInterrupted
                );
                assert!(failure.is_retryable());
                assert!(failure.message().contains("message_stop"));
            }
            other => panic!("expected an interrupted-stream failure, got {other:?}"),
        }
    }

    #[test]
    fn classified_malformed_tool_arguments_is_invalid_response() {
        let sse = [
            frame(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {
                        "type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {},
                    },
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "input_json_delta", "partial_json": "{\"path\":"},
                }),
            ),
            frame(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 0}),
            ),
            frame("message_stop", json!({"type": "message_stop"})),
        ]
        .concat();
        let items = collect_classified(&sse, 4096);

        assert_eq!(items.len(), 1);
        match &items[0] {
            Err(failure) => {
                assert_eq!(
                    failure.kind(),
                    crate::reliability::ProviderFailureKind::InvalidResponse
                );
                assert!(!failure.is_retryable());
                assert!(failure.message().contains("invalid JSON arguments"));
            }
            other => panic!("expected an invalid-response failure, got {other:?}"),
        }
    }
}
