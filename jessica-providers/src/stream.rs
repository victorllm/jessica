//! Streaming chunk parsing and tool-call assembly.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use futures::StreamExt;
use futures::stream::Stream;
use jessica_core::llm::{LlmError, LlmEvent, LlmStream};
use jessica_core::types::{TokenUsage, ToolCall};
use serde::Deserialize;
use serde_json::Value;

use crate::sse::SseDecoder;

/// Turns raw byte chunks into a stream of [`LlmEvent`]s.
///
/// The chunk type and error type are generic so tests can drive the exact
/// parsing pipeline used in production with an in-memory SSE stream.
pub(crate) fn event_stream<S, B, E>(chunks: S) -> LlmStream
where
    S: Stream<Item = Result<B, E>> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    let chunks = Box::pin(chunks);
    let state = StreamState::default();
    Box::pin(futures::stream::unfold(
        (chunks, state),
        |(mut chunks, mut state)| async move {
            loop {
                if let Some(event) = state.pending.pop_front() {
                    return Some((event, (chunks, state)));
                }
                if state.done {
                    return None;
                }
                match chunks.next().await {
                    Some(Ok(chunk)) => state.push_bytes(chunk.as_ref()),
                    Some(Err(error)) => state.fail(error.to_string()),
                    None => state.finish_stream(),
                }
            }
        },
    ))
}

#[derive(Debug, Default)]
struct StreamState {
    decoder: SseDecoder,
    assembler: ToolCallAssembler,
    usage: Option<TokenUsage>,
    pending: VecDeque<LlmEvent>,
    done: bool,
}

impl StreamState {
    fn push_bytes(&mut self, bytes: &[u8]) {
        match self.decoder.push(bytes) {
            Ok(payloads) => {
                for payload in payloads {
                    self.process_payload(&payload);
                    if self.done {
                        return;
                    }
                }
            }
            Err(error) => self.fail(error.to_string()),
        }
    }

    fn process_payload(&mut self, payload: &str) {
        if self.done {
            return;
        }

        if payload.trim() == "[DONE]" {
            self.finish_successfully();
            return;
        }

        let chunk: ChatChunk = match serde_json::from_str(payload) {
            Ok(chunk) => chunk,
            Err(error) => {
                self.fail(format!("invalid SSE payload: {error}"));
                return;
            }
        };

        if let Some(usage) = chunk.usage {
            self.usage = Some(TokenUsage::new(
                usage.prompt_tokens,
                usage.completion_tokens,
            ));
        }

        for choice in chunk.choices {
            if let Some(content) = choice.delta.content
                && !content.is_empty()
            {
                self.pending
                    .push_back(LlmEvent::TextDelta { text: content });
            }

            if !choice.delta.tool_calls.is_empty() {
                self.assembler.apply(&choice.delta.tool_calls);
            }

            if choice.finish_reason.as_deref() == Some("tool_calls") {
                self.emit_tool_calls();
                if self.done {
                    return;
                }
            }
        }
    }

    fn emit_tool_calls(&mut self) {
        match self.assembler.finish() {
            Ok(calls) => {
                for call in calls {
                    self.pending.push_back(LlmEvent::ToolCallRequested { call });
                }
            }
            Err(error) => self.fail(error.to_string()),
        }
    }

    fn finish_successfully(&mut self) {
        if self.done {
            return;
        }
        self.emit_tool_calls();
        if self.done {
            return;
        }
        self.pending.push_back(LlmEvent::Done {
            usage: self.usage.take(),
        });
        self.done = true;
    }

    fn finish_stream(&mut self) {
        if self.done {
            return;
        }
        match self.decoder.finish() {
            Ok(payloads) => {
                for payload in payloads {
                    self.process_payload(&payload);
                    if self.done {
                        return;
                    }
                }
            }
            Err(error) => {
                self.fail(error.to_string());
                return;
            }
        }
        self.finish_successfully();
    }

    fn fail(&mut self, message: String) {
        if self.done {
            return;
        }
        self.pending.push_back(LlmEvent::Error { message });
        self.done = true;
    }
}

#[derive(Debug, Default)]
struct ToolCallAssembler {
    calls: BTreeMap<usize, PartialToolCall>,
}

#[derive(Debug, Default)]
struct PartialToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

impl ToolCallAssembler {
    fn apply(&mut self, deltas: &[ToolCallDelta]) {
        for delta in deltas {
            let partial = self.calls.entry(delta.index).or_default();
            if let Some(id) = &delta.id {
                partial.id = Some(id.clone());
            }
            if let Some(function) = &delta.function {
                if let Some(name) = &function.name {
                    partial.name = Some(name.clone());
                }
                if let Some(arguments) = &function.arguments {
                    partial.arguments.push_str(arguments);
                }
            }
        }
    }

    fn finish(&mut self) -> Result<Vec<ToolCall>, LlmError> {
        let calls = std::mem::take(&mut self.calls);
        calls
            .into_values()
            .map(|partial| {
                let id = partial.id.ok_or_else(|| LlmError::InvalidResponse {
                    message: "tool call is missing an id".to_owned(),
                })?;
                let name = partial.name.ok_or_else(|| LlmError::InvalidResponse {
                    message: format!("tool call `{id}` is missing a function name"),
                })?;
                let arguments = if partial.arguments.trim().is_empty() {
                    Value::Object(serde_json::Map::new())
                } else {
                    serde_json::from_str(&partial.arguments).map_err(|error| {
                        LlmError::InvalidResponse {
                            message: format!(
                                "tool call `{id}` returned invalid JSON arguments: {error}"
                            ),
                        }
                    })?
                };
                Ok(ToolCall::new(id, name, arguments))
            })
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct ChatChunk {
    #[serde(default)]
    choices: Vec<ChunkChoice>,
    #[serde(default)]
    usage: Option<ApiUsage>,
}

#[derive(Debug, Deserialize)]
struct ChunkChoice {
    #[serde(default)]
    delta: ChunkDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ChunkDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ToolCallDelta>,
}

#[derive(Debug, Deserialize)]
struct ToolCallDelta {
    #[serde(default)]
    index: usize,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<FunctionDelta>,
}

#[derive(Debug, Deserialize)]
struct FunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiUsage {
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chunk(delta: Value, finish_reason: Option<&str>) -> String {
        let body = json!({
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }]
        });
        format!("data: {body}\n\n")
    }

    fn usage_chunk(prompt_tokens: u64, completion_tokens: u64) -> String {
        let body = json!({
            "choices": [],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
            }
        });
        format!("data: {body}\n\n")
    }

    /// Drives the production event pipeline with an in-memory SSE stream,
    /// splitting the payload every `chunk_size` bytes to exercise buffering.
    fn collect(sse: &str, chunk_size: usize) -> Vec<LlmEvent> {
        let chunks: Vec<Vec<u8>> = sse
            .as_bytes()
            .chunks(chunk_size)
            .map(|bytes| bytes.to_vec())
            .collect();
        let stream = event_stream(futures::stream::iter(
            chunks.into_iter().map(Ok::<_, String>),
        ));
        futures::executor::block_on(stream.collect())
    }

    #[test]
    fn text_deltas_are_mapped_and_done_is_emitted() {
        let sse = format!(
            "{}{}{}",
            chunk(json!({"content": "Hel"}), None),
            chunk(json!({"content": "lo"}), None),
            "data: [DONE]\n\n",
        );

        let events = collect(&sse, 5);

        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "Hel".to_owned()
                },
                LlmEvent::TextDelta {
                    text: "lo".to_owned()
                },
                LlmEvent::Done { usage: None },
            ]
        );
    }

    #[test]
    fn tool_call_arguments_are_assembled_across_deltas() {
        let sse = [
            chunk(
                json!({"tool_calls": [{"index": 0, "id": "call_1", "function": {"name": "read_file", "arguments": ""}}]}),
                None,
            ),
            chunk(
                json!({"tool_calls": [{"index": 0, "function": {"arguments": "{\"pa"}}]}),
                None,
            ),
            chunk(
                json!({"tool_calls": [{"index": 0, "function": {"arguments": "th\":\"Cargo.toml\"}"}}]}),
                None,
            ),
            chunk(json!({}), Some("tool_calls")),
            "data: [DONE]\n\n".to_owned(),
        ]
        .concat();

        let events = collect(&sse, 7);

        assert_eq!(
            events,
            vec![
                LlmEvent::ToolCallRequested {
                    call: ToolCall::new("call_1", "read_file", json!({"path": "Cargo.toml"})),
                },
                LlmEvent::Done { usage: None },
            ]
        );
    }

    #[test]
    fn multiple_tool_calls_are_emitted_in_index_order() {
        let sse = [
            chunk(
                json!({"tool_calls": [{"index": 1, "id": "call_b", "function": {"name": "b", "arguments": "{}"}}]}),
                None,
            ),
            chunk(
                json!({"tool_calls": [{"index": 0, "id": "call_a", "function": {"name": "a", "arguments": "{}"}}]}),
                None,
            ),
            chunk(json!({}), Some("tool_calls")),
            "data: [DONE]\n\n".to_owned(),
        ]
        .concat();

        let events = collect(&sse, 4096);

        assert_eq!(
            events,
            vec![
                LlmEvent::ToolCallRequested {
                    call: ToolCall::new("call_a", "a", json!({})),
                },
                LlmEvent::ToolCallRequested {
                    call: ToolCall::new("call_b", "b", json!({})),
                },
                LlmEvent::Done { usage: None },
            ]
        );
    }

    #[test]
    fn usage_is_reported_in_done() {
        let sse = format!("{}{}", usage_chunk(12, 3), "data: [DONE]\n\n");

        let events = collect(&sse, 4096);

        assert_eq!(
            events,
            vec![LlmEvent::Done {
                usage: Some(TokenUsage::new(12, 3)),
            }]
        );
    }

    #[test]
    fn invalid_payload_produces_an_error_event() {
        let events = collect("data: {not-json}\n\n", 4096);

        assert_eq!(events.len(), 1);
        match &events[0] {
            LlmEvent::Error { message } => assert!(message.contains("invalid SSE payload")),
            other => panic!("expected an error event, got {other:?}"),
        }
    }

    #[test]
    fn tool_call_without_id_produces_an_error_event() {
        let sse = [
            chunk(
                json!({"tool_calls": [{"index": 0, "function": {"name": "x", "arguments": "{}"}}]}),
                Some("tool_calls"),
            ),
            "data: [DONE]\n\n".to_owned(),
        ]
        .concat();

        let events = collect(&sse, 4096);

        assert_eq!(events.len(), 1);
        match &events[0] {
            LlmEvent::Error { message } => assert!(message.contains("missing an id")),
            other => panic!("expected an error event, got {other:?}"),
        }
    }

    #[test]
    fn tool_call_is_emitted_when_the_stream_ends_without_done() {
        let sse = chunk(
            json!({"tool_calls": [{"index": 0, "id": "call_1", "function": {"name": "bash", "arguments": "{}"}}]}),
            None,
        );

        let events = collect(&sse, 4096);

        assert_eq!(
            events,
            vec![
                LlmEvent::ToolCallRequested {
                    call: ToolCall::new("call_1", "bash", json!({})),
                },
                LlmEvent::Done { usage: None },
            ]
        );
    }

    #[test]
    fn stream_without_done_sentinel_still_finishes() {
        let sse = chunk(json!({"content": "hi"}), None);

        let events = collect(&sse, 4096);

        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "hi".to_owned()
                },
                LlmEvent::Done { usage: None },
            ]
        );
    }

    #[test]
    fn transport_errors_become_error_events() {
        let chunks = vec![Err::<Vec<u8>, String>("connection reset".to_owned())];
        let stream = event_stream(futures::stream::iter(chunks));
        let events = futures::executor::block_on(stream.collect::<Vec<_>>());

        assert_eq!(events.len(), 1);
        match &events[0] {
            LlmEvent::Error { message } => assert!(message.contains("connection reset")),
            other => panic!("expected an error event, got {other:?}"),
        }
    }
}
