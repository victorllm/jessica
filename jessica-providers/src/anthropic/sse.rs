//! Anthropic named-event SSE decoding.
//!
//! Anthropic frames every payload with both an `event:` line and a `data:`
//! line, and terminates a frame with a blank line. The decoder buffers raw
//! bytes so multi-byte UTF-8 characters survive arbitrary network splits, then
//! parses each complete frame into an [`AnthropicSseEvent`].

use jessica_core::llm::LlmError;
use serde::Deserialize;
use serde_json::Value;

/// One parsed SSE field line.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AnthropicSseLine {
    /// An `event:` field naming the payload type.
    Event(String),
    /// A `data:` field carrying JSON.
    Data(String),
    /// A comment or unrelated field.
    Ignored,
}

/// Parses one SSE line without interpreting its payload.
pub(crate) fn parse_anthropic_sse_line(line: &[u8]) -> Result<AnthropicSseLine, LlmError> {
    let line = std::str::from_utf8(line).map_err(|error| LlmError::Stream {
        message: format!("Anthropic SSE line is not valid UTF-8: {error}"),
    })?;
    let line = line.strip_suffix('\r').unwrap_or(line);

    if line.is_empty() || line.starts_with(':') {
        return Ok(AnthropicSseLine::Ignored);
    }

    if let Some(name) = line.strip_prefix("event:") {
        return Ok(AnthropicSseLine::Event(
            name.strip_prefix(' ').unwrap_or(name).to_owned(),
        ));
    }
    if let Some(data) = line.strip_prefix("data:") {
        return Ok(AnthropicSseLine::Data(
            data.strip_prefix(' ').unwrap_or(data).to_owned(),
        ));
    }

    Ok(AnthropicSseLine::Ignored)
}

/// Incremental decoder for Anthropic SSE frames.
#[derive(Debug, Default)]
pub(crate) struct AnthropicSseDecoder {
    buffer: Vec<u8>,
    event: Option<String>,
    data: Vec<String>,
}

impl AnthropicSseDecoder {
    /// Feeds a raw byte chunk and returns every complete SSE event.
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<Vec<AnthropicSseEvent>, LlmError> {
        self.buffer.extend_from_slice(chunk);

        let mut events = Vec::new();
        let mut consumed = 0;
        while let Some(offset) = self.buffer[consumed..]
            .iter()
            .position(|byte| *byte == b'\n')
        {
            let line_end = consumed + offset;
            let line = self.buffer[consumed..line_end].to_vec();
            self.process_line(&line, &mut events)?;
            consumed = line_end + 1;
        }
        if consumed > 0 {
            self.buffer.drain(..consumed);
        }
        Ok(events)
    }

    /// Flushes a trailing frame that arrived without a final newline.
    pub(crate) fn finish(&mut self) -> Result<Vec<AnthropicSseEvent>, LlmError> {
        let mut events = Vec::new();
        if !self.buffer.is_empty() {
            let remainder = std::mem::take(&mut self.buffer);
            self.process_line(&remainder, &mut events)?;
        }
        self.finish_event(&mut events)?;
        Ok(events)
    }

    /// Consumes one line, closing the frame on a blank line.
    fn process_line(
        &mut self,
        line: &[u8],
        events: &mut Vec<AnthropicSseEvent>,
    ) -> Result<(), LlmError> {
        if line.is_empty() || line == b"\r" {
            return self.finish_event(events);
        }
        match parse_anthropic_sse_line(line)? {
            AnthropicSseLine::Ignored => Ok(()),
            AnthropicSseLine::Event(name) => {
                self.event = Some(name);
                Ok(())
            }
            AnthropicSseLine::Data(data) => {
                self.data.push(data);
                Ok(())
            }
        }
    }

    /// Parses the accumulated event/data fields into a typed event.
    fn finish_event(&mut self, events: &mut Vec<AnthropicSseEvent>) -> Result<(), LlmError> {
        if self.data.is_empty() {
            self.event = None;
            return Ok(());
        }

        let payload = self.data.join("\n");
        self.data.clear();
        let declared = self.event.take();
        let event: AnthropicSseEvent =
            serde_json::from_str(&payload).map_err(|error| LlmError::Stream {
                message: format!("invalid Anthropic SSE payload: {error}"),
            })?;

        if let Some(declared) = declared
            && declared != event.event_name()
        {
            return Err(LlmError::Stream {
                message: format!(
                    "SSE event name `{declared}` does not match payload type `{}`",
                    event.event_name()
                ),
            });
        }

        events.push(event);
        Ok(())
    }
}

/// Strongly typed Anthropic streaming event.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum AnthropicSseEvent {
    /// Start of a message; carries the initial usage counters.
    MessageStart {
        /// Message metadata.
        message: MessageStart,
    },
    /// Start of a content block at `index`.
    ContentBlockStart {
        /// Zero-based content block index.
        index: usize,
        /// Initial block contents.
        content_block: ContentBlock,
    },
    /// Incremental update for the content block at `index`.
    ContentBlockDelta {
        /// Zero-based content block index.
        index: usize,
        /// Incremental block payload.
        delta: ContentBlockDelta,
    },
    /// End of the content block at `index`.
    ContentBlockStop {
        /// Zero-based content block index.
        index: usize,
    },
    /// Final message metadata and usage.
    MessageDelta {
        /// Updated usage counters.
        #[serde(default)]
        usage: Option<AnthropicUsage>,
    },
    /// Successful end of the message.
    MessageStop,
    /// Keep-alive event.
    Ping,
    /// Provider-reported streaming error.
    Error {
        /// Error details.
        error: AnthropicErrorBody,
    },
}

impl AnthropicSseEvent {
    /// Returns the SSE event name corresponding to this payload.
    pub(crate) fn event_name(&self) -> &'static str {
        match self {
            Self::MessageStart { .. } => "message_start",
            Self::ContentBlockStart { .. } => "content_block_start",
            Self::ContentBlockDelta { .. } => "content_block_delta",
            Self::ContentBlockStop { .. } => "content_block_stop",
            Self::MessageDelta { .. } => "message_delta",
            Self::MessageStop => "message_stop",
            Self::Ping => "ping",
            Self::Error { .. } => "error",
        }
    }
}

/// Message metadata carried by `message_start`.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
pub(crate) struct MessageStart {
    /// Initial token usage.
    #[serde(default)]
    pub(crate) usage: AnthropicUsage,
}

/// Start of a content block.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ContentBlock {
    /// Text block.
    Text {
        /// Initial text, normally empty.
        #[serde(default)]
        text: String,
    },
    /// Tool invocation block.
    ToolUse {
        /// Provider-assigned tool call id.
        id: String,
        /// Tool name.
        name: String,
        /// Initial arguments, normally an empty object.
        #[serde(default = "empty_object")]
        input: Value,
    },
    /// A block type outside this provider's scope, such as thinking or image.
    #[serde(other)]
    Other,
}

/// Incremental content block payload.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ContentBlockDelta {
    /// Text fragment.
    TextDelta {
        /// Text fragment.
        #[serde(default)]
        text: String,
    },
    /// Fragment of the tool arguments JSON.
    InputJsonDelta {
        /// JSON fragment.
        #[serde(default)]
        partial_json: String,
    },
    /// A delta type outside this provider's scope.
    #[serde(other)]
    Other,
}

/// Token counters reported by Anthropic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
pub(crate) struct AnthropicUsage {
    /// Input tokens, reported on `message_start`.
    #[serde(default)]
    pub(crate) input_tokens: Option<u64>,
    /// Output tokens, reported on `message_delta`.
    #[serde(default)]
    pub(crate) output_tokens: Option<u64>,
}

/// Error body carried by an `error` event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct AnthropicErrorBody {
    /// Provider error category, such as `overloaded_error`.
    #[serde(rename = "type", default)]
    pub(crate) error_type: String,
    /// Human-readable error message.
    #[serde(default)]
    pub(crate) message: String,
}

/// Default for a `tool_use` block whose `input` is omitted.
fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn fixture(name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
    }

    #[test]
    fn anthropic_sse_lines_are_classified() {
        assert_eq!(
            parse_anthropic_sse_line(b"event: content_block_delta").unwrap(),
            AnthropicSseLine::Event("content_block_delta".to_owned())
        );
        assert_eq!(
            parse_anthropic_sse_line(b"data:{\"type\":\"ping\"}").unwrap(),
            AnthropicSseLine::Data("{\"type\":\"ping\"}".to_owned())
        );
        assert_eq!(
            parse_anthropic_sse_line(b"data: {\"type\":\"ping\"}\r").unwrap(),
            AnthropicSseLine::Data("{\"type\":\"ping\"}".to_owned())
        );
        assert_eq!(
            parse_anthropic_sse_line(b": keep-alive").unwrap(),
            AnthropicSseLine::Ignored
        );
        assert_eq!(
            parse_anthropic_sse_line(b"id: 42").unwrap(),
            AnthropicSseLine::Ignored
        );
        assert_eq!(
            parse_anthropic_sse_line(b"").unwrap(),
            AnthropicSseLine::Ignored
        );
    }

    #[test]
    fn anthropic_decoder_reads_a_fixture_of_named_events() {
        let sse = fixture("anthropic_sse_text_and_tool.txt");
        let mut decoder = AnthropicSseDecoder::default();
        let events = decoder.push(sse.as_bytes()).expect("fixture should decode");
        assert!(decoder.finish().expect("nothing should remain").is_empty());

        assert!(matches!(
            events.first(),
            Some(AnthropicSseEvent::MessageStart { .. })
        ));
        assert!(matches!(
            events.last(),
            Some(AnthropicSseEvent::MessageStop)
        ));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, AnthropicSseEvent::Ping))
                .count(),
            0
        );
    }

    #[test]
    fn anthropic_decoder_preserves_multibyte_characters_across_byte_splits() {
        let sse = concat!(
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,",
            "\"delta\":{\"type\":\"text_delta\",\"text\":\"你好\"}}\n",
            "\n",
        );

        let mut decoder = AnthropicSseDecoder::default();
        let mut events = Vec::new();
        for byte in sse.as_bytes() {
            events.extend(decoder.push(&[*byte]).expect("byte split should decode"));
        }
        events.extend(decoder.finish().expect("trailing frame should decode"));

        let text = events
            .iter()
            .find_map(|event| match event {
                AnthropicSseEvent::ContentBlockDelta {
                    delta: ContentBlockDelta::TextDelta { text },
                    ..
                } => Some(text.as_str()),
                _ => None,
            })
            .expect("text delta should be present");
        assert_eq!(text, "你好");
    }

    #[test]
    fn anthropic_invalid_payload_is_a_stream_error() {
        let mut decoder = AnthropicSseDecoder::default();
        let error = decoder
            .push(b"event: content_block_delta\ndata: {not-json}\n\n")
            .expect_err("invalid payload must fail");
        assert!(matches!(error, LlmError::Stream { .. }));
        assert!(error.to_string().contains("invalid Anthropic SSE payload"));
    }

    #[test]
    fn anthropic_mismatched_event_name_is_a_stream_error() {
        let mut decoder = AnthropicSseDecoder::default();
        let error = decoder
            .push(b"event: message_stop\ndata: {\"type\":\"ping\"}\n\n")
            .expect_err("mismatched names must fail");
        assert!(matches!(error, LlmError::Stream { .. }));
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn anthropic_error_event_is_typed() {
        let sse = fixture("anthropic_sse_error.txt");
        let mut decoder = AnthropicSseDecoder::default();
        let events = decoder
            .push(sse.as_bytes())
            .expect("error fixture should decode");
        assert!(decoder.finish().expect("nothing should remain").is_empty());

        match events.as_slice() {
            [AnthropicSseEvent::Error { error }] => {
                assert_eq!(error.error_type, "overloaded_error");
                assert!(error.message.contains("Overloaded"));
            }
            other => panic!("expected one error event, got {other:?}"),
        }
    }
}
