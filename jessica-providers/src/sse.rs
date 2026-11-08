//! Incremental server-sent-events decoding.
//!
//! The decoder buffers raw bytes and yields the payload of every complete
//! `data:` line. Buffering at the byte level keeps multi-byte UTF-8 characters
//! intact even when a network chunk splits them.

use jessica_core::llm::LlmError;

/// Stateful decoder for SSE `data:` lines.
#[derive(Debug, Default)]
pub(crate) struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    /// Feeds a raw byte chunk and returns every complete `data:` payload.
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<Vec<String>, LlmError> {
        self.buffer.extend_from_slice(chunk);

        let mut payloads = Vec::new();
        let mut consumed = 0;
        while let Some(offset) = self.buffer[consumed..]
            .iter()
            .position(|byte| *byte == b'\n')
        {
            let line_end = consumed + offset;
            if let Some(payload) = parse_sse_line(&self.buffer[consumed..line_end])? {
                payloads.push(payload);
            }
            consumed = line_end + 1;
        }
        if consumed > 0 {
            self.buffer.drain(..consumed);
        }
        Ok(payloads)
    }

    /// Flushes a trailing line that arrived without a final newline.
    pub(crate) fn finish(&mut self) -> Result<Vec<String>, LlmError> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }
        let remainder = std::mem::take(&mut self.buffer);
        match parse_sse_line(&remainder)? {
            Some(payload) => Ok(vec![payload]),
            None => Ok(Vec::new()),
        }
    }
}

/// Parses one SSE line, returning its payload when it is a `data:` field.
fn parse_sse_line(line: &[u8]) -> Result<Option<String>, LlmError> {
    let line = std::str::from_utf8(line).map_err(|error| LlmError::Stream {
        message: format!("SSE line is not valid UTF-8: {error}"),
    })?;
    let line = line.strip_suffix('\r').unwrap_or(line);

    if line.is_empty() || line.starts_with(':') {
        return Ok(None);
    }

    let Some(payload) = line.strip_prefix("data:") else {
        return Ok(None);
    };
    Ok(Some(
        payload.strip_prefix(' ').unwrap_or(payload).to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_data_fields_and_ignores_other_lines() {
        assert_eq!(
            parse_sse_line(b"data: {\"a\":1}").unwrap(),
            Some("{\"a\":1}".to_owned())
        );
        assert_eq!(
            parse_sse_line(b"data:{\"a\":1}").unwrap(),
            Some("{\"a\":1}".to_owned())
        );
        assert_eq!(
            parse_sse_line(b"data: [DONE]\r").unwrap(),
            Some("[DONE]".to_owned())
        );
        assert_eq!(parse_sse_line(b": keep-alive").unwrap(), None);
        assert_eq!(parse_sse_line(b"event: message").unwrap(), None);
        assert_eq!(parse_sse_line(b"id: 42").unwrap(), None);
        assert_eq!(parse_sse_line(b"").unwrap(), None);
    }

    #[test]
    fn decodes_multiple_lines_from_one_chunk() {
        let mut decoder = SseDecoder::default();
        let payloads = decoder
            .push(b"data: one\n\ndata: two\n\n")
            .expect("valid UTF-8 should decode");
        assert_eq!(payloads, vec!["one".to_owned(), "two".to_owned()]);
        assert!(decoder.finish().unwrap().is_empty());
    }

    #[test]
    fn decodes_lines_split_across_chunks() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(b"da").unwrap().is_empty());
        assert!(decoder.push(b"ta: hel").unwrap().is_empty());
        let payloads = decoder.push(b"lo\r\n").unwrap();
        assert_eq!(payloads, vec!["hello".to_owned()]);
    }

    #[test]
    fn preserves_multibyte_characters_split_across_chunks() {
        let text = "data: 你好\n\n";
        let bytes = text.as_bytes();
        // Split in the middle of the UTF-8 encoding of the first character.
        let split = "data: ".len() + 1;
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(&bytes[..split]).unwrap().is_empty());
        let payloads = decoder.push(&bytes[split..]).unwrap();
        assert_eq!(payloads, vec!["你好".to_owned()]);
    }

    #[test]
    fn finish_flushes_a_trailing_line_without_newline() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(b"data: [DONE]").unwrap().is_empty());
        assert_eq!(decoder.finish().unwrap(), vec!["[DONE]".to_owned()]);
        assert!(decoder.finish().unwrap().is_empty());
    }

    #[test]
    fn invalid_utf8_is_reported() {
        let mut decoder = SseDecoder::default();
        let error = decoder
            .push(&[b'd', b'a', b't', b'a', b':', 0xff, b'\n'])
            .expect_err("invalid UTF-8 must fail");
        assert!(matches!(error, LlmError::Stream { .. }));
    }
}
