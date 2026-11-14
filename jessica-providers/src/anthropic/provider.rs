//! HTTP-backed Anthropic Messages API provider.

use async_trait::async_trait;
use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
use jessica_core::tool::ToolSchema;
use jessica_core::types::Message;

use crate::anthropic::config::AnthropicConfig;
use crate::anthropic::request::build_anthropic_request;
use crate::anthropic::stream::{anthropic_event_stream, classified_anthropic_event_stream};
use crate::config::ApiConfigError;
use crate::reliability::{
    ClassifiedProvider, ClassifiedStream, ProviderFailure, classify_http_status,
    classify_reqwest_error, local_failure, parse_retry_after,
};

/// Version header required by the Anthropic Messages API.
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Streaming provider for Anthropic-compatible Messages endpoints.
pub struct AnthropicProvider {
    config: AnthropicConfig,
    client: reqwest::Client,
}

impl AnthropicProvider {
    /// Creates a provider from explicit configuration.
    pub fn new(config: AnthropicConfig) -> Result<Self, LlmError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|error| LlmError::Request {
                message: format!("failed to build HTTP client: {error}"),
            })?;
        Ok(Self { config, client })
    }

    /// Creates a provider from environment configuration.
    ///
    /// See [`AnthropicConfig::from_env`] for the variables that are read.
    pub fn from_env() -> Result<Self, LlmError> {
        let config = AnthropicConfig::from_env().map_err(config_error)?;
        Self::new(config)
    }
}

fn config_error(error: ApiConfigError) -> LlmError {
    LlmError::Request {
        message: error.to_string(),
    }
}

/// Builds the Messages endpoint from a configurable base URL.
///
/// The default base URL has no version suffix, while third-party gateways often
/// already include `/v1`. Both forms resolve to `.../v1/messages`.
fn messages_url(base_url: &str) -> String {
    if base_url.ends_with("/v1/messages") {
        base_url.to_owned()
    } else if base_url.ends_with("/v1") {
        format!("{base_url}/messages")
    } else {
        format!("{base_url}/v1/messages")
    }
}

/// Wraps a provider failure in a single error event.
fn error_stream(message: String) -> LlmStream {
    Box::pin(futures::stream::iter(vec![LlmEvent::Error { message }]))
}

#[async_trait]
impl LlmProvider for AnthropicProvider {
    async fn stream_chat(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        let body = build_anthropic_request(self.config.model(), messages, tools)?;
        let url = messages_url(self.config.base_url());

        let response = self
            .client
            .post(url)
            .header("x-api-key", self.config.api_key())
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|error| LlmError::Request {
                message: format!("failed to send request: {error}"),
            })?;

        let status = response.status();
        if !status.is_success() {
            let detail = match response.text().await {
                Ok(text) => text,
                Err(error) => format!("<failed to read error body: {error}>"),
            };
            return Ok(error_stream(format!(
                "Anthropic API returned HTTP {status}: {detail}"
            )));
        }

        Ok(anthropic_event_stream(response.bytes_stream()))
    }
}

#[async_trait]
impl ClassifiedProvider for AnthropicProvider {
    async fn stream_chat_classified(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<ClassifiedStream, ProviderFailure> {
        let body =
            build_anthropic_request(self.config.model(), messages, tools).map_err(local_failure)?;
        let url = messages_url(self.config.base_url());

        let response = self
            .client
            .post(url)
            .header("x-api-key", self.config.api_key())
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|error| classify_reqwest_error(&error))?;

        let status = response.status();
        if !status.is_success() {
            let retry_after = parse_retry_after(response.headers());
            let detail = match response.text().await {
                Ok(text) => text,
                Err(error) => format!("<failed to read error body: {error}>"),
            };
            return Err(classify_http_status(
                status.as_u16(),
                format!("Anthropic API returned HTTP {status}: {detail}"),
                retry_after,
            ));
        }

        Ok(classified_anthropic_event_stream(response.bytes_stream()))
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    use futures::StreamExt;
    use jessica_core::types::{TokenUsage, ToolCall};
    use serde_json::json;

    use super::*;

    const FIXTURE: &str = include_str!("../../tests/fixtures/anthropic_sse_text_and_tool.txt");

    /// Serves one HTTP response over loopback and returns the base URL.
    fn serve_once(
        status: &'static str,
        content_type: &'static str,
        body: String,
    ) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener.local_addr().expect("address should be known");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("client should connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("timeout should be set");

            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                match stream.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => request.extend_from_slice(&buffer[..read]),
                    Err(error) => panic!("failed to read request: {error}"),
                }
            }

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("headers should be written");
            stream
                .write_all(body.as_bytes())
                .expect("body should be written");
            stream.flush().expect("response should flush");
        });
        (format!("http://{address}"), handle)
    }

    fn provider(base_url: String) -> AnthropicProvider {
        let config = AnthropicConfig::new("test-key", base_url, "claude-test")
            .expect("config should be valid");
        AnthropicProvider::new(config).expect("provider should be constructed")
    }

    #[test]
    fn anthropic_provider_builds_from_valid_config() {
        let config = AnthropicConfig::new("test-key", "http://localhost:1234", "claude-test")
            .expect("config should be valid");
        assert!(AnthropicProvider::new(config).is_ok());
    }

    #[test]
    fn anthropic_messages_url_handles_base_url_variants() {
        assert_eq!(
            messages_url("https://api.anthropic.com"),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(
            messages_url("https://gateway.example/v1"),
            "https://gateway.example/v1/messages"
        );
        assert_eq!(
            messages_url("https://gateway.example/v1/messages"),
            "https://gateway.example/v1/messages"
        );
        assert_eq!(
            messages_url("https://gateway.example/proxy"),
            "https://gateway.example/proxy/v1/messages"
        );
    }

    #[test]
    fn anthropic_config_errors_are_wrapped_as_request_errors() {
        let error = config_error(ApiConfigError::MissingEnv {
            name: "ANTHROPIC_API_KEY",
        });
        assert!(matches!(error, LlmError::Request { .. }));
        assert!(error.to_string().contains("ANTHROPIC_API_KEY"));
    }

    #[tokio::test]
    async fn anthropic_provider_streams_a_mock_sse_response() {
        let (base_url, server) = serve_once("200 OK", "text/event-stream", FIXTURE.to_owned());
        let events = provider(base_url)
            .stream_chat(&[Message::user("read the file")], &[])
            .await
            .expect("request should start")
            .collect::<Vec<_>>()
            .await;
        server.join().expect("server should finish");

        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "Let me ".to_owned(),
                },
                LlmEvent::TextDelta {
                    text: "check.".to_owned(),
                },
                LlmEvent::ToolCallRequested {
                    call: ToolCall::new("toolu_1", "read_file", json!({"path": "文档.txt"}),),
                },
                LlmEvent::Done {
                    usage: Some(TokenUsage::new(12, 25)),
                },
            ]
        );
    }

    #[tokio::test]
    async fn anthropic_http_error_becomes_an_llm_error_event() {
        let (base_url, server) = serve_once(
            "500 Internal Server Error",
            "application/json",
            "server exploded".to_owned(),
        );
        let events = provider(base_url)
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("HTTP failures must surface as a streamed error event")
            .collect::<Vec<_>>()
            .await;
        server.join().expect("server should finish");

        assert_eq!(events.len(), 1);
        match &events[0] {
            LlmEvent::Error { message } => {
                assert!(message.contains("HTTP 500"));
                assert!(message.contains("server exploded"));
            }
            other => panic!("expected an error event, got {other:?}"),
        }
    }
}
