//! HTTP-backed OpenAI-compatible provider.

use async_trait::async_trait;
use jessica_core::llm::{LlmError, LlmProvider, LlmStream};
use jessica_core::tool::ToolSchema;
use jessica_core::types::Message;

use crate::config::{ApiConfigError, OpenAiConfig};
use crate::request::build_chat_request;
use crate::stream::event_stream;

/// Streaming provider for OpenAI-compatible chat-completions endpoints.
pub struct OpenAiProvider {
    config: OpenAiConfig,
    client: reqwest::Client,
}

impl OpenAiProvider {
    /// Creates a provider from explicit configuration.
    pub fn new(config: OpenAiConfig) -> Result<Self, LlmError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|error| LlmError::Request {
                message: format!("failed to build HTTP client: {error}"),
            })?;
        Ok(Self { config, client })
    }

    /// Creates a provider from environment configuration.
    ///
    /// See [`OpenAiConfig::from_env`] for the variables that are read.
    pub fn from_env() -> Result<Self, LlmError> {
        let config = OpenAiConfig::from_env().map_err(config_error)?;
        Self::new(config)
    }
}

fn config_error(error: ApiConfigError) -> LlmError {
    LlmError::Request {
        message: error.to_string(),
    }
}

fn completions_url(base_url: &str) -> String {
    format!("{base_url}/chat/completions")
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn stream_chat(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        let body = build_chat_request(self.config.model(), messages, tools)?;
        let url = completions_url(self.config.base_url());

        let response = self
            .client
            .post(url)
            .bearer_auth(self.config.api_key())
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
            return Err(LlmError::Request {
                message: format!("provider returned HTTP {status}: {detail}"),
            });
        }

        Ok(event_stream(response.bytes_stream()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_builds_from_valid_config() {
        let config = OpenAiConfig::new("sk-test", "http://localhost:1234/v1", "test-model")
            .expect("config should be valid");
        assert!(OpenAiProvider::new(config).is_ok());
    }

    #[test]
    fn completions_url_appends_the_chat_path() {
        assert_eq!(
            completions_url("http://localhost:1234/v1"),
            "http://localhost:1234/v1/chat/completions"
        );
    }

    #[test]
    fn config_errors_are_wrapped_as_request_errors() {
        let error = config_error(ApiConfigError::MissingEnv {
            name: "OPENAI_API_KEY",
        });
        assert!(matches!(error, LlmError::Request { .. }));
        assert!(error.to_string().contains("OPENAI_API_KEY"));
    }
}
