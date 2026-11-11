//! Anthropic provider configuration and environment loading.

use std::fmt;

use crate::config::ApiConfigError;

/// Default endpoint for the Anthropic Messages API.
pub const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";

/// Default model used when `ANTHROPIC_MODEL` is not set.
pub const DEFAULT_ANTHROPIC_MODEL: &str = "claude-sonnet-5-5";

/// Environment variable holding the API key.
pub(crate) const API_KEY_ENV: &str = "ANTHROPIC_API_KEY";

/// Environment variable overriding the base URL.
pub(crate) const BASE_URL_ENV: &str = "ANTHROPIC_BASE_URL";

/// Environment variable overriding the model.
pub(crate) const MODEL_ENV: &str = "ANTHROPIC_MODEL";

/// Maximum number of output tokens requested from the Messages API.
pub(crate) const DEFAULT_MAX_TOKENS: u32 = 8192;

/// Configuration for an Anthropic-compatible provider.
#[derive(Clone)]
pub struct AnthropicConfig {
    api_key: String,
    base_url: String,
    model: String,
}

impl AnthropicConfig {
    /// Creates and validates a provider configuration.
    ///
    /// `base_url` is normalized by trimming whitespace and trailing slashes.
    pub fn new(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self, ApiConfigError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(ApiConfigError::Empty { name: "api_key" });
        }

        let base_url = base_url.into().trim().trim_end_matches('/').to_owned();
        if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
            return Err(ApiConfigError::InvalidUrl { name: "base_url" });
        }

        let model = model.into().trim().to_owned();
        if model.is_empty() {
            return Err(ApiConfigError::Empty { name: "model" });
        }

        Ok(Self {
            api_key,
            base_url,
            model,
        })
    }

    /// Loads configuration from the process environment.
    ///
    /// `ANTHROPIC_API_KEY` is required; the base URL and model fall back to
    /// [`DEFAULT_ANTHROPIC_BASE_URL`] and [`DEFAULT_ANTHROPIC_MODEL`].
    pub fn from_env() -> Result<Self, ApiConfigError> {
        let api_key = std::env::var(API_KEY_ENV)
            .map_err(|_| ApiConfigError::MissingEnv { name: API_KEY_ENV })?;
        let base_url =
            non_empty_env(BASE_URL_ENV).unwrap_or_else(|| DEFAULT_ANTHROPIC_BASE_URL.to_owned());
        let model = non_empty_env(MODEL_ENV).unwrap_or_else(|| DEFAULT_ANTHROPIC_MODEL.to_owned());
        Self::new(api_key, base_url, model)
    }

    /// Returns the API key.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Returns the normalized base URL, without a trailing slash.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns the model name.
    pub fn model(&self) -> &str {
        &self.model
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

impl fmt::Debug for AnthropicConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnthropicConfig")
            .field("api_key", &"<redacted>")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_config_is_normalized() {
        let config =
            AnthropicConfig::new("test-key", "  https://example.com/v1///  ", " claude-test ")
                .expect("valid config should be accepted");
        assert_eq!(config.api_key(), "test-key");
        assert_eq!(config.base_url(), "https://example.com/v1");
        assert_eq!(config.model(), "claude-test");
    }

    #[test]
    fn anthropic_blank_values_and_relative_urls_are_rejected() {
        assert_eq!(
            AnthropicConfig::new("   ", "https://example.com", "model").unwrap_err(),
            ApiConfigError::Empty { name: "api_key" }
        );
        assert_eq!(
            AnthropicConfig::new("key", "https://example.com", "   ").unwrap_err(),
            ApiConfigError::Empty { name: "model" }
        );
        assert_eq!(
            AnthropicConfig::new("key", "example.com", "model").unwrap_err(),
            ApiConfigError::InvalidUrl { name: "base_url" }
        );
    }

    #[test]
    fn anthropic_debug_output_redacts_the_api_key() {
        let config = AnthropicConfig::new("anthropic-secret", "https://example.com", "model")
            .expect("valid config should be accepted");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("anthropic-secret"));
        assert!(rendered.contains("<redacted>"));
        assert!(rendered.contains("https://example.com"));
    }

    #[test]
    fn anthropic_missing_key_error_names_the_variable() {
        let error = ApiConfigError::MissingEnv { name: API_KEY_ENV };
        assert!(error.to_string().contains(API_KEY_ENV));
    }
}
