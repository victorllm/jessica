//! Provider configuration and environment loading.

use std::fmt;

use thiserror::Error;

/// Default endpoint for the OpenAI API.
pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

/// Default model used when `OPENAI_MODEL` is not set.
pub const DEFAULT_MODEL: &str = "gpt-4o-mini";

/// Environment variable holding the API key.
pub const API_KEY_ENV: &str = "OPENAI_API_KEY";

/// Environment variable overriding the base URL.
pub const BASE_URL_ENV: &str = "OPENAI_BASE_URL";

/// Environment variable overriding the model.
pub const MODEL_ENV: &str = "OPENAI_MODEL";

/// Errors produced while building or loading provider configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ApiConfigError {
    /// A required environment variable is missing.
    #[error("environment variable `{name}` is not set")]
    MissingEnv {
        /// Name of the missing variable.
        name: &'static str,
    },
    /// A configuration value is empty.
    #[error("`{name}` must not be empty")]
    Empty {
        /// Name of the empty setting.
        name: &'static str,
    },
    /// The base URL is not an absolute HTTP(S) URL.
    #[error("`{name}` must start with http:// or https://")]
    InvalidUrl {
        /// Name of the invalid setting.
        name: &'static str,
    },
}

/// Configuration for an OpenAI-compatible provider.
#[derive(Clone)]
pub struct OpenAiConfig {
    api_key: String,
    base_url: String,
    model: String,
}

impl OpenAiConfig {
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
    /// `OPENAI_API_KEY` is required; the base URL and model fall back to
    /// [`DEFAULT_BASE_URL`] and [`DEFAULT_MODEL`].
    pub fn from_env() -> Result<Self, ApiConfigError> {
        let api_key = std::env::var(API_KEY_ENV)
            .map_err(|_| ApiConfigError::MissingEnv { name: API_KEY_ENV })?;
        let base_url = non_empty_env(BASE_URL_ENV).unwrap_or_else(|| DEFAULT_BASE_URL.to_owned());
        let model = non_empty_env(MODEL_ENV).unwrap_or_else(|| DEFAULT_MODEL.to_owned());
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

impl fmt::Debug for OpenAiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiConfig")
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
    fn valid_config_is_normalized() {
        let config = OpenAiConfig::new("sk-test", "  https://example.com/v1///  ", " gpt-test ")
            .expect("valid config should be accepted");
        assert_eq!(config.api_key(), "sk-test");
        assert_eq!(config.base_url(), "https://example.com/v1");
        assert_eq!(config.model(), "gpt-test");
    }

    #[test]
    fn blank_values_and_relative_urls_are_rejected() {
        assert_eq!(
            OpenAiConfig::new("   ", "https://example.com", "model").unwrap_err(),
            ApiConfigError::Empty { name: "api_key" }
        );
        assert_eq!(
            OpenAiConfig::new("key", "https://example.com", "   ").unwrap_err(),
            ApiConfigError::Empty { name: "model" }
        );
        assert_eq!(
            OpenAiConfig::new("key", "example.com/v1", "model").unwrap_err(),
            ApiConfigError::InvalidUrl { name: "base_url" }
        );
        assert_eq!(
            OpenAiConfig::new("key", "ftp://example.com", "model").unwrap_err(),
            ApiConfigError::InvalidUrl { name: "base_url" }
        );
    }

    #[test]
    fn debug_output_redacts_the_api_key() {
        let config = OpenAiConfig::new("sk-secret-key", "https://example.com", "model")
            .expect("valid config should be accepted");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("sk-secret-key"));
        assert!(rendered.contains("<redacted>"));
        assert!(rendered.contains("https://example.com"));
    }

    #[test]
    fn error_messages_name_the_problem() {
        let error = ApiConfigError::MissingEnv { name: API_KEY_ENV };
        assert!(error.to_string().contains(API_KEY_ENV));
    }
}
