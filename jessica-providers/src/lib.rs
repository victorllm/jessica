//! Streaming providers for OpenAI- and Anthropic-compatible endpoints.
//!
//! [`OpenAiProvider`] talks to any endpoint implementing the OpenAI
//! chat-completions streaming protocol. [`AnthropicProvider`] speaks the
//! Anthropic Messages API and absorbs its system-field, content-block, tool
//! schema, and named-SSE differences so both providers expose the same
//! [`jessica_core::llm::LlmProvider`] contract.
//!
//! Provider credentials and endpoints come from the environment:
//!
//! - OpenAI: `OPENAI_API_KEY` (required), `OPENAI_BASE_URL`, `OPENAI_MODEL`
//! - Anthropic: `ANTHROPIC_API_KEY` (required), `ANTHROPIC_BASE_URL`,
//!   `ANTHROPIC_MODEL`

mod anthropic;
mod config;
mod provider;
mod reliability;
mod request;
mod sse;
mod stream;

pub use anthropic::{
    AnthropicConfig, AnthropicProvider, DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_ANTHROPIC_MODEL,
};
pub use config::{ApiConfigError, DEFAULT_BASE_URL, DEFAULT_MODEL, OpenAiConfig};
pub use provider::OpenAiProvider;
pub use reliability::{
    ClassifiedProvider, ClassifiedStream, MAX_TIMEOUT_MS, ProviderFailure, ProviderFailureKind,
    ReliabilityConfig, ReliabilityConfigError, ReliableProvider,
};
