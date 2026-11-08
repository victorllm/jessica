//! OpenAI-compatible streaming provider.
//!
//! [`OpenAiProvider`] talks to any endpoint implementing the OpenAI
//! chat-completions streaming protocol. Credentials and endpoint selection come
//! from the environment:
//!
//! - `OPENAI_API_KEY` (required)
//! - `OPENAI_BASE_URL` (optional, defaults to [`DEFAULT_BASE_URL`])
//! - `OPENAI_MODEL` (optional, defaults to [`DEFAULT_MODEL`])

mod config;
mod provider;
mod request;
mod sse;
mod stream;

pub use config::{ApiConfigError, DEFAULT_BASE_URL, DEFAULT_MODEL, OpenAiConfig};
pub use provider::OpenAiProvider;
