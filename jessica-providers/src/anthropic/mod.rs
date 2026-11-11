//! Anthropic Messages API streaming provider.
//!
//! [`AnthropicProvider`] adapts Anthropic's protocol to the provider-neutral
//! [`jessica_core::llm::LlmProvider`] contract. Three protocol differences are
//! absorbed inside this module and never leak into `jessica-core`:
//!
//! - system messages move to a top-level `system` field, and messages carry
//!   content blocks (`text`, `tool_use`, `tool_result`) instead of a string
//!   with separate tool-call fields;
//! - tool definitions use `{name, description, input_schema}`;
//! - streaming uses named SSE events aggregated by content-block index.
//!
//! Configuration comes from `ANTHROPIC_API_KEY` (required),
//! `ANTHROPIC_BASE_URL` (default [`DEFAULT_ANTHROPIC_BASE_URL`]), and
//! `ANTHROPIC_MODEL` (default [`DEFAULT_ANTHROPIC_MODEL`]).

mod config;
mod provider;
mod request;
mod sse;
mod stream;

pub use config::{AnthropicConfig, DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_ANTHROPIC_MODEL};
pub use provider::AnthropicProvider;
