//! Backend-agnostic context guides and their memory-backed implementation.
//!
//! A [`Guide`] turns a session and query into an optional system message. The
//! harness owns this abstraction so downstream crates can supply retrieval
//! without coupling the harness to a storage backend. [`MemoryGuide`] adapts
//! the frozen [`MemoryStore`] contract and is fail-open: recall failures are
//! logged and skipped rather than failing the turn.

use std::sync::Arc;

use async_trait::async_trait;
use jessica_core::memory::MemoryStore;
use jessica_core::types::{Message, SessionId};
use thiserror::Error;

/// First line of every [`MemoryGuide`] message.
const MEMORY_GUIDE_TITLE: &str = "[记忆参考 — 以下内容来自历史会话，仅供参考，不代表当前事实]";

/// Separator placed between consecutive recalled entries.
const ENTRY_SEPARATOR: &str = "\n---\n";

/// Errors raised by a [`Guide`] that cannot render context.
#[derive(Debug, Error)]
pub enum GuideError {
    /// The guide failed to produce a context message.
    #[error("guide failed to render context: {message}")]
    Render {
        /// Human-readable failure description.
        message: String,
    },
}

/// Produces an optional system message from session context.
///
/// Implementations must be deterministic for a given session and query: they
/// must not read the clock, write memory, or call a language model. Returning
/// `Ok(None)` means no useful context is available and no message should be
/// injected. Errors are reserved for guides whose caller wants a hard failure;
/// [`MemoryGuide`] itself is fail-open.
#[async_trait]
pub trait Guide: Send + Sync {
    /// Renders context for `session` and `query`.
    async fn render(&self, session: &SessionId, query: &str)
    -> Result<Option<Message>, GuideError>;
}

/// A [`Guide`] that renders recalled memory as one system message.
///
/// The guide calls [`MemoryStore::recall`] with its configured `limit` and
/// renders the returned entries in the same order under a fixed title. Entries
/// are separated by a line containing `---`. The complete rendered content,
/// including the title and separators, is truncated to `max_inject_bytes` on a
/// UTF-8 character boundary.
///
/// Recall failures are fail-open: they are logged with `tracing::warn` and
/// produce `Ok(None)`. The guide never writes memory, calls a model, or reads
/// the host clock.
#[derive(Clone)]
pub struct MemoryGuide {
    /// Recall backend.
    store: Arc<dyn MemoryStore>,
    /// Maximum number of entries requested from the store.
    limit: usize,
    /// Maximum bytes in the final rendered system message.
    max_inject_bytes: usize,
}

impl MemoryGuide {
    /// Creates a guide backed by `store`.
    ///
    /// `limit` is passed to [`MemoryStore::recall`]. `max_inject_bytes` bounds
    /// the final message content, including the title and `---` separators. A
    /// zero byte budget cannot render any content and therefore yields
    /// `Ok(None)` for non-empty recall results.
    pub fn new(store: Arc<dyn MemoryStore>, limit: usize, max_inject_bytes: usize) -> Self {
        Self {
            store,
            limit,
            max_inject_bytes,
        }
    }
}

#[async_trait]
impl Guide for MemoryGuide {
    async fn render(
        &self,
        session: &SessionId,
        query: &str,
    ) -> Result<Option<Message>, GuideError> {
        let entries = match self.store.recall(session, query, self.limit).await {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "memory guide recall failed; skipping context injection"
                );
                return Ok(None);
            }
        };
        if entries.is_empty() {
            return Ok(None);
        }

        let mut content = String::from(MEMORY_GUIDE_TITLE);
        for (index, entry) in entries.iter().enumerate() {
            if index == 0 {
                content.push('\n');
            } else {
                content.push_str(ENTRY_SEPARATOR);
            }
            content.push_str(&entry.content);
        }

        let content = truncate_on_char_boundary(&content, self.max_inject_bytes);
        if content.is_empty() {
            return Ok(None);
        }
        Ok(Some(Message::system(content)))
    }
}

/// Returns the longest prefix of `content` of at most `max_bytes` bytes that
/// ends on a UTF-8 character boundary.
fn truncate_on_char_boundary(content: &str, max_bytes: usize) -> &str {
    if content.len() <= max_bytes {
        return content;
    }
    let mut end = max_bytes;
    while end > 0 && !content.is_char_boundary(end) {
        end -= 1;
    }
    &content[..end]
}
