//! Deterministic retrieval and injection of reusable skills.
//!
//! [`SkillSource`] is the backend-agnostic retrieval contract; concrete
//! implementations (the SQLite skill library is wired by the CLI) must return
//! only active, non-deleted skills in relevance order. [`SkillGuide`] adapts
//! that contract to the existing [`Guide`] trait and renders exactly one
//! bounded system message.
//!
//! # Retrieval contract
//!
//! Implementations must:
//!
//! - return only `status=active` skills whose `deleted_at` is `NULL`;
//! - return skills ordered by raw bm25 ascending (smaller is more relevant)
//!   and preserve that order without re-ranking by a model;
//! - return an empty vector when `limit == 0`;
//! - treat `min_score == None` and `min_score == Some(0.0)` as disabled;
//!   a positive value is a minimum normalized relevance score where larger is
//!   more relevant (consistent with bm25's direction). Implementations must
//!   not interpret `0.0` as a literal threshold. The LIKE fallback used when
//!   bm25 is unavailable returns its fixed sentinel `0.0`, so a positive
//!   threshold excludes those rows while `None`/`Some(0.0)` include them.
//!
//! # Side-effect boundary
//!
//! The guide never writes storage, never calls a language model to judge
//! relevance, and never reads the host clock. Recall failures are fail-open:
//! they are logged at `warn` and render as `Ok(None)` so retrieval cannot
//! change the turn's outcome. Skills are workspace-level; the session
//! argument required by [`Guide`] is accepted but not used for filtering.

use std::sync::Arc;

use async_trait::async_trait;
use jessica_core::types::{Message, SessionId};
use thiserror::Error;

use crate::guide::{Guide, GuideError};

/// Fixed first line of every injected skill message.
const SKILL_GUIDE_TITLE: &str = "[技能参考 — 以下内容由历史任务自动提炼，仅供参考，不代表当前事实]";

/// Separator placed between consecutive skills.
const SKILL_SEPARATOR: &str = "\n---\n";

/// Default maximum byte length of the injected skill message.
pub const DEFAULT_SKILL_INJECT_BYTES: usize = 2048;

/// One reusable skill returned by a [`SkillSource`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillContent {
    /// Short reusable title.
    pub name: String,
    /// One-sentence summary.
    pub description: String,
    /// Reusable step-by-step guidance.
    pub instructions: String,
}

/// Errors raised by a [`SkillSource`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SkillSourceError {
    /// The retrieval backend failed.
    #[error("skill source failed: {message}")]
    Backend {
        /// Human-readable failure description.
        message: String,
    },
}

/// Retrieves reusable skills for a query.
///
/// The full contract is documented on the [module](self). Implementations
/// must be deterministic for a given query, must not use a model to judge
/// relevance, and must not write storage or the event stream.
#[async_trait]
pub trait SkillSource: Send + Sync {
    /// Returns active skills for `query`, most relevant first.
    ///
    /// `limit == 0` returns an empty vector. `min_score == None` or
    /// `Some(0.0)` disables score filtering; a positive value is a minimum
    /// normalized relevance score. The LIKE fallback sentinel is `0.0`.
    async fn recall(
        &self,
        query: &str,
        limit: usize,
        min_score: Option<f64>,
    ) -> Result<Vec<SkillContent>, SkillSourceError>;
}

/// A [`Guide`] that injects recalled skills as one system message.
///
/// The guide forwards `query`, `limit`, and `min_score` unchanged, then
/// renders the returned skills in order under a fixed title. Each skill is
/// rendered as `## <name>`, its description, and its instructions;
/// consecutive skills are joined by `\n---\n`. The complete message is
/// truncated to `max_inject_bytes` on a UTF-8 character boundary.
///
/// A zero byte budget returns `Ok(None)` without calling the source. Recall
/// errors are logged at `warn` and also return `Ok(None)`. The guide never
/// writes storage, calls a model, or reads the clock.
#[derive(Clone)]
pub struct SkillGuide {
    /// Retrieval backend.
    source: Arc<dyn SkillSource>,
    /// Maximum number of skills requested.
    limit: usize,
    /// Optional minimum relevance score, passed through unchanged.
    min_score: Option<f64>,
    /// Maximum bytes in the final rendered system message.
    max_inject_bytes: usize,
}

impl SkillGuide {
    /// Creates a guide with an explicit byte budget.
    ///
    /// `max_inject_bytes` bounds the complete rendered content, including the
    /// title, headings, and separators. Zero means no skill message is
    /// injected.
    pub fn new(
        source: Arc<dyn SkillSource>,
        limit: usize,
        min_score: Option<f64>,
        max_inject_bytes: usize,
    ) -> Self {
        Self {
            source,
            limit,
            min_score,
            max_inject_bytes,
        }
    }

    /// Creates a guide with [`DEFAULT_SKILL_INJECT_BYTES`].
    pub fn with_default_budget(
        source: Arc<dyn SkillSource>,
        limit: usize,
        min_score: Option<f64>,
    ) -> Self {
        Self::new(source, limit, min_score, DEFAULT_SKILL_INJECT_BYTES)
    }
}

#[async_trait]
impl Guide for SkillGuide {
    async fn render(
        &self,
        _session: &SessionId,
        query: &str,
    ) -> Result<Option<Message>, GuideError> {
        if self.max_inject_bytes == 0 {
            return Ok(None);
        }

        let skills = match self.source.recall(query, self.limit, self.min_score).await {
            Ok(skills) => skills,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "skill guide recall failed; skipping context injection"
                );
                return Ok(None);
            }
        };
        if skills.is_empty() {
            return Ok(None);
        }

        let mut content = String::from(SKILL_GUIDE_TITLE);
        for (index, skill) in skills.iter().enumerate() {
            if index == 0 {
                content.push('\n');
            } else {
                content.push_str(SKILL_SEPARATOR);
            }
            content.push_str("## ");
            content.push_str(&skill.name);
            content.push('\n');
            content.push_str(&skill.description);
            content.push('\n');
            content.push_str(&skill.instructions);
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
