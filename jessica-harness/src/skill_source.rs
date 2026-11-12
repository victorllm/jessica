//! Deterministic skill-trigger decisions and bounded distillation sources.
//!
//! Skill distillation is considered only when structured turn counters cross
//! explicit thresholds. The trigger decision never reads model text, the clock,
//! random state, or an LLM. The module also prepares [`DistillSource`], which
//! escapes user-controlled text before applying byte budgets so untrusted
//! content cannot close the `<source>` boundary or inject chat-template role
//! markers.
//!
//! The source budget is measured over the escaped content fields (`user_goal`,
//! `final_answer`, and `tool_summaries`). The fixed wrapper and metadata labels
//! are overhead and are not counted. Each tool summary is capped individually
//! at [`MAX_TOOL_SUMMARY_BYTES`] after escaping.

use jessica_core::types::{SessionEndReason, SessionId};

use crate::acceptance::AcceptanceOutcome;

/// Default minimum number of tool calls required to consider a turn.
pub const DEFAULT_MIN_TOOL_CALLS: usize = 5;

/// Default minimum number of assistant rounds required to consider a turn.
pub const DEFAULT_MIN_ROUNDS: usize = 2;

/// Maximum escaped byte length of one tool summary.
pub const MAX_TOOL_SUMMARY_BYTES: usize = 512;

/// Deterministic thresholds for skill-distillation eligibility.
///
/// A turn is eligible only when it ended with [`SessionEndReason::Completed`],
/// had no acceptance failures, and met both thresholds. The decision uses
/// structured counters only; model output and LLM judgement never participate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkillTrigger {
    /// Minimum number of tool calls in the final attempt.
    pub min_tool_calls: usize,
    /// Minimum number of assistant rounds in the final attempt.
    pub min_rounds: usize,
}

impl Default for SkillTrigger {
    fn default() -> Self {
        Self {
            min_tool_calls: DEFAULT_MIN_TOOL_CALLS,
            min_rounds: DEFAULT_MIN_ROUNDS,
        }
    }
}

impl SkillTrigger {
    /// Creates a trigger with explicit thresholds.
    ///
    /// Zero is a valid threshold and makes that dimension unconditional.
    pub fn new(min_tool_calls: usize, min_rounds: usize) -> Self {
        Self {
            min_tool_calls,
            min_rounds,
        }
    }

    /// Returns whether `signals` satisfy every trigger rule.
    ///
    /// The check is exactly: final reason is
    /// [`SessionEndReason::Completed`], `acceptance_failures == 0`,
    /// `tool_call_count >= min_tool_calls`, and
    /// `assistant_rounds >= min_rounds`.
    #[must_use]
    pub fn should_trigger(&self, signals: &TurnSignals) -> bool {
        signals.reason == SessionEndReason::Completed
            && signals.acceptance_failures == 0
            && signals.tool_call_count >= self.min_tool_calls
            && signals.assistant_rounds >= self.min_rounds
    }
}

/// Structured counters from the final attempt of one turn.
///
/// Create signals from an [`AcceptanceOutcome`] when acceptance orchestration
/// ran, or with [`TurnSignals::new`] when it did not. `acceptance_failures` is
/// derived from `attempts.saturating_sub(1)` in the former case; callers must
/// not pass `attempts` directly as a failure count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnSignals {
    reason: SessionEndReason,
    tool_call_count: usize,
    assistant_rounds: usize,
    acceptance_failures: usize,
}

impl TurnSignals {
    /// Creates signals for a turn without acceptance retries.
    ///
    /// `tool_call_count` and `assistant_rounds` must be computed from the
    /// structured messages or event trace of the final attempt, never guessed
    /// from model text. The acceptance failure count is zero.
    pub fn new(reason: SessionEndReason, tool_call_count: usize, assistant_rounds: usize) -> Self {
        Self {
            reason,
            tool_call_count,
            assistant_rounds,
            acceptance_failures: 0,
        }
    }

    /// Creates signals from the final [`AcceptanceOutcome`].
    ///
    /// The first attempt is not a failure, so the failure count is always
    /// `outcome.attempts.saturating_sub(1)`. The counts still come from the
    /// final attempt supplied by the caller.
    pub fn from_acceptance(
        outcome: &AcceptanceOutcome,
        tool_call_count: usize,
        assistant_rounds: usize,
    ) -> Self {
        Self {
            reason: outcome.reason,
            tool_call_count,
            assistant_rounds,
            acceptance_failures: usize::try_from(outcome.attempts.saturating_sub(1))
                .unwrap_or(usize::MAX),
        }
    }

    /// Returns the structured terminal reason.
    #[must_use]
    pub fn reason(&self) -> SessionEndReason {
        self.reason
    }

    /// Returns the number of tool calls in the final attempt.
    #[must_use]
    pub fn tool_call_count(&self) -> usize {
        self.tool_call_count
    }

    /// Returns the number of assistant rounds in the final attempt.
    #[must_use]
    pub fn assistant_rounds(&self) -> usize {
        self.assistant_rounds
    }

    /// Returns the number of failed acceptance checks before the final attempt.
    #[must_use]
    pub fn acceptance_failures(&self) -> usize {
        self.acceptance_failures
    }
}

/// Raw inputs used to build a [`DistillSource`].
///
/// [`DistillSourceInput::new`] omits tool summaries by default. Tool summaries
/// are opt-in through [`DistillSourceInput::with_tool_summaries`]. Counts and
/// the acceptance failure count are copied from [`TurnSignals`], which keeps
/// the failure derivation in one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistillSourceInput {
    session_id: SessionId,
    turn_index: u64,
    user_goal: String,
    final_answer: String,
    tool_summaries: Vec<String>,
    tool_call_count: usize,
    assistant_rounds: usize,
    acceptance_failures: usize,
}

impl DistillSourceInput {
    /// Creates an input with no tool summaries.
    ///
    /// `user_goal` and `final_answer` are untrusted text and will be escaped
    /// and truncated by [`DistillSource::new`]. `signals` is the sole source of
    /// the structured counts and acceptance-failure accounting.
    pub fn new(
        session_id: SessionId,
        turn_index: u64,
        user_goal: impl Into<String>,
        final_answer: impl Into<String>,
        signals: &TurnSignals,
    ) -> Self {
        Self {
            session_id,
            turn_index,
            user_goal: user_goal.into(),
            final_answer: final_answer.into(),
            tool_summaries: Vec::new(),
            tool_call_count: signals.tool_call_count,
            assistant_rounds: signals.assistant_rounds,
            acceptance_failures: signals.acceptance_failures,
        }
    }

    /// Includes one summary per tool invocation, in call order.
    ///
    /// Each summary is escaped and capped at [`MAX_TOOL_SUMMARY_BYTES`] after
    /// escaping. Passing an empty iterator leaves the source with no tool
    /// summaries, which is the default.
    pub fn with_tool_summaries<I, S>(mut self, summaries: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tool_summaries = summaries.into_iter().map(Into::into).collect();
        self
    }
}

/// Bounded, escaped evidence for one eligible turn.
///
/// Escaping happens before any truncation. `max_source_bytes` bounds the sum
/// of the escaped byte lengths of `user_goal`, `final_answer`, and the tool
/// summaries. Fixed wrapper and metadata labels are not counted. When the
/// budget is insufficient, tool summaries are dropped from the tail first,
/// then `final_answer` is truncated, and only then `user_goal` is truncated.
/// Every truncation stops on a UTF-8 character boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistillSource {
    session_id: SessionId,
    turn_index: u64,
    user_goal: String,
    final_answer: String,
    tool_summaries: Vec<String>,
    tool_call_count: usize,
    assistant_rounds: usize,
    acceptance_failures: usize,
}

impl DistillSource {
    /// Builds a source, escaping untrusted text before applying the budget.
    ///
    /// `max_source_bytes` applies to the content fields only, as documented on
    /// the type. A zero budget leaves all three content fields empty.
    #[must_use]
    pub fn new(input: DistillSourceInput, max_source_bytes: usize) -> Self {
        let user_goal = escape_untrusted(&input.user_goal);
        let final_answer = escape_untrusted(&input.final_answer);
        let tool_summaries = input
            .tool_summaries
            .iter()
            .map(|summary| {
                let escaped = escape_untrusted(summary);
                truncate_on_char_boundary(&escaped, MAX_TOOL_SUMMARY_BYTES).to_owned()
            })
            .collect();
        let (user_goal, final_answer, tool_summaries) =
            apply_budget(user_goal, final_answer, tool_summaries, max_source_bytes);

        Self {
            session_id: input.session_id,
            turn_index: input.turn_index,
            user_goal,
            final_answer,
            tool_summaries,
            tool_call_count: input.tool_call_count,
            assistant_rounds: input.assistant_rounds,
            acceptance_failures: input.acceptance_failures,
        }
    }

    /// Returns the escaped, bounded user goal.
    #[must_use]
    pub fn user_goal(&self) -> &str {
        &self.user_goal
    }

    /// Returns the escaped, bounded final answer.
    #[must_use]
    pub fn final_answer(&self) -> &str {
        &self.final_answer
    }

    /// Returns the escaped, bounded tool summaries that survived the budget.
    #[must_use]
    pub fn tool_summaries(&self) -> &[String] {
        &self.tool_summaries
    }

    /// Returns the number of tool calls in the final attempt.
    #[must_use]
    pub fn tool_call_count(&self) -> usize {
        self.tool_call_count
    }

    /// Returns the number of assistant rounds in the final attempt.
    #[must_use]
    pub fn assistant_rounds(&self) -> usize {
        self.assistant_rounds
    }

    /// Returns the number of failed acceptance checks before the final attempt.
    #[must_use]
    pub fn acceptance_failures(&self) -> usize {
        self.acceptance_failures
    }

    /// Returns the session identifier.
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the stable turn index.
    #[must_use]
    pub fn turn_index(&self) -> u64 {
        self.turn_index
    }

    /// Renders the bounded content as one explicitly untrusted block.
    ///
    /// The returned text starts with `<source>` and ends with `</source>`.
    /// User-controlled values are already escaped, so the only literal closing
    /// tag is the wrapper itself. The block includes structured metadata
    /// (session, turn, and counts) followed by the escaped content fields. The
    /// fixed wrapper and metadata labels are outside the content budget.
    #[must_use]
    pub fn to_untrusted_block(&self) -> String {
        let mut block = String::new();
        block.push_str("<source>\n");
        block.push_str("session_id: ");
        block.push_str(&escape_untrusted(self.session_id.as_str()));
        block.push('\n');
        block.push_str("turn_index: ");
        block.push_str(&self.turn_index.to_string());
        block.push('\n');
        block.push_str("tool_call_count: ");
        block.push_str(&self.tool_call_count.to_string());
        block.push('\n');
        block.push_str("assistant_rounds: ");
        block.push_str(&self.assistant_rounds.to_string());
        block.push('\n');
        block.push_str("acceptance_failures: ");
        block.push_str(&self.acceptance_failures.to_string());
        block.push('\n');
        block.push_str("user_goal:\n");
        block.push_str(&self.user_goal);
        block.push('\n');
        block.push_str("final_answer:\n");
        block.push_str(&self.final_answer);
        block.push('\n');
        if !self.tool_summaries.is_empty() {
            block.push_str("tool_summaries:\n");
            for summary in &self.tool_summaries {
                block.push_str("- ");
                block.push_str(summary);
                block.push('\n');
            }
        }
        block.push_str("</source>");
        block
    }
}

/// Escapes untrusted boundary markers before any truncation.
///
/// The transformation is deterministic and byte-stable:
///
/// - every `<` becomes `&lt;` and every `>` becomes `&gt;`, so no literal
///   `<source>`, `</source>`, or `<|im_end|>` can survive, even when a later
///   truncation cuts through an escape sequence;
/// - `\n\nSystem:`, `\n\nHuman:`, `\n\nAssistant:`, `\n\nUser:`, and their
///   `\r\n\r\n` variants have the ASCII colon replaced with a fullwidth colon
///   (U+FF1A).
///
/// No other characters are changed.
#[must_use]
pub fn escape_untrusted(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            _ => escaped.push(character),
        }
    }

    for (marker, replacement) in [
        ("\r\n\r\nSystem:", "\r\n\r\nSystem\u{FF1A}"),
        ("\r\n\r\nHuman:", "\r\n\r\nHuman\u{FF1A}"),
        ("\r\n\r\nAssistant:", "\r\n\r\nAssistant\u{FF1A}"),
        ("\r\n\r\nUser:", "\r\n\r\nUser\u{FF1A}"),
        ("\n\nSystem:", "\n\nSystem\u{FF1A}"),
        ("\n\nHuman:", "\n\nHuman\u{FF1A}"),
        ("\n\nAssistant:", "\n\nAssistant\u{FF1A}"),
        ("\n\nUser:", "\n\nUser\u{FF1A}"),
    ] {
        escaped = escaped.replace(marker, replacement);
    }
    escaped
}

/// Applies the content budget in the locked order.
fn apply_budget(
    mut user_goal: String,
    mut final_answer: String,
    mut tool_summaries: Vec<String>,
    max_source_bytes: usize,
) -> (String, String, Vec<String>) {
    let mut total = user_goal
        .len()
        .saturating_add(final_answer.len())
        .saturating_add(
            tool_summaries
                .iter()
                .map(String::len)
                .fold(0_usize, usize::saturating_add),
        );

    while total > max_source_bytes {
        let Some(last) = tool_summaries.last() else {
            break;
        };
        total = total.saturating_sub(last.len());
        tool_summaries.pop();
    }

    if total > max_source_bytes {
        let final_budget = max_source_bytes.saturating_sub(user_goal.len());
        final_answer = truncate_on_char_boundary(&final_answer, final_budget).to_owned();
        total = user_goal.len().saturating_add(final_answer.len());
    }

    if total > max_source_bytes {
        user_goal = truncate_on_char_boundary(&user_goal, max_source_bytes).to_owned();
    }

    (user_goal, final_answer, tool_summaries)
}

/// Returns the longest prefix of `text` of at most `max_bytes` bytes that ends
/// on a UTF-8 character boundary.
fn truncate_on_char_boundary(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
