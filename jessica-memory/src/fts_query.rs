//! Shared FTS5 query sanitization used by memory and skill search.
//!
//! User input must never be interpolated into an FTS5 `MATCH` expression.
//! [`split_query`] only emits alphanumeric/underscore tokens, and
//! [`quote_fts_token`] wraps each token as an FTS5 string literal. Tokens
//! shorter than [`TRIGRAM_MIN_CHARS`] cannot use the trigram index, so callers
//! apply them with [`like_pattern`] and an explicit `ESCAPE '\'` clause.
//!
//! Keeping this logic in one module guarantees that memory and skill search
//! reject the same characters, fall back at the same length, and escape `%`,
//! `_`, and `\` identically.

/// Shortest query a trigram index can match.
pub(crate) const TRIGRAM_MIN_CHARS: usize = 3;

/// Query tokens split by the index path that can serve them.
#[derive(Debug, Default)]
pub(crate) struct QueryTokens {
    /// Tokens long enough for the trigram FTS5 index.
    pub(crate) long: Vec<String>,
    /// Tokens that must use the escaped `LIKE` fallback.
    pub(crate) short: Vec<String>,
}

impl QueryTokens {
    /// Returns `true` when the query produced no searchable token.
    pub(crate) fn is_empty(&self) -> bool {
        self.long.is_empty() && self.short.is_empty()
    }
}

/// Splits a query into FTS5-safe tokens and partitions them by length.
///
/// Punctuation and whitespace separate tokens; only alphanumeric characters
/// and underscores are retained. This is the only transformation applied before
/// building an FTS expression, so FTS5 operators and quoting cannot be injected
/// by callers.
pub(crate) fn split_query(query: &str) -> QueryTokens {
    let mut tokens = QueryTokens::default();
    let mut current = String::new();

    for character in query.chars() {
        if character.is_alphanumeric() || character == '_' {
            current.push(character);
        } else if !current.is_empty() {
            push_token(&mut tokens, std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        push_token(&mut tokens, current);
    }

    tokens
}

/// Adds one non-empty token to the correct search path.
fn push_token(tokens: &mut QueryTokens, token: String) {
    if token.chars().count() >= TRIGRAM_MIN_CHARS {
        tokens.long.push(token);
    } else {
        tokens.short.push(token);
    }
}

/// Quotes one token as an FTS5 string literal.
pub(crate) fn quote_fts_token(token: &str) -> String {
    let escaped = token.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Builds an escaped `%token%` pattern for `LIKE ... ESCAPE '\'`.
pub(crate) fn like_pattern(token: &str) -> String {
    let mut pattern = String::with_capacity(token.len() + 2);
    pattern.push('%');
    for character in token.chars() {
        match character {
            '\\' | '%' | '_' => {
                pattern.push('\\');
                pattern.push(character);
            }
            _ => pattern.push(character),
        }
    }
    pattern.push('%');
    pattern
}
