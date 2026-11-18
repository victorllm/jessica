//! Structural permission narrowing for delegated child agents.
//!
//! A parent delegates work by requesting a subset of its own tools. This module
//! computes the only set of tools the child may receive:
//!
//! - the request must not repeat a tool name;
//! - every requested name must be in the parent allowlist;
//! - the result is the request intersected with tools the parent can execute
//!   natively, minus [`DELEGATE_TASK_TOOL_NAME`].
//!
//! WASM tools and other non-native tools are silently dropped even when the
//! parent allowlist permits them: v1 has no isolated child WASM instances, so an
//! exposed instance would share mutable guest state between children. Delegation
//! is single-level, so `delegate_task` is always dropped even if a caller lists
//! it as native.
//!
//! The module is pure and deterministic: it performs no I/O, makes no model
//! call, and does not depend on the CLI or any concrete tool. The resulting
//! [`NarrowedTools`] is the structural input of the child tool factory; later
//! stages must build exactly these names and nothing else.
//!
//! It also owns the `delegate_task` schema, argument parsing, trace hash, the
//! exact JSON result contract returned to the parent model, and the single-level
//! child executor.
//!
//! [`AgentChildExecutor`] builds a fresh tool registry and transcript for every
//! delegation. It shares only the parent provider (and therefore the parent's
//! token budget); child model and tool events go to a private bounded broadcast
//! channel that is never forwarded or persisted. Only the parent-side
//! [`AgentEvent::ChildSessionStarted`] and [`AgentEvent::ChildSessionEnded`]
//! lifecycle events are offered to the parent sink and broadcast. The child is a
//! bare [`crate::AgentLoop`], so it has no harness, memory, skills, sensors, or
//! acceptance checks.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::events::EventSink;
use jessica_core::llm::LlmProvider;
use jessica_core::tool::{Tool, ToolRegistry, ToolSchema};
use jessica_core::types::{
    AgentEvent, Budget, Message, Role, SessionEndReason, SessionId, ToolCall, ToolResult,
};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::agent::{AgentLoop, TranscriptRun};

/// Name of the tool that delegates work to a child agent.
///
/// The child never receives this tool: [`narrow_tools`] always removes it, and
/// a child that forges a call gets the agent loop's unknown-tool error.
pub const DELEGATE_TASK_TOOL_NAME: &str = "delegate_task";

/// Wall-clock timeout applied to each individual child tool call.
///
/// v1 fixes this value instead of exposing a child-specific configuration. It
/// must stay strictly below [`DelegationLimits::timeout`], which in turn stays
/// below the parent's tool timeout; the CLI validates the full nesting.
pub const CHILD_TOOL_TIMEOUT: Duration = Duration::from_millis(5_000);

/// Smallest accepted [`DelegationLimits::max_result_bytes`].
///
/// The limit applies to the final serialized JSON. The empty-answer truncated
/// skeleton and the empty-error skeleton must both fit, so a positive limit
/// alone is not sufficient.
pub const MIN_RESULT_BYTES: usize = 128;

/// Fixed system prompt for every delegated child agent.
///
/// The prompt is a constant with no parent content interpolated. It establishes
/// the isolation boundary the runtime enforces structurally: the child has no
/// parent history, may use only the tools it was given, must answer directly,
/// and cannot delegate further.
pub const CHILD_AGENT_SYSTEM_PROMPT: &str = "You are an independent execution unit. You have no parent conversation, memory, or history; rely only on the current task and the tools you were given. Do not request additional tools. When you have finished, return the final answer directly. You cannot delegate work to another agent.";

/// Capacity of the private broadcast channel used for child events.
const CHILD_EVENT_CHANNEL_CAPACITY: usize = 256;

/// Tool names the parent agent is allowed to use.
///
/// Both sets are stored de-duplicated. `native` describes tools the parent can
/// execute without WASM; names outside `allowed` are harmless here because
/// [`narrow_tools`] checks the allowlist first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentCapabilities {
    /// Parent policy allowlist.
    allowed: BTreeSet<String>,
    /// Tools the parent can execute natively.
    native: BTreeSet<String>,
}

impl ParentCapabilities {
    /// Creates parent capabilities from an allowlist and a native tool set.
    ///
    /// Duplicate names in either input are collapsed. The constructor does not
    /// validate that `native` is a subset of `allowed`: a requested name must
    /// pass the allowlist check regardless, and a native-but-not-allowed name
    /// is therefore never exposed to a child.
    pub fn new(
        allowed: impl IntoIterator<Item = String>,
        native: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            allowed: allowed.into_iter().collect(),
            native: native.into_iter().collect(),
        }
    }
}

/// Deterministic set of tool names a child agent may receive.
///
/// Names are stored sorted and de-duplicated. The type exposes read-only
/// iteration only; once narrowing has produced it, the set cannot be widened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NarrowedTools {
    /// Permitted names in ascending order.
    names: BTreeSet<String>,
}

impl NarrowedTools {
    /// Returns the permitted names in ascending order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(String::as_str)
    }

    /// Returns whether no tool is permitted.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Returns how many tools are permitted.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Returns whether `name` is permitted.
    pub fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }
}

/// Narrows `requested` to the tools a child agent may receive.
///
/// Duplicate names are rejected before permission checks, so an invalid request
/// always yields the same error regardless of which names happen to be
/// authorized. Every requested name must appear in the parent allowlist;
/// unauthorized names are an error rather than being silently omitted.
///
/// After validation, the result is `requested ∩ parent.native` with
/// [`DELEGATE_TASK_TOOL_NAME`] removed unconditionally. Non-native tools such as
/// WASM plugins are therefore dropped even when the parent allowlist contains
/// them. An empty request and an empty result are both valid: the child then
/// runs with no tools.
///
/// # Errors
///
/// Returns [`DelegationError::DuplicateTool`] when a name repeats in
/// `requested`, and [`DelegationError::ToolNotPermitted`] when a requested name
/// is absent from the parent allowlist.
pub fn narrow_tools(
    parent: &ParentCapabilities,
    requested: &[String],
) -> Result<NarrowedTools, DelegationError> {
    let mut seen = BTreeSet::new();
    for name in requested {
        if !seen.insert(name.as_str()) {
            return Err(DelegationError::DuplicateTool { name: name.clone() });
        }
    }
    for name in requested {
        if !parent.allowed.contains(name) {
            return Err(DelegationError::ToolNotPermitted { name: name.clone() });
        }
    }

    let names = requested
        .iter()
        .filter(|name| parent.native.contains(name.as_str()))
        .filter(|name| name.as_str() != DELEGATE_TASK_TOOL_NAME)
        .cloned()
        .collect();

    Ok(NarrowedTools { names })
}

/// Parsed and validated arguments for [`DELEGATE_TASK_TOOL_NAME`].
///
/// The task is trimmed. Tool names preserve the model's order and any
/// duplicates; duplicate validation belongs to [`narrow_tools`], not to this
/// parser, so callers can still compute a deterministic trace hash before
/// reporting a narrowing error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegateTaskArgs {
    /// Trimmed, self-contained task.
    task: String,
    /// Requested tool names in model order.
    tools: Vec<String>,
}

impl DelegateTaskArgs {
    /// Returns the trimmed, self-contained task.
    pub fn task(&self) -> &str {
        &self.task
    }

    /// Returns the requested tool names in model order.
    pub fn tools(&self) -> &[String] {
        &self.tools
    }
}

/// Parses and validates `delegate_task` arguments.
///
/// The value must be an object containing exactly `task` and `tools`. `task`
/// must be a string with non-whitespace content and at most `max_task_bytes`
/// UTF-8 bytes after trimming; `tools` must be an array of strings. Duplicate
/// tool names are accepted here because [`narrow_tools`] owns that check.
///
/// # Errors
///
/// Returns a structured error when the value is not an object, a field is
/// unknown or missing, a field has the wrong JSON type, the trimmed task is
/// empty, or the trimmed task exceeds `max_task_bytes`.
pub fn parse_delegate_task_args(
    args: &Value,
    max_task_bytes: usize,
) -> Result<DelegateTaskArgs, DelegationError> {
    let object = args
        .as_object()
        .ok_or(DelegationError::ArgumentsNotObject)?;

    for field in object.keys() {
        if field.as_str() != "task" && field.as_str() != "tools" {
            return Err(DelegationError::UnknownArgument {
                field: field.clone(),
            });
        }
    }

    let task = object
        .get("task")
        .ok_or(DelegationError::MissingArgument { field: "task" })?
        .as_str()
        .ok_or(DelegationError::InvalidArgumentType {
            field: "task",
            expected: "a string",
        })?;
    let tools = object
        .get("tools")
        .ok_or(DelegationError::MissingArgument { field: "tools" })?
        .as_array()
        .ok_or(DelegationError::InvalidArgumentType {
            field: "tools",
            expected: "an array of strings",
        })?;

    let mut names = Vec::with_capacity(tools.len());
    for value in tools {
        let name = value.as_str().ok_or(DelegationError::InvalidArgumentType {
            field: "tools",
            expected: "an array of strings",
        })?;
        names.push(name.to_owned());
    }

    let task = task.trim();
    if task.is_empty() {
        return Err(DelegationError::EmptyTask);
    }
    if task.len() > max_task_bytes {
        return Err(DelegationError::TaskTooLong {
            actual: task.len(),
            limit: max_task_bytes,
        });
    }

    Ok(DelegateTaskArgs {
        task: task.to_owned(),
        tools: names,
    })
}

/// Returns the fixed provider-facing schema for [`DELEGATE_TASK_TOOL_NAME`].
///
/// The description states the child boundary the runtime enforces: the child is
/// an independent execution unit with no parent history, may use only the
/// requested native tool subset, and cannot delegate further.
pub fn delegate_task_schema() -> ToolSchema {
    json!({
        "type": "function",
        "function": {
            "name": DELEGATE_TASK_TOOL_NAME,
            "description": "Delegate a self-contained task to an independent child agent. The child has no parent conversation, history, or memory; it can use only the requested subset of native tools and cannot delegate further.",
            "parameters": {
                "type": "object",
                "properties": {
                    "task": {
                        "type": "string",
                        "description": "Self-contained task for the child."
                    },
                    "tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "uniqueItems": true,
                        "description": "Native tool names from the parent allowlist."
                    }
                },
                "required": ["task", "tools"],
                "additionalProperties": false
            }
        }
    })
}

/// Computes the trace-only child session identifier.
///
/// The input is FNV-1a 32-bit over the trimmed task, one NUL separator, and each
/// sorted narrowed tool name followed by a NUL. The result is eight lowercase
/// hexadecimal digits. Tool names are sorted by [`NarrowedTools`], so request
/// order does not affect the hash.
///
/// The value deliberately excludes the parent session id: a fresh turn builds
/// its tool set before the session id exists, and the same tool can span
/// several acceptance attempts. It is a trace label only: it is not persisted,
/// not an idempotency key, and not a security credential.
pub fn child_session_hash(task: &str, tools: &NarrowedTools) -> String {
    hash_names(task.trim(), tools.names())
}

/// Hashes a trimmed task and an ordered sequence of tool names.
fn hash_names<'a>(task: &str, names: impl IntoIterator<Item = &'a str>) -> String {
    let mut hash = 0x811c_9dc5_u32;
    for byte in task.as_bytes() {
        hash = hash_byte(hash, *byte);
    }
    hash = hash_byte(hash, 0);
    for name in names {
        for byte in name.as_bytes() {
            hash = hash_byte(hash, *byte);
        }
        hash = hash_byte(hash, 0);
    }
    format!("{hash:08x}")
}

/// Feeds one byte into an FNV-1a 32-bit hash.
fn hash_byte(hash: u32, byte: u8) -> u32 {
    (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
}

/// Computes a best-effort trace hash from arguments that failed validation.
///
/// Used only on the error path. The task is taken when it is a string and
/// trimmed (empty otherwise); tool names are taken from string elements of a
/// `tools` array, then sorted and de-duplicated. The encoding matches
/// [`child_session_hash`], but the input may be incomplete, so the result has no
/// identity meaning beyond determinism.
pub(crate) fn best_effort_child_session_hash(args: &Value) -> String {
    let object = args.as_object();
    let task = object
        .and_then(|object| object.get("task"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let mut names: Vec<&str> = object
        .and_then(|object| object.get("tools"))
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    names.sort_unstable();
    names.dedup();
    hash_names(task, names)
}

/// Resource limits for one delegated child run.
///
/// The runtime validates every limit that can be checked without the parent
/// budget. The CLI owns the remaining nesting check because only it knows
/// `Budget::tool_timeout`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelegationLimits {
    /// Maximum number of model rounds the child may run.
    pub max_rounds: u32,
    /// Wall-clock limit for the whole child run.
    pub timeout: Duration,
    /// Maximum UTF-8 byte length of the delegated task.
    pub max_task_bytes: usize,
    /// Maximum byte length of the serialized result JSON.
    pub max_result_bytes: usize,
    /// Maximum number of tool calls the child may request.
    pub max_child_tool_calls: u32,
}

impl Default for DelegationLimits {
    /// Returns the fixed v1 defaults: 4 rounds, 8000 ms, 8192 task bytes,
    /// 32768 result bytes, and 20 child tool calls.
    fn default() -> Self {
        Self {
            max_rounds: 4,
            timeout: Duration::from_millis(8_000),
            max_task_bytes: 8_192,
            max_result_bytes: 32_768,
            max_child_tool_calls: 20,
        }
    }
}

impl DelegationLimits {
    /// Validates every limit that does not depend on the parent budget.
    ///
    /// The child tool timeout is compared against [`CHILD_TOOL_TIMEOUT`], not
    /// against the parent's tool timeout: `timeout` must be strictly greater
    /// than 5000 ms. The CLI additionally checks that `timeout` is strictly
    /// below the parent tool timeout when delegation is enabled.
    ///
    /// # Errors
    ///
    /// Returns a structured error for every zero round/tool-call/task-byte
    /// limit, a result limit below [`MIN_RESULT_BYTES`], or a timeout that does
    /// not exceed [`CHILD_TOOL_TIMEOUT`].
    pub fn validate(&self) -> Result<(), DelegationError> {
        if self.max_rounds == 0 {
            return Err(DelegationError::ZeroMaxRounds);
        }
        if self.max_child_tool_calls == 0 {
            return Err(DelegationError::ZeroMaxChildToolCalls);
        }
        if self.max_task_bytes == 0 {
            return Err(DelegationError::ZeroMaxTaskBytes);
        }
        if self.max_result_bytes < MIN_RESULT_BYTES {
            return Err(DelegationError::ResultBytesTooSmall {
                actual: self.max_result_bytes,
            });
        }
        if self.timeout <= CHILD_TOOL_TIMEOUT {
            return Err(DelegationError::TimeoutTooShort {
                timeout: self.timeout,
            });
        }
        Ok(())
    }
}

/// Structured errors from permission narrowing and limit validation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DelegationError {
    /// A tool name was requested more than once.
    #[error("tool `{name}` was requested more than once")]
    DuplicateTool {
        /// Repeated tool name.
        name: String,
    },
    /// A requested tool is absent from the parent allowlist.
    #[error("tool `{name}` is not permitted by the parent allowlist")]
    ToolNotPermitted {
        /// Tool name that was not allowed.
        name: String,
    },
    /// `delegate_task` arguments were not a JSON object.
    #[error("delegate_task arguments must be a JSON object")]
    ArgumentsNotObject,
    /// An unknown argument field was supplied.
    #[error("delegate_task received unknown argument `{field}`")]
    UnknownArgument {
        /// Unknown field name.
        field: String,
    },
    /// A required argument field was absent.
    #[error("delegate_task is missing required argument `{field}`")]
    MissingArgument {
        /// Missing field name.
        field: &'static str,
    },
    /// An argument had the wrong JSON type.
    #[error("delegate_task argument `{field}` must be {expected}")]
    InvalidArgumentType {
        /// Field with the wrong type.
        field: &'static str,
        /// Human-readable expected type.
        expected: &'static str,
    },
    /// The task was empty after trimming.
    #[error("delegate_task task must not be empty after trimming")]
    EmptyTask,
    /// The trimmed task exceeded the byte limit.
    #[error("delegate_task task is {actual} bytes, exceeding the {limit}-byte limit")]
    TaskTooLong {
        /// Trimmed task length in UTF-8 bytes.
        actual: usize,
        /// Configured limit in bytes.
        limit: usize,
    },
    /// `max_rounds` was zero.
    #[error("delegation max_rounds must be at least 1")]
    ZeroMaxRounds,
    /// `max_child_tool_calls` was zero.
    #[error("delegation max_child_tool_calls must be at least 1")]
    ZeroMaxChildToolCalls,
    /// `max_task_bytes` was zero.
    #[error("delegation max_task_bytes must be at least 1")]
    ZeroMaxTaskBytes,
    /// `max_result_bytes` was below the minimum fixed JSON skeleton size.
    #[error("delegation max_result_bytes must be at least 128 bytes, got {actual}")]
    ResultBytesTooSmall {
        /// Configured limit.
        actual: usize,
    },
    /// The child run timeout did not exceed the fixed child tool timeout.
    #[error(
        "delegation timeout {timeout:?} must be greater than the fixed 5000ms child tool timeout"
    )]
    TimeoutTooShort {
        /// Configured child-run timeout.
        timeout: Duration,
    },
    /// The child tool factory could not construct the requested tools.
    #[error("child tool factory failed: {message}")]
    ToolFactory {
        /// Factory-supplied failure description.
        message: String,
    },
}

/// Terminal child outcome ready to render as `delegate_task` JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChildOutcome {
    /// The child completed without any failed tool call.
    Success {
        /// Completed model rounds.
        rounds: u32,
        /// Requested tool calls, including unknown tools.
        tool_calls: u32,
        /// Final assistant answer.
        answer: String,
        /// Trace-only child session hash.
        child_session_hash: String,
    },
    /// The child failed, timed out, or hit a limit.
    Error {
        /// Completed model rounds.
        rounds: u32,
        /// Requested tool calls, including unknown tools.
        tool_calls: u32,
        /// Unredacted failure text.
        error: String,
        /// Trace-only child session hash.
        child_session_hash: String,
    },
}

/// Renders a child outcome as the exact JSON returned to the parent model.
///
/// `max_result_bytes` must be at least [`MIN_RESULT_BYTES`]; validated
/// [`DelegationLimits`] guarantee that. A success whose full JSON exceeds the
/// limit becomes a fixed truncated object. An error keeps `status=error` and
/// shortens the redacted error text instead, so an oversized error can never be
/// mistaken for a truncated success.
pub(crate) fn render_child_outcome(outcome: ChildOutcome, max_result_bytes: usize) -> ToolResult {
    match outcome {
        ChildOutcome::Success {
            rounds,
            tool_calls,
            answer,
            child_session_hash,
        } => render_success(
            rounds,
            tool_calls,
            &answer,
            &child_session_hash,
            max_result_bytes,
        ),
        ChildOutcome::Error {
            rounds,
            tool_calls,
            error,
            child_session_hash,
        } => render_error(
            rounds,
            tool_calls,
            &error,
            &child_session_hash,
            max_result_bytes,
        ),
    }
}

/// Renders a successful child outcome, truncating the answer when necessary.
fn render_success(
    rounds: u32,
    tool_calls: u32,
    answer: &str,
    child_session_hash: &str,
    max_result_bytes: usize,
) -> ToolResult {
    let full_value = json!({
        "status": "ok",
        "rounds": rounds,
        "tool_calls": tool_calls,
        "answer": answer,
        "child_session_hash": child_session_hash,
    });
    let Ok(full) = serde_json::to_string(&full_value) else {
        return serialization_failure();
    };
    if full.len() <= max_result_bytes {
        return ToolResult::success(String::new(), full);
    }

    let original_bytes = full.len();
    let truncated = longest_prefix_by_serialized_len(answer, max_result_bytes, |answer_prefix| {
        json!({
            "status": "truncated",
            "answer": answer_prefix,
            "original_bytes": original_bytes,
        })
    });

    match truncated {
        Some(json) => ToolResult::success(String::new(), json),
        None => ToolResult::error(
            String::new(),
            "delegate_task result budget is below the minimum serialized size",
        ),
    }
}

/// Renders a failed child outcome, redacting and shortening the error text.
fn render_error(
    rounds: u32,
    tool_calls: u32,
    error: &str,
    child_session_hash: &str,
    max_result_bytes: usize,
) -> ToolResult {
    let redacted = redact_secrets(error);
    let full_value = error_json(rounds, tool_calls, &redacted, child_session_hash);
    let Ok(full) = serde_json::to_string(&full_value) else {
        return serialization_failure();
    };
    if full.len() <= max_result_bytes {
        return ToolResult::error(String::new(), full);
    }

    let shortened = longest_prefix_by_serialized_len(&redacted, max_result_bytes, |error_prefix| {
        error_json(rounds, tool_calls, error_prefix, child_session_hash)
    });

    match shortened {
        Some(json) => ToolResult::error(String::new(), json),
        None => ToolResult::error(
            String::new(),
            "delegate_task error budget is below the minimum serialized size",
        ),
    }
}

/// Builds the fixed error object, including exactly the six contract fields.
fn error_json(rounds: u32, tool_calls: u32, error: &str, child_session_hash: &str) -> Value {
    json!({
        "status": "error",
        "rounds": rounds,
        "tool_calls": tool_calls,
        "answer": "",
        "error": error,
        "child_session_hash": child_session_hash,
    })
}

/// Returns a fixed failure for the unreachable serialization-error branch.
fn serialization_failure() -> ToolResult {
    ToolResult::error(String::new(), "delegate_task result serialization failed")
}

/// Finds the longest UTF-8 prefix of `text` whose rendered JSON fits.
///
/// `build` must produce JSON whose serialized length is monotonic in the prefix
/// length. The empty prefix is included; `None` means even it does not fit.
fn longest_prefix_by_serialized_len<F>(text: &str, max_bytes: usize, build: F) -> Option<String>
where
    F: Fn(&str) -> Value,
{
    let mut boundaries: Vec<usize> = text.char_indices().map(|(index, _)| index).collect();
    boundaries.push(text.len());

    let mut low = 0;
    let mut high = boundaries.len() - 1;
    let mut best = None;
    while low <= high {
        let mid = low + (high - low) / 2;
        let prefix = &text[..boundaries[mid]];
        let Ok(serialized) = serde_json::to_string(&build(prefix)) else {
            return None;
        };
        if serialized.len() <= max_bytes {
            best = Some(serialized);
            low = mid + 1;
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }
    best
}

/// Replacement marker for credentials removed from error text.
const REDACTED: &str = "[REDACTED]";

/// Credential key names recognized in `key=value`, `key: value`, and JSON forms.
const SECRET_KEYS: [&str; 10] = [
    "authorization",
    "access_token",
    "access-token",
    "client_secret",
    "api_key",
    "api-key",
    "password",
    "secret",
    "token",
    "apikey",
];

/// Removes common credential forms from model-visible error text.
///
/// Bearer tokens and `sk-` prefixed keys are removed first, then values assigned
/// to known credential keys. Redaction is deliberately conservative: false
/// positives only shorten diagnostic text, while a missed credential would leak
/// into the parent transcript.
fn redact_secrets(input: &str) -> String {
    let text = redact_bearer_tokens(input);
    let text = redact_prefixed_tokens(&text, "sk-");
    redact_key_values(&text)
}

/// Replaces the token following each `Bearer` keyword.
fn redact_bearer_tokens(input: &str) -> String {
    const BEARER: &[u8] = b"bearer";
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    let mut index = 0;

    while index + BEARER.len() <= bytes.len() {
        if bytes[index..index + BEARER.len()].eq_ignore_ascii_case(BEARER)
            && (index == 0 || !is_word_byte(bytes[index - 1]))
            && bytes
                .get(index + BEARER.len())
                .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            let mut start = index + BEARER.len();
            while start < bytes.len() && bytes[start].is_ascii_whitespace() {
                start += 1;
            }
            if start < bytes.len() && (bytes[start] == b'"' || bytes[start] == b'\'') {
                start += 1;
            }
            let end = find_value_end(bytes, start);
            if end > start {
                output.push_str(&input[cursor..start]);
                output.push_str(REDACTED);
                cursor = end;
                index = end;
                continue;
            }
        }
        index += 1;
    }

    output.push_str(&input[cursor..]);
    output
}

/// Replaces tokens carrying a literal prefix such as `sk-`.
fn redact_prefixed_tokens(input: &str, prefix: &str) -> String {
    let prefix = prefix.as_bytes();
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    let mut index = 0;

    while index + prefix.len() <= bytes.len() {
        if &bytes[index..index + prefix.len()] == prefix {
            let mut end = index + prefix.len();
            while end < bytes.len() && is_secret_token_byte(bytes[end]) {
                end += 1;
            }
            output.push_str(&input[cursor..index]);
            output.push_str(REDACTED);
            cursor = end;
            index = end;
            continue;
        }
        index += 1;
    }

    output.push_str(&input[cursor..]);
    output
}

/// Replaces values assigned to known credential keys.
fn redact_key_values(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    let mut index = 0;

    while index < bytes.len() {
        if let Some((start, end)) = secret_assignment(bytes, index) {
            output.push_str(&input[cursor..start]);
            output.push_str(REDACTED);
            cursor = end;
            index = end;
            continue;
        }
        index += 1;
    }

    output.push_str(&input[cursor..]);
    output
}

/// Finds the value range of a credential assignment starting at `start`.
fn secret_assignment(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    if !bytes.get(start).is_some_and(u8::is_ascii_alphabetic) {
        return None;
    }
    if start > 0 && is_word_byte(bytes[start - 1]) {
        return None;
    }

    for key in SECRET_KEYS {
        let key = key.as_bytes();
        let key_end = start + key.len();
        if key_end > bytes.len()
            || !bytes[start..key_end].eq_ignore_ascii_case(key)
            || bytes.get(key_end).is_some_and(|byte| is_word_byte(*byte))
        {
            continue;
        }

        let mut position = key_end;
        if position < bytes.len() && (bytes[position] == b'"' || bytes[position] == b'\'') {
            position += 1;
        }
        while position < bytes.len() && bytes[position].is_ascii_whitespace() {
            position += 1;
        }
        if position >= bytes.len() || (bytes[position] != b'=' && bytes[position] != b':') {
            continue;
        }
        position += 1;
        while position < bytes.len() && bytes[position].is_ascii_whitespace() {
            position += 1;
        }
        if position < bytes.len() && (bytes[position] == b'"' || bytes[position] == b'\'') {
            position += 1;
        }

        let end = find_value_end(bytes, position);
        if end > position && !bytes[position..].starts_with(REDACTED.as_bytes()) {
            return Some((position, end));
        }
    }

    None
}

/// Returns the end of an unquoted value starting at `start`.
fn find_value_end(bytes: &[u8], start: usize) -> usize {
    let mut end = start;
    while end < bytes.len() && !is_value_terminator(bytes[end]) {
        end += 1;
    }
    end
}

/// Returns whether `byte` terminates an unquoted credential value.
fn is_value_terminator(byte: u8) -> bool {
    byte.is_ascii_whitespace()
        || matches!(byte, b'"' | b'\'' | b',' | b';' | b'&' | b'}' | b']' | b')')
}

/// Returns whether `byte` can continue an identifier or secret token.
fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

/// Returns whether `byte` can continue an `sk-` style token.
fn is_secret_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

/// Builds the tools a child agent may use.
///
/// The executor calls this for every delegation with the sorted names from
/// [`NarrowedTools`]. Implementations must return a fresh tool instance for
/// exactly those names: tools are never cached across children, and WASM tools
/// are not permitted in v1.
pub trait ChildToolFactory: Send + Sync {
    /// Builds fresh tool instances for exactly `names`.
    ///
    /// # Errors
    ///
    /// Returns [`DelegationError`] when construction fails. The executor turns
    /// the error into a child error result without calling the child model.
    fn build(&self, names: &[String]) -> Result<Vec<Arc<dyn Tool>>, DelegationError>;
}

/// Executes one delegated child agent.
///
/// The executor owns the parent provider, the tool factory, the parent
/// capabilities, and the delegation limits. Each [`execute`](Self::execute)
/// call builds a fresh registry, transcript, cancellation token, and private
/// event channel. Only the provider is shared, so the child consumes the
/// parent's token budget while sharing no mutable child state.
pub struct AgentChildExecutor {
    /// Parent provider, shared to preserve token accounting and cancellation.
    provider: Arc<dyn LlmProvider>,
    /// Factory for the narrowed tool set.
    factory: Arc<dyn ChildToolFactory>,
    /// Parent allowlist and native tool set.
    parent: ParentCapabilities,
    /// Limits for every child run.
    limits: DelegationLimits,
    /// Optional producer-side sink for child lifecycle events.
    sink: Option<Arc<dyn EventSink>>,
    /// Optional parent broadcast sender for child lifecycle events.
    events: Option<broadcast::Sender<AgentEvent>>,
}

impl AgentChildExecutor {
    /// Creates an executor after validating `limits`.
    ///
    /// # Errors
    ///
    /// Returns the structured errors from [`DelegationLimits::validate`].
    pub fn new(
        provider: Arc<dyn LlmProvider>,
        factory: Arc<dyn ChildToolFactory>,
        parent: ParentCapabilities,
        limits: DelegationLimits,
    ) -> Result<Self, DelegationError> {
        limits.validate()?;
        Ok(Self {
            provider,
            factory,
            parent,
            limits,
            sink: None,
            events: None,
        })
    }

    /// Attaches the parent's producer-side sink for child lifecycle events.
    ///
    /// The child's own model and tool events remain private. Only
    /// [`AgentEvent::ChildSessionStarted`] and
    /// [`AgentEvent::ChildSessionEnded`] are offered to the sink, before they
    /// are broadcast. A sink failure prevents the child from starting (for the
    /// start event) or turns the delegation into an error result (for the end
    /// event).
    pub fn with_event_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Attaches the parent's broadcast sender for child lifecycle events.
    ///
    /// The sender is used only after the optional sink accepts the event, so
    /// observers never see an event that the recorder rejected. A closed
    /// receiver is ignored, matching [`AgentLoop`] behaviour.
    pub fn with_event_sender(mut self, events: broadcast::Sender<AgentEvent>) -> Self {
        self.events = Some(events);
        self
    }

    /// Runs one child agent without an originating call id.
    ///
    /// This is the compatibility wrapper used when a caller has only the
    /// arguments. The child lifecycle events it emits carry an empty `call_id`;
    /// [`Tool::execute_with_call`] callers should use
    /// [`execute_with_call`](Self::execute_with_call) instead.
    pub async fn execute(&self, args: Value) -> ToolResult {
        self.execute_with_call(args, "").await
    }

    /// Runs one child agent and renders its outcome as `delegate_task` JSON.
    ///
    /// Every failure becomes a `status=error` [`ToolResult`]; this method never
    /// panics or propagates an error to the parent loop. The result's
    /// `tool_call_id` is left empty because the parent loop stamps the
    /// originating call id.
    ///
    /// When a sink or event sender is attached, the child's lifecycle is
    /// recorded as [`AgentEvent::ChildSessionStarted`] before the child model
    /// is called and as [`AgentEvent::ChildSessionEnded`] when it finishes or
    /// times out. A sink rejection before the child starts prevents the run; a
    /// rejection when recording the end turns the delegation into an error
    /// result. Child-internal events are never recorded on the parent stream.
    ///
    /// A delegation timeout drops the child future. Because no final transcript
    /// exists, that result reports zero rounds and zero tool calls.
    pub async fn execute_with_call(&self, args: Value, call_id: &str) -> ToolResult {
        let fallback_hash = best_effort_child_session_hash(&args);
        let parsed = match parse_delegate_task_args(&args, self.limits.max_task_bytes) {
            Ok(parsed) => parsed,
            Err(error) => return self.error_result(0, 0, error.to_string(), fallback_hash),
        };
        let narrowed = match narrow_tools(&self.parent, parsed.tools()) {
            Ok(narrowed) => narrowed,
            Err(error) => return self.error_result(0, 0, error.to_string(), fallback_hash),
        };
        let child_session_hash = child_session_hash(parsed.task(), &narrowed);
        let names: Vec<String> = narrowed.names().map(str::to_owned).collect();

        let tools = match self.factory.build(&names) {
            Ok(tools) => tools,
            Err(error) => return self.error_result(0, 0, error.to_string(), child_session_hash),
        };
        let registry = match build_child_registry(wrap_child_tools(tools), &narrowed) {
            Ok(registry) => registry,
            Err(error) => return self.error_result(0, 0, error, child_session_hash),
        };

        let child_session_id = SessionId::new(child_session_hash.clone());
        if !self
            .emit_child_event(AgentEvent::ChildSessionStarted {
                child_session_id: child_session_id.clone(),
                call_id: call_id.to_owned(),
            })
            .await
        {
            return self.error_result(
                0,
                0,
                "event sink failed while recording child session start".to_owned(),
                child_session_hash,
            );
        }

        let messages = vec![
            Message::system(CHILD_AGENT_SYSTEM_PROMPT),
            Message::user(parsed.task()),
        ];
        let (events, _receiver) = broadcast::channel(CHILD_EVENT_CHANNEL_CAPACITY);
        let child = AgentLoop::new(
            Arc::clone(&self.provider),
            Arc::new(registry),
            Budget::new(0, CHILD_TOOL_TIMEOUT, self.limits.max_rounds),
            events,
        );
        let cancel = CancellationToken::new();
        let run = tokio::time::timeout(
            self.limits.timeout,
            child.run_with_transcript(
                child_session_id.clone(),
                messages,
                Some(self.limits.max_child_tool_calls),
                cancel.clone(),
            ),
        )
        .await;

        let transcript = match run {
            Ok(transcript) => transcript,
            Err(_) => {
                cancel.cancel();
                if !self
                    .emit_child_event(AgentEvent::ChildSessionEnded {
                        child_session_id,
                        call_id: call_id.to_owned(),
                        reason: SessionEndReason::Cancelled,
                        rounds: 0,
                        tool_calls: 0,
                        tool_errors: 0,
                    })
                    .await
                {
                    return self.error_result(
                        0,
                        0,
                        "event sink failed while recording child session end".to_owned(),
                        child_session_hash,
                    );
                }
                return self.error_result(
                    0,
                    0,
                    format!(
                        "child agent timed out after {}ms",
                        self.limits.timeout.as_millis()
                    ),
                    child_session_hash,
                );
            }
        };

        let rounds = transcript.rounds;
        let tool_calls = transcript.tool_calls;
        let tool_errors = transcript.tool_errors;
        let reason = transcript.reason;
        let outcome = self.outcome(transcript, child_session_hash.clone());
        if !self
            .emit_child_event(AgentEvent::ChildSessionEnded {
                child_session_id,
                call_id: call_id.to_owned(),
                reason,
                rounds,
                tool_calls,
                tool_errors,
            })
            .await
        {
            return render_child_outcome(
                ChildOutcome::Error {
                    rounds,
                    tool_calls,
                    error: "event sink failed while recording child session end".to_owned(),
                    child_session_hash,
                },
                self.limits.max_result_bytes,
            );
        }
        render_child_outcome(outcome, self.limits.max_result_bytes)
    }

    /// Offers a child lifecycle event to the sink, then broadcasts it.
    ///
    /// Returns false when the sink rejected the event; the event is not
    /// broadcast in that case, so observers never see an event the recorder
    /// refused.
    async fn emit_child_event(&self, event: AgentEvent) -> bool {
        if let Some(sink) = &self.sink
            && sink.emit(event.clone()).await.is_err()
        {
            return false;
        }
        if let Some(events) = &self.events {
            let _ = events.send(event);
        }
        true
    }

    /// Maps a finished child transcript to the fixed outcome contract.
    fn outcome(&self, transcript: TranscriptRun, child_session_hash: String) -> ChildOutcome {
        let TranscriptRun {
            reason,
            messages,
            rounds,
            tool_calls,
            tool_errors,
            first_tool_error,
            error,
            tool_call_limit_hit,
        } = transcript;

        if tool_call_limit_hit {
            return ChildOutcome::Error {
                rounds,
                tool_calls,
                error: format!(
                    "child agent exceeded the tool call limit of {}",
                    self.limits.max_child_tool_calls
                ),
                child_session_hash,
            };
        }

        if tool_errors > 0 {
            let detail =
                first_tool_error.unwrap_or_else(|| "child tool returned an error".to_owned());
            return ChildOutcome::Error {
                rounds,
                tool_calls,
                error: format!("child tool failure: {detail}"),
                child_session_hash,
            };
        }

        match reason {
            SessionEndReason::Completed => ChildOutcome::Success {
                rounds,
                tool_calls,
                answer: last_assistant_text(&messages).to_owned(),
                child_session_hash,
            },
            SessionEndReason::BudgetExceeded => ChildOutcome::Error {
                rounds,
                tool_calls,
                error: format!(
                    "child agent exhausted its {} round limit",
                    self.limits.max_rounds
                ),
                child_session_hash,
            },
            SessionEndReason::Cancelled => ChildOutcome::Error {
                rounds,
                tool_calls,
                error: "child agent was cancelled".to_owned(),
                child_session_hash,
            },
            SessionEndReason::Error => ChildOutcome::Error {
                rounds,
                tool_calls,
                error: error.unwrap_or_else(|| "child agent failed".to_owned()),
                child_session_hash,
            },
            SessionEndReason::AcceptanceFailed => ChildOutcome::Error {
                rounds,
                tool_calls,
                error: "child agent ended with an acceptance failure".to_owned(),
                child_session_hash,
            },
        }
    }

    /// Renders a pre-run or timeout failure.
    fn error_result(
        &self,
        rounds: u32,
        tool_calls: u32,
        error: String,
        child_session_hash: String,
    ) -> ToolResult {
        render_child_outcome(
            ChildOutcome::Error {
                rounds,
                tool_calls,
                error,
                child_session_hash,
            },
            self.limits.max_result_bytes,
        )
    }
}

/// Builds a registry containing exactly the narrowed tools.
///
/// Duplicate names, extra tools, and missing tools are integration errors. They
/// are returned as a message so the executor can render them without starting
/// the child model.
fn build_child_registry(
    tools: Vec<Arc<dyn Tool>>,
    narrowed: &NarrowedTools,
) -> Result<ToolRegistry, String> {
    let mut seen = BTreeSet::new();
    for tool in &tools {
        if !seen.insert(tool.name().to_owned()) {
            return Err(format!(
                "child tool factory returned duplicate tool `{}`",
                tool.name()
            ));
        }
    }

    let expected: BTreeSet<String> = narrowed.names().map(str::to_owned).collect();
    if seen != expected {
        let missing: Vec<&str> = expected
            .iter()
            .filter(|name| !seen.contains(name.as_str()))
            .map(String::as_str)
            .collect();
        let extra: Vec<&str> = seen
            .iter()
            .filter(|name| !expected.contains(name.as_str()))
            .map(String::as_str)
            .collect();
        return Err(format!(
            "child tool factory returned a mismatched tool set (missing: [{}], extra: [{}])",
            missing.join(", "),
            extra.join(", ")
        ));
    }

    let mut registry = ToolRegistry::new();
    for tool in tools {
        registry
            .register(tool)
            .map_err(|error| format!("failed to register child tools: {error}"))?;
    }
    Ok(registry)
}

/// Wraps every child tool with the fixed per-call timeout.
fn wrap_child_tools(tools: Vec<Arc<dyn Tool>>) -> Vec<Arc<dyn Tool>> {
    tools
        .into_iter()
        .map(|inner| {
            Arc::new(TimeoutTool {
                inner,
                timeout: CHILD_TOOL_TIMEOUT,
            }) as Arc<dyn Tool>
        })
        .collect()
}

/// Child tool wrapper that enforces [`CHILD_TOOL_TIMEOUT`].
struct TimeoutTool {
    /// Wrapped tool.
    inner: Arc<dyn Tool>,
    /// Per-call wall-clock limit.
    timeout: Duration,
}

#[async_trait]
impl Tool for TimeoutTool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema(&self) -> ToolSchema {
        self.inner.schema()
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let name = self.inner.name().to_owned();
        match tokio::time::timeout(self.timeout, self.inner.execute(args)).await {
            Ok(result) => result,
            Err(_) => ToolResult::error(
                String::new(),
                format!(
                    "tool `{name}` timed out after {}ms",
                    self.timeout.as_millis()
                ),
            ),
        }
    }
}

/// Returns the last assistant text in `messages`, or an empty string.
fn last_assistant_text(messages: &[Message]) -> &str {
    messages
        .iter()
        .rev()
        .find(|message| message.role == Role::Assistant)
        .map_or("", |message| message.content.as_str())
}

/// [`Tool`] implementation exposing [`AgentChildExecutor`] as `delegate_task`.
pub struct DelegateTaskTool {
    /// Executor shared by every invocation.
    executor: Arc<AgentChildExecutor>,
}

impl DelegateTaskTool {
    /// Creates the tool from an executor.
    pub fn new(executor: Arc<AgentChildExecutor>) -> Self {
        Self { executor }
    }

    /// Marks the rendered result as an error unless the child succeeded.
    fn rendered(&self, mut result: ToolResult) -> ToolResult {
        let status = serde_json::from_str::<Value>(&result.content)
            .ok()
            .and_then(|value| {
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        result.is_error = !matches!(status.as_deref(), Some("ok" | "truncated"));
        result
    }
}

#[async_trait]
impl Tool for DelegateTaskTool {
    fn name(&self) -> &str {
        DELEGATE_TASK_TOOL_NAME
    }

    fn schema(&self) -> ToolSchema {
        delegate_task_schema()
    }

    async fn execute(&self, args: Value) -> ToolResult {
        self.rendered(self.executor.execute(args).await)
    }

    async fn execute_with_call(&self, call: &ToolCall) -> ToolResult {
        self.rendered(
            self.executor
                .execute_with_call(call.arguments.clone(), &call.id)
                .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds capabilities from name slices.
    fn parent(allowed: &[&str], native: &[&str]) -> ParentCapabilities {
        ParentCapabilities::new(
            allowed.iter().map(|name| (*name).to_owned()),
            native.iter().map(|name| (*name).to_owned()),
        )
    }

    /// Builds a request from name slices.
    fn request(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn narrowing_keeps_only_the_requested_native_subset_in_sorted_order() {
        let capabilities = parent(
            &["write_file", "bash", "read_file"],
            &["write_file", "bash", "read_file"],
        );

        let result = narrow_tools(&capabilities, &request(&["write_file", "bash"]))
            .expect("authorized native tools must narrow successfully");

        assert_eq!(
            result.names().collect::<Vec<_>>(),
            vec!["bash", "write_file"]
        );
        assert_eq!(result.len(), 2);
        assert!(!result.is_empty());
        assert!(result.contains("bash"));
        assert!(!result.contains("read_file"));
    }

    #[test]
    fn duplicate_request_names_are_rejected_even_when_allowed() {
        let capabilities = parent(&["bash"], &["bash"]);

        let error = narrow_tools(&capabilities, &request(&["bash", "bash"]))
            .expect_err("duplicate names must be rejected");

        assert_eq!(
            error,
            DelegationError::DuplicateTool {
                name: "bash".to_owned()
            }
        );
    }

    #[test]
    fn duplicate_detection_precedes_permission_checks() {
        let capabilities = parent(&["bash"], &["bash"]);

        let error = narrow_tools(&capabilities, &request(&["missing", "missing"]))
            .expect_err("duplicate names must be rejected");

        assert_eq!(
            error,
            DelegationError::DuplicateTool {
                name: "missing".to_owned()
            }
        );
    }

    #[test]
    fn requesting_a_tool_outside_the_parent_allowlist_is_an_error() {
        let capabilities = parent(&["bash"], &["bash"]);

        let error = narrow_tools(&capabilities, &request(&["bash", "read_file"]))
            .expect_err("an unauthorized tool must not be silently dropped");

        assert_eq!(
            error,
            DelegationError::ToolNotPermitted {
                name: "read_file".to_owned()
            }
        );
    }

    #[test]
    fn an_unauthorized_non_native_tool_is_not_silently_filtered() {
        let capabilities = parent(&["bash"], &["bash"]);

        let error = narrow_tools(&capabilities, &request(&["wasm_echo"]))
            .expect_err("allowlist validation must run before native filtering");

        assert_eq!(
            error,
            DelegationError::ToolNotPermitted {
                name: "wasm_echo".to_owned()
            }
        );
    }

    #[test]
    fn delegate_task_and_non_native_tools_are_removed_even_when_authorized() {
        let capabilities = parent(
            &["bash", DELEGATE_TASK_TOOL_NAME, "wasm_echo"],
            &["bash", DELEGATE_TASK_TOOL_NAME],
        );

        let result = narrow_tools(
            &capabilities,
            &request(&["bash", DELEGATE_TASK_TOOL_NAME, "wasm_echo"]),
        )
        .expect("authorized tools must narrow successfully");

        assert_eq!(result.names().collect::<Vec<_>>(), vec!["bash"]);
    }

    #[test]
    fn a_request_for_only_filtered_tools_yields_an_empty_set() {
        let capabilities = parent(
            &[DELEGATE_TASK_TOOL_NAME, "wasm_echo"],
            &[DELEGATE_TASK_TOOL_NAME],
        );

        let result = narrow_tools(
            &capabilities,
            &request(&[DELEGATE_TASK_TOOL_NAME, "wasm_echo"]),
        )
        .expect("authorized tools must narrow successfully");

        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
        assert_eq!(result.names().count(), 0);
        assert!(!result.contains(DELEGATE_TASK_TOOL_NAME));
    }

    #[test]
    fn an_empty_request_yields_an_empty_set() {
        let capabilities = parent(&["bash"], &["bash"]);

        let result = narrow_tools(&capabilities, &[]).expect("an empty request must be accepted");

        assert!(result.is_empty());
        assert_eq!(result.names().count(), 0);
    }

    #[test]
    fn parent_capability_sets_are_deduplicated() {
        let capabilities = parent(&["bash", "bash"], &["bash", "bash"]);

        let result = narrow_tools(&capabilities, &request(&["bash"]))
            .expect("authorized tools must narrow successfully");

        assert_eq!(result.names().collect::<Vec<_>>(), vec!["bash"]);
    }

    #[test]
    fn default_limits_match_the_v1_delegation_contract() {
        let limits = DelegationLimits::default();

        assert_eq!(limits.max_rounds, 4);
        assert_eq!(limits.timeout, Duration::from_millis(8_000));
        assert_eq!(limits.max_task_bytes, 8_192);
        assert_eq!(limits.max_result_bytes, 32_768);
        assert_eq!(limits.max_child_tool_calls, 20);
        assert_eq!(CHILD_TOOL_TIMEOUT, Duration::from_millis(5_000));
        assert_eq!(MIN_RESULT_BYTES, 128);
        assert_eq!(limits.validate(), Ok(()));
    }

    #[test]
    fn validate_rejects_each_invalid_boundary() {
        let cases = [
            (
                DelegationLimits {
                    max_rounds: 0,
                    ..DelegationLimits::default()
                },
                DelegationError::ZeroMaxRounds,
            ),
            (
                DelegationLimits {
                    max_child_tool_calls: 0,
                    ..DelegationLimits::default()
                },
                DelegationError::ZeroMaxChildToolCalls,
            ),
            (
                DelegationLimits {
                    max_task_bytes: 0,
                    ..DelegationLimits::default()
                },
                DelegationError::ZeroMaxTaskBytes,
            ),
            (
                DelegationLimits {
                    max_result_bytes: MIN_RESULT_BYTES - 1,
                    ..DelegationLimits::default()
                },
                DelegationError::ResultBytesTooSmall {
                    actual: MIN_RESULT_BYTES - 1,
                },
            ),
            (
                DelegationLimits {
                    timeout: CHILD_TOOL_TIMEOUT,
                    ..DelegationLimits::default()
                },
                DelegationError::TimeoutTooShort {
                    timeout: CHILD_TOOL_TIMEOUT,
                },
            ),
            (
                DelegationLimits {
                    timeout: Duration::from_millis(4_999),
                    ..DelegationLimits::default()
                },
                DelegationError::TimeoutTooShort {
                    timeout: Duration::from_millis(4_999),
                },
            ),
        ];

        for (limits, expected) in cases {
            assert_eq!(limits.validate(), Err(expected));
        }
    }

    #[test]
    fn validate_accepts_the_smallest_allowed_limits() {
        let limits = DelegationLimits {
            max_rounds: 1,
            timeout: CHILD_TOOL_TIMEOUT + Duration::from_millis(1),
            max_task_bytes: 1,
            max_result_bytes: MIN_RESULT_BYTES,
            max_child_tool_calls: 1,
        };

        assert_eq!(limits.validate(), Ok(()));
    }

    /// Returns the sorted keys of a JSON object.
    fn object_keys(value: &Value) -> Vec<&str> {
        let mut keys: Vec<&str> = value
            .as_object()
            .expect("value must be a JSON object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        keys
    }

    /// Parses a rendered tool result as JSON.
    fn result_json(result: &ToolResult) -> Value {
        serde_json::from_str(&result.content).expect("delegate result must be valid JSON")
    }

    /// Builds a narrowed set from a request order.
    fn narrowed(names: &[&str]) -> NarrowedTools {
        let capabilities = parent(names, names);
        narrow_tools(&capabilities, &request(names)).expect("test tools must narrow")
    }

    #[test]
    fn delegate_task_schema_locks_the_argument_contract() {
        let schema = delegate_task_schema();

        assert_eq!(schema["type"].as_str(), Some("function"));
        assert_eq!(
            schema["function"]["name"].as_str(),
            Some(DELEGATE_TASK_TOOL_NAME)
        );
        let parameters = &schema["function"]["parameters"];
        assert_eq!(parameters["type"].as_str(), Some("object"));
        assert_eq!(parameters["required"], json!(["task", "tools"]));
        assert_eq!(parameters["additionalProperties"].as_bool(), Some(false));
        assert_eq!(
            parameters["properties"]["task"]["type"].as_str(),
            Some("string")
        );
        assert_eq!(
            parameters["properties"]["tools"]["type"].as_str(),
            Some("array")
        );
        assert_eq!(
            parameters["properties"]["tools"]["items"]["type"].as_str(),
            Some("string")
        );
        assert_eq!(
            parameters["properties"]["tools"]["uniqueItems"].as_bool(),
            Some(true)
        );

        let description = schema["function"]["description"]
            .as_str()
            .expect("description must be a string");
        for phrase in [
            "independent",
            "no parent",
            "native tools",
            "cannot delegate",
        ] {
            assert!(
                description.contains(phrase),
                "description must mention `{phrase}`"
            );
        }
    }

    #[test]
    fn parse_trims_the_task_and_preserves_tool_order() {
        let args = json!({
            "task": "  inspect the repository  ",
            "tools": ["read_file", "bash"]
        });

        let parsed = parse_delegate_task_args(&args, 64).expect("valid arguments");

        assert_eq!(parsed.task(), "inspect the repository");
        assert_eq!(
            parsed.tools(),
            vec!["read_file".to_owned(), "bash".to_owned()].as_slice()
        );
    }

    #[test]
    fn parse_rejects_non_object_unknown_missing_and_wrong_typed_arguments() {
        assert_eq!(
            parse_delegate_task_args(&json!([]), 64),
            Err(DelegationError::ArgumentsNotObject)
        );
        assert_eq!(
            parse_delegate_task_args(&json!({"task": "x", "tools": [], "extra": true}), 64),
            Err(DelegationError::UnknownArgument {
                field: "extra".to_owned()
            })
        );
        assert_eq!(
            parse_delegate_task_args(&json!({}), 64),
            Err(DelegationError::MissingArgument { field: "task" })
        );
        assert_eq!(
            parse_delegate_task_args(&json!({"task": "x"}), 64),
            Err(DelegationError::MissingArgument { field: "tools" })
        );
        assert_eq!(
            parse_delegate_task_args(&json!({"task": 5, "tools": []}), 64),
            Err(DelegationError::InvalidArgumentType {
                field: "task",
                expected: "a string"
            })
        );
        assert_eq!(
            parse_delegate_task_args(&json!({"task": "x", "tools": "bash"}), 64),
            Err(DelegationError::InvalidArgumentType {
                field: "tools",
                expected: "an array of strings"
            })
        );
        assert_eq!(
            parse_delegate_task_args(&json!({"task": "x", "tools": ["bash", 5]}), 64),
            Err(DelegationError::InvalidArgumentType {
                field: "tools",
                expected: "an array of strings"
            })
        );
        assert_eq!(
            parse_delegate_task_args(&json!({"task": "   \n\t ", "tools": []}), 64),
            Err(DelegationError::EmptyTask)
        );
    }

    #[test]
    fn parse_measures_the_trimmed_task_in_utf8_bytes() {
        let exact = json!({"task": "  界界  ", "tools": []});

        let parsed = parse_delegate_task_args(&exact, 6).expect("six bytes fit");
        assert_eq!(parsed.task(), "界界");
        assert_eq!(
            parse_delegate_task_args(&exact, 5),
            Err(DelegationError::TaskTooLong {
                actual: 6,
                limit: 5
            })
        );

        let ascii = json!({"task": "  12345  ", "tools": []});
        assert_eq!(
            parse_delegate_task_args(&ascii, 5)
                .expect("trimmed ascii fits")
                .task(),
            "12345"
        );
    }

    #[test]
    fn parse_leaves_duplicate_tools_for_narrowing_to_reject() {
        let args = json!({"task": "do work", "tools": ["bash", "bash"]});
        let parsed = parse_delegate_task_args(&args, 64).expect("duplicates are a narrowing error");

        assert_eq!(
            parsed.tools(),
            vec!["bash".to_owned(), "bash".to_owned()].as_slice()
        );
        let capabilities = parent(&["bash"], &["bash"]);
        assert_eq!(
            narrow_tools(&capabilities, parsed.tools()),
            Err(DelegationError::DuplicateTool {
                name: "bash".to_owned()
            })
        );
    }

    #[test]
    fn success_json_has_exactly_the_contract_fields() {
        let outcome = ChildOutcome::Success {
            rounds: 3,
            tool_calls: 5,
            answer: "done".to_owned(),
            child_session_hash: "a1b2c3d4".to_owned(),
        };

        let result = render_child_outcome(outcome, 1024);

        assert!(!result.is_error);
        let value = result_json(&result);
        assert_eq!(
            object_keys(&value),
            vec![
                "answer",
                "child_session_hash",
                "rounds",
                "status",
                "tool_calls"
            ]
        );
        assert_eq!(value["status"].as_str(), Some("ok"));
        assert_eq!(value["rounds"].as_u64(), Some(3));
        assert_eq!(value["tool_calls"].as_u64(), Some(5));
        assert_eq!(value["answer"].as_str(), Some("done"));
        assert_eq!(value["child_session_hash"].as_str(), Some("a1b2c3d4"));
        assert!(value.get("error").is_none());
    }

    #[test]
    fn error_json_has_exactly_the_contract_fields() {
        let outcome = ChildOutcome::Error {
            rounds: 2,
            tool_calls: 3,
            error: "boom".to_owned(),
            child_session_hash: "deadbeef".to_owned(),
        };

        let result = render_child_outcome(outcome, 1024);

        assert!(result.is_error);
        let value = result_json(&result);
        assert_eq!(
            object_keys(&value),
            vec![
                "answer",
                "child_session_hash",
                "error",
                "rounds",
                "status",
                "tool_calls"
            ]
        );
        assert_eq!(value["status"].as_str(), Some("error"));
        assert_eq!(value["rounds"].as_u64(), Some(2));
        assert_eq!(value["tool_calls"].as_u64(), Some(3));
        assert_eq!(value["answer"].as_str(), Some(""));
        assert_eq!(value["error"].as_str(), Some("boom"));
        assert_eq!(value["child_session_hash"].as_str(), Some("deadbeef"));
    }

    #[test]
    fn success_at_the_exact_byte_limit_is_not_truncated_and_one_byte_less_is() {
        let outcome = ChildOutcome::Success {
            rounds: 1,
            tool_calls: 2,
            answer: "alpha".to_owned(),
            child_session_hash: "deadbeef".to_owned(),
        };
        let full = render_child_outcome(outcome.clone(), usize::MAX);
        let full_len = full.content.len();

        let exact = render_child_outcome(outcome.clone(), full_len);
        assert!(!exact.is_error);
        assert_eq!(exact.content.len(), full_len);
        assert_eq!(result_json(&exact)["status"].as_str(), Some("ok"));

        let one_less = render_child_outcome(outcome, full_len - 1);
        assert!(!one_less.is_error);
        assert!(one_less.content.len() < full_len);
        let truncated = result_json(&one_less);
        assert_eq!(truncated["status"].as_str(), Some("truncated"));
        assert_eq!(truncated["original_bytes"].as_u64(), Some(full_len as u64));
        assert_eq!(truncated["answer"].as_str(), Some("alpha"));
    }

    #[test]
    fn truncation_uses_a_utf8_prefix_and_reports_the_full_result_size() {
        let answer = "开头🙂中文🚀结尾".repeat(10);
        let outcome = ChildOutcome::Success {
            rounds: 0,
            tool_calls: 0,
            answer: answer.clone(),
            child_session_hash: "deadbeef".to_owned(),
        };
        let full = render_child_outcome(outcome.clone(), usize::MAX);
        let original_bytes = full.content.len();

        let result = render_child_outcome(outcome, MIN_RESULT_BYTES);

        assert!(!result.is_error);
        assert!(result.content.len() <= MIN_RESULT_BYTES);
        let value = result_json(&result);
        assert_eq!(
            object_keys(&value),
            vec!["answer", "original_bytes", "status"]
        );
        assert_eq!(value["status"].as_str(), Some("truncated"));
        assert_eq!(
            value["original_bytes"].as_u64(),
            Some(original_bytes as u64)
        );
        let prefix = value["answer"].as_str().expect("answer must be a string");
        assert!(answer.starts_with(prefix));
        assert!(!prefix.is_empty());
        assert!(prefix.len() < answer.len());
        assert!(value.get("error").is_none());
        assert!(value.get("rounds").is_none());
        assert!(value.get("child_session_hash").is_none());
    }

    #[test]
    fn error_results_redact_credentials_before_rendering() {
        let outcome = ChildOutcome::Error {
            rounds: 0,
            tool_calls: 1,
            error: "Authorization: Bearer bearer-secret api_key=key-secret token=tok-secret \
                    sk-live-secret"
                .to_owned(),
            child_session_hash: "deadbeef".to_owned(),
        };

        let result = render_child_outcome(outcome, 512);

        assert!(result.is_error);
        assert!(result.content.len() <= 512);
        let value = result_json(&result);
        assert_eq!(value["status"].as_str(), Some("error"));
        let text = value["error"].as_str().expect("error must be a string");
        for secret in [
            "bearer-secret",
            "key-secret",
            "tok-secret",
            "sk-live-secret",
        ] {
            assert!(!text.contains(secret), "`{secret}` leaked in `{text}`");
        }
        assert!(text.contains(REDACTED));
    }

    #[test]
    fn an_oversized_error_stays_an_error_at_the_minimum_limit() {
        let outcome = ChildOutcome::Error {
            rounds: 7,
            tool_calls: 9,
            error: format!("api_key={} {}", "x".repeat(2_000), "padding ".repeat(100)),
            child_session_hash: "deadbeef".to_owned(),
        };

        let result = render_child_outcome(outcome, MIN_RESULT_BYTES);

        assert!(result.is_error);
        assert!(result.content.len() <= MIN_RESULT_BYTES);
        let value = result_json(&result);
        assert_eq!(
            object_keys(&value),
            vec![
                "answer",
                "child_session_hash",
                "error",
                "rounds",
                "status",
                "tool_calls"
            ]
        );
        assert_eq!(value["status"].as_str(), Some("error"));
        assert_eq!(value["answer"].as_str(), Some(""));
        assert_eq!(value["rounds"].as_u64(), Some(7));
        assert_eq!(value["tool_calls"].as_u64(), Some(9));
        let text = value["error"].as_str().expect("error must be a string");
        assert!(text.starts_with("api_key=[REDACTED]"), "got `{text}`");
    }

    #[test]
    fn success_answers_are_not_secret_scanned() {
        let answer = "sk-this-is-part-of-the-answer";
        let outcome = ChildOutcome::Success {
            rounds: 0,
            tool_calls: 0,
            answer: answer.to_owned(),
            child_session_hash: "deadbeef".to_owned(),
        };

        let result = render_child_outcome(outcome, 1024);

        assert!(!result.is_error);
        assert_eq!(result_json(&result)["answer"].as_str(), Some(answer));
    }

    #[test]
    fn child_session_hash_is_deterministic_lowercase_hex_and_task_sensitive() {
        let tools = narrowed(&["read_file", "bash"]);

        let hash = child_session_hash("task", &tools);

        assert_eq!(hash.len(), 8);
        assert!(
            hash.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_eq!(hash, child_session_hash("task", &tools));
        assert_eq!(hash, child_session_hash("  task  ", &tools));
        assert_ne!(hash, child_session_hash("other task", &tools));
    }

    #[test]
    fn child_session_hash_is_tool_order_independent_but_separates_names() {
        let first = child_session_hash("task", &narrowed(&["read_file", "bash"]));
        let second = child_session_hash("task", &narrowed(&["bash", "read_file"]));
        assert_eq!(first, second);

        let split_one = child_session_hash("task", &narrowed(&["ab", "c"]));
        let split_two = child_session_hash("task", &narrowed(&["a", "bc"]));
        assert_ne!(split_one, split_two);
    }

    #[test]
    fn best_effort_hash_matches_the_narrowed_encoding_for_parseable_parts() {
        let args = json!({
            "task": "  task  ",
            "tools": ["read_file", "bash", "read_file"]
        });
        assert_eq!(
            best_effort_child_session_hash(&args),
            child_session_hash("task", &narrowed(&["bash", "read_file"]))
        );

        let partial = json!({"task": 7, "tools": ["bash", 9, "read_file"]});
        assert_eq!(
            best_effort_child_session_hash(&partial),
            child_session_hash("", &narrowed(&["read_file", "bash"]))
        );
    }

    #[test]
    fn redaction_handles_json_and_colon_assignments() {
        let redacted = redact_secrets(
            r#"{"api_key": "json-secret", "token": "token-secret", "Authorization": "Bearer bearer-secret"}"#,
        );

        for secret in ["json-secret", "token-secret", "bearer-secret"] {
            assert!(
                !redacted.contains(secret),
                "`{secret}` leaked in `{redacted}`"
            );
        }
        assert!(redacted.contains(REDACTED));
    }
}
