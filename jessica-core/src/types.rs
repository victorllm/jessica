//! Core data types shared by every Jessica crate.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Identifier of an agent session.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

impl SessionId {
    /// Creates a session identifier from any string-like value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for SessionId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for SessionId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

/// Role of a message in a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// System instructions that define the agent's behaviour.
    System,
    /// Input supplied by the user.
    User,
    /// Output produced by the model.
    Assistant,
    /// Output of a tool execution, addressed to a previous tool call.
    Tool,
}

/// A single message in a conversation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Who produced the message.
    pub role: Role,
    /// Textual content of the message.
    pub content: String,
    /// Tool calls requested by an assistant message.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    /// Identifier of the tool call this message responds to.
    ///
    /// Only meaningful for [`Role::Tool`] messages, which must reference the
    /// assistant tool call whose result they carry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl Message {
    /// Creates a system message.
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    /// Creates a user message.
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    /// Creates an assistant message without tool calls.
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    /// Creates an assistant message carrying tool calls.
    pub fn assistant_with_tool_calls(
        content: impl Into<String>,
        tool_calls: Vec<ToolCall>,
    ) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
            tool_calls,
            tool_call_id: None,
        }
    }

    /// Creates a tool message carrying the result of `tool_call_id`.
    pub fn tool(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: Some(tool_call_id.into()),
        }
    }
}

/// A tool invocation requested by the model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    /// Provider-assigned identifier used to correlate the call with its result.
    pub id: String,
    /// Name of the tool to invoke.
    pub name: String,
    /// JSON arguments for the tool.
    pub arguments: serde_json::Value,
}

impl ToolCall {
    /// Creates a tool call.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        arguments: serde_json::Value,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            arguments,
        }
    }
}

/// Result of executing a [`ToolCall`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    /// Identifier of the tool call this result answers.
    pub tool_call_id: String,
    /// Textual result returned to the model.
    pub content: String,
    /// Whether the tool failed.
    pub is_error: bool,
}

impl ToolResult {
    /// Creates a successful tool result.
    pub fn success(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            tool_call_id: tool_call_id.into(),
            content: content.into(),
            is_error: false,
        }
    }

    /// Creates a failed tool result.
    pub fn error(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            tool_call_id: tool_call_id.into(),
            content: content.into(),
            is_error: true,
        }
    }
}

/// Kind of budget boundary that was crossed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    /// The token budget was exhausted.
    TokenLimit,
    /// A tool exceeded its wall-clock timeout.
    ToolTimeout,
    /// The maximum number of agent rounds was reached.
    MaxRounds,
    /// The agent stopped making progress.
    Stuck,
    /// A tool result exceeded the allowed size.
    ResultSize,
}

/// Execution budget for a session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Budget {
    /// Maximum number of tokens (prompt plus completion) allowed for the session.
    pub max_tokens: u64,
    /// Maximum wall-clock time allowed for a single tool execution.
    pub tool_timeout: Duration,
    /// Maximum number of model rounds allowed for the session.
    pub max_rounds: u32,
}

impl Budget {
    /// Creates a budget.
    pub fn new(max_tokens: u64, tool_timeout: Duration, max_rounds: u32) -> Self {
        Self {
            max_tokens,
            tool_timeout,
            max_rounds,
        }
    }
}

/// Reason a session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEndReason {
    /// The loop finished normally.
    Completed,
    /// Execution was cancelled by the caller.
    Cancelled,
    /// A budget boundary was crossed.
    BudgetExceeded,
    /// Acceptance checks kept failing until retries were exhausted.
    AcceptanceFailed,
    /// The loop terminated because of an error.
    Error,
}

/// Token usage reported by a model provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    /// Tokens consumed by the prompt.
    pub prompt_tokens: u64,
    /// Tokens produced by the model.
    pub completion_tokens: u64,
}

impl TokenUsage {
    /// Creates a token usage record.
    pub fn new(prompt_tokens: u64, completion_tokens: u64) -> Self {
        Self {
            prompt_tokens,
            completion_tokens,
        }
    }

    /// Returns the total number of tokens.
    pub fn total(&self) -> u64 {
        self.prompt_tokens + self.completion_tokens
    }
}

/// Structured event emitted by the agent loop.
///
/// Events are the observable trace of a session and must remain serializable:
/// the event store persists them verbatim for replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    /// A session started.
    SessionStarted {
        /// Identifier of the session.
        session_id: SessionId,
    },
    /// The model produced a text delta.
    ModelDelta {
        /// Text fragment produced by the model.
        text: String,
    },
    /// A tool call started executing.
    ToolCallStarted {
        /// The tool call being executed.
        call: ToolCall,
    },
    /// A tool call finished executing.
    ToolCallFinished {
        /// The result of the execution.
        result: ToolResult,
    },
    /// A policy rejected a tool call.
    PolicyDenied {
        /// The rejected tool call.
        call: ToolCall,
        /// Reason supplied by the policy.
        reason: String,
    },
    /// A budget boundary was crossed.
    BudgetExceeded {
        /// Kind of budget that was exceeded.
        kind: BudgetKind,
        /// Human-readable explanation.
        message: String,
    },
    /// An acceptance check failed.
    AcceptanceFailed {
        /// One-based retry attempt number.
        attempt: u32,
        /// Feedback to inject into the conversation.
        feedback: String,
    },
    /// The loop encountered an error.
    Error {
        /// Human-readable error description.
        message: String,
    },
    /// The session ended.
    SessionEnded {
        /// Identifier of the session.
        session_id: SessionId,
        /// Why the session ended.
        reason: SessionEndReason,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use serde_json::json;
    use std::fmt::Debug;

    fn assert_round_trip<T>(value: &T)
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug,
    {
        let encoded = serde_json::to_string(value).expect("value should serialize");
        let decoded: T = serde_json::from_str(&encoded).expect("value should deserialize");
        assert_eq!(&decoded, value);
    }

    #[test]
    fn session_id_round_trips_and_displays() {
        let id = SessionId::new("session-42");
        assert_eq!(id.as_str(), "session-42");
        assert_eq!(id.to_string(), "session-42");
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"session-42\"");
        assert_round_trip(&id);
        assert_eq!(SessionId::from("from-str"), SessionId::new("from-str"));
        assert_eq!(
            SessionId::from(String::from("from-string")),
            SessionId::new("from-string")
        );
    }

    #[test]
    fn role_uses_openai_compatible_names() {
        assert_eq!(serde_json::to_string(&Role::System).unwrap(), "\"system\"");
        assert_eq!(serde_json::to_string(&Role::User).unwrap(), "\"user\"");
        assert_eq!(
            serde_json::to_string(&Role::Assistant).unwrap(),
            "\"assistant\""
        );
        assert_eq!(serde_json::to_string(&Role::Tool).unwrap(), "\"tool\"");
        for role in [Role::System, Role::User, Role::Assistant, Role::Tool] {
            assert_round_trip(&role);
        }
    }

    #[test]
    fn user_message_omits_empty_tool_fields() {
        let message = Message::user("hello");
        assert_eq!(message.role, Role::User);
        assert_eq!(message.content, "hello");
        assert!(message.tool_calls.is_empty());
        assert!(message.tool_call_id.is_none());

        let encoded = serde_json::to_value(&message).unwrap();
        assert_eq!(encoded, json!({"role": "user", "content": "hello"}));
        assert_round_trip(&message);
    }

    #[test]
    fn assistant_message_carries_tool_calls() {
        let call = ToolCall::new("call-1", "read_file", json!({"path": "README.md"}));
        let message = Message::assistant_with_tool_calls("", vec![call.clone()]);
        assert_eq!(message.role, Role::Assistant);
        assert_eq!(message.tool_calls, vec![call]);
        assert_round_trip(&message);
    }

    #[test]
    fn tool_message_carries_call_id() {
        let message = Message::tool("call-1", "file contents");
        assert_eq!(message.role, Role::Tool);
        assert_eq!(message.tool_call_id.as_deref(), Some("call-1"));
        assert_round_trip(&message);
    }

    #[test]
    fn tool_call_and_result_round_trip() {
        let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));
        assert_round_trip(&call);

        let success = ToolResult::success("call-1", "ok");
        assert!(!success.is_error);
        assert_round_trip(&success);

        let failure = ToolResult::error("call-1", "boom");
        assert!(failure.is_error);
        assert_round_trip(&failure);
    }

    #[test]
    fn budget_round_trips_with_duration() {
        let budget = Budget::new(1_000, Duration::from_millis(1500), 8);
        assert_eq!(budget.max_tokens, 1_000);
        assert_eq!(budget.tool_timeout, Duration::from_millis(1500));
        assert_eq!(budget.max_rounds, 8);
        assert_round_trip(&budget);
    }

    #[test]
    fn budget_kinds_and_end_reasons_round_trip() {
        for kind in [
            BudgetKind::TokenLimit,
            BudgetKind::ToolTimeout,
            BudgetKind::MaxRounds,
            BudgetKind::Stuck,
            BudgetKind::ResultSize,
        ] {
            assert_round_trip(&kind);
        }

        for reason in [
            SessionEndReason::Completed,
            SessionEndReason::Cancelled,
            SessionEndReason::BudgetExceeded,
            SessionEndReason::AcceptanceFailed,
            SessionEndReason::Error,
        ] {
            assert_round_trip(&reason);
        }
    }

    #[test]
    fn token_usage_totals_prompt_and_completion() {
        let usage = TokenUsage::new(120, 30);
        assert_eq!(usage.total(), 150);
        assert_eq!(usage.prompt_tokens, 120);
        assert_eq!(usage.completion_tokens, 30);
        assert_round_trip(&usage);
        assert_eq!(TokenUsage::default().total(), 0);
    }

    #[test]
    fn every_agent_event_round_trips_with_type_tag() {
        let session_id = SessionId::new("session-1");
        let call = ToolCall::new("call-1", "read_file", json!({"path": "Cargo.toml"}));
        let result = ToolResult::success("call-1", "contents");

        let events = vec![
            (
                AgentEvent::SessionStarted {
                    session_id: session_id.clone(),
                },
                "session_started",
            ),
            (AgentEvent::ModelDelta { text: "hi".into() }, "model_delta"),
            (
                AgentEvent::ToolCallStarted { call: call.clone() },
                "tool_call_started",
            ),
            (
                AgentEvent::ToolCallFinished {
                    result: result.clone(),
                },
                "tool_call_finished",
            ),
            (
                AgentEvent::PolicyDenied {
                    call: call.clone(),
                    reason: "not allowed".into(),
                },
                "policy_denied",
            ),
            (
                AgentEvent::BudgetExceeded {
                    kind: BudgetKind::MaxRounds,
                    message: "rounds exhausted".into(),
                },
                "budget_exceeded",
            ),
            (
                AgentEvent::AcceptanceFailed {
                    attempt: 1,
                    feedback: "tests failed".into(),
                },
                "acceptance_failed",
            ),
            (
                AgentEvent::Error {
                    message: "provider failed".into(),
                },
                "error",
            ),
            (
                AgentEvent::SessionEnded {
                    session_id,
                    reason: SessionEndReason::Completed,
                },
                "session_ended",
            ),
        ];

        for (event, tag) in events {
            let encoded = serde_json::to_value(&event).unwrap();
            assert_eq!(encoded.get("type").and_then(|v| v.as_str()), Some(tag));
            assert_round_trip(&event);
        }
    }
}
