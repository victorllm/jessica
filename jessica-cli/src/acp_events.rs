//! Mapping from Jessica's structured agent events to ACP session updates.
//!
//! Only model text and tool lifecycle events cross the protocol boundary.
//! Policy denials are already represented by the failed tool result, session
//! lifecycle events stay internal, and errors are returned on the prompt
//! response instead of the update stream.

use std::path::{Path, PathBuf};

use agent_client_protocol::v1::schema::{
    Content, ContentBlock, ContentChunk, MessageId, SessionUpdate, TextContent, ToolCall,
    ToolCallContent, ToolCallLocation, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
    ToolKind,
};
use jessica_core::types::{AgentEvent, BudgetKind};
use serde_json::Value;

/// Maximum characters retained in a tool-call title.
const TOOL_TITLE_CHARS: usize = 120;
/// Maximum bytes retained in a raw tool input or output value.
const RAW_VALUE_BYTES: usize = 16 * 1024;

/// Maps one prompt's agent events to ACP updates.
pub(crate) struct AcpEventMapper {
    /// Session working directory used to make tool locations absolute.
    cwd: PathBuf,
    /// The single message id shared by every text chunk in this prompt.
    message_id: MessageId,
}

impl AcpEventMapper {
    /// Creates a mapper for one prompt.
    pub(crate) fn new(cwd: PathBuf, message_id: MessageId) -> Self {
        Self { cwd, message_id }
    }

    /// Converts an event, returning `None` for events with no protocol update.
    pub(crate) fn map(&self, event: &AgentEvent) -> Option<SessionUpdate> {
        match event {
            AgentEvent::ModelDelta { text } if !text.is_empty() => {
                Some(SessionUpdate::AgentMessageChunk(
                    ContentChunk::new(ContentBlock::Text(TextContent::new(text.clone())))
                        .message_id(self.message_id.clone()),
                ))
            }
            AgentEvent::ToolCallStarted { call } => {
                let mut started = ToolCall::new(call.id.clone(), tool_title(&call.name, &call.arguments))
                    .name(call.name.clone())
                    .kind(tool_kind(&call.name))
                    .status(ToolCallStatus::InProgress)
                    .raw_input(truncate_raw_input(&call.arguments));
                let locations = tool_locations(&self.cwd, &call.name, &call.arguments);
                if !locations.is_empty() {
                    started = started.locations(locations);
                }
                Some(SessionUpdate::ToolCall(started))
            }
            AgentEvent::ToolCallFinished { result } => {
                let status = if result.is_error {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Completed
                };
                let text = truncate_text(&result.content, RAW_VALUE_BYTES);
                let content = vec![ToolCallContent::Content(Content::new(
                    ContentBlock::Text(TextContent::new(text.clone())),
                ))];
                let fields = ToolCallUpdateFields::new()
                    .status(status)
                    .content(content)
                    .raw_output(Value::String(text));
                Some(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                    result.tool_call_id.clone(),
                    fields,
                )))
            }
            // Policy denial is followed by a failed ToolCallFinished event,
            // which is the single failed update for that call.
            AgentEvent::PolicyDenied { .. }
            // Session lifecycle stays internal; the prompt response carries
            // the final reason.
            | AgentEvent::SessionStarted { .. }
            | AgentEvent::SessionEnded { .. }
            // Errors, acceptance failures, and budget boundaries are mapped
            // to prompt errors or stop reasons, not streamed text.
            | AgentEvent::BudgetExceeded { .. }
            | AgentEvent::AcceptanceFailed { .. }
            | AgentEvent::Error { .. } => None,
            AgentEvent::ModelDelta { .. } => None,
        }
    }
}

/// Maps a budget kind to the ACP stop reason it represents.
pub(crate) fn budget_stop_reason(kind: Option<BudgetKind>) -> BudgetStopReason {
    match kind {
        Some(BudgetKind::TokenLimit) => BudgetStopReason::MaxTokens,
        Some(_) | None => BudgetStopReason::MaxTurnRequests,
    }
}

/// Stop-reason class selected by a budget event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BudgetStopReason {
    /// The token budget was exhausted.
    MaxTokens,
    /// A round, time, stuck, or result-size boundary stopped the turn.
    MaxTurnRequests,
}

/// Maps a tool name to its ACP category.
fn tool_kind(name: &str) -> ToolKind {
    match name {
        "read_file" => ToolKind::Read,
        "write_file" => ToolKind::Edit,
        "bash" | "delegate_task" => ToolKind::Execute,
        _ => ToolKind::Other,
    }
}

/// Builds a concise, bounded title from a tool's key argument.
fn tool_title(name: &str, arguments: &Value) -> String {
    let detail = match name {
        "read_file" | "write_file" => string_argument(arguments, "path"),
        "bash" => string_argument(arguments, "command"),
        "delegate_task" => string_argument(arguments, "task"),
        _ => string_argument(arguments, "path")
            .or_else(|| string_argument(arguments, "command"))
            .or_else(|| string_argument(arguments, "task")),
    };
    let title = match detail {
        Some(detail) if !detail.trim().is_empty() => format!("{name}: {detail}"),
        _ => name.to_owned(),
    };
    truncate_chars(&single_line(&title), TOOL_TITLE_CHARS)
}

/// Returns a non-empty string argument.
fn string_argument<'a>(arguments: &'a Value, key: &str) -> Option<&'a str> {
    arguments.get(key).and_then(Value::as_str)
}

/// Collapses line breaks so a title remains one line.
fn single_line(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\n' | '\r' => ' ',
            other => other,
        })
        .collect()
}

/// Builds absolute file locations for read and write tools.
fn tool_locations(cwd: &Path, name: &str, arguments: &Value) -> Vec<ToolCallLocation> {
    if !matches!(name, "read_file" | "write_file") {
        return Vec::new();
    }
    let Some(path) = string_argument(arguments, "path").filter(|path| !path.is_empty()) else {
        return Vec::new();
    };
    let path = Path::new(path);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    vec![ToolCallLocation::new(absolute)]
}

/// Preserves a JSON value when it fits, otherwise truncates its encoding.
fn truncate_raw_input(value: &Value) -> Value {
    let encoded = value.to_string();
    if encoded.len() <= RAW_VALUE_BYTES {
        value.clone()
    } else {
        Value::String(truncate_text(&encoded, RAW_VALUE_BYTES))
    }
}

/// Truncates to at most `max_chars` Unicode scalar values.
fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        truncated
    } else {
        value.to_owned()
    }
}

/// Truncates to at most `max_bytes` without splitting a UTF-8 code point.
fn truncate_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    use jessica_core::types::{SessionEndReason, ToolCall, ToolResult};
    use serde_json::json;

    fn mapper() -> AcpEventMapper {
        AcpEventMapper::new(PathBuf::from("/workspace"), MessageId::new("turn-0"))
    }

    #[test]
    fn model_text_maps_to_one_message_id_and_empty_deltas_are_ignored() {
        let mapper = mapper();
        let first = mapper
            .map(&AgentEvent::ModelDelta {
                text: "hel".to_owned(),
            })
            .expect("text must map");
        let second = mapper
            .map(&AgentEvent::ModelDelta {
                text: "lo".to_owned(),
            })
            .expect("text must map");

        let text_of = |update: &SessionUpdate| match update {
            SessionUpdate::AgentMessageChunk(chunk) => {
                assert_eq!(
                    chunk
                        .message_id
                        .as_ref()
                        .expect("chunks must carry a message id")
                        .to_string(),
                    "turn-0"
                );
                match &chunk.content {
                    ContentBlock::Text(text) => text.text.clone(),
                    other => panic!("unexpected chunk content: {other:?}"),
                }
            }
            other => panic!("unexpected update: {other:?}"),
        };
        assert_eq!(text_of(&first), "hel");
        assert_eq!(text_of(&second), "lo");
        assert!(
            mapper
                .map(&AgentEvent::ModelDelta {
                    text: String::new()
                })
                .is_none(),
            "an empty delta must not produce a chunk"
        );
    }

    #[test]
    fn started_read_is_in_progress_with_kind_title_input_and_location() {
        let update = mapper()
            .map(&AgentEvent::ToolCallStarted {
                call: ToolCall::new("read-1", "read_file", json!({"path": "src/main.rs"})),
            })
            .expect("tool start must map");

        let SessionUpdate::ToolCall(call) = update else {
            panic!("expected a tool_call update");
        };
        assert_eq!(call.tool_call_id.to_string(), "read-1");
        assert_eq!(call.status, ToolCallStatus::InProgress);
        assert_eq!(call.kind, ToolKind::Read);
        assert_eq!(call.name.as_deref(), Some("read_file"));
        assert_eq!(call.title, "read_file: src/main.rs");
        assert_eq!(call.raw_input, Some(json!({"path": "src/main.rs"})));
        assert_eq!(call.locations.len(), 1);
        assert_eq!(
            call.locations[0].path,
            PathBuf::from("/workspace/src/main.rs")
        );
    }

    #[test]
    fn finished_tools_map_completed_and_failed_with_truncated_output() {
        let mapper = mapper();
        let success = mapper
            .map(&AgentEvent::ToolCallFinished {
                result: ToolResult::success("read-1", "contents"),
            })
            .expect("success must map");
        let failure = mapper
            .map(&AgentEvent::ToolCallFinished {
                result: ToolResult::error("read-2", "missing"),
            })
            .expect("failure must map");

        for (update, expected, output) in [
            (success, ToolCallStatus::Completed, "contents"),
            (failure, ToolCallStatus::Failed, "missing"),
        ] {
            let SessionUpdate::ToolCallUpdate(update) = update else {
                panic!("expected a tool_call_update");
            };
            assert_eq!(update.fields.status, Some(expected));
            assert_eq!(
                update.fields.raw_output,
                Some(Value::String(output.to_owned()))
            );
            let content = update.fields.content.expect("content must be present");
            assert_eq!(content.len(), 1);
            let ToolCallContent::Content(content) = &content[0] else {
                panic!("expected text content");
            };
            let ContentBlock::Text(text) = &content.content else {
                panic!("expected text content block");
            };
            assert_eq!(text.text, output);
        }
    }

    #[test]
    fn policy_denial_and_lifecycle_events_do_not_duplicate_updates() {
        let mapper = mapper();
        let denied = AgentEvent::PolicyDenied {
            call: ToolCall::new("call", "bash", json!({"command": "rm -rf /"})),
            reason: "not allowed".to_owned(),
        };
        assert!(mapper.map(&denied).is_none());
        assert!(
            mapper
                .map(&AgentEvent::SessionStarted {
                    session_id: jessica_core::types::SessionId::new("s")
                })
                .is_none()
        );
        assert!(
            mapper
                .map(&AgentEvent::SessionEnded {
                    session_id: jessica_core::types::SessionId::new("s"),
                    reason: SessionEndReason::Completed,
                })
                .is_none()
        );
    }

    #[test]
    fn titles_are_bounded_to_120_characters_and_raw_values_to_16_kib() {
        let long_command = "x".repeat(400);
        let update = mapper()
            .map(&AgentEvent::ToolCallStarted {
                call: ToolCall::new("bash-1", "bash", json!({"command": long_command})),
            })
            .expect("tool start must map");
        let SessionUpdate::ToolCall(call) = update else {
            panic!("expected tool call");
        };
        assert_eq!(call.title.chars().count(), 120);
        assert_eq!(call.kind, ToolKind::Execute);

        let oversized = "y".repeat(20 * 1024);
        let update = mapper()
            .map(&AgentEvent::ToolCallStarted {
                call: ToolCall::new("bash-2", "bash", json!({"command": oversized})),
            })
            .expect("tool start must map");
        let SessionUpdate::ToolCall(call) = update else {
            panic!("expected tool call");
        };
        let raw = call.raw_input.expect("raw input must be present");
        let encoded = raw
            .as_str()
            .expect("oversized raw input must become a truncated string");
        assert!(encoded.len() <= RAW_VALUE_BYTES);

        let update = mapper()
            .map(&AgentEvent::ToolCallFinished {
                result: ToolResult::success("bash-2", "z".repeat(20 * 1024)),
            })
            .expect("tool finish must map");
        let SessionUpdate::ToolCallUpdate(update) = update else {
            panic!("expected tool update");
        };
        let raw = update
            .fields
            .raw_output
            .and_then(|value| value.as_str().map(str::to_owned))
            .expect("raw output must be a string");
        assert_eq!(raw.len(), RAW_VALUE_BYTES);
    }

    #[test]
    fn unknown_tools_and_delegation_use_other_and_execute_kinds() {
        let mapper = mapper();
        let cases = [
            ("write_file", ToolKind::Edit),
            ("delegate_task", ToolKind::Execute),
            ("wasm_echo", ToolKind::Other),
        ];
        for (name, expected) in cases {
            let update = mapper
                .map(&AgentEvent::ToolCallStarted {
                    call: ToolCall::new("id", name, json!({"path": "a"})),
                })
                .expect("tool start must map");
            let SessionUpdate::ToolCall(call) = update else {
                panic!("expected tool call");
            };
            assert_eq!(call.kind, expected, "wrong kind for {name}");
        }
    }

    #[test]
    fn budget_stop_reasons_distinguish_tokens_from_other_boundaries() {
        assert_eq!(
            budget_stop_reason(Some(BudgetKind::TokenLimit)),
            BudgetStopReason::MaxTokens
        );
        for kind in [
            BudgetKind::MaxRounds,
            BudgetKind::ToolTimeout,
            BudgetKind::Stuck,
            BudgetKind::ResultSize,
        ] {
            assert_eq!(
                budget_stop_reason(Some(kind)),
                BudgetStopReason::MaxTurnRequests
            );
        }
        assert_eq!(budget_stop_reason(None), BudgetStopReason::MaxTurnRequests);
    }
}
