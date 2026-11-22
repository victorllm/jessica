//! Mapping from Jessica's structured agent events to ACP session updates.
//!
//! Only model text and tool lifecycle events cross the protocol boundary.
//! Policy denials are already represented by the failed tool result, session
//! lifecycle events stay internal, and errors are returned on the prompt
//! response instead of the update stream.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use agent_client_protocol::v1::schema::{
    Content, ContentBlock, ContentChunk, MessageId, SessionUpdate, TextContent,
    ToolCall as AcpToolCall, ToolCallContent, ToolCallLocation, ToolCallStatus, ToolCallUpdate,
    ToolCallUpdateFields, ToolKind,
};
use jessica_core::types::{AgentEvent, BudgetKind, Message, Role, ToolCall};
use serde_json::Value;

use crate::governance::permission_required;

/// Maximum characters retained in a tool-call title.
const TOOL_TITLE_CHARS: usize = 120;
/// Maximum bytes retained in a raw tool input or output value.
const RAW_VALUE_BYTES: usize = 16 * 1024;
/// Content used to fail a tool call that never produced a result.
const CANCELLED_TOOL_CONTENT: &str = "tool execution was cancelled";

/// Maps one prompt's agent events to ACP updates.
#[derive(Clone)]
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
                let status = if permission_required(&call.name) {
                    // The call waits for client approval; `in_progress` is
                    // sent only after the permission gate allows it.
                    ToolCallStatus::Pending
                } else {
                    ToolCallStatus::InProgress
                };
                let mut started = AcpToolCall::new(
                    call.id.clone(),
                    tool_title(&call.name, &call.arguments),
                )
                    .name(call.name.clone())
                    .kind(tool_kind(&call.name))
                    .status(status)
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
            // Session and round lifecycle stays internal; the prompt response
            // carries the final reason.
            | AgentEvent::SessionStarted { .. }
            | AgentEvent::RoundStarted { .. }
            | AgentEvent::RoundFinished { .. }
            | AgentEvent::SessionEnded { .. }
            // Permission outcomes are reflected by the failed tool result, and
            // child sessions never cross the protocol boundary.
            | AgentEvent::PermissionDecided { .. }
            | AgentEvent::ChildSessionStarted { .. }
            | AgentEvent::ChildSessionEnded { .. }
            // Errors, acceptance failures, and budget boundaries are mapped
            // to prompt errors or stop reasons, not streamed text.
            | AgentEvent::BudgetExceeded { .. }
            | AgentEvent::AcceptanceFailed { .. }
            | AgentEvent::Error { .. } => None,
            AgentEvent::ModelDelta { .. } => None,
        }
    }

    /// Builds the `pending` tool-call payload of a permission request.
    ///
    /// The request repeats the identity and display fields of the call the
    /// client already received through `tool_call`, using the same provider
    /// id. It carries no command output or environment data.
    pub(crate) fn permission_request_update(&self, call: &ToolCall) -> ToolCallUpdate {
        let mut fields = ToolCallUpdateFields::new()
            .title(tool_title(&call.name, &call.arguments))
            .name(call.name.clone())
            .kind(tool_kind(&call.name))
            .status(ToolCallStatus::Pending)
            .raw_input(truncate_raw_input(&call.arguments));
        let locations = tool_locations(&self.cwd, &call.name, &call.arguments);
        if !locations.is_empty() {
            fields = fields.locations(locations);
        }
        ToolCallUpdate::new(call.id.clone(), fields)
    }

    /// Builds the update that moves an approved call to `in_progress`.
    ///
    /// This is sent only after the client selected an allowing option and
    /// before the tool starts, so `pending` never turns directly into a
    /// terminal status without an execution phase.
    pub(crate) fn permission_granted_update(&self, call: &ToolCall) -> ToolCallUpdate {
        ToolCallUpdate::new(
            call.id.clone(),
            ToolCallUpdateFields::new().status(ToolCallStatus::InProgress),
        )
    }
}

/// Tracks tool calls announced but not yet finished for one prompt.
///
/// A cancelled or failed turn can end between `ToolCallStarted` and
/// `ToolCallFinished`. The worker drains this set and emits one failed update
/// for each remainder, so a client never observes a permanently in-progress
/// tool call.
#[derive(Default)]
pub(crate) struct InFlightTools {
    /// Calls in announcement order.
    calls: Mutex<Vec<ToolCall>>,
}

impl InFlightTools {
    /// Records a started call or removes a finished one.
    pub(crate) fn observe(&self, event: &AgentEvent) {
        match event {
            AgentEvent::ToolCallStarted { call } => {
                let mut calls = self.lock();
                if !calls.iter().any(|tracked| tracked.id == call.id) {
                    calls.push(call.clone());
                }
            }
            AgentEvent::ToolCallFinished { result } => {
                self.lock()
                    .retain(|tracked| tracked.id != result.tool_call_id);
            }
            _ => {}
        }
    }

    /// Removes and returns every call that has not finished.
    pub(crate) fn take_pending(&self) -> Vec<ToolCall> {
        std::mem::take(&mut *self.lock())
    }

    /// Locks the tracked calls, recovering from poisoning.
    fn lock(&self) -> MutexGuard<'_, Vec<ToolCall>> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Builds the failed update that closes a tool call interrupted by the turn.
pub(crate) fn interrupted_tool_update(call: &jessica_core::types::ToolCall) -> SessionUpdate {
    let content = vec![ToolCallContent::Content(Content::new(ContentBlock::Text(
        TextContent::new(CANCELLED_TOOL_CONTENT),
    )))];
    let fields = ToolCallUpdateFields::new()
        .status(ToolCallStatus::Failed)
        .content(content)
        .raw_output(Value::String(CANCELLED_TOOL_CONTENT.to_owned()));
    SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(call.id.clone(), fields))
}

/// Projects committed conversation history into replay updates.
///
/// ACP requires `session/load` to send the stored conversation through
/// `session/update` before its response. System prompts and injected memory or
/// skill guides stay internal. Each user and assistant text message gets its
/// own message id, and tool calls are announced before their recorded results.
/// Tool results without a preceding tool call are omitted so a client never
/// receives an update for an id it has not seen.
pub(crate) fn history_updates(cwd: &Path, history: &[Message]) -> Vec<SessionUpdate> {
    let mut updates = Vec::new();
    let mut announced_calls = HashSet::new();
    for (position, message) in history.iter().enumerate() {
        match message.role {
            Role::System => {}
            Role::User if !message.content.is_empty() => {
                updates.push(SessionUpdate::UserMessageChunk(
                    ContentChunk::new(ContentBlock::Text(TextContent::new(
                        message.content.clone(),
                    )))
                    .message_id(MessageId::new(format!("history-{position}-user"))),
                ));
            }
            Role::User => {}
            Role::Assistant => {
                if !message.content.is_empty() {
                    updates.push(SessionUpdate::AgentMessageChunk(
                        ContentChunk::new(ContentBlock::Text(TextContent::new(
                            message.content.clone(),
                        )))
                        .message_id(MessageId::new(format!("history-{position}-assistant"))),
                    ));
                }
                for call in &message.tool_calls {
                    announced_calls.insert(call.id.as_str());
                    let mut started =
                        AcpToolCall::new(call.id.clone(), tool_title(&call.name, &call.arguments))
                            .name(call.name.clone())
                            .kind(tool_kind(&call.name))
                            .status(ToolCallStatus::InProgress)
                            .raw_input(truncate_raw_input(&call.arguments));
                    let locations = tool_locations(cwd, &call.name, &call.arguments);
                    if !locations.is_empty() {
                        started = started.locations(locations);
                    }
                    updates.push(SessionUpdate::ToolCall(started));
                }
            }
            Role::Tool => {
                let Some(tool_call_id) = &message.tool_call_id else {
                    continue;
                };
                if !announced_calls.contains(tool_call_id.as_str()) {
                    continue;
                }
                let status = if message.content.starts_with("tool error:") {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Completed
                };
                let text = truncate_text(&message.content, RAW_VALUE_BYTES);
                let content = vec![ToolCallContent::Content(Content::new(ContentBlock::Text(
                    TextContent::new(text.clone()),
                )))];
                let fields = ToolCallUpdateFields::new()
                    .status(status)
                    .content(content)
                    .raw_output(Value::String(text));
                updates.push(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                    tool_call_id.clone(),
                    fields,
                )));
            }
        }
    }
    updates
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
        "read_file" | "list_dir" | "grep" | "git_status" | "git_diff" => ToolKind::Read,
        "write_file" => ToolKind::Edit,
        "bash" | "exec" | "delegate_task" => ToolKind::Execute,
        _ => ToolKind::Other,
    }
}

/// Builds a concise, bounded title from a tool's key argument.
fn tool_title(name: &str, arguments: &Value) -> String {
    let detail = match name {
        "read_file" | "write_file" | "list_dir" => string_argument(arguments, "path"),
        "grep" => {
            string_argument(arguments, "pattern").or_else(|| string_argument(arguments, "path"))
        }
        "bash" => string_argument(arguments, "command"),
        "exec" => string_argument(arguments, "program"),
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

/// Builds absolute file locations for path-taking file tools.
fn tool_locations(cwd: &Path, name: &str, arguments: &Value) -> Vec<ToolCallLocation> {
    if !matches!(name, "read_file" | "write_file" | "list_dir" | "grep") {
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

    use jessica_core::types::{Message, SessionEndReason, ToolCall, ToolResult};
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
    fn sensitive_tools_start_pending_and_others_start_in_progress() {
        let mapper = mapper();
        for (name, expected) in [
            ("bash", ToolCallStatus::Pending),
            ("exec", ToolCallStatus::Pending),
            ("write_file", ToolCallStatus::Pending),
            ("delegate_task", ToolCallStatus::Pending),
            ("read_file", ToolCallStatus::InProgress),
            ("list_dir", ToolCallStatus::InProgress),
            ("grep", ToolCallStatus::InProgress),
            ("git_status", ToolCallStatus::InProgress),
            ("git_diff", ToolCallStatus::InProgress),
            ("mcp__stub__echo", ToolCallStatus::InProgress),
        ] {
            let update = mapper
                .map(&AgentEvent::ToolCallStarted {
                    call: ToolCall::new("call-1", name, json!({"path": "a"})),
                })
                .expect("tool start must map");
            let SessionUpdate::ToolCall(call) = update else {
                panic!("expected a tool_call update");
            };
            assert_eq!(call.status, expected, "wrong start status for {name}");
        }
    }

    #[test]
    fn permission_request_update_reuses_the_call_id_and_display_fields() {
        let mapper = mapper();
        let call = ToolCall::new(
            "call_write",
            "write_file",
            json!({"path": "out/result.txt", "content": "data"}),
        );

        let update = mapper.permission_request_update(&call);

        assert_eq!(update.tool_call_id.to_string(), "call_write");
        assert_eq!(update.fields.status, Some(ToolCallStatus::Pending));
        assert_eq!(
            update.fields.title.as_deref(),
            Some("write_file: out/result.txt")
        );
        assert_eq!(update.fields.name.as_deref(), Some("write_file"));
        assert_eq!(update.fields.kind, Some(ToolKind::Edit));
        assert_eq!(
            update.fields.raw_input,
            Some(json!({"path": "out/result.txt", "content": "data"}))
        );
        assert_eq!(update.fields.locations.as_ref().map(Vec::len), Some(1));
        assert_eq!(
            update.fields.locations.expect("locations must be present")[0].path,
            PathBuf::from("/workspace/out/result.txt")
        );
    }

    #[test]
    fn permission_granted_update_moves_only_the_same_call_to_in_progress() {
        let mapper = mapper();
        let call = ToolCall::new("call_bash", "bash", json!({"command": "true"}));

        let update = mapper.permission_granted_update(&call);

        assert_eq!(update.tool_call_id.to_string(), "call_bash");
        assert_eq!(update.fields.status, Some(ToolCallStatus::InProgress));
        assert!(update.fields.title.is_none());
        assert!(update.fields.content.is_none());
        assert!(update.fields.raw_output.is_none());
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
                    session_id: jessica_core::types::SessionId::new("s"),
                    messages: Vec::new(),
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
            ("exec", ToolKind::Execute),
            ("delegate_task", ToolKind::Execute),
            ("list_dir", ToolKind::Read),
            ("grep", ToolKind::Read),
            ("git_status", ToolKind::Read),
            ("git_diff", ToolKind::Read),
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
    fn new_native_tools_map_their_primary_argument_to_kind_and_location() {
        let mapper = mapper();
        let cases = [
            (
                "exec",
                json!({"program": "git", "args": ["status"]}),
                ToolKind::Execute,
                "exec: git",
                None,
            ),
            (
                "grep",
                json!({"pattern": "needle", "path": "src"}),
                ToolKind::Read,
                "grep: needle",
                Some(PathBuf::from("/workspace/src")),
            ),
            (
                "list_dir",
                json!({"path": "src"}),
                ToolKind::Read,
                "list_dir: src",
                Some(PathBuf::from("/workspace/src")),
            ),
            ("git_status", json!({}), ToolKind::Read, "git_status", None),
            ("git_diff", json!({}), ToolKind::Read, "git_diff", None),
        ];
        for (name, arguments, expected_kind, expected_title, expected_location) in cases {
            let update = mapper
                .map(&AgentEvent::ToolCallStarted {
                    call: ToolCall::new("id", name, arguments),
                })
                .expect("tool start must map");
            let SessionUpdate::ToolCall(call) = update else {
                panic!("expected a tool call");
            };
            assert_eq!(call.kind, expected_kind, "wrong kind for {name}");
            assert_eq!(call.title, expected_title, "wrong title for {name}");
            assert_eq!(
                call.locations.first().map(|location| location.path.clone()),
                expected_location,
                "wrong location for {name}"
            );
        }
    }

    #[test]
    fn interrupted_tools_are_closed_once_and_only_unfinished_calls_remain() {
        let tools = InFlightTools::default();
        tools.observe(&AgentEvent::ToolCallStarted {
            call: ToolCall::new("read", "read_file", json!({"path": "a"})),
        });
        tools.observe(&AgentEvent::ToolCallStarted {
            call: ToolCall::new("bash", "bash", json!({"command": "sleep 1"})),
        });
        tools.observe(&AgentEvent::ToolCallFinished {
            result: ToolResult::success("read", "contents"),
        });

        let pending = tools.take_pending();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "bash");
        let update = interrupted_tool_update(&pending[0]);
        let SessionUpdate::ToolCallUpdate(update) = update else {
            panic!("expected a tool update");
        };
        assert_eq!(update.tool_call_id.to_string(), "bash");
        assert_eq!(update.fields.status, Some(ToolCallStatus::Failed));
        assert_eq!(
            update.fields.raw_output,
            Some(Value::String(CANCELLED_TOOL_CONTENT.to_owned()))
        );
        assert!(
            tools.take_pending().is_empty(),
            "closing an interrupted call must be a one-shot drain"
        );
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

    #[test]
    fn history_replays_user_assistant_text_and_tool_lifecycle_in_order() {
        let history = vec![
            Message::system("internal bootstrap"),
            Message::user("read NOTE.txt"),
            Message::assistant_with_tool_calls(
                "checking",
                vec![ToolCall::new(
                    "call_read",
                    "read_file",
                    json!({"path": "NOTE.txt"}),
                )],
            ),
            Message::tool("call_read", "note contents"),
            Message::assistant("done"),
            Message::user("thanks"),
        ];

        let updates = history_updates(Path::new("/workspace"), &history);

        let kinds: Vec<&str> = updates
            .iter()
            .map(|update| match update {
                SessionUpdate::UserMessageChunk(_) => "user_message_chunk",
                SessionUpdate::AgentMessageChunk(_) => "agent_message_chunk",
                SessionUpdate::ToolCall(_) => "tool_call",
                SessionUpdate::ToolCallUpdate(_) => "tool_call_update",
                other => panic!("unexpected history update: {other:?}"),
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                "user_message_chunk",
                "agent_message_chunk",
                "tool_call",
                "tool_call_update",
                "agent_message_chunk",
                "user_message_chunk",
            ],
            "system bootstrap must be omitted and tool calls must precede results"
        );
        assert!(
            !serde_json::to_string(&updates)
                .expect("updates must serialize")
                .contains("internal bootstrap")
        );

        let SessionUpdate::ToolCall(started) = &updates[2] else {
            panic!("expected tool call");
        };
        assert_eq!(started.tool_call_id.to_string(), "call_read");
        assert_eq!(started.title, "read_file: NOTE.txt");
        assert_eq!(started.status, ToolCallStatus::InProgress);
        assert_eq!(started.kind, ToolKind::Read);
        assert_eq!(
            started.locations[0].path,
            PathBuf::from("/workspace/NOTE.txt")
        );

        let SessionUpdate::ToolCallUpdate(finished) = &updates[3] else {
            panic!("expected tool update");
        };
        assert_eq!(finished.tool_call_id.to_string(), "call_read");
        assert_eq!(finished.fields.status, Some(ToolCallStatus::Completed));
        assert_eq!(
            finished.fields.raw_output,
            Some(Value::String("note contents".to_owned()))
        );
    }

    #[test]
    fn history_replays_failed_tools_and_gives_text_messages_distinct_ids() {
        let history = vec![
            Message::user("first"),
            Message::assistant("first answer"),
            Message::assistant_with_tool_calls(
                "",
                vec![ToolCall::new(
                    "call_bash",
                    "bash",
                    json!({"command": "false"}),
                )],
            ),
            Message::tool("call_bash", "tool error: exit status 1"),
        ];

        let updates = history_updates(Path::new("/workspace"), &history);

        let SessionUpdate::AgentMessageChunk(answer) = &updates[1] else {
            panic!("expected assistant text");
        };
        let SessionUpdate::UserMessageChunk(question) = &updates[0] else {
            panic!("expected user text");
        };
        assert_ne!(
            answer.message_id, question.message_id,
            "distinct history messages must not merge in the client"
        );
        let SessionUpdate::ToolCallUpdate(failed) = &updates[3] else {
            panic!("expected tool update");
        };
        assert_eq!(failed.fields.status, Some(ToolCallStatus::Failed));
        assert_eq!(
            failed.fields.raw_output,
            Some(Value::String("tool error: exit status 1".to_owned()))
        );
    }

    #[test]
    fn history_skips_orphan_tool_messages_and_truncates_large_results() {
        let history = vec![
            Message::tool("orphan", "unattached"),
            Message::assistant_with_tool_calls(
                "",
                vec![ToolCall::new("call", "read_file", json!({"path": "a"}))],
            ),
            Message::tool("call", "x".repeat(RAW_VALUE_BYTES + 100)),
        ];

        let updates = history_updates(Path::new("/workspace"), &history);

        assert_eq!(updates.len(), 2, "the orphan tool message must be skipped");
        assert!(matches!(updates[0], SessionUpdate::ToolCall(_)));
        let SessionUpdate::ToolCallUpdate(update) = &updates[1] else {
            panic!("expected tool update");
        };
        let raw = update
            .fields
            .raw_output
            .as_ref()
            .and_then(Value::as_str)
            .expect("raw output must be text");
        assert_eq!(raw.len(), RAW_VALUE_BYTES);
    }
}
