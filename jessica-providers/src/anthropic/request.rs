//! Anthropic Messages API request construction.

use jessica_core::llm::LlmError;
use jessica_core::tool::ToolSchema;
use jessica_core::types::{Message, Role, ToolCall};
use serde_json::{Map, Value, json};

use super::config::DEFAULT_MAX_TOKENS;

/// Builds the JSON body for a streaming Messages API request.
///
/// Anthropic differs from OpenAI in three ways that are absorbed here:
/// system messages move to a top-level `system` field, messages carry content
/// blocks instead of strings, and tools use `input_schema` rather than the
/// OpenAI `function.parameters` wrapper.
pub(crate) fn build_anthropic_request(
    model: &str,
    messages: &[Message],
    tools: &[ToolSchema],
) -> Result<Value, LlmError> {
    let (system, messages) = convert_messages(messages)?;

    let mut body = json!({
        "model": model,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "stream": true,
        "messages": messages,
    });

    if !system.is_empty() {
        body["system"] = Value::String(system);
    }

    if !tools.is_empty() {
        let tools = tools
            .iter()
            .map(tool_schema_to_anthropic)
            .collect::<Result<Vec<_>, _>>()?;
        body["tools"] = Value::Array(tools);
    }

    Ok(body)
}

/// Separates the system prompt from conversation messages.
fn convert_messages(messages: &[Message]) -> Result<(String, Vec<Value>), LlmError> {
    let mut system = Vec::new();
    let mut converted = Vec::new();
    let mut previous_was_tool_result = false;

    for message in messages {
        match message.role {
            Role::System => {
                if !message.content.trim().is_empty() {
                    system.push(message.content.clone());
                }
            }
            Role::User => {
                converted.push(json!({
                    "role": "user",
                    "content": [{"type": "text", "text": message.content}],
                }));
                previous_was_tool_result = false;
            }
            Role::Assistant => {
                let mut blocks = Vec::new();
                if !message.content.is_empty() {
                    blocks.push(json!({"type": "text", "text": message.content}));
                }
                for call in &message.tool_calls {
                    blocks.push(tool_use_block(call));
                }
                if !blocks.is_empty() {
                    converted.push(json!({"role": "assistant", "content": blocks}));
                }
                previous_was_tool_result = false;
            }
            Role::Tool => {
                let tool_call_id =
                    message
                        .tool_call_id
                        .as_ref()
                        .ok_or_else(|| LlmError::Request {
                            message: "tool message is missing tool_call_id".to_owned(),
                        })?;
                let block = json!({
                    "type": "tool_result",
                    "tool_use_id": tool_call_id,
                    "content": message.content,
                });

                if previous_was_tool_result
                    && let Some(last) = converted.last_mut()
                    && let Some(blocks) = last.get_mut("content").and_then(Value::as_array_mut)
                {
                    blocks.push(block);
                    continue;
                }

                converted.push(json!({"role": "user", "content": [block]}));
                previous_was_tool_result = true;
            }
        }
    }

    Ok((system.join("\n\n"), converted))
}

/// Maps a core tool call to an Anthropic `tool_use` content block.
fn tool_use_block(call: &ToolCall) -> Value {
    json!({
        "type": "tool_use",
        "id": call.id,
        "name": call.name,
        "input": call.arguments,
    })
}

/// Maps an OpenAI-style tool schema to Anthropic's tool shape.
///
/// Tools produced by Jessica all carry the OpenAI
/// `{type: "function", function: {...}}` wrapper, but a direct
/// `{name, parameters|input_schema}` object is also accepted.
fn tool_schema_to_anthropic(schema: &ToolSchema) -> Result<Value, LlmError> {
    let definition = schema.get("function").unwrap_or(schema);
    let name = definition
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| LlmError::Request {
            message: "tool schema is missing a name".to_owned(),
        })?;
    let input_schema = definition
        .get("input_schema")
        .or_else(|| definition.get("parameters"))
        .cloned()
        .unwrap_or_else(|| json!({"type": "object"}));

    let mut tool = Map::new();
    tool.insert("name".to_owned(), Value::String(name.to_owned()));
    if let Some(description) = definition.get("description").and_then(Value::as_str)
        && !description.is_empty()
    {
        tool.insert(
            "description".to_owned(),
            Value::String(description.to_owned()),
        );
    }
    tool.insert("input_schema".to_owned(), input_schema);
    Ok(Value::Object(tool))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a file",
                "parameters": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                },
            }
        })
    }

    #[test]
    fn anthropic_system_messages_move_to_the_top_level() {
        let messages = vec![
            Message::system("be careful"),
            Message::system("answer in Chinese"),
            Message::user("hello"),
        ];

        let body = build_anthropic_request("claude-test", &messages, &[])
            .expect("request should be constructed");

        assert_eq!(body["system"], "be careful\n\nanswer in Chinese");
        let sent = body["messages"].as_array().expect("messages array");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["role"], "user");
        assert_eq!(sent[0]["content"][0]["type"], "text");
        assert_eq!(sent[0]["content"][0]["text"], "hello");
    }

    #[test]
    fn anthropic_tool_use_and_tool_result_round_trip() {
        let call = ToolCall::new("toolu_1", "read_file", json!({"path": "Cargo.toml"}));
        let messages = vec![
            Message::user("read it"),
            Message::assistant_with_tool_calls("checking", vec![call]),
            Message::tool("toolu_1", "file contents"),
        ];

        let body = build_anthropic_request("claude-test", &messages, &[])
            .expect("request should be constructed");
        let sent = body["messages"].as_array().expect("messages array");
        assert_eq!(sent.len(), 3);

        assert_eq!(sent[1]["role"], "assistant");
        assert_eq!(
            sent[1]["content"][0],
            json!({"type": "text", "text": "checking"})
        );
        assert_eq!(
            sent[1]["content"][1],
            json!({
                "type": "tool_use",
                "id": "toolu_1",
                "name": "read_file",
                "input": {"path": "Cargo.toml"},
            })
        );

        assert_eq!(sent[2]["role"], "user");
        assert_eq!(
            sent[2]["content"][0],
            json!({
                "type": "tool_result",
                "tool_use_id": "toolu_1",
                "content": "file contents",
            })
        );
    }

    #[test]
    fn anthropic_consecutive_tool_results_share_one_user_message() {
        let messages = vec![
            Message::assistant_with_tool_calls(
                "",
                vec![
                    ToolCall::new("toolu_1", "read_file", json!({"path": "a"})),
                    ToolCall::new("toolu_2", "read_file", json!({"path": "b"})),
                ],
            ),
            Message::tool("toolu_1", "a contents"),
            Message::tool("toolu_2", "b contents"),
        ];

        let body = build_anthropic_request("claude-test", &messages, &[])
            .expect("request should be constructed");
        let sent = body["messages"].as_array().expect("messages array");
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[1]["role"], "user");
        assert_eq!(sent[1]["content"].as_array().map(Vec::len), Some(2));
        assert_eq!(sent[1]["content"][0]["tool_use_id"], "toolu_1");
        assert_eq!(sent[1]["content"][1]["tool_use_id"], "toolu_2");
    }

    #[test]
    fn anthropic_tool_schemas_are_unwrapped() {
        let body =
            build_anthropic_request("claude-test", &[Message::user("hi")], &[sample_schema()])
                .expect("request should be constructed");

        assert_eq!(
            body["tools"],
            json!([{
                "name": "read_file",
                "description": "Read a file",
                "input_schema": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                },
            }])
        );
    }

    #[test]
    fn anthropic_empty_tool_list_is_omitted() {
        let body = build_anthropic_request("claude-test", &[Message::user("hi")], &[])
            .expect("request should be constructed");
        assert!(body.get("tools").is_none());
        assert_eq!(body["stream"], true);
        assert_eq!(body["max_tokens"], DEFAULT_MAX_TOKENS);
    }

    #[test]
    fn anthropic_tool_message_without_call_id_is_rejected() {
        let message = Message {
            role: Role::Tool,
            content: "orphan result".to_owned(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        };
        let error = build_anthropic_request("claude-test", &[message], &[])
            .expect_err("orphan tool message must be rejected");
        assert!(matches!(error, LlmError::Request { .. }));
        assert!(error.to_string().contains("tool_call_id"));
    }

    #[test]
    fn anthropic_tool_schema_without_a_name_is_rejected() {
        let error = build_anthropic_request(
            "claude-test",
            &[Message::user("hi")],
            &[json!({"type": "function", "function": {"parameters": {}}})],
        )
        .expect_err("nameless schema must be rejected");
        assert!(matches!(error, LlmError::Request { .. }));
        assert!(error.to_string().contains("missing a name"));
    }
}
