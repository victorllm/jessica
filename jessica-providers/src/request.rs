//! OpenAI chat-completions request construction.

use jessica_core::llm::LlmError;
use jessica_core::tool::ToolSchema;
use jessica_core::types::{Message, Role, ToolCall};
use serde_json::{Value, json};

/// Builds the JSON body for a streaming chat-completions request.
pub(crate) fn build_chat_request(
    model: &str,
    messages: &[Message],
    tools: &[ToolSchema],
) -> Result<Value, LlmError> {
    let messages = messages
        .iter()
        .map(message_to_openai)
        .collect::<Result<Vec<_>, _>>()?;

    let mut body = json!({
        "model": model,
        "messages": messages,
        "stream": true,
        "stream_options": {"include_usage": true},
    });

    if !tools.is_empty() {
        body["tools"] = Value::Array(tools.to_vec());
    }

    Ok(body)
}

fn message_to_openai(message: &Message) -> Result<Value, LlmError> {
    let mut object = serde_json::Map::new();
    object.insert(
        "role".to_owned(),
        Value::String(role_name(message.role).to_owned()),
    );
    object.insert("content".to_owned(), Value::String(message.content.clone()));

    match message.role {
        Role::Assistant if !message.tool_calls.is_empty() => {
            let calls = message
                .tool_calls
                .iter()
                .map(tool_call_to_openai)
                .collect::<Result<Vec<_>, _>>()?;
            object.insert("tool_calls".to_owned(), Value::Array(calls));
        }
        Role::Tool => {
            let tool_call_id = message
                .tool_call_id
                .as_ref()
                .ok_or_else(|| LlmError::Request {
                    message: "tool message is missing tool_call_id".to_owned(),
                })?;
            object.insert(
                "tool_call_id".to_owned(),
                Value::String(tool_call_id.clone()),
            );
        }
        _ => {}
    }

    Ok(Value::Object(object))
}

fn tool_call_to_openai(call: &ToolCall) -> Result<Value, LlmError> {
    let arguments = serde_json::to_string(&call.arguments).map_err(|error| LlmError::Request {
        message: format!(
            "failed to serialize arguments for tool `{}`: {error}",
            call.name
        ),
    })?;
    let id = call.id.clone();
    let name = call.name.clone();
    Ok(json!({
        "id": id,
        "type": "function",
        "function": {
            "name": name,
            "arguments": arguments,
        }
    }))
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tool_schema() -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a file",
                "parameters": {"type": "object"},
            }
        })
    }

    #[test]
    fn request_contains_model_messages_tools_and_streaming_options() {
        let messages = vec![
            Message::system("be helpful"),
            Message::user("read the file"),
            Message::assistant_with_tool_calls(
                "",
                vec![ToolCall::new(
                    "call_1",
                    "read_file",
                    json!({"path": "Cargo.toml"}),
                )],
            ),
            Message::tool("call_1", "file contents"),
        ];
        let tools = vec![sample_tool_schema()];

        let body = build_chat_request("gpt-test", &messages, &tools)
            .expect("request should be constructed");

        assert_eq!(body["model"], "gpt-test");
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
        assert_eq!(body["tools"], json!(tools));

        let sent = body["messages"].as_array().expect("messages array");
        assert_eq!(sent.len(), 4);
        assert_eq!(sent[0], json!({"role": "system", "content": "be helpful"}));
        assert_eq!(sent[1], json!({"role": "user", "content": "read the file"}));
        assert_eq!(sent[2]["role"], "assistant");
        assert_eq!(
            sent[2]["tool_calls"][0]["function"]["arguments"],
            "{\"path\":\"Cargo.toml\"}"
        );
        assert_eq!(sent[3]["role"], "tool");
        assert_eq!(sent[3]["tool_call_id"], "call_1");
        assert_eq!(sent[3]["content"], "file contents");
    }

    #[test]
    fn empty_tool_list_is_omitted() {
        let body = build_chat_request("gpt-test", &[Message::user("hi")], &[])
            .expect("request should be constructed");
        assert!(body.get("tools").is_none());
    }

    #[test]
    fn assistant_without_tool_calls_has_no_tool_calls_field() {
        let body = build_chat_request("gpt-test", &[Message::assistant("hi")], &[])
            .expect("request should be constructed");
        let message = &body["messages"][0];
        assert!(message.get("tool_calls").is_none());
        assert!(message.get("tool_call_id").is_none());
    }

    #[test]
    fn tool_message_without_call_id_is_rejected() {
        let message = Message {
            role: Role::Tool,
            content: "orphan result".to_owned(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        };
        let error = build_chat_request("gpt-test", &[message], &[])
            .expect_err("orphan tool message must be rejected");
        assert!(matches!(error, LlmError::Request { .. }));
        assert!(error.to_string().contains("tool_call_id"));
    }
}
