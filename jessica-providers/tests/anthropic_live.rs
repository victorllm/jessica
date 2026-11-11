//! Live smoke tests against a real Anthropic-compatible Messages endpoint.
//!
//! These tests read `ANTHROPIC_API_KEY`, `ANTHROPIC_BASE_URL` and
//! `ANTHROPIC_MODEL` from the environment and make real network calls. They are
//! ignored by default so the normal test suite stays deterministic and offline.
//! Run them explicitly:
//!
//! ```bash
//! cargo test -p jessica-providers --test anthropic_live -- --ignored --nocapture
//! ```

use std::time::Duration;

use futures::StreamExt;
use jessica_core::llm::{LlmEvent, LlmProvider};
use jessica_core::tool::ToolSchema;
use jessica_core::types::Message;
use jessica_providers::AnthropicProvider;
use serde_json::json;

/// Upper bound for a single live request/stream so a stuck endpoint fails the
/// test instead of hanging the suite.
const STREAM_TIMEOUT: Duration = Duration::from_secs(120);

fn provider() -> AnthropicProvider {
    AnthropicProvider::from_env().expect("ANTHROPIC_API_KEY/BASE_URL/MODEL must be set")
}

/// Drives one complete request and returns every observed event.
async fn stream_events(
    provider: &AnthropicProvider,
    messages: &[Message],
    tools: &[ToolSchema],
) -> Vec<LlmEvent> {
    let stream = tokio::time::timeout(STREAM_TIMEOUT, provider.stream_chat(messages, tools))
        .await
        .expect("stream_chat should start within the timeout")
        .expect("the endpoint should accept the request");
    tokio::time::timeout(STREAM_TIMEOUT, stream.collect::<Vec<_>>())
        .await
        .expect("the stream should finish within the timeout")
}

fn describe(events: &[LlmEvent]) -> String {
    events
        .iter()
        .map(|event| format!("{event:?}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_no_errors(events: &[LlmEvent]) {
    if let Some(message) = events.iter().find_map(|event| match event {
        LlmEvent::Error { message } => Some(message),
        _ => None,
    }) {
        panic!(
            "provider reported an error: {message}\nevents:\n{}",
            describe(events)
        );
    }
}

fn text_of(events: &[LlmEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            LlmEvent::TextDelta { text } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn assert_done(events: &[LlmEvent]) {
    assert!(
        events
            .iter()
            .any(|event| matches!(event, LlmEvent::Done { .. })),
        "expected a terminal Done event, got:\n{}",
        describe(events)
    );
}

#[tokio::test]
#[ignore = "requires a live ANTHROPIC_API_KEY and network access"]
async fn anthropic_live_streams_text_from_the_real_endpoint() {
    let provider = provider();
    let messages = vec![
        Message::system("You are a connectivity test. Reply with one short sentence, no markdown."),
        Message::user("Reply with the single word: pong"),
    ];

    let events = stream_events(&provider, &messages, &[]).await;
    assert_no_errors(&events);
    assert_done(&events);

    let text = text_of(&events);
    println!("live text response: {text:?}");
    assert!(
        text.to_lowercase().contains("pong"),
        "expected the model to echo `pong`, got:\n{text}\nevents:\n{}",
        describe(&events)
    );
}

#[tokio::test]
#[ignore = "requires a live ANTHROPIC_API_KEY and network access"]
async fn anthropic_live_round_trips_a_tool_call() {
    let provider = provider();
    let tools = vec![json!({
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get the current weather for a city",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {"type": "string", "description": "City name"}
                },
                "required": ["city"]
            }
        }
    })];
    let system = Message::system(
        "You must call the get_weather tool for any weather question. \
         Do not answer with text before calling the tool.",
    );
    let user = Message::user("What is the weather in Paris? Use the get_weather tool.");

    // First turn: the provider must translate the OpenAI-style schema into an
    // Anthropic tool definition and assemble a complete ToolCall from the
    // streamed input_json_delta fragments.
    let first = stream_events(&provider, &[system.clone(), user.clone()], &tools).await;
    assert_no_errors(&first);
    assert_done(&first);

    let call = first
        .iter()
        .find_map(|event| match event {
            LlmEvent::ToolCallRequested { call } => Some(call.clone()),
            _ => None,
        })
        .unwrap_or_else(|| {
            panic!(
                "expected a tool call for get_weather, got:\n{}",
                describe(&first)
            )
        });
    assert_eq!(call.name, "get_weather");
    let city = call
        .arguments
        .get("city")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("tool arguments are missing `city`: {}", call.arguments));
    assert!(
        city.to_lowercase().contains("paris"),
        "expected the call to target Paris, got: {}",
        call.arguments
    );
    println!("live tool call: {} {}", call.name, call.arguments);

    // Second turn: send the recorded tool_use back with its tool_result and
    // require a normal text completion.
    let messages = vec![
        system,
        user,
        Message::assistant_with_tool_calls("", vec![call.clone()]),
        Message::tool(&call.id, r#"{"temperature_c":21,"summary":"clear"}"#),
    ];
    let second = stream_events(&provider, &messages, &tools).await;
    assert_no_errors(&second);
    assert_done(&second);
    let answer = text_of(&second);
    println!("live follow-up answer: {answer:?}");
    assert!(
        !answer.trim().is_empty(),
        "expected a text answer after the tool result, got:\n{}",
        describe(&second)
    );
}
