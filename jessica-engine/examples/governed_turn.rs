//! Runs one complete governed turn through `jessica-engine`'s public API.
//!
//! The example supplies a scripted provider, one native tool, an approval
//! gate, and a SQLite event recorder. A parameter-level `ask` rule forces the
//! tool call through the approval gate, so the turn exercises governance, the
//! event pump, and the durable trace without depending on `jessica-cli`.
//!
//! Run with:
//!
//! ```text
//! cargo run -p jessica-engine --example governed_turn
//! ```

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::StreamExt;
use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::trace::{TracePayload, TraceTurnMode};
use jessica_core::types::{
    AgentEvent, Budget, Message, SessionEndReason, TokenUsage, ToolCall, ToolResult,
};
use jessica_engine::{
    ApprovalConfig, ApprovalMode, ApprovalPolicy, EngineBuilder, EngineConfig, EngineError,
    EventRecorder, PolicyConfig, PolicyRule, RuleAction, ToolCatalog, ToolContext, ToolDescriptor,
    ToolEntry, ToolLimits, ToolMetadata, ToolPermissionDecision, ToolPermissionGate, ToolProvider,
    ToolSelector, ToolSource, TracePolicy, TurnRequest,
};
use jessica_memory::EventStore;
use serde_json::{Value, json};

/// Provider that returns one scripted batch per model round.
struct ScriptedProvider {
    /// Responses in call order.
    responses: Mutex<VecDeque<Vec<LlmEvent>>>,
}

#[async_trait]
impl LlmProvider for ScriptedProvider {
    async fn stream_chat(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        let events = self
            .responses
            .lock()
            .expect("scripted responses must be available")
            .pop_front()
            .ok_or_else(|| LlmError::InvalidResponse {
                message: "scripted provider ran out of responses".to_owned(),
            })?;
        Ok(futures::stream::iter(events).boxed())
    }
}

/// Tool that echoes one text argument.
struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": "echo",
                "parameters": {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"]
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let text = args["text"].as_str().unwrap_or_default();
        ToolResult::success("call-1", format!("echo: {text}"))
    }
}

/// Tool provider that exposes exactly the echo tool.
struct EchoTools;

#[async_trait]
impl ToolProvider for EchoTools {
    async fn provide(&self, _context: &ToolContext<'_>) -> Result<ToolCatalog, EngineError> {
        let tool: Arc<dyn Tool> = Arc::new(EchoTool);
        let descriptor = ToolDescriptor::new(
            "echo".to_owned(),
            ToolSource::Native,
            tool.schema(),
            "example-v1",
            ToolLimits::default(),
            ApprovalPolicy::NotRequired,
            &ToolMetadata::default(),
        )
        .map_err(|source| EngineError::HostPort {
            component: "example tool provider".to_owned(),
            message: source.to_string(),
        })?;
        let mut catalog = ToolCatalog::new();
        catalog.insert(ToolEntry { tool, descriptor })?;
        Ok(catalog)
    }
}

/// Approval gate that allows every request and counts how often it ran.
struct CountingGate {
    /// Permission requests observed so far.
    requests: AtomicU32,
}

#[async_trait]
impl ToolPermissionGate for CountingGate {
    async fn request_permission(&self, _call: &ToolCall) -> ToolPermissionDecision {
        self.requests.fetch_add(1, Ordering::SeqCst);
        ToolPermissionDecision::Allow
    }
}

/// Creates a unique workspace under the system temporary root.
async fn temp_workspace() -> Result<PathBuf, std::io::Error> {
    let path = std::env::temp_dir().join(format!(
        "jessica-engine-example-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0)
    ));
    tokio::fs::create_dir_all(&path).await?;
    tokio::fs::canonicalize(&path).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = temp_workspace().await?;
    let database = workspace.join("events.sqlite");
    let recorder = EventRecorder::open(database.clone()).await?;

    let provider = Arc::new(ScriptedProvider {
        responses: Mutex::new(VecDeque::from(vec![
            vec![
                LlmEvent::ToolCallRequested {
                    call: ToolCall::new("call-1", "echo", json!({"text": "hello"})),
                },
                LlmEvent::Done {
                    usage: Some(TokenUsage::new(1, 1)),
                },
            ],
            vec![
                LlmEvent::TextDelta {
                    text: "done".to_owned(),
                },
                LlmEvent::Done {
                    usage: Some(TokenUsage::new(1, 1)),
                },
            ],
        ])),
    });

    let config = EngineConfig::new(TracePolicy::Required)
        .with_budget(Budget::new(1_000, Duration::from_secs(5), 8))
        .with_policy(PolicyConfig {
            allowed_tools: vec!["echo".to_owned()],
            rules: vec![PolicyRule {
                id: "ask-echo".to_owned(),
                tool: Some(ToolSelector::One("echo".to_owned())),
                path_glob: None,
                program: None,
                args_prefix: None,
                action: RuleAction::Ask,
                reason: Some("the example asks before every echo".to_owned()),
            }],
        })
        .with_approval(ApprovalConfig {
            mode: ApprovalMode::Ask,
            explicit: true,
        });

    let gate = Arc::new(CountingGate {
        requests: AtomicU32::new(0),
    });
    let engine = EngineBuilder::new()
        .provider(provider)
        .tool_provider(Arc::new(EchoTools))
        .config(config)
        .event_journal(Arc::new(recorder))
        .approval_gate(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>)
        .build()?;

    let outcome = engine
        .run_turn_request(
            TurnRequest::new("use the echo tool").with_workspace_root(workspace.as_path()),
        )
        .await?;

    assert_eq!(outcome.reason, SessionEndReason::Completed);
    assert_eq!(outcome.last_output, "done");
    assert_eq!(
        gate.requests.load(Ordering::SeqCst),
        1,
        "the ask rule must consult the approval gate exactly once"
    );
    assert!(
        outcome
            .messages
            .iter()
            .any(|message| message == &Message::tool("call-1", "echo: hello")),
        "the model must observe the governed tool result: {:?}",
        outcome.messages
    );

    let turn_id = outcome.turn_id.clone();
    let summary = tokio::task::spawn_blocking(move || -> Result<String, String> {
        let store = EventStore::open(&database).map_err(|error| error.to_string())?;
        let turns = store.trace_turns(10).map_err(|error| error.to_string())?;
        if turns.len() != 1 {
            return Err(format!("expected one trace turn, found {}", turns.len()));
        }
        let trace = store
            .trace_turn(turns[0].turn_id())
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "the summarized turn has no trace".to_owned())?;
        if !trace.closed() {
            return Err("the turn trace is not closed".to_owned());
        }
        let events = trace.events();
        let started = events.first().is_some_and(|stored| {
            matches!(
                &stored.event().payload,
                TracePayload::TurnStarted {
                    mode: TraceTurnMode::Fresh,
                    ..
                }
            )
        });
        let ended = events.last().is_some_and(|stored| {
            matches!(
                &stored.event().payload,
                TracePayload::TurnEnded {
                    reason: SessionEndReason::Completed
                }
            )
        });
        let permission = events.iter().any(|stored| {
            matches!(
                &stored.event().payload,
                TracePayload::Agent {
                    event: AgentEvent::PermissionDecided { .. }
                }
            )
        });
        let tool_call = events.iter().any(|stored| {
            matches!(
                &stored.event().payload,
                TracePayload::Agent {
                    event: AgentEvent::ToolCallStarted { call }
                } if call.name == "echo"
            )
        });
        if !started || !ended || !permission || !tool_call {
            return Err(format!(
                "trace is missing an expected marker: started={started} ended={ended} \
                 permission={permission} tool_call={tool_call}"
            ));
        }
        Ok(format!("{} durable events", events.len()))
    })
    .await??;

    println!("turn {turn_id} completed with a closed trace ({summary})");

    drop(engine);
    tokio::fs::remove_dir_all(&workspace).await?;
    Ok(())
}
