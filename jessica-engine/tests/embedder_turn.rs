//! External-embedder proof for Task E3.
//!
//! This test intentionally depends on `jessica-engine` (and its public ports)
//! only, never on `jessica-cli`. It builds an engine by hand, runs one turn
//! that executes a tool, and verifies the persisted `turn_started` /
//! `turn_ended` trace pair.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::StreamExt;
use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::trace::{TracePayload, TraceTurnMode};
use jessica_core::types::{
    AgentEvent, Budget, Message, SessionEndReason, SessionId, TokenUsage, ToolCall, ToolResult,
};
use jessica_engine::{
    ApprovalPolicy, Engine, EngineBuilder, EngineConfig, EngineError, EventRecorder, IdSource,
    PolicyConfig, SystemPromptSource, ToolCatalog, ToolContext, ToolDescriptor, ToolEntry,
    ToolLimits, ToolMetadata, ToolProvider, ToolSource, TracePolicy,
};
use jessica_memory::EventStore;
use serde_json::{Value, json};

/// Temporary directory removed when the test ends.
struct TempDir {
    /// Unique path owned by this test.
    path: PathBuf,
}

impl TempDir {
    /// Creates a unique directory under the system temporary root.
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "jessica-engine-embedder-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the clock must be after the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).expect("the temporary workspace must be creatable");
        let path = std::fs::canonicalize(&path).expect("the temporary workspace must resolve");
        Self { path }
    }

    /// Returns the canonical path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Provider that replays scripted batches and records every request.
struct ScriptedProvider {
    /// Responses in call order.
    responses: Mutex<VecDeque<Vec<LlmEvent>>>,
    /// Message transcripts observed per call.
    calls: Mutex<Vec<Vec<Message>>>,
}

impl ScriptedProvider {
    /// Creates a provider from response batches.
    fn new(responses: Vec<Vec<LlmEvent>>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Returns the transcripts observed so far.
    fn calls(&self) -> Vec<Vec<Message>> {
        self.calls
            .lock()
            .expect("provider calls mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl LlmProvider for ScriptedProvider {
    async fn stream_chat(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        self.calls
            .lock()
            .expect("provider calls mutex poisoned")
            .push(messages.to_vec());
        let events = self
            .responses
            .lock()
            .expect("provider responses mutex poisoned")
            .pop_front()
            .expect("scripted provider ran out of responses");
        Ok(futures::stream::iter(events).boxed())
    }
}

/// Tool that echoes one text argument and counts executions.
struct EchoTool {
    /// Executions observed so far.
    executions: Arc<AtomicU32>,
}

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
        self.executions.fetch_add(1, Ordering::SeqCst);
        let text = args["text"].as_str().unwrap_or_default();
        ToolResult::success("call-1", format!("echo: {text}"))
    }
}

/// Tool provider that exposes exactly the echo tool.
struct EchoTools {
    /// Shared execution counter owned by the test.
    executions: Arc<AtomicU32>,
}

#[async_trait]
impl ToolProvider for EchoTools {
    async fn provide(&self, _context: &ToolContext<'_>) -> Result<ToolCatalog, EngineError> {
        let tool: Arc<dyn Tool> = Arc::new(EchoTool {
            executions: Arc::clone(&self.executions),
        });
        let descriptor = ToolDescriptor::new(
            "echo".to_owned(),
            ToolSource::Native,
            tool.schema(),
            "embedder-v1",
            ToolLimits::default(),
            ApprovalPolicy::NotRequired,
            &ToolMetadata::default(),
        )
        .map_err(|source| EngineError::HostPort {
            component: "embedder tool provider".to_owned(),
            message: source.to_string(),
        })?;
        let mut catalog = ToolCatalog::new();
        catalog.insert(ToolEntry { tool, descriptor })?;
        Ok(catalog)
    }
}

/// Deterministic identity source.
struct FixedIds;

impl IdSource for FixedIds {
    fn next_turn_id(&self) -> String {
        "embedder-turn-1".to_owned()
    }

    fn next_session_id(&self) -> SessionId {
        SessionId::new("embedder-session-1")
    }
}

/// Fixed system prompt source.
struct FixedPrompt;

impl SystemPromptSource for FixedPrompt {
    fn system_prompt(&self) -> String {
        "You are an embedder smoke test.".to_owned()
    }
}

/// Builds the embedder engine over `recorder`.
fn build_engine(
    provider: Arc<ScriptedProvider>,
    executions: Arc<AtomicU32>,
    recorder: EventRecorder,
) -> Engine {
    let config = EngineConfig::new(TracePolicy::Required)
        .with_budget(Budget::new(1_000, Duration::from_secs(5), 8))
        .with_policy(PolicyConfig {
            allowed_tools: vec!["echo".to_owned()],
            rules: Vec::new(),
        });
    EngineBuilder::new()
        .provider(provider)
        .tool_provider(Arc::new(EchoTools { executions }))
        .config(config)
        .event_journal(Arc::new(recorder))
        .system_prompt(Arc::new(FixedPrompt))
        .id_source(Arc::new(FixedIds))
        .build()
        .expect("the embedder engine must assemble from public ports")
}

/// Reads the only stored trace turn.
fn load_trace(database: &Path) -> jessica_memory::StoredTraceTurn {
    let store = EventStore::open(database).expect("event store must open");
    let summaries = store.trace_turns(10).expect("turn summaries must read");
    assert_eq!(
        summaries.len(),
        1,
        "exactly one turn must be recorded: {summaries:?}"
    );
    store
        .trace_turn(summaries[0].turn_id())
        .expect("turn trace must read")
        .expect("the summarized turn must have a trace")
}

#[tokio::test]
async fn an_external_embedder_runs_a_tool_turn_with_a_required_trace() {
    let workspace = TempDir::new("turn");
    let database = workspace.path().join("events.sqlite");
    let recorder = EventRecorder::open(database.clone())
        .await
        .expect("event recorder must open");
    let executions = Arc::new(AtomicU32::new(0));
    let provider = Arc::new(ScriptedProvider::new(vec![
        vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new("call-1", "echo", json!({"text": "hi"})),
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
    ]));
    let engine = build_engine(Arc::clone(&provider), Arc::clone(&executions), recorder);

    let outcome = engine
        .run_turn_request(
            jessica_engine::TurnRequest::new("use the echo tool")
                .with_workspace_root(workspace.path()),
        )
        .await
        .expect("the embedder turn must complete");

    assert_eq!(outcome.reason, SessionEndReason::Completed);
    assert_eq!(outcome.turn_id, "embedder-turn-1");
    assert_eq!(outcome.last_output, "done");
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "the host tool must execute exactly once"
    );

    let calls = provider.calls();
    assert_eq!(calls.len(), 2, "one tool round and one final answer");
    assert_eq!(
        calls[1].last(),
        Some(&Message::tool("call-1", "echo: hi")),
        "the model must observe the governed tool result: {:?}",
        calls[1]
    );

    let trace = load_trace(&database);
    assert!(trace.closed(), "a completed turn must close its trace");
    assert_eq!(trace.turn_id().as_str(), "embedder-turn-1");
    let events = trace.events();
    assert!(
        matches!(
            &events[0].event().payload,
            TracePayload::TurnStarted {
                mode: TraceTurnMode::Fresh,
                messages,
            } if messages.first().is_some_and(|message| message.content
                == "You are an embedder smoke test.")
        ),
        "the trace must start with turn_started and the supplied prompt: {events:?}"
    );
    assert!(
        matches!(
            &events
                .last()
                .expect("the trace must not be empty")
                .event()
                .payload,
            TracePayload::TurnEnded {
                reason: SessionEndReason::Completed
            }
        ),
        "the trace must end with turn_ended: {events:?}"
    );
    assert!(
        events.iter().any(|stored| matches!(
            &stored.event().payload,
            TracePayload::Agent {
                event: AgentEvent::ToolCallStarted { call }
            } if call.name == "echo"
        )),
        "the trace must contain the tool call: {events:?}"
    );
}
