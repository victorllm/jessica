//! Public engine assembly and turn types.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::events::{EventSink, EventSinkError};
use jessica_core::llm::LlmProvider;
use jessica_core::policy::Policy;
use jessica_core::tool::ToolRegistry;
use jessica_core::trace::TraceTurnMode;
use jessica_core::types::{AgentEvent, Message, Role, SessionEndReason, SessionId};
use jessica_harness::{
    AllowlistPolicy, Harness, ResultSizeGuard, TokenBudgetGuard, ToolTimeoutGuard,
};
use tokio_util::sync::CancellationToken;

use crate::artifacts::ResultManager;
use crate::config::{EngineConfig, ResultConfig, TracePolicy};
use crate::error::EngineError;
use crate::governance::{GovernanceState, GovernedTool, TokenCountingProvider};
use crate::policy::{PolicyTool, PolicyV2};
use crate::ports::{
    ToolContext, ToolPermissionGate, ToolProvider, TraceStore, TurnObserver, TurnTrace,
};
use crate::pump::{EventPump, LagHandling, PumpIntegrity, PumpOutput};
use crate::tooling::ToolEntry;

/// Maximum time to wait for the pump to observe a session's terminal event.
const PUMP_TIMEOUT: Duration = Duration::from_secs(30);

/// Input for one governed turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRequest {
    input: String,
    workspace_root: PathBuf,
}

impl TurnRequest {
    /// Creates a request from the user's input.
    ///
    /// The workspace root defaults to the current directory and can be
    /// replaced with [`Self::with_workspace_root`]. The engine canonicalizes
    /// it before building tools, policy, or result management.
    pub fn new(input: impl Into<String>) -> Self {
        Self {
            input: input.into(),
            workspace_root: PathBuf::from("."),
        }
    }

    /// Sets the workspace root for this turn.
    pub fn with_workspace_root(mut self, workspace_root: impl Into<PathBuf>) -> Self {
        self.workspace_root = workspace_root.into();
        self
    }

    /// Returns the user's input.
    pub fn input(&self) -> &str {
        &self.input
    }

    /// Returns the configured workspace root.
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }
}

/// Observable result of one governed turn.
///
/// This is a successful turn record. A trace that could not be closed is
/// returned as [`EngineError`] instead, never as an outcome with a misleading
/// `Completed` reason.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnOutcome {
    /// Why the turn stopped.
    pub reason: SessionEndReason,
    /// Conversation after the turn.
    pub messages: Vec<Message>,
    /// Final text produced by the model.
    pub last_output: String,
    /// Index in `messages` where this turn's initial conversation begins.
    pub turn_start: usize,
    /// Stable identifier for this turn.
    pub turn_id: String,
    /// Whether the event pump observed a broadcast lag.
    pub lagged: bool,
}

/// Dependencies retained by a built [`Engine`].
struct EngineDependencies {
    /// LLM provider wrapped by the engine's token accounting.
    provider: Arc<dyn LlmProvider>,
    /// Host-supplied tool catalog builder.
    tool_provider: Arc<dyn ToolProvider>,
    /// Optional approval gate.
    approval_gate: Option<Arc<dyn ToolPermissionGate>>,
    /// Durable trace store, required by [`TracePolicy::Required`].
    trace_store: Option<Arc<dyn TraceStore>>,
    /// Optional observer of every event the pump processes.
    observer: Option<Arc<dyn TurnObserver>>,
}

/// Shared governance objects wrapped around every exposed tool in one turn.
struct TurnTooling {
    /// Allowlist policy.
    allowlist: Arc<dyn Policy>,
    /// Budget and cancellation state.
    governance: Arc<GovernanceState>,
    /// Parameter-level policy evaluator.
    policy_v2: Arc<PolicyV2>,
    /// Result manager.
    result_manager: Arc<ResultManager>,
    /// Shared timeout guard.
    timeout_guard: Arc<ToolTimeoutGuard>,
    /// Shared result-size guard.
    result_guard: Arc<ResultSizeGuard>,
}

/// A configured governed engine.
///
/// The engine owns one immutable [`EngineConfig`] snapshot and runs the
/// provider-to-tool vertical slice for each [`TurnRequest`]. Task E2 added the
/// producer-side trace sink, the event pump, and the required-trace lifecycle.
/// Acceptance orchestration, memory, skills, delegation, and resume remain
/// later Phase 11 tasks.
pub struct Engine {
    config: EngineConfig,
    result_config: ResultConfig,
    dependencies: EngineDependencies,
    next_turn: AtomicU64,
}

impl Engine {
    /// Returns the configuration snapshot owned by this engine.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Returns the result-management configuration owned by this engine.
    pub fn result_config(&self) -> &ResultConfig {
        &self.result_config
    }

    /// Runs one governed turn.
    ///
    /// The turn builds a fresh tool catalog, filters it through the allowlist,
    /// wraps every exposed tool with [`GovernedTool`], and drives
    /// [`jessica_runtime::AgentLoop`] through the token-counting provider. A
    /// required trace is opened before the agent starts, every producer event
    /// is recorded before it is broadcast, and the turn only succeeds after
    /// `turn_ended` is acknowledged. Acceptance orchestration, memory, skills,
    /// delegation, and resume belong to later Phase 11 tasks.
    ///
    /// A budget trip is reported as [`SessionEndReason::BudgetExceeded`] even
    /// when the runtime loop itself returned another reason, because the
    /// governance state is the authoritative budget signal.
    ///
    /// A pump gap does not discard a producer-complete trace: it sets
    /// [`TurnOutcome::lagged`] so the caller knows the observer-side transcript
    /// and sensor path are incomplete. Any trace write or close failure is
    /// returned as [`EngineError::TraceIncomplete`] instead of a success.
    pub async fn run_turn(&self, request: TurnRequest) -> Result<TurnOutcome, EngineError> {
        let workspace_root = tokio::fs::canonicalize(request.workspace_root())
            .await
            .map_err(|source| EngineError::Workspace {
                root: request.workspace_root().display().to_string(),
                source,
            })?;
        let sequence = self.next_turn.fetch_add(1, Ordering::SeqCst) + 1;
        let turn_id = format!("turn-{sequence}");
        let session_id = SessionId::new(turn_id.clone());
        let cancel = CancellationToken::new();
        let initial = vec![Message::user(request.input())];
        let turn_start = initial.len();
        let trace = TurnTrace {
            turn_id: turn_id.clone(),
            session_id: session_id.clone(),
            attempt: Some(1),
        };
        let trace_store = match self.config.trace_policy() {
            TracePolicy::Disabled => None,
            TracePolicy::Required => Some(Arc::clone(
                self.dependencies
                    .trace_store
                    .as_ref()
                    .ok_or(EngineError::MissingTraceStore)?,
            )),
        };

        let harness = Arc::new(Harness::new(Vec::new()));
        let events = harness.events();
        let trace_failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let sink: Option<Arc<dyn EventSink>> = trace_store.as_ref().map(|store| {
            Arc::new(TraceEventSink {
                store: Arc::clone(store),
                trace: trace.clone(),
                cancel: cancel.clone(),
                failure: Arc::clone(&trace_failure),
            }) as Arc<dyn EventSink>
        });
        let governance = Arc::new(
            GovernanceState::new(events.clone(), cancel.clone()).with_event_sink(sink.clone()),
        );

        let provider: Arc<dyn LlmProvider> = Arc::new(TokenCountingProvider::new(
            Arc::clone(&self.dependencies.provider),
            Arc::clone(&governance),
            Arc::new(TokenBudgetGuard::new(self.config.budget().max_tokens)),
        ));
        let context = ToolContext {
            workspace_root: &workspace_root,
            cancellation: &cancel,
            request: &request,
        };
        let catalog = self.dependencies.tool_provider.provide(&context).await?;
        let policy_v2 = Arc::new(
            PolicyV2::new(
                &workspace_root,
                self.config.policy(),
                self.config.approval(),
            )
            .await
            .map_err(EngineError::Policy)?,
        );
        let allowlist: Arc<dyn Policy> = Arc::new(AllowlistPolicy::new(
            self.config.policy().allowed_tools.clone(),
        ));
        let allowed: BTreeSet<&str> = self
            .config
            .policy()
            .allowed_tools
            .iter()
            .map(String::as_str)
            .collect();
        let result_manager = Arc::new(ResultManager::new(
            &workspace_root,
            &self.result_config,
            self.config.tools().max_result_bytes,
        ));
        let timeout_guard = Arc::new(ToolTimeoutGuard::new(self.config.tools().timeout));
        let result_guard = Arc::new(ResultSizeGuard::new(self.config.tools().max_result_bytes));
        let tooling = TurnTooling {
            allowlist,
            governance: Arc::clone(&governance),
            policy_v2,
            result_manager,
            timeout_guard,
            result_guard,
        };
        let mut registry = ToolRegistry::new();
        for entry in catalog.iter() {
            if !allowed.contains(entry.descriptor.name.as_str()) {
                continue;
            }
            let governed = self.govern_tool(entry, &tooling);
            let name = entry.descriptor.name.clone();
            registry
                .register(Arc::new(governed))
                .map_err(|_| EngineError::DuplicateTool { name })?;
        }

        let agent = jessica_runtime::AgentLoop::new(
            provider,
            Arc::new(registry),
            self.config.budget().clone(),
            events,
        );
        let agent = match &sink {
            Some(sink) => agent.with_event_sink(Arc::clone(sink)),
            None => agent,
        };
        let pump = Arc::new(EventPump::new(
            Arc::clone(&harness),
            Arc::clone(&governance),
            Arc::new(EnginePumpIntegrity),
            PumpOutput::Silent,
            self.dependencies.observer.clone(),
        ));

        if let Some(store) = &trace_store {
            store
                .begin_turn(&trace, TraceTurnMode::Fresh, &initial)
                .await
                .map_err(|error| trace_incomplete(&turn_id, error))?;
        }

        let pump_task = tokio::spawn(Arc::clone(&pump).run(harness.subscribe()));
        pump.register(session_id.clone(), initial.clone());

        let transcript = agent
            .run_with_transcript(
                session_id.clone(),
                initial,
                self.config.max_tool_calls(),
                cancel.clone(),
            )
            .await;
        if let Some(cause) = failure_slot(&trace_failure).clone() {
            pump_task.abort();
            return Err(EngineError::TraceIncomplete { turn_id, cause });
        }

        let snapshot = match tokio::time::timeout(PUMP_TIMEOUT, pump.wait_for(&session_id)).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                pump_task.abort();
                return Err(EngineError::TraceIncomplete {
                    turn_id,
                    cause: "the event pump did not observe the session terminal event".to_owned(),
                });
            }
            Err(_) => {
                pump_task.abort();
                return Err(EngineError::TraceIncomplete {
                    turn_id,
                    cause: "timed out waiting for the event pump".to_owned(),
                });
            }
        };
        let reason =
            if governance.tripped() && transcript.reason != SessionEndReason::BudgetExceeded {
                SessionEndReason::BudgetExceeded
            } else {
                transcript.reason
            };
        let lagged = pump.has_lagged();
        let (messages, last_output) = if lagged {
            // The observer lost events, so its transcript is not authoritative.
            // The runtime transcript and the producer-side trace are complete;
            // expose the gap instead of returning a silently shortened
            // conversation as if it were whole.
            let last_output = transcript
                .messages
                .iter()
                .rev()
                .find(|message| message.role == Role::Assistant)
                .map(|message| message.content.clone())
                .unwrap_or_default();
            (transcript.messages.clone(), last_output)
        } else {
            (snapshot.messages, snapshot.last_output)
        };

        let outcome = TurnOutcome {
            reason,
            messages,
            last_output,
            turn_start,
            turn_id: turn_id.clone(),
            lagged,
        };
        if let Some(store) = &trace_store
            && let Err(error) = store.finish_turn(&trace, &outcome).await
        {
            pump_task.abort();
            return Err(trace_incomplete(&turn_id, error));
        }
        pump_task.abort();
        Ok(outcome)
    }

    /// Wraps one host tool in the complete E1 governance chain.
    fn govern_tool(&self, entry: &ToolEntry, tooling: &TurnTooling) -> GovernedTool {
        let timeout = entry
            .descriptor
            .limits
            .timeout
            .unwrap_or(self.config.tools().timeout);
        let max_args = entry
            .descriptor
            .limits
            .max_args_bytes
            .or(self.config.tools().max_args_bytes);
        let permission = self.dependencies.approval_gate.clone();
        GovernedTool::new(
            Arc::clone(&entry.tool),
            Arc::clone(&tooling.allowlist),
            Arc::clone(&tooling.governance),
            timeout,
            Arc::clone(&tooling.timeout_guard),
            Arc::clone(&tooling.result_guard),
        )
        .with_policy_v2(
            Arc::clone(&tooling.policy_v2),
            PolicyTool::from_descriptor(&entry.descriptor),
        )
        .with_max_args_bytes(max_args)
        .with_result_manager(
            Some(Arc::clone(&tooling.result_manager)),
            entry.descriptor.result_format,
        )
        .with_permission_gate(permission.clone())
        .with_ask_gate(permission)
    }
}

/// Producer-side sink that records every event before it is broadcast.
///
/// The first failure is retained so `run_turn` can return the authoritative
/// cause even though the runtime only reports a generic sink failure.
struct TraceEventSink {
    /// Durable trace store for the turn.
    store: Arc<dyn TraceStore>,
    /// Turn and session attribution for every recorded event.
    trace: TurnTrace,
    /// Cancels the agent so no later event is produced.
    cancel: CancellationToken,
    /// First trace failure, shared with `run_turn`.
    failure: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl EventSink for TraceEventSink {
    async fn emit(&self, event: AgentEvent) -> Result<(), EventSinkError> {
        if let Some(cause) = failure_slot(&self.failure).clone() {
            return Err(EventSinkError::new(cause));
        }
        match self.store.record(&self.trace, event).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let cause = trace_cause(error);
                *failure_slot(&self.failure) = Some(cause.clone());
                self.cancel.cancel();
                Err(EventSinkError::new(cause))
            }
        }
    }
}

/// Pump policy for engine-owned turns: a gap is reported in the outcome.
struct EnginePumpIntegrity;

impl PumpIntegrity for EnginePumpIntegrity {
    fn broadcast_lagged(&self, _skipped: u64, _output: PumpOutput) -> LagHandling {
        LagHandling::Continue
    }
}

/// Locks the first trace-failure slot, recovering from poisoning.
fn failure_slot(failure: &Mutex<Option<String>>) -> MutexGuard<'_, Option<String>> {
    failure
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Normalizes any trace-port failure into [`EngineError::TraceIncomplete`].
fn trace_incomplete(turn_id: &str, error: EngineError) -> EngineError {
    match error {
        error @ EngineError::TraceIncomplete { .. } => error,
        other => EngineError::TraceIncomplete {
            turn_id: turn_id.to_owned(),
            cause: other.to_string(),
        },
    }
}

/// Extracts the underlying cause from a trace-port failure.
fn trace_cause(error: EngineError) -> String {
    match error {
        EngineError::TraceIncomplete { cause, .. } => cause,
        other => other.to_string(),
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("config", &self.config)
            .field("result_config", &self.result_config)
            .finish_non_exhaustive()
    }
}

/// Builds an [`Engine`] from explicit host ports and engine decisions.
///
/// The builder deliberately accepts no pre-assembled `Harness` or
/// `ToolRegistry`: every governed dependency crosses a named port, and the
/// trace policy must be explicit. A required trace needs a [`TraceStore`]; a
/// policy that can ask for approval also requires a permission gate at build
/// time.
#[derive(Default)]
pub struct EngineBuilder {
    provider: Option<Arc<dyn LlmProvider>>,
    tool_provider: Option<Arc<dyn ToolProvider>>,
    approval_gate: Option<Arc<dyn ToolPermissionGate>>,
    trace_store: Option<Arc<dyn TraceStore>>,
    observer: Option<Arc<dyn TurnObserver>>,
    result_config: Option<ResultConfig>,
    config: Option<EngineConfig>,
}

impl EngineBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the LLM provider.
    pub fn provider(mut self, provider: Arc<dyn LlmProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Sets the turn-scoped tool provider.
    pub fn tool_provider(mut self, tool_provider: Arc<dyn ToolProvider>) -> Self {
        self.tool_provider = Some(tool_provider);
        self
    }

    /// Sets the permission gate used by ask rules and explicit ask mode.
    pub fn approval_gate(mut self, approval_gate: Arc<dyn ToolPermissionGate>) -> Self {
        self.approval_gate = Some(approval_gate);
        self
    }

    /// Sets the durable trace store.
    ///
    /// [`TracePolicy::Required`] is the default and fails assembly without a
    /// store. [`TracePolicy::Disabled`] writes no trace and leaves a configured
    /// store unused.
    pub fn trace_store(mut self, trace_store: Arc<dyn TraceStore>) -> Self {
        self.trace_store = Some(trace_store);
        self
    }

    /// Sets the observer that receives every event the pump processes.
    ///
    /// The observer runs after durable recording and before human-console
    /// rendering. It is optional even when a trace is required.
    pub fn observer(mut self, observer: Arc<dyn TurnObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Sets the result-management configuration.
    pub fn result_config(mut self, result_config: ResultConfig) -> Self {
        self.result_config = Some(result_config);
        self
    }

    /// Sets the immutable engine configuration.
    pub fn config(mut self, config: EngineConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Validates the supplied ports and configuration and builds an engine.
    ///
    /// The missing-dependency checks run in a fixed order so callers get a
    /// deterministic first error. A required trace needs a store, and an
    /// approval gate is required when the policy contains an ask rule or when
    /// an explicit ask mode is configured.
    pub fn build(self) -> Result<Engine, EngineError> {
        let provider = self.provider.ok_or(EngineError::MissingProvider)?;
        let tool_provider = self.tool_provider.ok_or(EngineError::MissingToolProvider)?;
        let config = self.config.ok_or(EngineError::MissingConfig)?;
        let result_config = self.result_config.ok_or(EngineError::MissingResultConfig)?;
        if config.trace_policy() == TracePolicy::Required && self.trace_store.is_none() {
            return Err(EngineError::MissingTraceStore);
        }
        if config.requires_approval_gate() && self.approval_gate.is_none() {
            return Err(EngineError::MissingApprovalGate);
        }

        Ok(Engine {
            config,
            result_config,
            dependencies: EngineDependencies {
                provider,
                tool_provider,
                approval_gate: self.approval_gate,
                trace_store: self.trace_store,
                observer: self.observer,
            },
            next_turn: AtomicU64::new(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
    use std::time::Duration;

    use async_trait::async_trait;
    use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
    use jessica_core::tool::{Tool, ToolSchema};
    use jessica_core::trace::{TracePayload, TraceTurnMode, TurnId};
    use jessica_core::types::{Budget, Message, TokenUsage, ToolCall, ToolResult};
    use jessica_memory::EventStore;
    use serde_json::{Value, json};

    use super::*;
    use crate::config::{
        ApprovalConfig, ApprovalMode, PolicyConfig, PolicyRule, RuleAction, ToolExecutionConfig,
        ToolSelector, TracePolicy,
    };
    use crate::ports::{ToolContext, ToolPermissionDecision, TraceStore, TurnObserver, TurnTrace};
    use crate::recorder::SqliteTraceStore;
    use crate::tooling::{
        ApprovalPolicy, ToolCatalog, ToolDescriptor, ToolLimits, ToolMetadata, ToolSource,
    };

    struct UnusedProvider;

    #[async_trait]
    impl LlmProvider for UnusedProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            Err(LlmError::Request {
                message: "test provider must not be called during assembly".to_owned(),
            })
        }
    }

    struct EmptyTools;

    #[async_trait]
    impl ToolProvider for EmptyTools {
        async fn provide(&self, _context: &ToolContext<'_>) -> Result<ToolCatalog, EngineError> {
            Ok(ToolCatalog::new())
        }
    }

    struct AllowGate;

    #[async_trait]
    impl ToolPermissionGate for AllowGate {
        async fn request_permission(&self, _call: &ToolCall) -> ToolPermissionDecision {
            ToolPermissionDecision::Allow
        }
    }

    fn provider() -> Arc<dyn LlmProvider> {
        Arc::new(UnusedProvider)
    }

    fn tools() -> Arc<dyn ToolProvider> {
        Arc::new(EmptyTools)
    }

    fn config() -> EngineConfig {
        EngineConfig::new(TracePolicy::Disabled)
    }

    fn base_builder() -> EngineBuilder {
        EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .config(config())
            .result_config(ResultConfig::default())
    }

    #[test]
    fn every_required_host_dependency_is_reported_by_its_own_variant() {
        let missing_provider = EngineBuilder::new()
            .tool_provider(tools())
            .config(config())
            .result_config(ResultConfig::default())
            .build()
            .expect_err("a provider is mandatory");
        assert!(matches!(missing_provider, EngineError::MissingProvider));

        let missing_tools = EngineBuilder::new()
            .provider(provider())
            .config(config())
            .result_config(ResultConfig::default())
            .build()
            .expect_err("a tool provider is mandatory");
        assert!(matches!(missing_tools, EngineError::MissingToolProvider));

        let missing_config = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .result_config(ResultConfig::default())
            .build()
            .expect_err("an engine config is mandatory");
        assert!(matches!(missing_config, EngineError::MissingConfig));

        let missing_result_config = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .config(config())
            .build()
            .expect_err("a result config is mandatory");
        assert!(matches!(
            missing_result_config,
            EngineError::MissingResultConfig
        ));
    }

    #[test]
    fn an_ask_rule_without_a_gate_fails_assembly() {
        let policy = PolicyConfig {
            allowed_tools: vec!["read_file".to_owned()],
            rules: vec![PolicyRule {
                id: "ask-read".to_owned(),
                tool: Some(ToolSelector::One("read_file".to_owned())),
                path_glob: None,
                program: None,
                args_prefix: None,
                action: RuleAction::Ask,
                reason: None,
            }],
        };
        let configured = EngineConfig::new(TracePolicy::Disabled).with_policy(policy);

        let error = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .config(configured)
            .result_config(ResultConfig::default())
            .build()
            .expect_err("an ask rule cannot run without a gate");

        assert!(matches!(error, EngineError::MissingApprovalGate));
    }

    #[test]
    fn a_gate_lets_an_explicit_ask_configuration_assemble() {
        let configured = EngineConfig::new(TracePolicy::Disabled).with_approval(ApprovalConfig {
            mode: ApprovalMode::Ask,
            explicit: true,
        });

        let engine = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .approval_gate(Arc::new(AllowGate))
            .config(configured)
            .result_config(ResultConfig::default())
            .build()
            .expect("explicit ask with a gate must assemble");

        assert_eq!(engine.config().approval().mode, ApprovalMode::Ask);
        assert!(engine.config().approval().explicit);
    }

    #[test]
    fn engine_exposes_the_result_and_turn_configuration_it_was_built_with() {
        let result_config = ResultConfig {
            max_inline_bytes: 4096,
            ..ResultConfig::default()
        };
        let engine = base_builder()
            .result_config(result_config.clone())
            .build()
            .expect("the complete builder must assemble");

        assert_eq!(engine.result_config(), &result_config);
        assert_eq!(engine.config().trace_policy(), TracePolicy::Disabled);
    }

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Creates a unique directory and removes it on drop.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "jessica-engine-api-{label}-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temp dir must be created");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct MockTool {
        name: &'static str,
        content: String,
        calls: Arc<AtomicU32>,
    }

    impl MockTool {
        fn new(name: &'static str, content: impl Into<String>) -> Self {
            Self {
                name,
                content: content.into(),
                calls: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    #[async_trait]
    impl Tool for MockTool {
        fn name(&self) -> &str {
            self.name
        }

        fn schema(&self) -> ToolSchema {
            json!({"type": "function", "function": {"name": self.name}})
        }

        async fn execute(&self, _args: Value) -> ToolResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ToolResult::success(String::new(), self.content.clone())
        }
    }

    struct StaticTools {
        catalog: ToolCatalog,
    }

    #[async_trait]
    impl ToolProvider for StaticTools {
        async fn provide(&self, _context: &ToolContext<'_>) -> Result<ToolCatalog, EngineError> {
            Ok(self.catalog.clone())
        }
    }

    struct ScriptedProvider {
        calls: AtomicU32,
        tool_name: &'static str,
        final_text: &'static str,
        usage: TokenUsage,
    }

    impl ScriptedProvider {
        fn new(tool_name: &'static str, final_text: &'static str, usage: TokenUsage) -> Self {
            Self {
                calls: AtomicU32::new(0),
                tool_name,
                final_text,
                usage,
            }
        }
    }

    #[async_trait]
    impl LlmProvider for ScriptedProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let events = if call == 0 {
                vec![
                    LlmEvent::ToolCallRequested {
                        call: ToolCall::new("call-1", self.tool_name, json!({})),
                    },
                    LlmEvent::Done {
                        usage: Some(self.usage),
                    },
                ]
            } else {
                vec![
                    LlmEvent::TextDelta {
                        text: self.final_text.to_owned(),
                    },
                    LlmEvent::Done {
                        usage: Some(self.usage),
                    },
                ]
            };
            Ok(Box::pin(futures::stream::iter(events)))
        }
    }

    struct TextProvider {
        text: &'static str,
        usage: TokenUsage,
    }

    #[async_trait]
    impl LlmProvider for TextProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            Ok(Box::pin(futures::stream::iter(vec![
                LlmEvent::TextDelta {
                    text: self.text.to_owned(),
                },
                LlmEvent::Done {
                    usage: Some(self.usage),
                },
            ])))
        }
    }

    /// Emits one text delta per entry, then a single `Done`.
    struct DeltaProvider {
        texts: Vec<String>,
        usage: TokenUsage,
    }

    #[async_trait]
    impl LlmProvider for DeltaProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            let mut events: Vec<LlmEvent> = self
                .texts
                .iter()
                .map(|text| LlmEvent::TextDelta { text: text.clone() })
                .collect();
            events.push(LlmEvent::Done {
                usage: Some(self.usage),
            });
            Ok(Box::pin(futures::stream::iter(events)))
        }
    }

    /// Counts provider calls so a test can prove the model was never reached.
    struct CountingProvider {
        calls: Arc<AtomicU32>,
        usage: TokenUsage,
    }

    #[async_trait]
    impl LlmProvider for CountingProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter(vec![LlmEvent::Done {
                usage: Some(self.usage),
            }])))
        }
    }

    fn descriptor(name: &str) -> ToolDescriptor {
        ToolDescriptor::new(
            name.to_owned(),
            ToolSource::Native,
            json!({
                "type": "function",
                "function": {"name": name, "parameters": {"type": "object"}}
            }),
            "test-version",
            ToolLimits::default(),
            ApprovalPolicy::NotRequired,
            &ToolMetadata::default(),
        )
        .expect("test schema must serialize")
    }

    fn catalog_with(name: &'static str, tool: Arc<MockTool>) -> ToolCatalog {
        let mut catalog = ToolCatalog::new();
        catalog
            .insert(ToolEntry {
                tool,
                descriptor: descriptor(name),
            })
            .expect("catalog insert must succeed");
        catalog
    }

    fn run_config(allowed: &[&str], rules: Vec<PolicyRule>, max_tokens: u64) -> EngineConfig {
        EngineConfig::new(TracePolicy::Disabled)
            .with_budget(Budget::new(max_tokens, Duration::from_secs(1), 4))
            .with_policy(PolicyConfig {
                allowed_tools: allowed.iter().map(|name| (*name).to_owned()).collect(),
                rules,
            })
            .with_tools(ToolExecutionConfig {
                timeout: Duration::from_secs(1),
                max_args_bytes: None,
                max_result_bytes: 1024,
            })
    }

    fn build_engine(
        provider: Arc<dyn LlmProvider>,
        tools: Arc<dyn ToolProvider>,
        config: EngineConfig,
    ) -> Engine {
        EngineBuilder::new()
            .provider(provider)
            .tool_provider(tools)
            .config(config)
            .result_config(ResultConfig::default())
            .build()
            .expect("engine must assemble")
    }

    /// A trace store that logs accepted events and can inject failures.
    #[derive(Default)]
    struct MockTraceStore {
        begin_calls: AtomicU32,
        record_calls: AtomicU32,
        finish_calls: AtomicU32,
        fail_begin: AtomicBool,
        fail_record_at: AtomicU32,
        fail_finish: AtomicBool,
        recorded: Mutex<Vec<AgentEvent>>,
        observed_prefix: Mutex<Option<(Arc<RecordingObserver>, usize)>>,
    }

    impl MockTraceStore {
        fn fail_begin(&self) {
            self.fail_begin.store(true, Ordering::SeqCst);
        }

        fn fail_record_at(&self, call: u32) {
            self.fail_record_at.store(call, Ordering::SeqCst);
        }

        fn fail_finish(&self) {
            self.fail_finish.store(true, Ordering::SeqCst);
        }

        fn begin_count(&self) -> u32 {
            self.begin_calls.load(Ordering::SeqCst)
        }

        fn record_count(&self) -> u32 {
            self.record_calls.load(Ordering::SeqCst)
        }

        fn finish_count(&self) -> u32 {
            self.finish_calls.load(Ordering::SeqCst)
        }

        fn recorded(&self) -> Vec<AgentEvent> {
            self.recorded
                .lock()
                .expect("recorded events must not be poisoned")
                .clone()
        }

        /// Makes the injected record failure wait until `observer` has seen
        /// `count` broadcast events, so the test can prove the sink ran first.
        fn observe_prefix(&self, observer: Arc<RecordingObserver>, count: usize) {
            *self
                .observed_prefix
                .lock()
                .expect("observed-prefix slot must not be poisoned") = Some((observer, count));
        }
    }

    #[async_trait]
    impl TraceStore for MockTraceStore {
        async fn begin_turn(
            &self,
            trace: &TurnTrace,
            _mode: TraceTurnMode,
            _messages: &[Message],
        ) -> Result<(), EngineError> {
            self.begin_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_begin.load(Ordering::SeqCst) {
                return Err(mock_trace_failure(trace, "injected begin failure"));
            }
            Ok(())
        }

        async fn record(&self, trace: &TurnTrace, event: AgentEvent) -> Result<(), EngineError> {
            let call = self.record_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_record_at.load(Ordering::SeqCst) == call {
                let watched = self
                    .observed_prefix
                    .lock()
                    .expect("observed-prefix slot must not be poisoned")
                    .clone();
                if let Some((observer, count)) = watched {
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while observer.event_count() < count {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect(
                        "the pump must observe every durable event before the injected failure",
                    );
                }
                return Err(mock_trace_failure(trace, "injected record failure"));
            }
            self.recorded
                .lock()
                .expect("recorded events must not be poisoned")
                .push(event);
            Ok(())
        }

        async fn finish_turn(
            &self,
            trace: &TurnTrace,
            _outcome: &TurnOutcome,
        ) -> Result<(), EngineError> {
            self.finish_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_finish.load(Ordering::SeqCst) {
                return Err(mock_trace_failure(trace, "injected finish failure"));
            }
            Ok(())
        }
    }

    fn mock_trace_failure(trace: &TurnTrace, cause: &str) -> EngineError {
        EngineError::TraceIncomplete {
            turn_id: trace.turn_id.clone(),
            cause: cause.to_owned(),
        }
    }

    /// Records every event the pump publishes after durable recording.
    #[derive(Default)]
    struct RecordingObserver {
        events: Mutex<Vec<AgentEvent>>,
    }

    impl RecordingObserver {
        fn events(&self) -> Vec<AgentEvent> {
            self.events
                .lock()
                .expect("observed events must not be poisoned")
                .clone()
        }

        fn event_count(&self) -> usize {
            self.events
                .lock()
                .expect("observed events must not be poisoned")
                .len()
        }
    }

    impl TurnObserver for RecordingObserver {
        fn publish(&self, event: &AgentEvent) {
            self.events
                .lock()
                .expect("observed events must not be poisoned")
                .push(event.clone());
        }
    }

    fn required_config() -> EngineConfig {
        EngineConfig::default()
            .with_budget(Budget::new(100, Duration::from_secs(1), 4))
            .with_policy(PolicyConfig {
                allowed_tools: Vec::new(),
                rules: Vec::new(),
            })
            .with_tools(ToolExecutionConfig {
                timeout: Duration::from_secs(1),
                max_args_bytes: None,
                max_result_bytes: 1024,
            })
    }

    fn required_engine(provider: Arc<dyn LlmProvider>, store: Arc<dyn TraceStore>) -> Engine {
        EngineBuilder::new()
            .provider(provider)
            .tool_provider(tools())
            .trace_store(store)
            .config(required_config())
            .result_config(ResultConfig::default())
            .build()
            .expect("required-trace engine must assemble")
    }

    #[test]
    fn a_required_trace_without_a_store_fails_assembly() {
        assert_eq!(
            EngineConfig::default().trace_policy(),
            TracePolicy::Required
        );

        let error = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .config(EngineConfig::default())
            .result_config(ResultConfig::default())
            .build()
            .expect_err("a required trace must reject a missing store");

        assert!(matches!(error, EngineError::MissingTraceStore));
    }

    #[tokio::test]
    async fn a_trace_that_cannot_open_never_reaches_the_provider() {
        let temp = TempDir::new("trace-begin-failure");
        let store = Arc::new(MockTraceStore::default());
        store.fail_begin();
        let calls = Arc::new(AtomicU32::new(0));
        let provider: Arc<dyn LlmProvider> = Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let engine = required_engine(provider, store_port);

        let error = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect_err("an unopened trace must fail the turn");

        match &error {
            EngineError::TraceIncomplete { turn_id, cause } => {
                assert_eq!(turn_id, "turn-1");
                assert!(
                    cause.contains("injected begin failure"),
                    "unexpected cause: {cause}"
                );
            }
            other => panic!("expected TraceIncomplete, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0, "the model must not run");
        assert_eq!(store.begin_count(), 1);
        assert_eq!(store.record_count(), 0);
        assert_eq!(store.finish_count(), 0);
    }

    #[tokio::test]
    async fn a_trace_write_failure_returns_trace_incomplete_without_turn_ended() {
        let temp = TempDir::new("trace-write-failure");
        let store = Arc::new(MockTraceStore::default());
        store.fail_record_at(3);
        let provider: Arc<dyn LlmProvider> = Arc::new(DeltaProvider {
            texts: vec!["first".to_owned(), "second".to_owned()],
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let engine = required_engine(provider, store_port);

        let error = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect_err("an unrecorded event must fail the turn");

        match &error {
            EngineError::TraceIncomplete { turn_id, cause } => {
                assert_eq!(turn_id, "turn-1");
                assert!(
                    cause.contains("injected record failure"),
                    "unexpected cause: {cause}"
                );
            }
            other => panic!("expected TraceIncomplete, got {other:?}"),
        }
        let recorded = store.recorded();
        assert_eq!(recorded.len(), 2);
        assert!(matches!(
            recorded.first(),
            Some(AgentEvent::SessionStarted { .. })
        ));
        assert!(matches!(
            recorded.get(1),
            Some(AgentEvent::RoundStarted { .. })
        ));
        assert!(
            !recorded
                .iter()
                .any(|event| matches!(event, AgentEvent::SessionEnded { .. })),
            "a rejected trace must not observe the terminal event"
        );
        assert_eq!(
            store.finish_count(),
            0,
            "a rejected trace must never receive turn_ended"
        );
    }

    #[tokio::test]
    async fn a_rejected_event_is_never_broadcast_and_no_later_event_follows() {
        let temp = TempDir::new("sink-before-broadcast");
        let observer = Arc::new(RecordingObserver::default());
        let store = Arc::new(MockTraceStore::default());
        store.fail_record_at(4);
        store.observe_prefix(Arc::clone(&observer), 3);
        let provider: Arc<dyn LlmProvider> = Arc::new(DeltaProvider {
            texts: vec!["first".to_owned(), "second".to_owned()],
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let observer_port: Arc<dyn TurnObserver> = observer.clone();
        let engine = EngineBuilder::new()
            .provider(provider)
            .tool_provider(tools())
            .trace_store(store_port)
            .observer(observer_port)
            .config(required_config())
            .result_config(ResultConfig::default())
            .build()
            .expect("the observer engine must assemble");

        let error = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect_err("the rejected event must fail the turn");

        assert!(matches!(error, EngineError::TraceIncomplete { .. }));
        let observed = observer.events();
        assert_eq!(
            observed.len(),
            3,
            "the rejected event and everything after it must stay off the broadcast"
        );
        assert!(matches!(
            observed.first(),
            Some(AgentEvent::SessionStarted { .. })
        ));
        assert!(matches!(
            observed.get(1),
            Some(AgentEvent::RoundStarted { .. })
        ));
        assert!(matches!(
            observed.get(2),
            Some(AgentEvent::ModelDelta { text }) if text == "first"
        ));
        assert_eq!(store.recorded().len(), 3);
        assert_eq!(store.finish_count(), 0);
    }

    #[tokio::test]
    async fn an_unacknowledged_turn_end_is_not_a_successful_turn() {
        let temp = TempDir::new("trace-finish-failure");
        let store = Arc::new(MockTraceStore::default());
        store.fail_finish();
        let provider: Arc<dyn LlmProvider> = Arc::new(TextProvider {
            text: "answer",
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let engine = required_engine(provider, store_port);

        let error = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect_err("a missing close acknowledgement must not be success");

        match &error {
            EngineError::TraceIncomplete { turn_id, cause } => {
                assert_eq!(turn_id, "turn-1");
                assert!(
                    cause.contains("injected finish failure"),
                    "unexpected cause: {cause}"
                );
            }
            other => panic!("expected TraceIncomplete, got {other:?}"),
        }
        let recorded = store.recorded();
        assert!(
            matches!(recorded.last(), Some(AgentEvent::SessionEnded { .. })),
            "the producer stream must be complete before the close is attempted"
        );
        assert_eq!(store.finish_count(), 1);
    }

    #[tokio::test]
    async fn a_pump_gap_is_exposed_in_an_otherwise_complete_turn() {
        let temp = TempDir::new("pump-lag");
        let store = Arc::new(MockTraceStore::default());
        let provider: Arc<dyn LlmProvider> = Arc::new(DeltaProvider {
            texts: vec!["x".to_owned(); 2000],
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let engine = required_engine(provider, store_port);

        let outcome = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect("the producer trace has no gap and must succeed");

        assert!(
            outcome.lagged,
            "the 1024-event broadcast buffer must have overflowed"
        );
        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(
            outcome.messages,
            vec![Message::user("go"), Message::assistant("x".repeat(2000))]
        );
        assert_eq!(outcome.last_output, "x".repeat(2000));
        let recorded = store.recorded();
        assert_eq!(
            recorded
                .iter()
                .filter(|event| matches!(event, AgentEvent::ModelDelta { .. }))
                .count(),
            2000,
            "the producer trace must retain every delta despite the observer gap"
        );
        assert!(matches!(
            recorded.first(),
            Some(AgentEvent::SessionStarted { .. })
        ));
        assert!(matches!(
            recorded.last(),
            Some(AgentEvent::SessionEnded { .. })
        ));
        assert_eq!(store.finish_count(), 1);
    }

    #[tokio::test]
    async fn the_sqlite_store_closes_a_trace_and_keeps_the_legacy_mirror() {
        let temp = TempDir::new("sqlite-trace");
        let database = temp.path.join("trace.sqlite3");
        let provider: Arc<dyn LlmProvider> = Arc::new(TextProvider {
            text: "answer",
            usage: TokenUsage::new(1, 1),
        });
        {
            let store: Arc<dyn TraceStore> = Arc::new(
                SqliteTraceStore::open(database.clone())
                    .await
                    .expect("the trace store must open"),
            );
            let engine = required_engine(provider, Arc::clone(&store));

            let outcome = engine
                .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
                .await
                .expect("a durable required trace must close");
            assert_eq!(outcome.reason, SessionEndReason::Completed);
            assert!(!outcome.lagged);
        }

        let store = EventStore::open(&database).expect("the database must reopen");
        let turn_id = TurnId::new("turn-1");
        let trace = store
            .trace_turn(&turn_id)
            .expect("the trace must load")
            .expect("the turn must exist");
        assert!(trace.closed(), "the trace must end with turn_ended");
        assert_eq!(trace.events().len(), 7);
        match &trace.events()[0].event().payload {
            TracePayload::TurnStarted { mode, messages } => {
                assert_eq!(*mode, TraceTurnMode::Fresh);
                assert_eq!(messages, &vec![Message::user("go")]);
            }
            other => panic!("expected turn_started, got {other:?}"),
        }
        match &trace
            .events()
            .last()
            .expect("the trace must not be empty")
            .event()
            .payload
        {
            TracePayload::TurnEnded { reason } => {
                assert_eq!(*reason, SessionEndReason::Completed);
            }
            other => panic!("expected turn_ended, got {other:?}"),
        }

        let session_id = SessionId::new("turn-1");
        let legacy = store
            .events(&session_id)
            .expect("the legacy mirror must load");
        assert_eq!(legacy.len(), 5);
        assert!(matches!(
            legacy.first().map(|event| event.event()),
            Some(AgentEvent::SessionStarted { .. })
        ));
        assert!(matches!(
            legacy.last().map(|event| event.event()),
            Some(AgentEvent::SessionEnded { .. })
        ));
        assert_eq!(
            store
                .session_messages(&session_id)
                .expect("session metadata must load"),
            Some(vec![Message::user("go")])
        );
    }

    #[tokio::test]
    async fn run_turn_executes_an_allowlisted_tool_and_returns_the_transcript() {
        let temp = TempDir::new("happy-path");
        let inner = Arc::new(MockTool::new("echo", "tool-output"));
        let tools: Arc<dyn ToolProvider> = Arc::new(StaticTools {
            catalog: catalog_with("echo", Arc::clone(&inner)),
        });
        let provider: Arc<dyn LlmProvider> = Arc::new(ScriptedProvider::new(
            "echo",
            "final answer",
            TokenUsage::new(2, 3),
        ));
        let engine = build_engine(provider, tools, run_config(&["echo"], Vec::new(), 100));

        let outcome = engine
            .run_turn(TurnRequest::new("say hi").with_workspace_root(&temp.path))
            .await
            .expect("turn must complete");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(outcome.turn_id, "turn-1");
        assert_eq!(outcome.turn_start, 1);
        assert!(!outcome.lagged);
        assert_eq!(outcome.last_output, "final answer");
        assert_eq!(outcome.messages.len(), 4);
        assert_eq!(outcome.messages[2].role, Role::Tool);
        assert_eq!(outcome.messages[2].content, "tool-output");
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn policy_denial_reaches_the_model_without_running_the_tool() {
        let temp = TempDir::new("policy-denial");
        let inner = Arc::new(MockTool::new("blocked", "must not run"));
        let tools: Arc<dyn ToolProvider> = Arc::new(StaticTools {
            catalog: catalog_with("blocked", Arc::clone(&inner)),
        });
        let provider: Arc<dyn LlmProvider> = Arc::new(ScriptedProvider::new(
            "blocked",
            "adapted",
            TokenUsage::new(1, 1),
        ));
        let rules = vec![PolicyRule {
            id: "deny-blocked".to_owned(),
            tool: Some(ToolSelector::One("blocked".to_owned())),
            path_glob: None,
            program: None,
            args_prefix: None,
            action: RuleAction::Deny,
            reason: Some("blocked by test policy".to_owned()),
        }];
        let engine = build_engine(provider, tools, run_config(&["blocked"], rules, 100));

        let outcome = engine
            .run_turn(TurnRequest::new("try it").with_workspace_root(&temp.path))
            .await
            .expect("a policy denial must not fail the turn");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert_eq!(outcome.messages.len(), 4);
        assert!(outcome.messages[2].content.contains("denied by policy"));
        assert_eq!(outcome.last_output, "adapted");
    }

    #[tokio::test]
    async fn a_token_trip_overrides_a_completed_runtime_reason() {
        let temp = TempDir::new("token-budget");
        let provider: Arc<dyn LlmProvider> = Arc::new(TextProvider {
            text: "too expensive",
            usage: TokenUsage::new(60, 60),
        });
        let tools: Arc<dyn ToolProvider> = Arc::new(EmptyTools);
        let engine = build_engine(provider, tools, run_config(&[], Vec::new(), 10));

        let outcome = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect("budget exhaustion is a turn outcome, not an engine error");

        assert_eq!(outcome.reason, SessionEndReason::BudgetExceeded);
        assert_eq!(outcome.last_output, "too expensive");
    }
}
