//! Public engine assembly and turn types.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_core::llm::LlmProvider;
use jessica_core::policy::Policy;
use jessica_core::tool::ToolRegistry;
use jessica_core::types::{Message, Role, SessionEndReason, SessionId};
use jessica_harness::{AllowlistPolicy, ResultSizeGuard, TokenBudgetGuard, ToolTimeoutGuard};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::artifacts::ResultManager;
use crate::config::{EngineConfig, ResultConfig};
use crate::error::EngineError;
use crate::governance::{GovernanceState, GovernedTool, TokenCountingProvider};
use crate::policy::{PolicyTool, PolicyV2};
use crate::ports::{ToolContext, ToolPermissionGate, ToolProvider};
use crate::tooling::ToolEntry;

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
/// provider-to-tool vertical slice for each [`TurnRequest`]. Journal, pump,
/// acceptance, memory, skill, delegation, and resume orchestration are later
/// Phase 11 tasks.
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

    /// Runs one minimal governed turn.
    ///
    /// The turn builds a fresh tool catalog, filters it through the allowlist,
    /// wraps every exposed tool with [`GovernedTool`], and drives
    /// [`jessica_runtime::AgentLoop`] through the token-counting provider. It
    /// deliberately does not persist a trace, pump UI events, run acceptance,
    /// recall memory or skills, delegate, or resume; those belong to E2–E5.
    ///
    /// A budget trip is reported as [`SessionEndReason::BudgetExceeded`] even
    /// when the runtime loop itself returned another reason, because the
    /// governance state is the authoritative budget signal.
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
        let (events, _receiver) = broadcast::channel(1024);
        let governance = Arc::new(GovernanceState::new(events.clone(), cancel.clone()));

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
            governance,
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
        let initial = vec![Message::user(request.input())];
        let turn_start = initial.len();
        let transcript = agent
            .run_with_transcript(
                session_id,
                initial,
                self.config.max_tool_calls(),
                cancel.clone(),
            )
            .await;
        let reason = if tooling.governance.tripped()
            && transcript.reason != SessionEndReason::BudgetExceeded
        {
            SessionEndReason::BudgetExceeded
        } else {
            transcript.reason
        };
        let last_output = transcript
            .messages
            .iter()
            .rev()
            .find(|message| message.role == Role::Assistant)
            .map(|message| message.content.clone())
            .unwrap_or_default();

        Ok(TurnOutcome {
            reason,
            messages: transcript.messages,
            last_output,
            turn_start,
            turn_id,
            lagged: false,
        })
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
/// The builder deliberately accepts no pre-assembled `Harness`, `ToolRegistry`,
/// or trace store: every governed dependency crosses a named port, and the
/// trace policy must be explicit. A policy that can ask for approval also
/// requires a permission gate at build time.
#[derive(Default)]
pub struct EngineBuilder {
    provider: Option<Arc<dyn LlmProvider>>,
    tool_provider: Option<Arc<dyn ToolProvider>>,
    approval_gate: Option<Arc<dyn ToolPermissionGate>>,
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
    /// deterministic first error. An approval gate is required when the policy
    /// contains an ask rule or when an explicit ask mode is configured.
    pub fn build(self) -> Result<Engine, EngineError> {
        let provider = self.provider.ok_or(EngineError::MissingProvider)?;
        let tool_provider = self.tool_provider.ok_or(EngineError::MissingToolProvider)?;
        let config = self.config.ok_or(EngineError::MissingConfig)?;
        let result_config = self.result_config.ok_or(EngineError::MissingResultConfig)?;
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
            },
            next_turn: AtomicU64::new(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::time::Duration;

    use async_trait::async_trait;
    use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
    use jessica_core::tool::{Tool, ToolSchema};
    use jessica_core::types::{Budget, Message, TokenUsage, ToolCall, ToolResult};
    use serde_json::{Value, json};

    use super::*;
    use crate::config::{
        ApprovalConfig, ApprovalMode, PolicyConfig, PolicyRule, RuleAction, ToolExecutionConfig,
        ToolSelector, TracePolicy,
    };
    use crate::ports::{ToolContext, ToolPermissionDecision};
    use crate::tooling::{
        ApprovalPolicy, Capabilities, ResultFormat, ToolCatalog, ToolDescriptor, ToolLimits,
        ToolSource,
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
        EngineConfig::new(TracePolicy::Disabled).expect("disabled trace is supported")
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
        let configured = EngineConfig::new(TracePolicy::Disabled)
            .expect("disabled trace is supported")
            .with_policy(policy);

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
        let configured = EngineConfig::new(TracePolicy::Disabled)
            .expect("disabled trace is supported")
            .with_approval(ApprovalConfig {
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

    fn descriptor(name: &str) -> ToolDescriptor {
        ToolDescriptor {
            name: name.to_owned(),
            source: ToolSource::Native,
            capabilities: Capabilities::default(),
            approval: ApprovalPolicy::NotRequired,
            result_format: ResultFormat::Text,
            path_field: None,
            limits: ToolLimits::default(),
        }
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
            .expect("disabled trace")
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
