//! CLI implementations of the host ports consumed by `jessica-engine`.
//!
//! The engine owns turn ordering and governance. This module supplies the
//! host-owned pieces: the TOML-derived configuration snapshot, native and WASM
//! tools, the event journal, memory and skill assemblies, the system prompt,
//! process-local identifiers, sensors, acceptance checks, and the child tool
//! factory. Nothing here parses TOML or constructs an `Engine` outside
//! [`build_engine`].

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use jessica_core::acceptance::AcceptanceCheck;
use jessica_core::llm::LlmProvider;
use jessica_core::sensor::Sensor;
use jessica_core::trace::TurnId;
use jessica_core::types::{Message, SessionId};
use jessica_engine::{
    AcceptanceConfig, ApprovalConfig, ApprovalMode, CatalogLimits, ChildToolFactory, Engine,
    EngineBuilder, EngineConfig, EngineError, EventJournal, IdSource, MemoryPort, PolicyConfig,
    PolicyRule, PumpConsole, ResultConfig, RuleAction, SessionStore, SkillPort, StoredSession,
    SystemPromptSource, ToolCatalog as EngineToolCatalog, ToolContext, ToolEntry,
    ToolExecutionConfig, ToolPermissionDecision, ToolPermissionGate, ToolProvider,
    ToolResultManager, ToolSelector, TracePolicy,
};
use jessica_harness::{CommandSensor, TurnSignals};
use jessica_runtime::DelegationLimits;

use crate::acceptance_check::CommandAcceptanceCheck;
use crate::app::LoadedWasmTool;
use crate::artifacts::ResultManager;
use crate::config::{
    ApprovalConfig as CliApprovalConfig, ApprovalMode as CliApprovalMode, HarnessConfig,
    PolicyConfig as CliPolicyConfig, RuleAction as CliRuleAction, ToolProfile,
    ToolSelector as CliToolSelector,
};
use crate::delegation::NativeChildToolFactory;
use crate::events::{EventRecorder, RecorderError};
use crate::governance::ToolPermissionGate as CliToolPermissionGate;
use crate::memory::MemoryAssembly;
use crate::skills::SkillsAssembly;
use crate::tools::{CatalogCandidate, CatalogError, ToolCatalog, ToolSource};

/// Builds a per-turn engine from one immutable CLI configuration snapshot.
///
/// `extra_tools` are session-scoped tools such as ACP MCP discoveries; the
/// tool provider merges them with native tools and process-wide WASM plugins.
/// `allow_delegation` is false on resume so the engine never assembles a child
/// executor for a recorded loop. The engine configuration carries all
/// governance decisions; the CLI keeps TOML parsing, provider construction,
/// paths, and profile presets out of the engine.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_engine(
    provider: Arc<dyn LlmProvider>,
    config: Arc<HarnessConfig>,
    root: &Path,
    recorder: Option<&Arc<EventRecorder>>,
    wasm_tools: &Arc<Vec<LoadedWasmTool>>,
    memory: Option<&Arc<MemoryAssembly>>,
    skills: Option<&Arc<SkillsAssembly>>,
    extra_tools: &[CatalogCandidate],
    permission_gate: Option<Arc<dyn CliToolPermissionGate>>,
    allow_delegation: bool,
) -> Result<Engine, EngineError> {
    let delegation = delegation_limits(&config, allow_delegation);
    let result_manager = Arc::new(ResultManager::new(root, &config));
    let engine_result_manager: Arc<dyn ToolResultManager> = Arc::new(EngineResultManager {
        inner: Arc::clone(&result_manager),
    });

    let protocol_gate = permission_gate;
    let approval_gate = protocol_gate
        .clone()
        .map(|gate| Arc::new(EngineGate { inner: gate }) as Arc<dyn ToolPermissionGate>);
    let ask_gate = protocol_gate
        .or_else(|| crate::governance::cli_permission_gate(root, &config))
        .map(|gate| Arc::new(EngineGate { inner: gate }) as Arc<dyn ToolPermissionGate>);

    let trace_policy = if recorder.is_some() {
        TracePolicy::Required
    } else {
        TracePolicy::Disabled
    };
    let mut engine_config = EngineConfig::new(trace_policy)
        .with_budget(config.budget.to_budget())
        .with_max_stuck_rounds(config.budget.max_stuck_rounds)
        .with_policy(engine_policy(&config.policy))
        .with_approval(engine_approval(&config.approval))
        .with_tools(ToolExecutionConfig {
            timeout: config.budget.tool_timeout(),
            max_args_bytes: None,
            max_result_bytes: config.budget.max_result_bytes,
        })
        .with_result(ResultConfig {
            inline_threshold_bytes: config
                .tools
                .effective_inline_threshold(config.budget.max_result_bytes),
            max_inline_bytes: config
                .tools
                .effective_max_inline_bytes(config.budget.max_result_bytes),
            max_artifact_bytes: config.tools.max_artifact_bytes,
            artifact_dir: config.tools.artifact_dir.clone(),
            artifact_retention_days: config.tools.artifact_retention_days,
            artifact_max_bytes: config.tools.artifact_max_bytes,
        })
        .with_catalog_limits(CatalogLimits {
            max_exposed_tools: config.tools.max_exposed_tools,
            max_tool_schema_bytes: config.tools.max_tool_schema_bytes,
        })
        .with_delegation(delegation);
    if let Some(acceptance) = &config.acceptance {
        engine_config =
            engine_config.with_acceptance(Some(AcceptanceConfig::new(acceptance.max_retries)));
    }

    let tool_provider = Arc::new(CliToolProvider {
        config: Arc::clone(&config),
        wasm_tools: Arc::clone(wasm_tools),
        extra_tools: extra_tools.to_vec(),
    });
    let mut builder = EngineBuilder::new()
        .provider(provider)
        .tool_provider(tool_provider)
        .config(engine_config)
        .result_manager(engine_result_manager)
        .console(Arc::new(CliConsole))
        .sensors(build_sensors(root, &config))
        .system_prompt(Arc::new(CliSystemPrompt {
            root: root.to_path_buf(),
            config: Arc::clone(&config),
        }))
        .id_source(Arc::new(CliIdSource))
        // A CLI turn without a terminal installs no gate. An explicit `ask`
        // mode or an `ask` rule then fails closed when the tool executes,
        // which is the pre-v2 behavior this adapter preserves.
        .allow_missing_approval_gate();

    if let Some(recorder) = recorder {
        builder = builder
            .event_journal(Arc::clone(recorder) as Arc<dyn EventJournal>)
            .session_store(Arc::new(RecorderSessionStore {
                inner: Arc::clone(recorder),
            }));
    }
    if let Some(gate) = approval_gate {
        builder = builder.approval_gate(gate);
    }
    if let Some(gate) = ask_gate {
        builder = builder.ask_gate(gate);
    }
    if delegation.is_some() {
        let factory: Arc<dyn ChildToolFactory> = Arc::new(NativeChildToolFactory::new(
            root.to_path_buf(),
            &config,
            Arc::clone(&result_manager),
        ));
        builder = builder.child_tool_factory(factory);
    }
    if let Some(memory) = memory {
        builder = builder.memory(Arc::new(CliMemoryPort {
            inner: Arc::clone(memory),
        }));
    }
    if let Some(skills) = skills {
        builder = builder.skills(Arc::new(CliSkillPort {
            inner: Arc::clone(skills),
        }));
    }
    if let Some(check) = acceptance_check(root, &config) {
        builder = builder.acceptance_check(check);
    }

    builder.build()
}

/// Decides whether this turn may expose `delegate_task`.
///
/// Resume never enables delegation. A fresh turn enables it only when
/// `[delegation]` is enabled and the profile exposes non-native sources; the
/// CLI has always hidden `delegate_task` outside the full profile.
fn delegation_limits(config: &HarnessConfig, allow_delegation: bool) -> Option<DelegationLimits> {
    if allow_delegation && config.delegation.enabled && config.tools.profile == ToolProfile::Full {
        Some(config.delegation.to_limits())
    } else {
        None
    }
}

/// Builds the post-tool sensor list for one turn.
pub(crate) fn build_sensors(root: &Path, config: &HarnessConfig) -> Vec<Arc<dyn Sensor>> {
    let Some(sensor) = &config.sensor else {
        return Vec::new();
    };
    let supervisor = config.resolved_supervisor();
    let command = CommandSensor::new(&sensor.name, &sensor.command)
        .args(sensor.args.clone())
        .with_current_dir(resolve_dir(root, sensor.working_dir.as_deref()))
        .with_timeout(supervisor.sensor_timeout)
        .with_output_limit(supervisor.max_output_bytes)
        .with_env_allowlist(supervisor.env_allowlist);
    vec![Arc::new(command) as Arc<dyn Sensor>]
}

/// Builds the configured acceptance check for one workspace.
fn acceptance_check(root: &Path, config: &HarnessConfig) -> Option<Arc<dyn AcceptanceCheck>> {
    let acceptance = config.acceptance.as_ref()?;
    let check = CommandAcceptanceCheck::new(
        acceptance.name.clone(),
        acceptance.command.clone(),
        acceptance.args.clone(),
        resolve_dir(root, acceptance.working_dir.as_deref()),
    )
    .with_supervisor(&config.resolved_supervisor());
    Some(Arc::new(check) as Arc<dyn AcceptanceCheck>)
}

/// Resolves a configured path against the workspace root.
pub(crate) fn resolve_dir(root: &Path, configured: Option<&Path>) -> PathBuf {
    match configured {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => root.join(path),
        None => root.to_path_buf(),
    }
}

/// Returns the engine policy decision for one CLI rule action.
fn engine_rule_action(action: CliRuleAction) -> RuleAction {
    match action {
        CliRuleAction::Deny => RuleAction::Deny,
        CliRuleAction::Ask => RuleAction::Ask,
        CliRuleAction::Allow => RuleAction::Allow,
    }
}

/// Maps the CLI policy configuration into the engine's rule model.
fn engine_policy(config: &CliPolicyConfig) -> PolicyConfig {
    PolicyConfig {
        allowed_tools: config.allowed_tools.clone(),
        rules: config
            .rules
            .iter()
            .map(|rule| PolicyRule {
                id: rule.id.clone(),
                tool: rule.tool.as_ref().map(|tool| match tool {
                    CliToolSelector::One(name) => ToolSelector::One(name.clone()),
                    CliToolSelector::Many(names) => ToolSelector::Many(names.clone()),
                }),
                path_glob: rule.path_glob.clone(),
                program: rule.program.clone(),
                args_prefix: rule.args_prefix.clone(),
                action: engine_rule_action(rule.action),
                reason: rule.reason.clone(),
            })
            .collect(),
    }
}

/// Maps the CLI approval configuration into the engine's approval model.
fn engine_approval(config: &CliApprovalConfig) -> ApprovalConfig {
    ApprovalConfig {
        mode: match config.mode {
            CliApprovalMode::Ask => ApprovalMode::Ask,
            CliApprovalMode::Allow => ApprovalMode::Allow,
            CliApprovalMode::Deny => ApprovalMode::Deny,
        },
        explicit: config.explicit,
    }
}

/// Supplies native, WASM, and session-scoped tools to one engine turn.
struct CliToolProvider {
    /// CLI configuration snapshot for this turn.
    config: Arc<HarnessConfig>,
    /// Process-wide compiled WASM plugins.
    wasm_tools: Arc<Vec<LoadedWasmTool>>,
    /// ACP MCP tools discovered for this session.
    extra_tools: Vec<CatalogCandidate>,
}

#[async_trait]
impl ToolProvider for CliToolProvider {
    async fn provide(&self, context: &ToolContext<'_>) -> Result<EngineToolCatalog, EngineError> {
        let mut candidates = crate::tools::native_candidates(context.workspace_root, &self.config);
        candidates.extend(self.wasm_tools.iter().map(|loaded| {
            CatalogCandidate::from_tool(Arc::clone(&loaded.tool), ToolSource::Wasm)
                .with_metadata(loaded.metadata.clone())
        }));
        candidates.extend(self.extra_tools.iter().cloned());

        let catalog = ToolCatalog::build(candidates, &self.config).map_err(catalog_error)?;
        let mut engine_catalog = EngineToolCatalog::new();
        for entry in catalog.exposed() {
            let Some(tool) = entry.tool.as_ref() else {
                return Err(EngineError::MissingToolImplementation {
                    name: entry.descriptor.name.clone(),
                });
            };
            engine_catalog.insert(ToolEntry {
                tool: Arc::clone(tool),
                descriptor: entry.descriptor.clone(),
            })?;
        }
        Ok(engine_catalog)
    }
}

/// Maps a CLI catalog error into an engine error without losing its cause.
fn catalog_error(error: CatalogError) -> EngineError {
    match error {
        CatalogError::Duplicate { name } => EngineError::DuplicateTool { name },
        CatalogError::MissingImplementation { name } => {
            EngineError::MissingToolImplementation { name }
        }
        other => EngineError::HostPort {
            component: "tool catalog".to_owned(),
            message: other.to_string(),
        },
    }
}

/// Adapts the CLI artifact manager to the engine result-manager port.
struct EngineResultManager {
    /// CLI manager bound to this turn's canonical root.
    inner: Arc<ResultManager>,
}

#[async_trait]
impl ToolResultManager for EngineResultManager {
    async fn manage(
        &self,
        tool: &str,
        format: jessica_engine::ResultFormat,
        result: jessica_core::types::ToolResult,
    ) -> jessica_core::types::ToolResult {
        self.inner.manage(tool, format, result).await
    }
}

/// Loads recorded sessions through the CLI event recorder.
struct RecorderSessionStore {
    /// Durable recorder shared with the turn journal.
    inner: Arc<EventRecorder>,
}

#[async_trait]
impl SessionStore for RecorderSessionStore {
    async fn load_session(
        &self,
        session_id: SessionId,
    ) -> Result<Option<StoredSession>, EngineError> {
        self.inner
            .load(session_id)
            .await
            .map_err(|error: RecorderError| EngineError::Recorder(error))
    }
}

/// Supplies the CLI's fixed initial conversation context.
struct CliSystemPrompt {
    /// Canonical workspace root for this turn.
    root: PathBuf,
    /// Configuration snapshot that decides the allowlist text.
    config: Arc<HarnessConfig>,
}

impl SystemPromptSource for CliSystemPrompt {
    fn system_prompt(&self) -> String {
        system_prompt(&self.root, &self.config)
    }
}

/// Renders the CLI system prompt for one configuration snapshot.
fn system_prompt(root: &Path, config: &HarnessConfig) -> String {
    let allowed = config
        .policy
        .allowed_tools
        .iter()
        .filter(|name| config.tools.bash.enabled || *name != "bash")
        .map(String::as_str)
        .collect::<Vec<_>>();
    let allowed = if allowed.is_empty() {
        "none".to_owned()
    } else {
        allowed.join(", ")
    };
    format!(
        "You are Jessica, a deterministic coding agent. The workspace root is {}. \
         Use read_file, list_dir, grep, git_status, git_diff, write_file, apply_patch, and \
         edit_file with paths relative to the workspace root. exec runs one program with an \
         explicit argument \
         array and never goes through a shell. Tool calls are subject to this allowlist: \
         {allowed}. A denied call returns an error result, so adapt instead of repeating it. \
         A command sensor runs after every tool result. If an acceptance check fails, its \
         feedback is injected as a user message: fix the underlying problem and retry. \
         Verify your work with tools instead of claiming success. When the task is complete, \
         reply with a concise summary.",
        root.display()
    )
}

/// Supplies the CLI's process-local turn and session identifiers.
struct CliIdSource;

impl IdSource for CliIdSource {
    fn next_turn_id(&self) -> String {
        next_turn_id().to_string()
    }

    fn next_session_id(&self) -> SessionId {
        next_session_id()
    }
}

/// Recalls and writes long-term memory through the CLI assembly.
struct CliMemoryPort {
    /// Memory assembly owned by the runtime.
    inner: Arc<MemoryAssembly>,
}

#[async_trait]
impl MemoryPort for CliMemoryPort {
    async fn recall(
        &self,
        session_id: &SessionId,
        user_input: &str,
    ) -> Result<Option<Message>, EngineError> {
        Ok(self.inner.recall(session_id, user_input).await)
    }

    async fn record_completed_turn(
        &self,
        session_id: &SessionId,
        turn_index: u64,
        user_input: &str,
        assistant_answer: &str,
        tool_call_count: usize,
    ) -> Result<(), EngineError> {
        self.inner
            .record_completed_turn(
                session_id,
                user_input,
                assistant_answer,
                tool_call_count,
                turn_index,
            )
            .await
            .map_err(|error| EngineError::HostPort {
                component: "memory".to_owned(),
                message: error.to_string(),
            })
    }
}

/// Recalls and enqueues skills through the CLI assembly.
struct CliSkillPort {
    /// Skill assembly owned by the runtime.
    inner: Arc<SkillsAssembly>,
}

#[async_trait]
impl SkillPort for CliSkillPort {
    async fn recall(
        &self,
        session_id: &SessionId,
        user_input: &str,
    ) -> Result<Option<Message>, EngineError> {
        Ok(self.inner.recall(session_id, user_input).await)
    }

    async fn after_completed_turn(
        &self,
        session_id: &SessionId,
        turn_index: u64,
        user_input: &str,
        assistant_answer: &str,
        signals: &TurnSignals,
        messages: &[Message],
    ) -> Result<(), EngineError> {
        self.inner
            .after_completed_turn(
                session_id,
                turn_index,
                user_input,
                assistant_answer,
                signals,
                messages,
            )
            .await;
        Ok(())
    }
}

/// Adapts a CLI permission gate to the engine port.
struct EngineGate {
    /// CLI gate implementation.
    inner: Arc<dyn CliToolPermissionGate>,
}

#[async_trait]
impl ToolPermissionGate for EngineGate {
    async fn request_permission(
        &self,
        call: &jessica_core::types::ToolCall,
    ) -> ToolPermissionDecision {
        match self.inner.request_permission(call).await {
            crate::governance::ToolPermissionDecision::Allow => ToolPermissionDecision::Allow,
            crate::governance::ToolPermissionDecision::Reject => ToolPermissionDecision::Reject,
            crate::governance::ToolPermissionDecision::Cancelled => {
                ToolPermissionDecision::Cancelled
            }
            crate::governance::ToolPermissionDecision::TimedOut => ToolPermissionDecision::TimedOut,
            crate::governance::ToolPermissionDecision::TransportFailed => {
                ToolPermissionDecision::TransportFailed
            }
        }
    }
}

/// Host console that renders pump output to stdout.
///
/// The engine falls back to its built-in console when none is installed; the
/// CLI keeps this adapter explicit so terminal rendering is owned by the host
/// assembly rather than by the engine.
pub(crate) struct CliConsole;

impl PumpConsole for CliConsole {
    fn line(&self, text: &str) {
        println!("{text}");
    }

    fn inline(&self, text: &str) {
        print!("{text}");
    }

    fn diagnostic(&self, text: &str) {
        eprintln!("{text}");
    }
}

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);
static TURN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a process-local session id.
fn next_session_id() -> SessionId {
    let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    SessionId::new(format!("cli-{}-{counter}", std::process::id()))
}

/// Generates a process-unique turn id.
///
/// The timestamp, process id, and per-process counter make collisions
/// practically impossible. The store rejects a duplicate id instead of
/// silently merging two turns, so a collision still fails closed.
fn next_turn_id() -> TurnId {
    let counter = TURN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    TurnId::new(format!("turn-{}-{nanos}-{counter}", std::process::id()))
}
