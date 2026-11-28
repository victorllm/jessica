//! CLI-side adapters for engine governance primitives.
//!
//! [`GovernanceState`] and [`TokenCountingProvider`] are engine types
//! re-exported here for existing CLI call sites. [`GovernedTool`] remains the
//! CLI-facing wrapper: it adapts the harness allowlist, CLI policy v2
//! evaluator, terminal and ACP permission gates, and artifact result manager
//! into engine ports, then delegates ordering and enforcement to
//! [`jessica_engine::GovernedTool`].
//!
//! Keeping the adapter in the CLI preserves host-owned configuration and
//! descriptor types without duplicating the governance order in two crates.

use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::policy::{Policy, PolicyDecision};
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::{AgentEvent, ToolCall, ToolResult};
use jessica_harness::Harness;
use serde_json::Value;
use tokio::io::AsyncBufReadExt;

use crate::approval::approval_request_input;
use crate::artifacts::ResultManager;
use crate::config::{ApprovalMode, HarnessConfig, RuleAction};
use crate::policy_v2::{
    PolicyError as CliPolicyError, PolicyTool, PolicyV2, PolicyVerdict as CliPolicyVerdict,
    RuleMatch as CliRuleMatch,
};
use crate::tools::ResultFormat;

pub(crate) use jessica_engine::{GovernanceState, TokenCountingProvider};

/// Names of native tools that require explicit approval before running.
///
/// The set is deliberately fixed: `read_file`, `list_dir`, `grep`,
/// `git_status`, and `git_diff` remain governed by the allowlist policy alone.
/// Configured WASM and MCP tools are not named here because the catalog assigns
/// their approval from host-owned capability classification, where a
/// destructive or open-world descriptor becomes interactive. Delegated child
/// tools are also outside this set because the single top-level
/// `delegate_task` approval covers the child run.
pub(crate) fn permission_required(name: &str) -> bool {
    matches!(
        name,
        "bash"
            | "exec"
            | "write_file"
            | "apply_patch"
            | "edit_file"
            | jessica_runtime::DELEGATE_TASK_TOOL_NAME
    )
}

/// Decision produced by a [`ToolPermissionGate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolPermissionDecision {
    /// The user approved this one execution.
    Allow,
    /// The user rejected this one execution.
    Reject,
    /// The prompt or the permission request was cancelled.
    Cancelled,
    /// No decision arrived before the gate's fixed deadline.
    TimedOut,
    /// The request could not be delivered or answered.
    TransportFailed,
}

/// Maps a CLI gate decision to the engine port decision.
fn engine_decision(decision: ToolPermissionDecision) -> jessica_engine::ToolPermissionDecision {
    match decision {
        ToolPermissionDecision::Allow => jessica_engine::ToolPermissionDecision::Allow,
        ToolPermissionDecision::Reject => jessica_engine::ToolPermissionDecision::Reject,
        ToolPermissionDecision::Cancelled => jessica_engine::ToolPermissionDecision::Cancelled,
        ToolPermissionDecision::TimedOut => jessica_engine::ToolPermissionDecision::TimedOut,
        ToolPermissionDecision::TransportFailed => {
            jessica_engine::ToolPermissionDecision::TransportFailed
        }
    }
}

/// Asks a client for approval before a sensitive tool runs.
///
/// Implementations run after policy authorization and before the tool timeout
/// starts. They never execute the tool themselves. On [`Allow`] the gate has
/// already arranged any protocol-visible transition from `pending` to
/// `in_progress`; every other decision must leave the tool unexecuted.
///
/// [`Allow`]: ToolPermissionDecision::Allow
#[async_trait]
pub(crate) trait ToolPermissionGate: Send + Sync {
    /// Requests permission for `call`.
    ///
    /// `call.id` is the provider-assigned id. Gates must not synthesize or
    /// send an empty id to the client.
    async fn request_permission(&self, call: &ToolCall) -> ToolPermissionDecision;
}

/// Returns the interactive terminal gate for policy `ask` verdicts.
///
/// One-shot and REPL turns have no protocol client. If stdin is a terminal and
/// the configuration needs an answer (an explicit `ask` mode or at least one
/// `ask` rule), the CLI prompts for a one-time decision. Otherwise no gate is
/// installed and an `ask` outcome fails closed. ACP always supplies its own
/// gate. This gate is not used for the legacy sensitive-tool flow.
pub(crate) fn cli_permission_gate(
    root: &Path,
    config: &HarnessConfig,
) -> Option<Arc<dyn ToolPermissionGate>> {
    let explicit_ask = config.approval.explicit && config.approval.mode == ApprovalMode::Ask;
    let rule_asks = config
        .policy
        .rules
        .iter()
        .any(|rule| rule.action == RuleAction::Ask);
    if (explicit_ask || rule_asks) && std::io::stdin().is_terminal() {
        Some(Arc::new(CliPermissionGate {
            root: root.to_path_buf(),
            timeout: Duration::from_secs(120),
        }))
    } else {
        None
    }
}

/// Permission gate that prompts on stderr and reads one line from stdin.
struct CliPermissionGate {
    /// Workspace root used for the bounded write-file diff summary.
    root: std::path::PathBuf,
    /// Maximum time to wait for an answer.
    timeout: Duration,
}

#[async_trait]
impl ToolPermissionGate for CliPermissionGate {
    async fn request_permission(&self, call: &ToolCall) -> ToolPermissionDecision {
        let summary = approval_request_input(&self.root, call).await;
        eprintln!(
            "[approval] tool `{}` requires approval for this execution",
            call.name
        );
        eprintln!("[approval] arguments: {summary}");
        eprintln!("[approval] allow once? [y/N]");

        let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
        let mut answer = String::new();
        match tokio::time::timeout(self.timeout, reader.read_line(&mut answer)).await {
            Err(_) => ToolPermissionDecision::TimedOut,
            Ok(Err(_)) => ToolPermissionDecision::TransportFailed,
            Ok(Ok(0)) => ToolPermissionDecision::Reject,
            Ok(Ok(_)) => match answer.trim().to_ascii_lowercase().as_str() {
                "y" | "yes" => ToolPermissionDecision::Allow,
                _ => ToolPermissionDecision::Reject,
            },
        }
    }
}

/// Adapts the CLI harness allowlist to the core policy contract.
struct HarnessPolicy {
    /// Harness holding the configured allowlist.
    harness: Arc<Harness>,
}

impl Policy for HarnessPolicy {
    fn check(&self, call: &ToolCall) -> PolicyDecision {
        match self.harness.authorize_tool_call(call) {
            None => PolicyDecision::Allow,
            Some(AgentEvent::PolicyDenied { reason, .. }) => PolicyDecision::Deny(reason),
            Some(_) => PolicyDecision::Deny("tool denied by policy".to_owned()),
        }
    }
}

/// Adapts a CLI permission gate to the engine port.
struct EngineGate {
    /// CLI gate implementation.
    inner: Arc<dyn ToolPermissionGate>,
}

#[async_trait]
impl jessica_engine::ToolPermissionGate for EngineGate {
    async fn request_permission(&self, call: &ToolCall) -> jessica_engine::ToolPermissionDecision {
        engine_decision(self.inner.request_permission(call).await)
    }
}

/// Adapts the CLI policy v2 evaluator to the engine port.
struct EngineToolPolicy {
    /// CLI evaluator.
    inner: Arc<PolicyV2>,
    /// Host facts for the tool.
    tool: PolicyTool,
}

#[async_trait]
impl jessica_engine::ToolPolicy for EngineToolPolicy {
    async fn evaluate(
        &self,
        call: &ToolCall,
    ) -> Result<jessica_engine::PolicyVerdict, jessica_engine::PolicyError> {
        self.inner
            .evaluate(&self.tool, call)
            .await
            .map(engine_verdict)
            .map_err(engine_policy_error)
    }
}

/// Maps a CLI policy verdict to the engine verdict.
fn engine_verdict(verdict: CliPolicyVerdict) -> jessica_engine::PolicyVerdict {
    match verdict {
        CliPolicyVerdict::Allow { reason, rule } => jessica_engine::PolicyVerdict::Allow {
            reason,
            rule: rule.map(engine_rule),
        },
        CliPolicyVerdict::Legacy { sensitive } => {
            jessica_engine::PolicyVerdict::Legacy { sensitive }
        }
        CliPolicyVerdict::AutoApprove { rule, reason } => {
            jessica_engine::PolicyVerdict::AutoApprove {
                rule: rule.map(engine_rule),
                reason,
            }
        }
        CliPolicyVerdict::Ask { rule, reason } => jessica_engine::PolicyVerdict::Ask {
            rule: rule.map(engine_rule),
            reason,
        },
        CliPolicyVerdict::Deny { rule, reason } => jessica_engine::PolicyVerdict::Deny {
            rule: rule.map(engine_rule),
            reason,
        },
    }
}

/// Maps a CLI matched rule to the engine matched rule.
fn engine_rule(rule: CliRuleMatch) -> jessica_engine::RuleMatch {
    jessica_engine::RuleMatch {
        rule_id: rule.rule_id,
        reason: rule.reason,
    }
}

/// Maps a CLI policy error to the engine policy error without losing context.
fn engine_policy_error(error: CliPolicyError) -> jessica_engine::PolicyError {
    use jessica_engine::PolicyError as EnginePolicyError;
    match error {
        CliPolicyError::Root { root, source } => EnginePolicyError::Root { root, source },
        CliPolicyError::EmptyPath => EnginePolicyError::EmptyPath,
        CliPolicyError::Escape { path } => EnginePolicyError::Escape { path },
        CliPolicyError::NotFound { path } => EnginePolicyError::NotFound { path },
        CliPolicyError::Io { path, source } => EnginePolicyError::Io { path, source },
        CliPolicyError::NonUtf8 { path } => EnginePolicyError::NonUtf8 { path },
        CliPolicyError::InvalidGlob { glob, reason } => {
            EnginePolicyError::InvalidGlob { glob, reason }
        }
        CliPolicyError::InvalidPatch { reason } => EnginePolicyError::InvalidPatch { reason },
    }
}

/// Adapts the CLI result manager to the engine port.
struct EngineResultManager {
    /// CLI manager implementation.
    inner: Arc<ResultManager>,
}

#[async_trait]
impl jessica_engine::ToolResultManager for EngineResultManager {
    async fn manage(
        &self,
        tool: &str,
        format: jessica_engine::ResultFormat,
        result: ToolResult,
    ) -> ToolResult {
        self.inner
            .manage(tool, cli_result_format(format), result)
            .await
    }
}

/// Maps a CLI result format to the engine result format.
fn engine_result_format(format: ResultFormat) -> jessica_engine::ResultFormat {
    match format {
        ResultFormat::Text => jessica_engine::ResultFormat::Text,
        ResultFormat::Json => jessica_engine::ResultFormat::Json,
    }
}

/// Maps an engine result format to the CLI result format.
fn cli_result_format(format: jessica_engine::ResultFormat) -> ResultFormat {
    match format {
        jessica_engine::ResultFormat::Text => ResultFormat::Text,
        jessica_engine::ResultFormat::Json => ResultFormat::Json,
    }
}

/// Tool adapter that preserves the CLI builder surface while delegating the
/// governed execution order to [`jessica_engine::GovernedTool`].
pub(crate) struct GovernedTool {
    /// Engine-owned wrapper.
    inner: jessica_engine::GovernedTool,
}

impl GovernedTool {
    /// Wraps `inner` with the governance owned by one CLI turn.
    ///
    /// The guard instances are the same ones registered with the harness, so a
    /// trip is visible to both the tool path and model-round checkpoints.
    pub(crate) fn new(
        inner: Arc<dyn Tool>,
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        timeout: Duration,
        timeout_guard: Arc<jessica_harness::ToolTimeoutGuard>,
        result_guard: Arc<jessica_harness::ResultSizeGuard>,
    ) -> Self {
        let tool_name = inner.name().to_owned();
        Self {
            inner: jessica_engine::GovernedTool::new(
                inner,
                Arc::new(HarnessPolicy { harness }),
                governance,
                timeout,
                timeout_guard,
                result_guard,
            )
            .with_legacy_sensitive(permission_required(&tool_name)),
        }
    }

    /// Attaches the policy v2 evaluator and the host facts for this tool.
    ///
    /// The evaluator runs after the harness allowlist and before any
    /// permission gate or execution. Without it the wrapper keeps the pre-v2
    /// approval behavior.
    pub(crate) fn with_policy_v2(mut self, policy: Arc<PolicyV2>, tool: PolicyTool) -> Self {
        let legacy_sensitive = tool.legacy_sensitive;
        self.inner = self
            .inner
            .with_tool_policy(Arc::new(EngineToolPolicy {
                inner: policy,
                tool,
            }))
            .with_legacy_sensitive(legacy_sensitive);
        self
    }

    /// Sets the optional serialized-argument size limit.
    ///
    /// The precheck runs before policy, permission, and execution, so an
    /// oversized call cannot reach any of them. Exceeding the limit returns an
    /// error result without tripping the session budget.
    pub(crate) fn with_max_args_bytes(mut self, max_args_bytes: Option<usize>) -> Self {
        self.inner = self.inner.with_max_args_bytes(max_args_bytes);
        self
    }

    /// Attaches the optional legacy approval gate for sensitive tools.
    ///
    /// The gate is consulted only when policy returns
    /// [`jessica_engine::PolicyVerdict::Legacy`] for a sensitive tool. ACP
    /// supplies it; a CLI turn normally leaves it absent to preserve pre-v2
    /// behavior.
    pub(crate) fn with_permission_gate(
        mut self,
        permission: Option<Arc<dyn ToolPermissionGate>>,
    ) -> Self {
        let gate = permission.map(|gate| {
            Arc::new(EngineGate { inner: gate }) as Arc<dyn jessica_engine::ToolPermissionGate>
        });
        self.inner = self.inner.with_permission_gate(gate);
        self
    }

    /// Attaches the optional gate used by policy `ask` verdicts.
    ///
    /// This gate covers `ask` rules and explicit approval modes. It is
    /// separate from [`Self::with_permission_gate`] so a CLI `ask` rule does
    /// not start prompting for unrelated legacy-sensitive tools.
    pub(crate) fn with_ask_gate(mut self, ask_gate: Option<Arc<dyn ToolPermissionGate>>) -> Self {
        let gate = ask_gate.map(|gate| {
            Arc::new(EngineGate { inner: gate }) as Arc<dyn jessica_engine::ToolPermissionGate>
        });
        self.inner = self.inner.with_ask_gate(gate);
        self
    }

    /// Attaches the turn's result manager and the tool's descriptor format.
    ///
    /// Managed results replace oversized content with a bounded preview and a
    /// digest before the result-size guard, sensor, transcript, trace, or model
    /// observes them. `read_artifact` skips management inside the manager so
    /// pagination cannot recurse.
    pub(crate) fn with_result_manager(
        mut self,
        result_manager: Option<Arc<ResultManager>>,
        result_format: ResultFormat,
    ) -> Self {
        let manager = result_manager.map(|inner| Arc::new(EngineResultManager { inner }));
        self.inner = self
            .inner
            .with_result_manager(manager, engine_result_format(result_format));
        self
    }
}

#[async_trait]
impl Tool for GovernedTool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema(&self) -> ToolSchema {
        self.inner.schema()
    }

    async fn execute(&self, args: Value) -> ToolResult {
        self.inner.execute(args).await
    }

    async fn execute_with_call(&self, call: &ToolCall) -> ToolResult {
        self.inner.execute_with_call(call).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use futures::StreamExt;
    use jessica_core::events::{EventSink, EventSinkError};
    use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
    use jessica_core::types::{BudgetKind, Message, PermissionDecision, TokenUsage};
    use jessica_harness::{AllowlistPolicy, ResultSizeGuard, TokenBudgetGuard, ToolTimeoutGuard};
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;

    use crate::config::{ApprovalConfig, PolicyConfig, PolicyRuleConfig, RuleAction, ToolSelector};
    use crate::tools::{Capabilities, ToolSource};

    struct MockTool {
        name: &'static str,
        content: String,
        delay: Duration,
        calls: Arc<AtomicU32>,
    }

    impl MockTool {
        fn new(name: &'static str, content: impl Into<String>, delay: Duration) -> Self {
            Self {
                name,
                content: content.into(),
                delay,
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
            serde_json::json!({"type": "function", "function": {"name": self.name}})
        }

        async fn execute(&self, _args: Value) -> ToolResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            ToolResult::success(String::new(), self.content.clone())
        }
    }

    fn governance() -> (
        Arc<GovernanceState>,
        CancellationToken,
        broadcast::Receiver<AgentEvent>,
    ) {
        let (events, receiver) = broadcast::channel(16);
        let cancel = CancellationToken::new();
        let state = Arc::new(GovernanceState::new(events, cancel.clone()));
        (state, cancel, receiver)
    }

    fn harness(allowed: &[&str]) -> Arc<Harness> {
        let policy = AllowlistPolicy::new(allowed.iter().map(|name| (*name).to_owned()));
        Arc::new(Harness::new(Vec::new()).with_policy(Arc::new(policy)))
    }

    fn expect_budget_event(
        receiver: &mut broadcast::Receiver<AgentEvent>,
        expected: BudgetKind,
    ) -> String {
        match receiver.try_recv() {
            Ok(AgentEvent::BudgetExceeded { kind, message }) => {
                assert_eq!(kind, expected);
                message
            }
            other => panic!("expected BudgetExceeded({expected:?}), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn policy_denial_returns_an_error_result_and_does_not_stop_the_session() {
        let inner = Arc::new(MockTool::new("blocked", "should not run", Duration::ZERO));
        let harness = harness(&["allowed"]);
        let (governance, cancel, mut events) = governance();
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness,
            Arc::clone(&governance),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        );

        let result = tool.execute(serde_json::json!({"value": 1})).await;

        assert!(result.is_error);
        assert!(result.content.contains("denied by policy"));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert!(!governance.tripped());
        assert!(!cancel.is_cancelled());
        match events.try_recv() {
            Ok(AgentEvent::PolicyDenied { call, reason }) => {
                assert_eq!(call.name, "blocked");
                assert!(reason.contains("allowlist"));
            }
            other => panic!("expected PolicyDenied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn max_args_bytes_allows_the_exact_limit_and_rejects_one_byte_over() {
        let args = serde_json::json!({"value": 1});
        let limit = serde_json::to_vec(&args)
            .expect("arguments must serialize")
            .len();

        let inner = Arc::new(MockTool::new("fits", "ran", Duration::ZERO));
        let (fits_governance, _, _events) = governance();
        let fits = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["fits"]),
            Arc::clone(&fits_governance),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_max_args_bytes(Some(limit));
        let result = fits.execute(args.clone()).await;
        assert!(!result.is_error, "the exact limit must pass: {result:?}");
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);

        let inner = Arc::new(MockTool::new("too_big", "ran", Duration::ZERO));
        let (governance, cancel, mut events) = governance();
        let too_big = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["too_big"]),
            Arc::clone(&governance),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_max_args_bytes(Some(limit - 1));
        let result = too_big.execute(args).await;
        assert!(result.is_error);
        assert!(
            result.content.contains("max_args_bytes"),
            "the error must explain the configured limit: {}",
            result.content
        );
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            0,
            "oversized arguments must never reach the tool"
        );
        assert!(!governance.tripped());
        assert!(!cancel.is_cancelled());
        assert!(
            events.try_recv().is_err(),
            "the argument check must not emit a policy or budget event"
        );
    }

    #[tokio::test]
    async fn max_args_bytes_is_checked_before_policy_authorization() {
        let inner = Arc::new(MockTool::new("blocked", "must not run", Duration::ZERO));
        let (governance, _, mut events) = governance();
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&[]),
            governance,
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_max_args_bytes(Some(1));

        let result = tool.execute(serde_json::json!({"value": 1})).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("max_args_bytes"),
            "the argument precheck must win over the policy denial: {}",
            result.content
        );
        assert!(!result.content.contains("denied by policy"));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert!(
            events.try_recv().is_err(),
            "no PolicyDenied event may be recorded when the call never reaches policy"
        );
    }

    #[tokio::test]
    async fn oversized_result_trips_result_size_guard_and_cancels() {
        let inner = Arc::new(MockTool::new("large", "x".repeat(65), Duration::ZERO));
        let (governance, cancel, mut events) = governance();
        let tool = GovernedTool::new(
            inner as Arc<dyn Tool>,
            harness(&["large"]),
            Arc::clone(&governance),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(64)),
        );

        let result = tool.execute(serde_json::json!({})).await;

        assert!(result.is_error);
        assert!(governance.tripped());
        assert!(cancel.is_cancelled());
        let message = expect_budget_event(&mut events, BudgetKind::ResultSize);
        assert!(message.contains("65"));
        assert!(message.contains("64"));
    }

    #[tokio::test]
    async fn slow_tool_trips_timeout_guard_and_cancels() {
        let inner = Arc::new(MockTool::new("slow", "late", Duration::from_millis(200)));
        let (governance, cancel, mut events) = governance();
        let tool = GovernedTool::new(
            inner as Arc<dyn Tool>,
            harness(&["slow"]),
            Arc::clone(&governance),
            Duration::from_millis(5),
            Arc::new(ToolTimeoutGuard::new(Duration::from_millis(5))),
            Arc::new(ResultSizeGuard::new(1024)),
        );

        let result = tool.execute(serde_json::json!({})).await;

        assert!(result.is_error);
        assert!(governance.tripped());
        assert!(cancel.is_cancelled());
        let message = expect_budget_event(&mut events, BudgetKind::ToolTimeout);
        assert!(message.contains("slow"));
    }

    struct UsageProvider {
        usage: TokenUsage,
    }

    #[async_trait]
    impl LlmProvider for UsageProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            Ok(futures::stream::iter(vec![LlmEvent::Done {
                usage: Some(self.usage),
            }])
            .boxed())
        }
    }

    async fn charge(usage: TokenUsage, limit: u64) -> (bool, u64, CancellationToken) {
        let (governance, cancel, _events) = governance();
        let guard = Arc::new(TokenBudgetGuard::new(limit));
        let provider = TokenCountingProvider::new(
            Arc::new(UsageProvider { usage }),
            Arc::clone(&governance),
            guard,
        );

        let stream = provider
            .stream_chat(&[Message::user("hi")], &[])
            .await
            .expect("usage provider must stream");
        let events: Vec<_> = stream.collect().await;
        assert_eq!(events, vec![LlmEvent::Done { usage: Some(usage) }]);
        (governance.tripped(), governance.tokens_used(), cancel)
    }

    #[tokio::test]
    async fn token_usage_at_the_limit_is_allowed() {
        let (tripped, used, cancel) = charge(TokenUsage::new(6, 5), 11).await;
        assert!(!tripped);
        assert!(!cancel.is_cancelled());
        assert_eq!(used, 11);
    }

    #[tokio::test]
    async fn token_usage_above_the_limit_trips_and_cancels() {
        let (tripped, used, cancel) = charge(TokenUsage::new(6, 6), 11).await;
        assert!(tripped);
        assert!(cancel.is_cancelled());
        assert_eq!(used, 12);
    }

    #[tokio::test]
    async fn the_first_budget_trip_wins_and_later_trips_do_not_duplicate_events() {
        let (governance, _cancel, mut events) = governance();
        let first = AgentEvent::BudgetExceeded {
            kind: BudgetKind::TokenLimit,
            message: "first".into(),
        };
        let second = AgentEvent::BudgetExceeded {
            kind: BudgetKind::Stuck,
            message: "second".into(),
        };

        assert!(governance.trip(first).await);
        assert!(!governance.trip(second).await);

        let message = expect_budget_event(&mut events, BudgetKind::TokenLimit);
        assert_eq!(message, "first");
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    /// Tool that records the full call it receives through `execute_with_call`.
    struct RecordingTool {
        name: &'static str,
        calls: AtomicU32,
        recorded: Mutex<Vec<ToolCall>>,
    }

    impl RecordingTool {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                calls: AtomicU32::new(0),
                recorded: Mutex::new(Vec::new()),
            }
        }

        fn recorded_calls(&self) -> Vec<ToolCall> {
            self.recorded
                .lock()
                .expect("recorded calls must not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl Tool for RecordingTool {
        fn name(&self) -> &str {
            self.name
        }

        fn schema(&self) -> ToolSchema {
            serde_json::json!({"type": "function", "function": {"name": self.name}})
        }

        async fn execute(&self, _args: Value) -> ToolResult {
            ToolResult::error("unused", "execute must not be called")
        }

        async fn execute_with_call(&self, call: &ToolCall) -> ToolResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.recorded
                .lock()
                .expect("recorded calls must not be poisoned")
                .push(call.clone());
            ToolResult::success(call.id.clone(), "ran")
        }
    }

    /// Gate that records each request and returns a fixed decision after a
    /// configurable delay.
    struct MockGate {
        decision: ToolPermissionDecision,
        delay: Duration,
        calls: Mutex<Vec<ToolCall>>,
    }

    impl MockGate {
        fn new(decision: ToolPermissionDecision, delay: Duration) -> Self {
            Self {
                decision,
                delay,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<ToolCall> {
            self.calls
                .lock()
                .expect("gate calls must not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl ToolPermissionGate for MockGate {
        async fn request_permission(&self, call: &ToolCall) -> ToolPermissionDecision {
            self.calls
                .lock()
                .expect("gate calls must not be poisoned")
                .push(call.clone());
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.decision
        }
    }

    /// Builds a governed tool with one second of timeout and a 16 KiB result
    /// limit, optionally attaching `gate`.
    fn governed(
        inner: Arc<dyn Tool>,
        allowed: &[&str],
        timeout: Duration,
        gate: Option<Arc<dyn ToolPermissionGate>>,
    ) -> GovernedTool {
        GovernedTool::new(
            inner,
            harness(allowed),
            Arc::new(GovernanceState::new(
                broadcast::channel(16).0,
                CancellationToken::new(),
            )),
            timeout,
            Arc::new(ToolTimeoutGuard::new(timeout)),
            Arc::new(ResultSizeGuard::new(16 * 1024)),
        )
        .with_permission_gate(gate)
    }

    #[test]
    fn permission_required_is_a_fixed_native_set() {
        for name in [
            "bash",
            "exec",
            "write_file",
            "apply_patch",
            "edit_file",
            jessica_runtime::DELEGATE_TASK_TOOL_NAME,
        ] {
            assert!(
                permission_required(name),
                "`{name}` must require permission"
            );
        }
        for name in [
            "read_file",
            "list_dir",
            "grep",
            "git_status",
            "git_diff",
            "wasm_echo",
            "mcp__stub__echo",
            "",
            "Bash",
            "bash ",
            "exec ",
        ] {
            assert!(
                !permission_required(name),
                "`{name}` must remain policy-only"
            );
        }
    }

    #[tokio::test]
    async fn an_approved_sensitive_tool_receives_the_original_call_and_runs() {
        let inner = Arc::new(RecordingTool::new("bash"));
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));
        let tool = governed(
            Arc::clone(&inner) as Arc<dyn Tool>,
            &["bash"],
            Duration::from_secs(1),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        );
        let call = ToolCall::new("call-real", "bash", serde_json::json!({"command": "true"}));

        let result = tool.execute_with_call(&call).await;

        assert!(
            !result.is_error,
            "approved execution must succeed: {result:?}"
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.recorded_calls(), vec![call.clone()]);
        assert_eq!(gate.calls(), vec![call]);
    }

    #[tokio::test]
    async fn non_allow_decisions_fail_closed_without_running_the_tool() {
        let cases = [
            (ToolPermissionDecision::Reject, "denied by the user"),
            (ToolPermissionDecision::Cancelled, "was cancelled"),
            (ToolPermissionDecision::TimedOut, "timed out"),
            (
                ToolPermissionDecision::TransportFailed,
                "could not be completed",
            ),
        ];
        for (decision, expected) in cases {
            let inner = Arc::new(MockTool::new("bash", "must not run", Duration::ZERO));
            let gate = Arc::new(MockGate::new(decision, Duration::ZERO));
            let tool = governed(
                Arc::clone(&inner) as Arc<dyn Tool>,
                &["bash"],
                Duration::from_secs(1),
                Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
            );
            let call = ToolCall::new("call-1", "bash", serde_json::json!({"command": "touch x"}));

            let result = tool.execute_with_call(&call).await;

            assert!(result.is_error, "{decision:?} must fail the tool");
            assert!(
                result.content.contains(expected),
                "{decision:?} error must explain the outcome: {}",
                result.content
            );
            assert_eq!(
                inner.calls.load(Ordering::SeqCst),
                0,
                "{decision:?} must never execute the tool"
            );
            assert_eq!(gate.calls().len(), 1, "{decision:?} must ask exactly once");
        }
    }

    #[tokio::test]
    async fn policy_denial_precedes_permission_and_never_asks_the_client() {
        let inner = Arc::new(MockTool::new("bash", "must not run", Duration::ZERO));
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));
        let tool = governed(
            Arc::clone(&inner) as Arc<dyn Tool>,
            &["read_file"],
            Duration::from_secs(1),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        );
        let call = ToolCall::new("call-1", "bash", serde_json::json!({"command": "true"}));

        let result = tool.execute_with_call(&call).await;

        assert!(result.is_error);
        assert!(result.content.contains("denied by policy"));
        assert!(
            gate.calls().is_empty(),
            "policy must reject before the client is asked"
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_exhausted_budget_skips_permission_entirely() {
        let inner = Arc::new(MockTool::new("bash", "must not run", Duration::ZERO));
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));
        let (governance, _cancel, _events) = governance();
        assert!(
            governance
                .trip(AgentEvent::BudgetExceeded {
                    kind: BudgetKind::TokenLimit,
                    message: "exhausted".into(),
                })
                .await
        );
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["bash"]),
            governance,
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_permission_gate(Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>));

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-1",
                "bash",
                serde_json::json!({"command": "true"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(gate.calls().is_empty());
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn the_permission_wait_does_not_consume_the_tool_timeout() {
        let inner = Arc::new(RecordingTool::new("bash"));
        let slow_gate = Arc::new(MockGate::new(
            ToolPermissionDecision::Allow,
            Duration::from_millis(60),
        ));
        let tool = governed(
            Arc::clone(&inner) as Arc<dyn Tool>,
            &["bash"],
            Duration::from_millis(10),
            Some(Arc::clone(&slow_gate) as Arc<dyn ToolPermissionGate>),
        );

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-1",
                "bash",
                serde_json::json!({"command": "true"}),
            ))
            .await;

        assert!(
            !result.is_error,
            "a slow approval must not trip the tool timeout: {result:?}"
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn the_tool_timeout_still_starts_after_approval() {
        let inner = Arc::new(MockTool::new("bash", "late", Duration::from_millis(80)));
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));
        let (governance, cancel, mut events) = governance();
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["bash"]),
            Arc::clone(&governance),
            Duration::from_millis(10),
            Arc::new(ToolTimeoutGuard::new(Duration::from_millis(10))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_permission_gate(Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>));

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-1",
                "bash",
                serde_json::json!({"command": "sleep"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(governance.tripped());
        assert!(cancel.is_cancelled());
        match events.try_recv() {
            Ok(AgentEvent::PermissionDecided { call, decision }) => {
                assert_eq!(call.id, "call-1");
                assert_eq!(decision, PermissionDecision::Allow);
            }
            other => panic!("expected PermissionDecided(Allow), got {other:?}"),
        }
        let message = expect_budget_event(&mut events, BudgetKind::ToolTimeout);
        assert!(message.contains("bash"));
        assert_eq!(gate.calls().len(), 1);
    }

    #[tokio::test]
    async fn policy_only_tools_never_consult_the_gate() {
        let inner = Arc::new(RecordingTool::new("read_file"));
        let gate = Arc::new(MockGate::new(
            ToolPermissionDecision::Reject,
            Duration::ZERO,
        ));
        let tool = governed(
            Arc::clone(&inner) as Arc<dyn Tool>,
            &["read_file"],
            Duration::from_secs(1),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        );

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-read",
                "read_file",
                serde_json::json!({"path": "a"}),
            ))
            .await;

        assert!(!result.is_error);
        assert!(gate.calls().is_empty());
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_sensitive_call_without_a_provider_id_fails_closed() {
        let inner = Arc::new(MockTool::new("bash", "must not run", Duration::ZERO));
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));
        let tool = governed(
            Arc::clone(&inner) as Arc<dyn Tool>,
            &["bash"],
            Duration::from_secs(1),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        );

        let result = tool
            .execute_with_call(&ToolCall::new(
                "",
                "bash",
                serde_json::json!({"command": "true"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(
            result.content.contains("provider tool-call id"),
            "the failure must explain the missing id: {}",
            result.content
        );
        assert!(
            gate.calls().is_empty(),
            "an empty id must never reach the client"
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn sensitive_tools_run_policy_only_without_a_gate() {
        let inner = Arc::new(MockTool::new("bash", "ran", Duration::ZERO));
        let tool = governed(
            Arc::clone(&inner) as Arc<dyn Tool>,
            &["bash"],
            Duration::from_secs(1),
            None,
        );

        let result = tool.execute(serde_json::json!({"command": "true"})).await;

        assert!(!result.is_error);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }
    /// Sink that records every offered event.
    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<AgentEvent>>,
    }

    impl RecordingSink {
        fn events(&self) -> Vec<AgentEvent> {
            self.events
                .lock()
                .expect("sink events mutex poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl EventSink for RecordingSink {
        async fn emit(&self, event: AgentEvent) -> Result<(), EventSinkError> {
            self.events
                .lock()
                .expect("sink events mutex poisoned")
                .push(event);
            Ok(())
        }
    }

    /// Sink that rejects every event.
    struct RejectingSink;

    #[async_trait]
    impl EventSink for RejectingSink {
        async fn emit(&self, _event: AgentEvent) -> Result<(), EventSinkError> {
            Err(EventSinkError::new("recorder unavailable"))
        }
    }

    /// Sink that observes the broadcast channel while it records.
    struct EmptyReceiverSink {
        /// Observer that must not see the event before the sink returns.
        receiver: Mutex<broadcast::Receiver<AgentEvent>>,
    }

    #[async_trait]
    impl EventSink for EmptyReceiverSink {
        async fn emit(&self, _event: AgentEvent) -> Result<(), EventSinkError> {
            let mut receiver = self.receiver.lock().expect("sink receiver mutex poisoned");
            assert_eq!(
                receiver.try_recv(),
                Err(broadcast::error::TryRecvError::Empty),
                "the sink must run before any observer can receive the event"
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn every_permission_decision_is_recorded_before_it_takes_effect() {
        let cases = [
            (ToolPermissionDecision::Allow, PermissionDecision::Allow),
            (ToolPermissionDecision::Reject, PermissionDecision::Deny),
            (
                ToolPermissionDecision::Cancelled,
                PermissionDecision::Cancelled,
            ),
            (
                ToolPermissionDecision::TimedOut,
                PermissionDecision::TimedOut,
            ),
            (
                ToolPermissionDecision::TransportFailed,
                PermissionDecision::Failed,
            ),
        ];
        for (gate_decision, expected) in cases {
            let inner = Arc::new(MockTool::new("bash", "ran", Duration::ZERO));
            let gate = Arc::new(MockGate::new(gate_decision, Duration::ZERO));
            let sink = Arc::new(RecordingSink::default());
            let (events, mut receiver) = broadcast::channel(16);
            let cancel = CancellationToken::new();
            let governance = Arc::new(
                GovernanceState::new(events, cancel.clone())
                    .with_event_sink(Some(Arc::clone(&sink) as Arc<dyn EventSink>)),
            );
            let tool = GovernedTool::new(
                Arc::clone(&inner) as Arc<dyn Tool>,
                harness(&["bash"]),
                Arc::clone(&governance),
                Duration::from_secs(1),
                Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
                Arc::new(ResultSizeGuard::new(1024)),
            )
            .with_permission_gate(Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>));
            let call = ToolCall::new("call-1", "bash", serde_json::json!({"command": "true"}));

            let result = tool.execute_with_call(&call).await;

            let recorded = sink.events();
            assert_eq!(
                recorded.len(),
                1,
                "{gate_decision:?} must record exactly one decision"
            );
            match &recorded[0] {
                AgentEvent::PermissionDecided {
                    call: recorded_call,
                    decision,
                } => {
                    assert_eq!(recorded_call, &call);
                    assert_eq!(*decision, expected);
                }
                other => panic!("expected PermissionDecided, got {other:?}"),
            }
            assert_eq!(
                receiver.try_recv(),
                Ok(recorded[0].clone()),
                "an accepted decision must broadcast after the sink"
            );
            if expected == PermissionDecision::Allow {
                assert!(!result.is_error);
                assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
            } else {
                assert!(result.is_error);
                assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
            }
        }
    }

    #[tokio::test]
    async fn an_allow_that_cannot_be_recorded_never_executes_the_tool() {
        let inner = Arc::new(MockTool::new("bash", "must not run", Duration::ZERO));
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));
        let (events, mut receiver) = broadcast::channel(16);
        let cancel = CancellationToken::new();
        let governance = Arc::new(
            GovernanceState::new(events, cancel.clone())
                .with_event_sink(Some(Arc::new(RejectingSink) as Arc<dyn EventSink>)),
        );
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["bash"]),
            Arc::clone(&governance),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_permission_gate(Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>));

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-1",
                "bash",
                serde_json::json!({"command": "true"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(
            result.content.contains("could not be recorded"),
            "the error must explain the failed record: {}",
            result.content
        );
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            0,
            "an unrecorded allow must not run the tool"
        );
        assert!(
            cancel.is_cancelled(),
            "the turn must stop after a sink failure"
        );
        assert_eq!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty),
            "a rejected event must not be broadcast"
        );
    }

    #[tokio::test]
    async fn an_accepted_governance_event_is_broadcast_only_after_the_sink() {
        let (events, mut observer) = broadcast::channel(16);
        let sink = Arc::new(EmptyReceiverSink {
            receiver: Mutex::new(events.subscribe()),
        });
        let cancel = CancellationToken::new();
        let governance = GovernanceState::new(events, cancel.clone())
            .with_event_sink(Some(sink as Arc<dyn EventSink>));
        let event = AgentEvent::PolicyDenied {
            call: ToolCall::new("call-1", "bash", serde_json::json!({"command": "true"})),
            reason: "denied for the ordering test".to_owned(),
        };

        assert!(
            governance.emit(event.clone()).await,
            "the sink accepted the event"
        );
        assert_eq!(
            observer.try_recv(),
            Ok(event),
            "the accepted event must reach observers after the sink"
        );
        assert!(
            !cancel.is_cancelled(),
            "an accepted event must not cancel the turn"
        );
    }

    /// Temporary policy workspace removed recursively.
    struct PolicyTempDir {
        path: std::path::PathBuf,
    }

    impl PolicyTempDir {
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "jessica-governance-policy-{label}-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("policy temp dir must be created");
            Self { path }
        }
    }

    impl Drop for PolicyTempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn policy_rule(id: &str, action: RuleAction, tool: &str) -> PolicyRuleConfig {
        PolicyRuleConfig {
            id: id.to_owned(),
            tool: Some(ToolSelector::One(tool.to_owned())),
            path_glob: None,
            program: None,
            args_prefix: None,
            action,
            reason: Some(format!("test reason for {id}")),
        }
    }

    fn policy_config(
        rules: Vec<PolicyRuleConfig>,
        mode: ApprovalMode,
        explicit: bool,
    ) -> HarnessConfig {
        HarnessConfig {
            policy: PolicyConfig {
                allowed_tools: vec![
                    "bash".to_owned(),
                    "exec".to_owned(),
                    "read_file".to_owned(),
                    "write_file".to_owned(),
                    "apply_patch".to_owned(),
                    "edit_file".to_owned(),
                    "delegate_task".to_owned(),
                ],
                rules,
            },
            approval: ApprovalConfig { mode, explicit },
            ..HarnessConfig::default()
        }
    }

    fn policy_tool_for(name: &str) -> PolicyTool {
        let path_field = if matches!(name, "read_file" | "read_artifact" | "list_dir" | "grep") {
            Some("path".to_owned())
        } else if name == "apply_patch" {
            Some("patch".to_owned())
        } else if name == "edit_file" {
            Some("path".to_owned())
        } else {
            None
        };
        PolicyTool {
            name: name.to_owned(),
            source: ToolSource::Native,
            capabilities: Capabilities::for_tool(ToolSource::Native, name),
            path_field,
            legacy_sensitive: permission_required(name),
        }
    }

    /// Builds a governed tool with policy v2 attached.
    async fn governed_with_policy(
        inner: Arc<dyn Tool>,
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        root: &Path,
        config: &HarnessConfig,
        policy_tool: PolicyTool,
        gate: Option<Arc<dyn ToolPermissionGate>>,
    ) -> GovernedTool {
        let policy = Arc::new(
            PolicyV2::new(root, config)
                .await
                .expect("policy evaluator must build"),
        );
        GovernedTool::new(
            inner,
            harness,
            governance,
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(16 * 1024)),
        )
        .with_permission_gate(gate)
        .with_policy_v2(policy, policy_tool)
    }

    #[tokio::test]
    async fn a_deny_rule_emits_policy_denied_without_asking_or_running() {
        let temp = PolicyTempDir::new("deny");
        let config = policy_config(
            vec![policy_rule("no-bash", RuleAction::Deny, "bash")],
            ApprovalMode::Ask,
            true,
        );
        let inner = Arc::new(MockTool::new("bash", "must not run", Duration::ZERO));
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));
        let (governance, cancel, mut events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["bash"]),
            Arc::clone(&governance),
            &temp.path,
            &config,
            policy_tool_for("bash"),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        )
        .await;
        let call = ToolCall::new("call-1", "bash", serde_json::json!({"command": "rm -rf /"}));

        let result = tool.execute_with_call(&call).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("no-bash"),
            "the model-facing error must name the rule: {}",
            result.content
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert!(
            gate.calls().is_empty(),
            "a deny rule must be decided before any permission prompt"
        );
        assert!(!governance.tripped());
        assert!(!cancel.is_cancelled());
        match events.try_recv() {
            Ok(AgentEvent::PolicyDenied {
                call: denied_call,
                reason,
            }) => {
                assert_eq!(denied_call, call);
                assert!(reason.contains("policy rule `no-bash`"));
                assert!(reason.contains("test reason for no-bash"));
            }
            other => panic!("expected PolicyDenied, got {other:?}"),
        }
        assert!(
            events.try_recv().is_err(),
            "exactly one denial event must be emitted"
        );
    }

    #[tokio::test]
    async fn an_ask_rule_runs_only_after_the_gate_allows_and_records_the_decision() {
        let temp = PolicyTempDir::new("ask-allow");
        let config = policy_config(
            vec![policy_rule("ask-read", RuleAction::Ask, "read_file")],
            ApprovalMode::Ask,
            true,
        );
        let inner = Arc::new(MockTool::new("read_file", "contents", Duration::ZERO));
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));
        let (governance, _, mut events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["read_file"]),
            Arc::clone(&governance),
            &temp.path,
            &config,
            policy_tool_for("read_file"),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        )
        .await;
        let call = ToolCall::new(
            "call-read",
            "read_file",
            serde_json::json!({"path": "a.txt"}),
        );

        let result = tool.execute_with_call(&call).await;

        assert!(!result.is_error, "an allowed ask rule must run: {result:?}");
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(gate.calls(), vec![call.clone()]);
        match events.try_recv() {
            Ok(AgentEvent::PermissionDecided {
                call: decided_call,
                decision,
            }) => {
                assert_eq!(decided_call, call);
                assert_eq!(decision, PermissionDecision::Allow);
            }
            other => panic!("expected PermissionDecided(Allow), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_ask_rule_rejection_fails_closed_and_records_the_denial() {
        let temp = PolicyTempDir::new("ask-reject");
        let config = policy_config(
            vec![policy_rule("ask-read", RuleAction::Ask, "read_file")],
            ApprovalMode::Ask,
            true,
        );
        let inner = Arc::new(MockTool::new("read_file", "must not run", Duration::ZERO));
        let gate = Arc::new(MockGate::new(
            ToolPermissionDecision::Reject,
            Duration::ZERO,
        ));
        let (governance, _, mut events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["read_file"]),
            Arc::clone(&governance),
            &temp.path,
            &config,
            policy_tool_for("read_file"),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        )
        .await;

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-read",
                "read_file",
                serde_json::json!({"path": "a.txt"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(result.content.contains("denied by the user"));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert_eq!(gate.calls().len(), 1);
        match events.try_recv() {
            Ok(AgentEvent::PermissionDecided { decision, .. }) => {
                assert_eq!(decision, PermissionDecision::Deny);
            }
            other => panic!("expected PermissionDecided(Deny), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_ask_rule_without_a_gate_fails_closed() {
        let temp = PolicyTempDir::new("ask-no-gate");
        let config = policy_config(
            vec![policy_rule("ask-bash", RuleAction::Ask, "bash")],
            ApprovalMode::Ask,
            true,
        );
        let inner = Arc::new(MockTool::new("bash", "must not run", Duration::ZERO));
        let (governance, _, mut events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["bash"]),
            Arc::clone(&governance),
            &temp.path,
            &config,
            policy_tool_for("bash"),
            None,
        )
        .await;

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-bash",
                "bash",
                serde_json::json!({"command": "true"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(
            result.content.contains("no permission gate"),
            "the failure must explain the missing gate: {}",
            result.content
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        match events.try_recv() {
            Ok(AgentEvent::PolicyDenied { reason, .. }) => {
                assert!(reason.contains("requires approval"));
            }
            other => panic!("expected PolicyDenied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn explicit_deny_mode_denies_sensitive_tools_before_the_gate() {
        let temp = PolicyTempDir::new("mode-deny");
        let config = policy_config(Vec::new(), ApprovalMode::Deny, true);
        let inner = Arc::new(MockTool::new("bash", "must not run", Duration::ZERO));
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));
        let (governance, _, mut events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["bash"]),
            Arc::clone(&governance),
            &temp.path,
            &config,
            policy_tool_for("bash"),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        )
        .await;

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-bash",
                "bash",
                serde_json::json!({"command": "true"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(result.content.contains("approval mode `deny`"));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert!(gate.calls().is_empty());
        assert!(matches!(
            events.try_recv(),
            Ok(AgentEvent::PolicyDenied { .. })
        ));
    }

    /// Sink that remembers how many tool executions had happened when each
    /// event was recorded.
    struct ExecutionProbe {
        calls: Arc<AtomicU32>,
        events: Mutex<Vec<AgentEvent>>,
        calls_at_emit: Mutex<Vec<u32>>,
    }

    #[async_trait]
    impl EventSink for ExecutionProbe {
        async fn emit(&self, event: AgentEvent) -> Result<(), EventSinkError> {
            let calls = self.calls.load(Ordering::SeqCst);
            self.calls_at_emit
                .lock()
                .expect("probe mutex must not be poisoned")
                .push(calls);
            self.events
                .lock()
                .expect("probe mutex must not be poisoned")
                .push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn explicit_allow_mode_auto_approves_and_records_before_execution() {
        let temp = PolicyTempDir::new("mode-allow");
        let config = policy_config(Vec::new(), ApprovalMode::Allow, true);
        let inner = Arc::new(MockTool::new("bash", "ran", Duration::ZERO));
        let gate = Arc::new(MockGate::new(
            ToolPermissionDecision::Reject,
            Duration::ZERO,
        ));
        let probe = Arc::new(ExecutionProbe {
            calls: Arc::clone(&inner.calls),
            events: Mutex::new(Vec::new()),
            calls_at_emit: Mutex::new(Vec::new()),
        });
        let (events, _receiver) = broadcast::channel(16);
        let governance = Arc::new(
            GovernanceState::new(events, CancellationToken::new())
                .with_event_sink(Some(Arc::clone(&probe) as Arc<dyn EventSink>)),
        );
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["bash"]),
            governance,
            &temp.path,
            &config,
            policy_tool_for("bash"),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        )
        .await;
        let call = ToolCall::new("call-bash", "bash", serde_json::json!({"command": "true"}));

        let result = tool.execute_with_call(&call).await;

        assert!(!result.is_error, "auto-approved call must run: {result:?}");
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert!(
            gate.calls().is_empty(),
            "approval mode `allow` must not prompt"
        );
        let recorded = probe
            .events
            .lock()
            .expect("probe events must not be poisoned")
            .clone();
        assert_eq!(recorded.len(), 1, "exactly one approval event is required");
        match &recorded[0] {
            AgentEvent::PermissionDecided {
                call: decided_call,
                decision,
            } => {
                assert_eq!(decided_call, &call);
                assert_eq!(*decision, PermissionDecision::Allow);
            }
            other => panic!("expected PermissionDecided(Allow), got {other:?}"),
        }
        assert_eq!(
            probe
                .calls_at_emit
                .lock()
                .expect("probe counters must not be poisoned")
                .as_slice(),
            [0],
            "the auto-approval must be recorded before the tool executes"
        );
    }

    #[tokio::test]
    async fn an_allow_rule_overrides_explicit_deny_mode_without_asking() {
        let temp = PolicyTempDir::new("allow-overrides-deny");
        let config = policy_config(
            vec![policy_rule("allow-bash", RuleAction::Allow, "bash")],
            ApprovalMode::Deny,
            true,
        );
        let inner = Arc::new(MockTool::new("bash", "ran", Duration::ZERO));
        let gate = Arc::new(MockGate::new(
            ToolPermissionDecision::Reject,
            Duration::ZERO,
        ));
        let (governance, _, mut events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["bash"]),
            governance,
            &temp.path,
            &config,
            policy_tool_for("bash"),
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        )
        .await;

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-bash",
                "bash",
                serde_json::json!({"command": "true"}),
            ))
            .await;

        assert!(!result.is_error, "an allow rule must win: {result:?}");
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert!(gate.calls().is_empty());
        assert!(
            events.try_recv().is_err(),
            "an explicit allow rule is not an auto-approval and records no decision"
        );
    }

    #[tokio::test]
    async fn explicit_approval_extends_sensitivity_to_destructive_mcp_tools() {
        let temp = PolicyTempDir::new("mcp-explicit");
        let name = "mcp__srv__wipe";
        let config = policy_config(Vec::new(), ApprovalMode::Ask, true);
        let inner = Arc::new(MockTool::new(name, "must not run", Duration::ZERO));
        let gate = Arc::new(MockGate::new(
            ToolPermissionDecision::Reject,
            Duration::ZERO,
        ));
        let (governance, _, _events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&[name]),
            governance,
            &temp.path,
            &config,
            PolicyTool {
                name: name.to_owned(),
                source: ToolSource::Mcp,
                capabilities: Capabilities::for_tool(ToolSource::Mcp, "wipe"),
                path_field: None,
                legacy_sensitive: false,
            },
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        )
        .await;

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-mcp",
                name,
                serde_json::json!({"target": "everything"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(result.content.contains("denied by the user"));
        assert_eq!(gate.calls().len(), 1);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn mcp_tools_keep_legacy_behavior_without_an_explicit_approval_section() {
        let temp = PolicyTempDir::new("mcp-legacy");
        let name = "mcp__srv__wipe";
        let config = policy_config(Vec::new(), ApprovalMode::Ask, false);
        let inner = Arc::new(MockTool::new(name, "ran", Duration::ZERO));
        let gate = Arc::new(MockGate::new(
            ToolPermissionDecision::Reject,
            Duration::ZERO,
        ));
        let (governance, _, _events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&[name]),
            governance,
            &temp.path,
            &config,
            PolicyTool {
                name: name.to_owned(),
                source: ToolSource::Mcp,
                capabilities: Capabilities::for_tool(ToolSource::Mcp, "wipe"),
                path_field: None,
                legacy_sensitive: false,
            },
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        )
        .await;

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-mcp",
                name,
                serde_json::json!({"target": "everything"}),
            ))
            .await;

        assert!(
            !result.is_error,
            "without an explicit approval section MCP tools keep the 9.1 behavior: {result:?}"
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert!(
            gate.calls().is_empty(),
            "legacy MCP tools are not sent to the native permission gate"
        );
    }

    #[tokio::test]
    async fn delegate_task_is_destructive_and_requires_approval_under_explicit_ask_mode() {
        let temp = PolicyTempDir::new("delegate");
        let name = jessica_runtime::DELEGATE_TASK_TOOL_NAME;
        let capabilities = Capabilities::for_tool(ToolSource::Delegation, name);
        assert!(
            capabilities.destructive,
            "the delegation descriptor must mark delegate_task destructive"
        );
        let config = policy_config(Vec::new(), ApprovalMode::Ask, true);
        let inner = Arc::new(MockTool::new(name, "must not run", Duration::ZERO));
        let gate = Arc::new(MockGate::new(
            ToolPermissionDecision::Reject,
            Duration::ZERO,
        ));
        let (governance, _, _events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&[name]),
            governance,
            &temp.path,
            &config,
            PolicyTool {
                name: name.to_owned(),
                source: ToolSource::Delegation,
                capabilities,
                path_field: None,
                legacy_sensitive: permission_required(name),
            },
            Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
        )
        .await;

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-delegate",
                name,
                serde_json::json!({"task": "delete everything"}),
            ))
            .await;

        assert!(result.is_error, "a rejected delegation must not run");
        assert!(result.content.contains("denied by the user"));
        assert_eq!(
            gate.calls().len(),
            1,
            "explicit ask mode must send delegate_task to the approval gate"
        );
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            0,
            "the delegated child run must not start after a rejection"
        );
    }

    #[tokio::test]
    async fn editor_tools_require_approval_under_explicit_ask_mode() {
        for (name, arguments) in [
            (
                "edit_file",
                serde_json::json!({"path": "file.txt", "old": "alpha", "new": "beta"}),
            ),
            (
                "apply_patch",
                serde_json::json!({
                    "patch": "--- a/file.txt\n+++ b/file.txt\n@@ -1 +1 @@\n-alpha\n+beta\n"
                }),
            ),
        ] {
            let temp = PolicyTempDir::new(name);
            std::fs::write(temp.path.join("file.txt"), "alpha\n").expect("target must exist");
            let config = policy_config(Vec::new(), ApprovalMode::Ask, true);
            let inner = Arc::new(MockTool::new(name, "must not run", Duration::ZERO));
            let gate = Arc::new(MockGate::new(
                ToolPermissionDecision::Reject,
                Duration::ZERO,
            ));
            let (governance, _, _events) = governance();
            let tool = governed_with_policy(
                Arc::clone(&inner) as Arc<dyn Tool>,
                harness(&[name]),
                governance,
                &temp.path,
                &config,
                policy_tool_for(name),
                Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
            )
            .await;

            let result = tool
                .execute_with_call(&ToolCall::new("call-editor", name, arguments))
                .await;

            assert!(result.is_error, "a rejected `{name}` call must not run");
            assert!(result.content.contains("denied by the user"));
            assert_eq!(
                gate.calls().len(),
                1,
                "explicit ask mode must send `{name}` to the approval gate"
            );
            assert_eq!(
                inner.calls.load(Ordering::SeqCst),
                0,
                "`{name}` must not execute after a rejection"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_escape_is_a_policy_denial_before_execution() {
        use std::os::unix::fs::symlink;

        let temp = PolicyTempDir::new("escape");
        let outside = PolicyTempDir::new("escape-outside");
        std::fs::write(outside.path.join("secret.txt"), "outside").expect("outside file");
        symlink(&outside.path, temp.path.join("link")).expect("symlink must be created");
        let mut config = policy_config(Vec::new(), ApprovalMode::Ask, false);
        config.policy.rules = vec![PolicyRuleConfig {
            id: "protect-secrets".to_owned(),
            tool: Some(ToolSelector::One("read_file".to_owned())),
            path_glob: Some("**/secret.txt".to_owned()),
            program: None,
            args_prefix: None,
            action: RuleAction::Deny,
            reason: Some("secrets are protected".to_owned()),
        }];
        let inner = Arc::new(MockTool::new("read_file", "must not run", Duration::ZERO));
        let (governance, _, mut events) = governance();
        let tool = governed_with_policy(
            Arc::clone(&inner) as Arc<dyn Tool>,
            harness(&["read_file"]),
            governance,
            &temp.path,
            &config,
            policy_tool_for("read_file"),
            None,
        )
        .await;

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-read",
                "read_file",
                serde_json::json!({"path": "link/secret.txt"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(
            result.content.contains("escapes the workspace root"),
            "the structured path error must be visible: {}",
            result.content
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        match events.try_recv() {
            Ok(AgentEvent::PolicyDenied { reason, .. }) => {
                assert!(reason.contains("escapes the workspace root"));
            }
            other => panic!("expected PolicyDenied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_ask_rule_without_explicit_approval_uses_only_the_ask_gate() {
        let temp = PolicyTempDir::new("ask-gate-only");
        let config = policy_config(
            vec![policy_rule("ask-read", RuleAction::Ask, "read_file")],
            ApprovalMode::Ask,
            false,
        );
        let gate = Arc::new(MockGate::new(ToolPermissionDecision::Allow, Duration::ZERO));

        // The ask rule prompts through the ask gate even though the legacy
        // approval flow was not explicitly configured.
        let read_inner = Arc::new(MockTool::new("read_file", "contents", Duration::ZERO));
        let (read_governance, _, _) = governance();
        let read_tool = governed_with_policy(
            Arc::clone(&read_inner) as Arc<dyn Tool>,
            harness(&["read_file"]),
            read_governance,
            &temp.path,
            &config,
            policy_tool_for("read_file"),
            None,
        )
        .await
        .with_ask_gate(Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>));
        let result = read_tool
            .execute_with_call(&ToolCall::new(
                "read-1",
                "read_file",
                serde_json::json!({"path": "a.txt"}),
            ))
            .await;
        assert!(
            !result.is_error,
            "the approved ask rule must run: {result:?}"
        );
        assert_eq!(read_inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(gate.calls().len(), 1);

        // A legacy-sensitive tool that no rule mentions keeps the pre-v2 CLI
        // behavior: the ask-only gate is not consulted.
        let bash_inner = Arc::new(MockTool::new("bash", "ran", Duration::ZERO));
        let (bash_governance, _, _) = governance();
        let bash_tool = governed_with_policy(
            Arc::clone(&bash_inner) as Arc<dyn Tool>,
            harness(&["bash"]),
            bash_governance,
            &temp.path,
            &config,
            policy_tool_for("bash"),
            None,
        )
        .await
        .with_ask_gate(Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>));
        let result = bash_tool
            .execute_with_call(&ToolCall::new(
                "bash-1",
                "bash",
                serde_json::json!({"command": "true"}),
            ))
            .await;
        assert!(!result.is_error);
        assert_eq!(bash_inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            gate.calls().len(),
            1,
            "the legacy-sensitive tool must not consult the ask-only gate"
        );
    }
}
