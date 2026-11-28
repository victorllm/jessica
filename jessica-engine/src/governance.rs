//! Engine-side governance wrappers around the frozen runtime loop.
//!
//! The runtime loop knows nothing about token usage, measured tool durations,
//! result sizes, or parameter-level policy. The engine therefore wraps its
//! provider and tools:
//!
//! - [`TokenCountingProvider`] observes `LlmEvent::Done` and charges usage
//!   against a shared [`TokenBudgetGuard`];
//! - [`GovernedTool`] evaluates the allowlist policy, then policy v2, then the
//!   approval gate, then runs the tool under its timeout, manages oversized
//!   results, and finally checks the result-size guard.
//!
//! Both wrappers report budget trips through [`GovernanceState`], which emits
//! the structured [`AgentEvent::BudgetExceeded`] event and cancels the session
//! token. Policy denials are emitted but do not cancel: the model receives an
//! error result and may adapt. Permission denial, cancellation, timeout, and
//! delivery failure are also model-facing errors and never run the tool.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use jessica_core::events::EventSink;
use jessica_core::guard::{Guard, GuardContext, GuardDecision};
use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
use jessica_core::policy::{Policy, PolicyDecision};
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::{
    AgentEvent, BudgetKind, Message, PermissionDecision, ToolCall, ToolResult,
};
use jessica_harness::{ResultSizeGuard, TokenBudgetGuard, ToolTimeoutGuard};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::policy::{PolicyError, PolicyTool, PolicyV2, PolicyVerdict, RuleMatch};
use crate::ports::{ToolPermissionDecision, ToolPermissionGate, ToolPolicy, ToolResultManager};
use crate::tooling::ResultFormat;

/// Shared budget and cancellation state for one engine turn.
pub struct GovernanceState {
    /// Event bus for this turn.
    events: broadcast::Sender<AgentEvent>,
    /// Optional producer-side sink that must accept an event before it is
    /// broadcast.
    sink: Option<Arc<dyn EventSink>>,
    /// Cancellation token fired on a budget trip.
    cancel: CancellationToken,
    /// Total tokens charged so far.
    tokens_used: AtomicU64,
    /// Whether a terminal budget trip has already been emitted.
    tripped: AtomicBool,
}

impl GovernanceState {
    /// Creates governance state that emits on `events` and cancels `cancel`.
    pub fn new(events: broadcast::Sender<AgentEvent>, cancel: CancellationToken) -> Self {
        Self {
            events,
            sink: None,
            cancel,
            tokens_used: AtomicU64::new(0),
            tripped: AtomicBool::new(false),
        }
    }

    /// Attaches the producer-side sink used before broadcasting.
    ///
    /// Governance events are part of a turn's trace, so they must reach the
    /// sink before any observer. If the sink rejects one, the event is not
    /// broadcast and the turn's cancellation token is fired.
    pub fn with_event_sink(mut self, sink: Option<Arc<dyn EventSink>>) -> Self {
        self.sink = sink;
        self
    }

    /// Returns whether a budget trip has already occurred.
    pub fn tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }

    /// Returns the number of tokens charged so far.
    pub fn tokens_used(&self) -> u64 {
        self.tokens_used.load(Ordering::SeqCst)
    }

    /// Returns the turn's cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Sends an event without stopping the session.
    ///
    /// Returns false when the producer-side sink rejected the event. In that
    /// case nothing is broadcast and the turn is cancelled by the sink's
    /// integrity failure.
    pub async fn emit(&self, event: AgentEvent) -> bool {
        if let Some(sink) = &self.sink
            && sink.emit(event.clone()).await.is_err()
        {
            self.cancel.cancel();
            return false;
        }
        let _ = self.events.send(event);
        true
    }

    /// Emits one budget event and cancels the session.
    ///
    /// Only the first trip emits; later trips are no-ops so the terminal trace
    /// contains exactly one budget failure.
    pub async fn trip(&self, event: AgentEvent) -> bool {
        if self.tripped.swap(true, Ordering::SeqCst) {
            return false;
        }
        if let Some(sink) = &self.sink
            && sink.emit(event.clone()).await.is_err()
        {
            self.cancel.cancel();
            return false;
        }
        let _ = self.events.send(event);
        self.cancel.cancel();
        true
    }

    /// Charges `tokens` and trips `guard` once the configured limit is crossed.
    async fn charge_tokens(&self, tokens: u64, guard: &TokenBudgetGuard) {
        let previous = self.tokens_used.fetch_add(tokens, Ordering::SeqCst);
        let used = previous.saturating_add(tokens);
        if let GuardDecision::Exceeded { kind, reason } =
            guard.check(&GuardContext::new(0, used, ""))
        {
            self.trip(AgentEvent::BudgetExceeded {
                kind,
                message: reason,
            })
            .await;
        }
    }
}

/// Provider adapter that charges token usage before the loop sees `Done`.
pub struct TokenCountingProvider {
    /// Wrapped provider.
    inner: Arc<dyn LlmProvider>,
    /// Shared turn governance.
    governance: Arc<GovernanceState>,
    /// Token budget guard.
    guard: Arc<TokenBudgetGuard>,
}

impl TokenCountingProvider {
    /// Wraps `inner`, charging each reported usage against `guard`.
    pub fn new(
        inner: Arc<dyn LlmProvider>,
        governance: Arc<GovernanceState>,
        guard: Arc<TokenBudgetGuard>,
    ) -> Self {
        Self {
            inner,
            governance,
            guard,
        }
    }
}

#[async_trait]
impl LlmProvider for TokenCountingProvider {
    async fn stream_chat(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        let stream = self.inner.stream_chat(messages, tools).await?;
        let governance = Arc::clone(&self.governance);
        let guard = Arc::clone(&self.guard);

        Ok(stream
            .then(move |event| {
                let governance = Arc::clone(&governance);
                let guard = Arc::clone(&guard);
                async move {
                    if let LlmEvent::Done { usage: Some(usage) } = &event {
                        governance.charge_tokens(usage.total(), &guard).await;
                    }
                    event
                }
            })
            .boxed())
    }
}

/// Tool adapter that applies policy, permission, timeout, result management,
/// and result-size governance in a fixed order.
pub struct GovernedTool {
    /// Wrapped tool.
    inner: Arc<dyn Tool>,
    /// Allowlist policy evaluated before policy v2.
    policy: Arc<dyn Policy>,
    /// Shared turn governance.
    governance: Arc<GovernanceState>,
    /// Effective wall-clock timeout.
    timeout: Duration,
    /// Timeout guard used for the terminal event.
    timeout_guard: Arc<ToolTimeoutGuard>,
    /// Result-size guard evaluated after result management.
    result_guard: Arc<ResultSizeGuard>,
    /// Optional serialized-argument limit.
    max_args_bytes: Option<usize>,
    /// Legacy approval gate.
    permission: Option<Arc<dyn ToolPermissionGate>>,
    /// Gate used by policy `ask` verdicts.
    ask_gate: Option<Arc<dyn ToolPermissionGate>>,
    /// Optional result manager.
    result_manager: Option<Arc<dyn ToolResultManager>>,
    /// Host-declared result format.
    result_format: ResultFormat,
    /// Optional host policy evaluator.
    policy_v2: Option<Arc<dyn ToolPolicy>>,
    /// Whether the host descriptor marks the tool as legacy-sensitive when no
    /// policy evaluator is attached.
    legacy_sensitive: bool,
}

impl GovernedTool {
    /// Wraps `inner` with the governance owned by one engine turn.
    pub fn new(
        inner: Arc<dyn Tool>,
        policy: Arc<dyn Policy>,
        governance: Arc<GovernanceState>,
        timeout: Duration,
        timeout_guard: Arc<ToolTimeoutGuard>,
        result_guard: Arc<ResultSizeGuard>,
    ) -> Self {
        Self {
            inner,
            policy,
            governance,
            timeout,
            timeout_guard,
            result_guard,
            max_args_bytes: None,
            permission: None,
            ask_gate: None,
            result_manager: None,
            result_format: ResultFormat::Text,
            policy_v2: None,
            legacy_sensitive: false,
        }
    }

    /// Attaches the policy v2 evaluator and the host facts for this tool.
    ///
    /// The evaluator runs after the allowlist and before any permission gate
    /// or execution.
    pub fn with_policy_v2(mut self, policy: Arc<PolicyV2>, tool: PolicyTool) -> Self {
        self.legacy_sensitive = tool.legacy_sensitive;
        self.policy_v2 = Some(Arc::new(EngineToolPolicy { policy, tool }));
        self
    }

    /// Attaches a host-provided policy evaluator.
    ///
    /// The evaluator runs after the allowlist and before any permission gate
    /// or execution. When attached, it replaces the legacy-sensitive fallback.
    pub fn with_tool_policy(mut self, policy: Arc<dyn ToolPolicy>) -> Self {
        self.policy_v2 = Some(policy);
        self
    }

    /// Sets whether the legacy approval path treats this tool as sensitive.
    ///
    /// This fallback is used only when no [`ToolPolicy`] is attached.
    pub fn with_legacy_sensitive(mut self, sensitive: bool) -> Self {
        self.legacy_sensitive = sensitive;
        self
    }

    /// Sets the optional serialized-argument size limit.
    ///
    /// The precheck runs before policy, permission, and execution, so an
    /// oversized call cannot reach any of them. Exceeding the limit returns an
    /// error result without tripping the session budget.
    pub fn with_max_args_bytes(mut self, max_args_bytes: Option<usize>) -> Self {
        self.max_args_bytes = max_args_bytes;
        self
    }

    /// Attaches the optional legacy approval gate for sensitive tools.
    pub fn with_permission_gate(mut self, permission: Option<Arc<dyn ToolPermissionGate>>) -> Self {
        self.permission = permission;
        self
    }

    /// Attaches the gate used by policy `ask` verdicts.
    pub fn with_ask_gate(mut self, ask_gate: Option<Arc<dyn ToolPermissionGate>>) -> Self {
        self.ask_gate = ask_gate;
        self
    }

    /// Attaches the result manager and the tool's result format.
    ///
    /// Managed results replace oversized content with a bounded preview and a
    /// digest before the result-size guard, transcript, or model observes
    /// them.
    pub fn with_result_manager<M>(
        mut self,
        result_manager: Option<Arc<M>>,
        result_format: ResultFormat,
    ) -> Self
    where
        M: ToolResultManager + 'static,
    {
        self.result_manager = result_manager.map(|manager| manager as Arc<dyn ToolResultManager>);
        self.result_format = result_format;
        self
    }

    /// Applies every pre-execution check and runs the tool under its timeout.
    async fn execute_call(&self, call: &ToolCall) -> ToolResult {
        if self.governance.tripped() {
            return ToolResult::error(
                String::new(),
                "tool execution skipped because the session budget was exceeded",
            );
        }

        if let Some(limit) = self.max_args_bytes {
            let bytes = match serde_json::to_vec(&call.arguments) {
                Ok(bytes) => bytes.len(),
                Err(error) => {
                    return ToolResult::error(
                        String::new(),
                        format!(
                            "tool `{}` arguments could not be serialized for the size check: {error}",
                            self.inner.name()
                        ),
                    );
                }
            };
            if bytes > limit {
                return ToolResult::error(
                    String::new(),
                    format!(
                        "tool `{}` arguments are {bytes} bytes, exceeding the configured max_args_bytes of {limit}",
                        self.inner.name()
                    ),
                );
            }
        }

        match self.policy.check(call) {
            PolicyDecision::Allow => {}
            PolicyDecision::Deny(reason) => {
                self.governance
                    .emit(AgentEvent::PolicyDenied {
                        call: call.clone(),
                        reason: reason.clone(),
                    })
                    .await;
                return ToolResult::error(
                    String::new(),
                    format!("tool `{}` was denied by policy", self.inner.name()),
                );
            }
        }

        let verdict = match &self.policy_v2 {
            Some(policy) => match policy.evaluate(call).await {
                Ok(verdict) => verdict,
                Err(error) => {
                    // A path that cannot be canonicalized or that escapes the
                    // workspace must fail closed instead of falling through to
                    // the allowlist decision.
                    let reason = error.to_string();
                    self.governance
                        .emit(AgentEvent::PolicyDenied {
                            call: call.clone(),
                            reason: reason.clone(),
                        })
                        .await;
                    return ToolResult::error(
                        String::new(),
                        format!(
                            "tool `{}` was denied by policy: {reason}",
                            self.inner.name()
                        ),
                    );
                }
            },
            None => PolicyVerdict::Legacy {
                sensitive: self.legacy_sensitive,
            },
        };

        match verdict {
            PolicyVerdict::Deny { reason, rule } => {
                return self.policy_denial(call, reason, rule).await;
            }
            PolicyVerdict::Ask { reason, .. } => {
                if let Err(result) = self.require_approval(call, reason).await {
                    return result;
                }
            }
            PolicyVerdict::AutoApprove { reason, .. } => {
                if let Err(result) = self.record_auto_approval(call, &reason).await {
                    return result;
                }
            }
            PolicyVerdict::Allow { .. } => {}
            PolicyVerdict::Legacy { sensitive } => {
                if sensitive
                    && let Some(gate) = &self.permission
                    && let Err(result) = self.request_gate_decision(gate, call).await
                {
                    return result;
                }
            }
        }

        // The permission wait is not tool time: the timeout starts only after
        // every pre-execution checkpoint has allowed the call.
        let started = Instant::now();
        let outcome = tokio::time::timeout(self.timeout, self.inner.execute_with_call(call)).await;
        let elapsed = started.elapsed();

        match outcome {
            Err(_) => {
                let crossed = self.timeout + Duration::from_nanos(1);
                self.timeout_result(call, elapsed.max(crossed)).await
            }
            Ok(result) => {
                if elapsed >= self.timeout {
                    let crossed = self.timeout + Duration::from_nanos(1);
                    return self.timeout_result(call, elapsed.max(crossed)).await;
                }
                let result = match &self.result_manager {
                    Some(manager) => {
                        manager
                            .manage(self.inner.name(), self.result_format, result)
                            .await
                    }
                    None => result,
                };
                let context = GuardContext::new(0, self.governance.tokens_used(), "")
                    .with_tool_result(&result, Some(elapsed));
                if let GuardDecision::Exceeded { kind, reason } = self.result_guard.check(&context)
                {
                    self.governance
                        .trip(AgentEvent::BudgetExceeded {
                            kind,
                            message: reason.clone(),
                        })
                        .await;
                    return ToolResult::error(String::new(), reason);
                }
                result
            }
        }
    }

    /// Emits a policy denial and returns the model-facing error result.
    async fn policy_denial(
        &self,
        call: &ToolCall,
        reason: String,
        rule: Option<RuleMatch>,
    ) -> ToolResult {
        let reason = match rule {
            Some(rule) => format!("policy rule `{}`: {reason}", rule.rule_id),
            None => reason,
        };
        self.governance
            .emit(AgentEvent::PolicyDenied {
                call: call.clone(),
                reason: reason.clone(),
            })
            .await;
        ToolResult::error(
            String::new(),
            format!(
                "tool `{}` was denied by policy: {reason}",
                self.inner.name()
            ),
        )
    }

    /// Requires a one-time approval decision from an installed gate.
    ///
    /// The ask gate takes precedence because it exists specifically for policy
    /// `ask` verdicts. Without either gate this fails closed.
    async fn require_approval(&self, call: &ToolCall, reason: String) -> Result<(), ToolResult> {
        let Some(gate) = self.ask_gate.as_ref().or(self.permission.as_ref()) else {
            let message = format!(
                "tool `{}` requires approval ({reason}) but no permission gate is available",
                self.inner.name()
            );
            self.governance
                .emit(AgentEvent::PolicyDenied {
                    call: call.clone(),
                    reason: message.clone(),
                })
                .await;
            return Err(ToolResult::error(String::new(), message));
        };
        self.request_gate_decision(gate, call).await
    }

    /// Runs the selected permission gate and records its decision.
    async fn request_gate_decision(
        &self,
        gate: &Arc<dyn ToolPermissionGate>,
        call: &ToolCall,
    ) -> Result<(), ToolResult> {
        if call.id.is_empty() {
            return Err(ToolResult::error(
                String::new(),
                format!(
                    "tool `{}` requires a provider tool-call id for permission",
                    self.inner.name()
                ),
            ));
        }
        let decision = gate.request_permission(call).await;
        let recorded = self
            .governance
            .emit(AgentEvent::PermissionDecided {
                call: call.clone(),
                decision: permission_decision(decision),
            })
            .await;
        match decision {
            ToolPermissionDecision::Allow if recorded => Ok(()),
            ToolPermissionDecision::Allow => Err(ToolResult::error(
                String::new(),
                format!(
                    "tool `{}` was not executed because the permission decision could not be recorded",
                    self.inner.name()
                ),
            )),
            decision => {
                let outcome = match decision {
                    ToolPermissionDecision::Reject => "was denied by the user",
                    ToolPermissionDecision::Cancelled => "permission request was cancelled",
                    ToolPermissionDecision::TimedOut => "permission request timed out",
                    ToolPermissionDecision::TransportFailed => {
                        "permission request could not be completed"
                    }
                    ToolPermissionDecision::Allow => {
                        "was not executed because its approval could not be recorded"
                    }
                };
                Err(ToolResult::error(
                    String::new(),
                    format!("tool `{}` {outcome}", self.inner.name()),
                ))
            }
        }
    }

    /// Records an auto-approval before execution.
    async fn record_auto_approval(&self, call: &ToolCall, reason: &str) -> Result<(), ToolResult> {
        if call.id.is_empty() {
            return Err(ToolResult::error(
                String::new(),
                format!(
                    "tool `{}` requires a provider tool-call id for an approval record",
                    self.inner.name()
                ),
            ));
        }
        let recorded = self
            .governance
            .emit(AgentEvent::PermissionDecided {
                call: call.clone(),
                decision: PermissionDecision::Allow,
            })
            .await;
        if !recorded {
            return Err(ToolResult::error(
                String::new(),
                format!(
                    "tool `{}` was not executed because the auto-approval could not be recorded",
                    self.inner.name()
                ),
            ));
        }
        tracing::debug!(
            tool = self.inner.name(),
            reason,
            "policy auto-approved a sensitive tool"
        );
        Ok(())
    }

    /// Reports a timeout and returns the model-facing error result.
    async fn timeout_result(&self, call: &ToolCall, elapsed: Duration) -> ToolResult {
        let mut context =
            GuardContext::new(0, self.governance.tokens_used(), "").with_tool_call(call);
        context.tool_elapsed = Some(elapsed);

        let message = match self.timeout_guard.check(&context) {
            GuardDecision::Exceeded { reason, .. } => reason,
            GuardDecision::Allow => {
                format!("tool `{}` exceeded its timeout", self.inner.name())
            }
        };
        self.governance
            .trip(AgentEvent::BudgetExceeded {
                kind: BudgetKind::ToolTimeout,
                message: message.clone(),
            })
            .await;
        ToolResult::error(String::new(), message)
    }
}

/// Maps the host permission outcome to the persisted core outcome.
fn permission_decision(decision: ToolPermissionDecision) -> PermissionDecision {
    decision.as_core()
}

/// Binds a [`PolicyV2`] evaluator to the host facts for one tool.
struct EngineToolPolicy {
    policy: Arc<PolicyV2>,
    tool: PolicyTool,
}

#[async_trait]
impl ToolPolicy for EngineToolPolicy {
    async fn evaluate(&self, call: &ToolCall) -> Result<PolicyVerdict, PolicyError> {
        self.policy.evaluate(&self.tool, call).await
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
        let call = ToolCall::new(String::new(), self.inner.name(), args);
        self.execute_call(&call).await
    }

    async fn execute_with_call(&self, call: &ToolCall) -> ToolResult {
        self.execute_call(call).await
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, AtomicU64};

    use jessica_core::events::{EventSink, EventSinkError};
    use jessica_core::llm::LlmProvider;
    use jessica_harness::AllowlistPolicy;
    use serde_json::json;

    use super::*;
    use crate::artifacts::ResultManager;
    use crate::config::{
        ApprovalConfig, ApprovalMode, PolicyConfig, PolicyRule, ResultConfig, RuleAction,
        ToolSelector,
    };
    use crate::policy::PolicyTool;
    use crate::tooling::{Capabilities, ToolSource};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Creates a unique directory and removes it on drop.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "jessica-engine-governance-{label}-{}-{id}",
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
            json!({"type": "function", "function": {"name": self.name}})
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

    fn allowlist(names: &[&str]) -> Arc<dyn Policy> {
        Arc::new(AllowlistPolicy::new(
            names.iter().map(|name| (*name).to_owned()),
        ))
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

    /// Sink that rejects every event.
    struct RejectingSink;

    #[async_trait]
    impl EventSink for RejectingSink {
        async fn emit(&self, _event: AgentEvent) -> Result<(), EventSinkError> {
            Err(EventSinkError::new("recorder unavailable"))
        }
    }

    #[tokio::test]
    async fn a_governance_event_reaches_the_sink_before_observers() {
        let (events, mut observer) = broadcast::channel(16);
        let sink = Arc::new(EmptyReceiverSink {
            receiver: Mutex::new(events.subscribe()),
        });
        let cancel = CancellationToken::new();
        let governance = GovernanceState::new(events, cancel.clone())
            .with_event_sink(Some(sink as Arc<dyn EventSink>));
        let event = AgentEvent::PolicyDenied {
            call: ToolCall::new("call-1", "bash", json!({"command": "true"})),
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

    #[tokio::test]
    async fn a_rejected_governance_event_is_not_broadcast_and_cancels() {
        let (events, mut observer) = broadcast::channel(16);
        let cancel = CancellationToken::new();
        let governance = GovernanceState::new(events, cancel.clone())
            .with_event_sink(Some(Arc::new(RejectingSink) as Arc<dyn EventSink>));
        let event = AgentEvent::PolicyDenied {
            call: ToolCall::new("call-1", "bash", json!({"command": "true"})),
            reason: "denied for the rejection test".to_owned(),
        };

        assert!(!governance.emit(event).await, "the sink rejected the event");
        assert!(cancel.is_cancelled(), "a sink failure must cancel the turn");
        assert_eq!(
            observer.try_recv(),
            Err(broadcast::error::TryRecvError::Empty),
            "a rejected event must not be broadcast"
        );
    }

    /// Host policy that denies every call without inspecting arguments.
    struct DenyEverything;

    #[async_trait]
    impl ToolPolicy for DenyEverything {
        async fn evaluate(&self, _call: &ToolCall) -> Result<PolicyVerdict, PolicyError> {
            Ok(PolicyVerdict::Deny {
                reason: "host policy denial".to_owned(),
                rule: None,
            })
        }
    }

    #[tokio::test]
    async fn a_host_policy_port_denies_before_the_tool_runs() {
        let inner = Arc::new(MockTool::new("blocked", "should not run", Duration::ZERO));
        let (state, cancel, mut events) = governance();
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            allowlist(&["blocked"]),
            Arc::clone(&state),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_tool_policy(Arc::new(DenyEverything));

        let result = tool
            .execute_with_call(&ToolCall::new("call-1", "blocked", json!({})))
            .await;

        assert!(result.is_error);
        assert!(result.content.contains("host policy denial"));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert!(!state.tripped());
        assert!(!cancel.is_cancelled());
        assert!(matches!(
            events.try_recv(),
            Ok(AgentEvent::PolicyDenied { .. })
        ));
    }

    fn policy_tool(name: &str) -> PolicyTool {
        PolicyTool {
            name: name.to_owned(),
            source: ToolSource::Native,
            capabilities: Capabilities {
                writes_files: true,
                ..Capabilities::default()
            },
            path_field: Some("path".to_owned()),
            legacy_sensitive: false,
        }
    }

    async fn denying_policy(root: &std::path::Path, name: &str) -> Arc<PolicyV2> {
        let config = PolicyConfig {
            allowed_tools: vec![name.to_owned()],
            rules: vec![PolicyRule {
                id: "deny-everything".to_owned(),
                tool: Some(ToolSelector::One(name.to_owned())),
                path_glob: None,
                program: None,
                args_prefix: None,
                action: RuleAction::Deny,
                reason: Some("test denial".to_owned()),
            }],
        };
        Arc::new(
            PolicyV2::new(root, &config, &ApprovalConfig::default())
                .await
                .expect("policy must build"),
        )
    }

    #[tokio::test]
    async fn policy_v2_denial_returns_an_error_and_never_runs_the_tool() {
        let temp = TempDir::new("deny");
        let inner = Arc::new(MockTool::new("blocked", "should not run", Duration::ZERO));
        let policy = denying_policy(&temp.path, "blocked").await;
        let (state, cancel, mut events) = governance();
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            allowlist(&["blocked"]),
            Arc::clone(&state),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_policy_v2(policy, policy_tool("blocked"));

        let result = tool
            .execute_with_call(&ToolCall::new("call-1", "blocked", json!({"path": "x"})))
            .await;

        assert!(result.is_error);
        assert!(result.content.contains("denied by policy"));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert!(!state.tripped());
        assert!(!cancel.is_cancelled());
        match events.try_recv() {
            Ok(AgentEvent::PolicyDenied { call, reason }) => {
                assert_eq!(call.name, "blocked");
                assert!(reason.contains("test denial"));
            }
            other => panic!("expected PolicyDenied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn max_args_bytes_is_checked_before_policy_and_tool_execution() {
        let temp = TempDir::new("args-before-policy");
        let inner = Arc::new(MockTool::new("blocked", "should not run", Duration::ZERO));
        let policy = denying_policy(&temp.path, "blocked").await;
        let (state, cancel, mut events) = governance();
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            allowlist(&["blocked"]),
            Arc::clone(&state),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_max_args_bytes(Some(1))
        .with_policy_v2(policy, policy_tool("blocked"));

        let result = tool
            .execute_with_call(&ToolCall::new(
                "call-1",
                "blocked",
                json!({"path": "too-large"}),
            ))
            .await;

        assert!(result.is_error);
        assert!(
            result.content.contains("max_args_bytes"),
            "{}",
            result.content
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert!(
            matches!(
                events.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "no policy event may be emitted before the argument check"
        );
        assert!(!state.tripped());
        assert!(!cancel.is_cancelled());
    }

    #[tokio::test]
    async fn result_manager_runs_before_the_result_size_guard() {
        let temp = TempDir::new("manager-before-guard");
        let inner = Arc::new(MockTool::new("large", "x".repeat(512), Duration::ZERO));
        let (state, cancel, mut events) = governance();
        let result_config = ResultConfig {
            inline_threshold_bytes: 8,
            max_inline_bytes: 240,
            max_artifact_bytes: 1024,
            artifact_dir: PathBuf::from(".jessica/artifacts"),
            artifact_retention_days: 7,
            artifact_max_bytes: 1024 * 1024,
        };
        let manager = Arc::new(ResultManager::new(&temp.path, &result_config, 320));
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            allowlist(&["large"]),
            Arc::clone(&state),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(320)),
        )
        .with_result_manager(Some(manager), ResultFormat::Text);

        let result = tool
            .execute_with_call(&ToolCall::new("call-1", "large", json!({})))
            .await;

        assert!(
            !result.is_error,
            "managed result must pass the guard: {result:?}"
        );
        assert!(result.content.len() <= 320);
        assert!(result.content.contains("[result artifact]"));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert!(!state.tripped());
        assert!(!cancel.is_cancelled());
        assert!(
            matches!(
                events.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "the guard must not see the pre-managed 512-byte result"
        );
    }

    #[tokio::test]
    async fn policy_ask_verdict_runs_only_after_the_gate_allows() {
        let temp = TempDir::new("ask-gate");
        let inner = Arc::new(MockTool::new("sensitive", "ran", Duration::ZERO));
        let policy = PolicyConfig {
            allowed_tools: vec!["sensitive".to_owned()],
            rules: vec![PolicyRule {
                id: "ask-sensitive".to_owned(),
                tool: Some(ToolSelector::One("sensitive".to_owned())),
                path_glob: None,
                program: None,
                args_prefix: None,
                action: RuleAction::Ask,
                reason: Some("needs approval".to_owned()),
            }],
        };
        let evaluator = Arc::new(
            PolicyV2::new(
                &temp.path,
                &policy,
                &ApprovalConfig {
                    mode: ApprovalMode::Ask,
                    explicit: true,
                },
            )
            .await
            .expect("policy must build"),
        );
        let (state, cancel, mut events) = governance();
        let tool = GovernedTool::new(
            Arc::clone(&inner) as Arc<dyn Tool>,
            allowlist(&["sensitive"]),
            Arc::clone(&state),
            Duration::from_secs(1),
            Arc::new(ToolTimeoutGuard::new(Duration::from_secs(1))),
            Arc::new(ResultSizeGuard::new(1024)),
        )
        .with_policy_v2(evaluator, policy_tool("sensitive"))
        .with_ask_gate(Some(Arc::new(AllowGate)));

        let result = tool
            .execute_with_call(&ToolCall::new("call-1", "sensitive", json!({})))
            .await;

        assert!(!result.is_error, "{result:?}");
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert!(!state.tripped());
        assert!(!cancel.is_cancelled());
        match events.try_recv() {
            Ok(AgentEvent::PermissionDecided { decision, .. }) => {
                assert_eq!(decision, PermissionDecision::Allow);
            }
            other => panic!("expected PermissionDecided, got {other:?}"),
        }
    }

    struct AllowGate;

    #[async_trait]
    impl ToolPermissionGate for AllowGate {
        async fn request_permission(&self, _call: &ToolCall) -> ToolPermissionDecision {
            ToolPermissionDecision::Allow
        }
    }

    #[tokio::test]
    async fn token_counting_provider_trips_the_budget_and_cancels_the_turn() {
        struct UsageProvider;

        #[async_trait]
        impl LlmProvider for UsageProvider {
            async fn stream_chat(
                &self,
                _messages: &[Message],
                _tools: &[ToolSchema],
            ) -> Result<LlmStream, LlmError> {
                Ok(Box::pin(futures::stream::iter(vec![LlmEvent::Done {
                    usage: Some(jessica_core::types::TokenUsage::new(30, 30)),
                }])))
            }
        }

        let (events, mut receiver) = broadcast::channel(16);
        let cancel = CancellationToken::new();
        let state = Arc::new(GovernanceState::new(events, cancel.clone()));
        let provider = TokenCountingProvider::new(
            Arc::new(UsageProvider),
            Arc::clone(&state),
            Arc::new(TokenBudgetGuard::new(50)),
        );
        let mut stream = provider
            .stream_chat(&[], &[])
            .await
            .expect("provider stream must start");
        while stream.next().await.is_some() {}

        assert!(state.tripped());
        assert!(cancel.is_cancelled());
        assert_eq!(state.tokens_used(), 60);
        match receiver.try_recv() {
            Ok(AgentEvent::BudgetExceeded { kind, .. }) => {
                assert_eq!(kind, BudgetKind::TokenLimit);
            }
            other => panic!("expected token BudgetExceeded, got {other:?}"),
        }
    }
}
