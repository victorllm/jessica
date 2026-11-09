//! CLI-side enforcement wrappers around the frozen runtime loop.
//!
//! `AgentLoop` deliberately knows nothing about token usage, measured tool
//! durations, result sizes, or the tool policy. The CLI therefore wraps its
//! provider and tools:
//!
//! - [`TokenCountingProvider`] observes `LlmEvent::Done` and charges usage
//!   against a shared [`TokenBudgetGuard`];
//! - [`GovernedTool`] evaluates the harness policy before execution, enforces
//!   the wall-clock timeout, and checks the result-size guard afterwards.
//!
//! Both wrappers report budget trips through [`GovernanceState`], which emits
//! the structured [`AgentEvent::BudgetExceeded`] event and cancels the session
//! token. Policy denials are emitted but do not cancel: the model receives an
//! error result and may adapt.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use jessica_core::guard::{Guard, GuardContext, GuardDecision};
use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::{AgentEvent, BudgetKind, ToolCall, ToolResult};
use jessica_harness::{Harness, ResultSizeGuard, TokenBudgetGuard, ToolTimeoutGuard};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Shared budget state for one CLI turn.
pub(crate) struct GovernanceState {
    events: broadcast::Sender<AgentEvent>,
    cancel: CancellationToken,
    tokens_used: AtomicU64,
    tripped: AtomicBool,
}

impl GovernanceState {
    /// Creates governance state that emits on `events` and cancels `cancel`.
    pub(crate) fn new(events: broadcast::Sender<AgentEvent>, cancel: CancellationToken) -> Self {
        Self {
            events,
            cancel,
            tokens_used: AtomicU64::new(0),
            tripped: AtomicBool::new(false),
        }
    }

    /// Returns whether a budget trip has already occurred.
    pub(crate) fn tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }

    /// Returns the number of tokens charged so far.
    pub(crate) fn tokens_used(&self) -> u64 {
        self.tokens_used.load(Ordering::SeqCst)
    }

    /// Sends an event without stopping the session.
    pub(crate) fn emit(&self, event: AgentEvent) {
        let _ = self.events.send(event);
    }

    /// Emits one budget event and cancels the session.
    ///
    /// Only the first trip emits; later trips are no-ops so the terminal trace
    /// contains exactly one budget failure.
    pub(crate) fn trip(&self, event: AgentEvent) -> bool {
        if self.tripped.swap(true, Ordering::SeqCst) {
            return false;
        }
        let _ = self.events.send(event);
        self.cancel.cancel();
        true
    }

    /// Charges `tokens` and trips `guard` once the configured limit is crossed.
    fn charge_tokens(&self, tokens: u64, guard: &TokenBudgetGuard) {
        let previous = self.tokens_used.fetch_add(tokens, Ordering::SeqCst);
        let used = previous.saturating_add(tokens);
        if let GuardDecision::Exceeded { kind, reason } =
            guard.check(&GuardContext::new(0, used, ""))
        {
            self.trip(AgentEvent::BudgetExceeded {
                kind,
                message: reason,
            });
        }
    }
}

/// Provider adapter that charges token usage before the loop sees `Done`.
pub(crate) struct TokenCountingProvider {
    inner: Arc<dyn LlmProvider>,
    governance: Arc<GovernanceState>,
    guard: Arc<TokenBudgetGuard>,
}

impl TokenCountingProvider {
    /// Wraps `inner`, charging each reported usage against `guard`.
    pub(crate) fn new(
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
        messages: &[jessica_core::types::Message],
        tools: &[ToolSchema],
    ) -> Result<LlmStream, LlmError> {
        let stream = self.inner.stream_chat(messages, tools).await?;
        let governance = Arc::clone(&self.governance);
        let guard = Arc::clone(&self.guard);

        Ok(stream
            .map(move |event| {
                if let LlmEvent::Done { usage: Some(usage) } = &event {
                    governance.charge_tokens(usage.total(), &guard);
                }
                event
            })
            .boxed())
    }
}

/// Tool adapter that applies policy, timeout, and result-size governance.
pub(crate) struct GovernedTool {
    inner: Arc<dyn Tool>,
    harness: Arc<Harness>,
    governance: Arc<GovernanceState>,
    timeout: Duration,
    timeout_guard: Arc<ToolTimeoutGuard>,
    result_guard: Arc<ResultSizeGuard>,
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
        timeout_guard: Arc<ToolTimeoutGuard>,
        result_guard: Arc<ResultSizeGuard>,
    ) -> Self {
        Self {
            inner,
            harness,
            governance,
            timeout,
            timeout_guard,
            result_guard,
        }
    }

    /// Reports a timeout and returns the model-facing error result.
    fn timeout_result(&self, call: &ToolCall, elapsed: Duration) -> ToolResult {
        let mut context =
            GuardContext::new(0, self.governance.tokens_used(), "").with_tool_call(call);
        context.tool_elapsed = Some(elapsed);

        let message = match self.timeout_guard.check(&context) {
            GuardDecision::Exceeded { reason, .. } => reason,
            GuardDecision::Allow => {
                format!("tool `{}` exceeded its timeout", self.inner.name())
            }
        };
        self.governance.trip(AgentEvent::BudgetExceeded {
            kind: BudgetKind::ToolTimeout,
            message: message.clone(),
        });
        ToolResult::error(String::new(), message)
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
        if self.governance.tripped() {
            return ToolResult::error(
                String::new(),
                "tool execution skipped because the session budget was exceeded",
            );
        }

        // `Tool::execute` has no call id. The policy only needs name and
        // arguments, so an empty id is sufficient for this checkpoint.
        let call = ToolCall::new(String::new(), self.inner.name(), args.clone());
        if let Some(event) = self.harness.authorize_tool_call(&call) {
            self.governance.emit(event);
            return ToolResult::error(
                String::new(),
                format!("tool `{}` was denied by policy", self.inner.name()),
            );
        }

        let started = Instant::now();
        let outcome = tokio::time::timeout(self.timeout, self.inner.execute(args)).await;
        let elapsed = started.elapsed();

        match outcome {
            Err(_) => {
                // The guard compares elapsed time to the configured limit. Use
                // a value strictly past the limit so the trip is guaranteed
                // even if the timer fired a hair before `elapsed` crossed it.
                let crossed = self.timeout + Duration::from_nanos(1);
                self.timeout_result(&call, elapsed.max(crossed))
            }
            Ok(result) => {
                let context = GuardContext::new(0, self.governance.tokens_used(), "")
                    .with_tool_result(&result, Some(elapsed));
                if let GuardDecision::Exceeded { kind, reason } = self.result_guard.check(&context)
                {
                    self.governance.trip(AgentEvent::BudgetExceeded {
                        kind,
                        message: reason.clone(),
                    });
                    return ToolResult::error(String::new(), reason);
                }
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    use jessica_core::types::{Message, TokenUsage};
    use jessica_harness::AllowlistPolicy;

    struct MockTool {
        name: &'static str,
        content: String,
        delay: Duration,
        calls: AtomicU32,
    }

    impl MockTool {
        fn new(name: &'static str, content: impl Into<String>, delay: Duration) -> Self {
            Self {
                name,
                content: content.into(),
                delay,
                calls: AtomicU32::new(0),
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

        assert!(governance.trip(first));
        assert!(!governance.trip(second));

        let message = expect_budget_event(&mut events, BudgetKind::TokenLimit);
        assert_eq!(message, "first");
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }
}
