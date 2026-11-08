//! Behaviour tests for deterministic guards and tool policy.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use jessica_core::guard::{Guard, GuardContext};
use jessica_core::types::{AgentEvent, BudgetKind, ToolCall, ToolResult};
use jessica_harness::{
    AllowlistPolicy, Harness, ResultSizeGuard, StuckDetectionGuard, TokenBudgetGuard,
    ToolTimeoutGuard,
};
use serde_json::json;

fn expect_budget_event(event: Option<AgentEvent>, expected: BudgetKind) -> String {
    match event {
        Some(AgentEvent::BudgetExceeded { kind, message }) => {
            assert_eq!(kind, expected);
            message
        }
        other => panic!("expected BudgetExceeded({expected:?}), got {other:?}"),
    }
}

#[test]
fn token_budget_guard_trips_only_above_its_limit() {
    let harness = Harness::new(Vec::new()).with_guards(vec![Arc::new(TokenBudgetGuard::new(100))]);

    assert!(
        harness
            .check_guards(&GuardContext::new(1, 100, ""))
            .is_none()
    );

    let message = expect_budget_event(
        harness.check_guards(&GuardContext::new(1, 101, "")),
        BudgetKind::TokenLimit,
    );
    assert!(message.contains("101"));
    assert!(message.contains("100"));
}

#[test]
fn tool_timeout_guard_trips_only_above_its_limit() {
    let harness = Harness::new(Vec::new()).with_guards(vec![Arc::new(ToolTimeoutGuard::new(
        Duration::from_millis(50),
    ))]);
    let call = ToolCall::new("call-1", "bash", json!({"command": "sleep 1"}));
    let result = ToolResult::success("call-1", "ok");

    let at_limit = GuardContext::new(1, 0, "")
        .with_tool_call(&call)
        .with_tool_result(&result, Some(Duration::from_millis(50)));
    assert!(harness.check_guards(&at_limit).is_none());

    let over_limit = GuardContext::new(1, 0, "")
        .with_tool_call(&call)
        .with_tool_result(&result, Some(Duration::from_millis(51)));
    let message = expect_budget_event(harness.check_guards(&over_limit), BudgetKind::ToolTimeout);
    assert!(message.contains("bash"));
    assert!(message.contains("51ms"));
}

#[test]
fn result_size_guard_trips_only_above_its_limit() {
    let harness = Harness::new(Vec::new()).with_guards(vec![Arc::new(ResultSizeGuard::new(8))]);

    let at_limit = ToolResult::success("call-1", "12345678");
    let context = GuardContext::new(1, 0, "").with_tool_result(&at_limit, None);
    assert!(harness.check_guards(&context).is_none());

    let over_limit = ToolResult::success("call-1", "123456789");
    let context = GuardContext::new(1, 0, "").with_tool_result(&over_limit, None);
    let message = expect_budget_event(harness.check_guards(&context), BudgetKind::ResultSize);
    assert!(message.contains('9'));
    assert!(message.contains('8'));
}

#[test]
fn stuck_detection_guard_trips_after_consecutive_stalled_rounds() {
    let harness = Harness::new(Vec::new()).with_guards(vec![Arc::new(StuckDetectionGuard::new(2))]);

    assert!(
        harness
            .check_guards(&GuardContext::new(1, 0, "stable"))
            .is_none()
    );
    assert!(
        harness
            .check_guards(&GuardContext::new(2, 0, "stable"))
            .is_none()
    );

    let message = expect_budget_event(
        harness.check_guards(&GuardContext::new(3, 0, "stable")),
        BudgetKind::Stuck,
    );
    assert!(message.contains("2 consecutive"));
}

#[test]
fn stuck_detection_guard_resets_on_tool_or_output_progress() {
    let harness = Harness::new(Vec::new()).with_guards(vec![Arc::new(StuckDetectionGuard::new(2))]);
    let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));

    assert!(
        harness
            .check_guards(&GuardContext::new(1, 0, "stable"))
            .is_none()
    );
    assert!(
        harness
            .check_guards(&GuardContext::new(2, 0, "stable"))
            .is_none()
    );
    assert!(
        harness
            .check_guards(&GuardContext::new(3, 0, "changed"))
            .is_none()
    );
    assert!(
        harness
            .check_guards(&GuardContext::new(4, 0, "changed"))
            .is_none()
    );
    assert!(
        harness
            .check_guards(&GuardContext::new(5, 0, "changed").with_tool_call(&call))
            .is_none()
    );
    assert!(
        harness
            .check_guards(&GuardContext::new(6, 0, "changed"))
            .is_none()
    );

    let message = expect_budget_event(
        harness.check_guards(&GuardContext::new(7, 0, "changed")),
        BudgetKind::Stuck,
    );
    assert!(message.contains("2 consecutive"));
}

#[test]
fn stuck_detection_guard_is_idempotent_within_a_round_and_resettable() {
    let guard = Arc::new(StuckDetectionGuard::new(2));
    let harness = Harness::new(Vec::new()).with_guards(vec![Arc::clone(&guard) as Arc<dyn Guard>]);

    assert!(
        harness
            .check_guards(&GuardContext::new(1, 0, "stable"))
            .is_none()
    );
    // Re-checking the same round must not advance the stall counter.
    assert!(
        harness
            .check_guards(&GuardContext::new(1, 0, "stable"))
            .is_none()
    );
    assert!(
        harness
            .check_guards(&GuardContext::new(2, 0, "stable"))
            .is_none()
    );
    assert!(
        harness
            .check_guards(&GuardContext::new(2, 0, "stable"))
            .is_none()
    );
    expect_budget_event(
        harness.check_guards(&GuardContext::new(3, 0, "stable")),
        BudgetKind::Stuck,
    );

    guard.reset();
    assert!(
        harness
            .check_guards(&GuardContext::new(3, 0, "stable"))
            .is_none()
    );
    assert!(
        harness
            .check_guards(&GuardContext::new(4, 0, "stable"))
            .is_none()
    );
    expect_budget_event(
        harness.check_guards(&GuardContext::new(5, 0, "stable")),
        BudgetKind::Stuck,
    );
}

#[test]
fn allowlist_policy_allows_listed_tools_and_denies_others() {
    let policy = AllowlistPolicy::new(vec!["read_file".to_owned()]);
    let harness = Harness::new(Vec::new()).with_policy(Arc::new(policy));

    let allowed = ToolCall::new("call-1", "read_file", json!({"path": "README.md"}));
    assert!(harness.authorize_tool_call(&allowed).is_none());

    let denied = ToolCall::new("call-2", "bash", json!({"command": "id"}));
    let event = harness
        .authorize_tool_call(&denied)
        .expect("unlisted tool must be denied");
    match event {
        AgentEvent::PolicyDenied { call, reason } => {
            assert_eq!(call.name, "bash");
            assert!(reason.contains("allowlist"));
        }
        other => panic!("expected PolicyDenied, got {other:?}"),
    }
}

#[test]
fn default_policy_denies_every_tool_call() {
    let harness = Harness::new(Vec::new());
    let call = ToolCall::new("call-1", "read_file", json!({"path": "README.md"}));

    let event = harness
        .authorize_tool_call(&call)
        .expect("default policy must deny");

    match event {
        AgentEvent::PolicyDenied { call, reason } => {
            assert_eq!(call.name, "read_file");
            assert!(reason.contains("denied"));
        }
        other => panic!("expected PolicyDenied, got {other:?}"),
    }
}

#[test]
fn first_exceeded_guard_wins_in_registration_order() {
    let harness = Harness::new(Vec::new()).with_guards(vec![
        Arc::new(TokenBudgetGuard::new(0)),
        Arc::new(ResultSizeGuard::new(0)),
    ]);
    let result = ToolResult::success("call-1", "x");
    let context = GuardContext::new(1, 1, "").with_tool_result(&result, None);

    expect_budget_event(harness.check_guards(&context), BudgetKind::TokenLimit);
}

#[test]
fn guards_and_policy_do_not_panic_on_empty_contexts() {
    let harness = Harness::new(Vec::new()).with_guards(vec![
        Arc::new(TokenBudgetGuard::new(10)),
        Arc::new(ToolTimeoutGuard::new(Duration::from_millis(10))),
        Arc::new(StuckDetectionGuard::new(1)),
        Arc::new(ResultSizeGuard::new(10)),
    ]);
    let context = GuardContext::new(1, 0, "");

    let outcome = catch_unwind(AssertUnwindSafe(|| harness.check_guards(&context)));
    assert!(outcome.expect("guard evaluation must not panic").is_none());

    let call = ToolCall::new("call-1", "bash", json!({"command": "true"}));
    let outcome = catch_unwind(AssertUnwindSafe(|| harness.authorize_tool_call(&call)));
    assert!(matches!(
        outcome.expect("policy evaluation must not panic"),
        Some(AgentEvent::PolicyDenied { .. })
    ));
}
