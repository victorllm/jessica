//! Policy enforcement contract.

use serde::{Deserialize, Serialize};

use crate::types::ToolCall;

/// Outcome of evaluating a tool call against a policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecision {
    /// The tool call may proceed.
    Allow,
    /// The tool call is rejected; the string explains why.
    Deny(String),
}

/// Decides whether a tool call is permitted.
pub trait Policy: Send + Sync {
    /// Checks `call` and returns an allow/deny decision.
    fn check(&self, call: &ToolCall) -> PolicyDecision;
}

/// Policy that denies every tool call.
///
/// This is the safe default: capabilities must be granted explicitly.
#[derive(Debug, Default, Clone, Copy)]
pub struct DenyAll;

impl Policy for DenyAll {
    fn check(&self, _call: &ToolCall) -> PolicyDecision {
        PolicyDecision::Deny("all tool calls are denied by default".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assert_round_trip<T>(value: &T)
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_string(value).expect("value should serialize");
        let decoded: T = serde_json::from_str(&encoded).expect("value should deserialize");
        assert_eq!(&decoded, value);
    }

    #[test]
    fn decisions_round_trip() {
        assert_round_trip(&PolicyDecision::Allow);
        assert_round_trip(&PolicyDecision::Deny("not allowed".into()));
    }

    #[test]
    fn deny_all_rejects_every_call() {
        let call = ToolCall::new("call-1", "bash", json!({"command": "rm -rf /"}));
        let policy: &dyn Policy = &DenyAll;
        match policy.check(&call) {
            PolicyDecision::Deny(reason) => {
                assert!(!reason.is_empty(), "denial must explain itself");
            }
            PolicyDecision::Allow => panic!("DenyAll must not allow tool calls"),
        }
    }
}
