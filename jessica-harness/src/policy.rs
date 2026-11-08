//! Tool policy implementations.

use std::collections::BTreeSet;

use jessica_core::policy::{Policy, PolicyDecision};
use jessica_core::types::ToolCall;

/// Allows only tools named in an explicit allowlist.
///
/// The default is an empty allowlist, which denies every tool call and matches
/// the safe [`jessica_core::policy::DenyAll`] behaviour. Names are compared
/// case-sensitively.
#[derive(Debug, Clone, Default)]
pub struct AllowlistPolicy {
    allowed: BTreeSet<String>,
}

impl AllowlistPolicy {
    /// Creates a policy from the configured tool names.
    pub fn new(allowed_tools: impl IntoIterator<Item = String>) -> Self {
        Self {
            allowed: allowed_tools.into_iter().collect(),
        }
    }
}

impl Policy for AllowlistPolicy {
    fn check(&self, call: &ToolCall) -> PolicyDecision {
        if self.allowed.contains(&call.name) {
            PolicyDecision::Allow
        } else {
            PolicyDecision::Deny(format!("tool `{}` is not in the allowlist", call.name))
        }
    }
}
