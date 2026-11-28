//! CLI permission gates and the fixed native sensitive-tool set.
//!
//! Governance ordering, policy evaluation, and result management are owned by
//! `jessica-engine`. This module keeps only the host-side approval prompts and
//! the classification the CLI catalog needs before a descriptor is built.

use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::types::ToolCall;
use tokio::io::AsyncBufReadExt;

use crate::approval::approval_request_input;
use crate::config::{ApprovalMode, HarnessConfig, RuleAction};

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

#[cfg(test)]
mod tests {
    use super::*;

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
            "read_artifact",
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
}
