//! `jessica policy` inspection commands.
//!
//! `policy simulate` loads the same catalog as `tools` and evaluates one
//! synthetic tool call with policy v2. It never constructs a provider, opens
//! the event store, memory, or skill database, starts an MCP server, or
//! executes a tool.

use std::path::Path;

use jessica_core::types::ToolCall;
use serde_json::Value;

use crate::app::CliError;
use crate::config::HarnessConfig;
use crate::policy_v2::{PolicyTool, PolicyV2};
use crate::tools::{CatalogError, inspection_catalog};

/// Subcommand requested under `jessica policy`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PolicyCommand {
    /// Evaluate one tool call without executing it.
    Simulate {
        /// Tool name to evaluate.
        tool: String,
        /// JSON argument object.
        args: String,
    },
}

/// Runs a `jessica policy` command.
///
/// The caller resolves `root` before dispatch and must not assemble the
/// runtime first. All errors are reported through [`CliError`], so the process
/// exits nonzero with a one-line diagnostic.
pub(crate) async fn run_policy_command(
    config: &HarnessConfig,
    root: &Path,
    command: PolicyCommand,
) -> Result<(), CliError> {
    match command {
        PolicyCommand::Simulate { tool, args } => simulate(config, root, &tool, &args).await,
    }
}

/// Evaluates one synthetic call and prints the decision.
async fn simulate(
    config: &HarnessConfig,
    root: &Path,
    name: &str,
    args: &str,
) -> Result<(), CliError> {
    let catalog = inspection_catalog(config, root).await?;
    let entry = catalog.find(name).ok_or(CatalogError::UnknownTool {
        name: name.to_owned(),
    })?;
    if !entry.is_exposed() {
        return Err(CliError::PolicySimulation {
            message: format!(
                "tool `{name}` is not exposed: {}",
                entry.hidden_reason_text()
            ),
        });
    }

    let arguments: Value =
        serde_json::from_str(args).map_err(|error| CliError::PolicySimulation {
            message: format!("invalid JSON arguments: {error}"),
        })?;
    if !arguments.is_object() {
        return Err(CliError::PolicySimulation {
            message: "arguments must be a JSON object".to_owned(),
        });
    }

    let policy = PolicyV2::new(root, config).await?;
    let call = ToolCall::new("policy-simulate", name, arguments);
    let verdict = policy
        .evaluate(&PolicyTool::from_descriptor(&entry.descriptor), &call)
        .await?;
    let (decision, approval) = verdict.explain();
    let rule = verdict.rule_id().unwrap_or("-");
    let reason = single_line(verdict.reason());

    println!("tool: {name}");
    println!("source: {}", entry.descriptor.source.as_str());
    println!("decision: {}", decision.as_str());
    println!("rule: {rule}");
    println!("approval: {}", approval.as_str());
    println!("reason: {reason}");
    Ok(())
}

/// Collapses line breaks so each reported field stays one line.
fn single_line(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\n' | '\r' => ' ',
            other => other,
        })
        .collect()
}
