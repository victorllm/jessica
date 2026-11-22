//! Tool catalog assembly and the `jessica tools` inspection commands.

mod catalog;
mod descriptor;

use std::path::Path;

use jessica_core::tool::Tool;
use jessica_runtime::{DELEGATE_TASK_TOOL_NAME, delegate_task_schema};
use jessica_tools::{BashTool, ReadFileTool, WriteFileTool};

use crate::app::{CliError, load_wasm_tools};
use crate::config::HarnessConfig;

pub(crate) use catalog::{CatalogCandidate, CatalogEntry, CatalogError, ToolCatalog};
pub(crate) use descriptor::{ApprovalPolicy, ToolSource};

use std::sync::Arc;

/// Subcommand requested under `jessica tools`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ToolsCommand {
    /// List configured tools. `all` includes hidden tools and their reasons.
    List {
        /// Whether to include tools that are configured but not exposed.
        all: bool,
    },
    /// Show one tool's descriptor, schema digest, and limits.
    Inspect {
        /// Tool name to inspect.
        name: String,
    },
}

/// Builds executable candidates for the native tools.
///
/// The tools are bound to the turn's canonical workspace root. Constructing
/// them has no side effects; `tools list` uses the same candidates as a turn.
pub(crate) fn native_candidates(root: &Path) -> Vec<CatalogCandidate> {
    let read: Arc<dyn Tool> = Arc::new(ReadFileTool::new(root.to_path_buf()));
    let write: Arc<dyn Tool> = Arc::new(WriteFileTool::new(root.to_path_buf()));
    let bash: Arc<dyn Tool> = Arc::new(BashTool::new().with_current_dir(root.to_path_buf()));
    vec![
        CatalogCandidate::from_tool(read, ToolSource::Native),
        CatalogCandidate::from_tool(write, ToolSource::Native),
        CatalogCandidate::from_tool(bash, ToolSource::Native),
    ]
}

/// Builds the descriptor-only delegation candidate used by inspection.
///
/// A turn replaces this with the executable `DelegateTaskTool` after the child
/// executor exists. Inspection must not construct a provider, so it uses the
/// fixed schema directly.
pub(crate) fn delegation_candidate() -> CatalogCandidate {
    CatalogCandidate::descriptor_only(
        DELEGATE_TASK_TOOL_NAME,
        delegate_task_schema(),
        ToolSource::Delegation,
    )
}

/// Builds the catalog visible to `jessica tools`.
///
/// The command loads WASM manifests because they are part of the configured
/// catalog, but it never constructs a provider, event store, memory database,
/// skill database, or MCP connection.
async fn inspection_catalog(config: &HarnessConfig, root: &Path) -> Result<ToolCatalog, CliError> {
    let mut candidates = native_candidates(root);
    for tool in load_wasm_tools(config, root).await? {
        candidates.push(CatalogCandidate::from_tool(tool, ToolSource::Wasm));
    }
    if config.delegation.enabled {
        candidates.push(delegation_candidate());
    }
    ToolCatalog::build(candidates, config).map_err(CliError::ToolCatalog)
}

/// Runs `jessica tools list` or `jessica tools inspect`.
///
/// Output is printed to stdout, which is reserved for user-facing CLI output.
/// An over-budget catalog is reported as a warning by `list` and as an error by
/// a turn; inspection must remain possible so the misconfiguration can be
/// diagnosed.
pub(crate) async fn run_tools_command(
    config: &HarnessConfig,
    root: &Path,
    command: ToolsCommand,
) -> Result<(), CliError> {
    let catalog = inspection_catalog(config, root).await?;
    match command {
        ToolsCommand::List { all } => {
            if let Err(error) = catalog.enforce_budget(config) {
                eprintln!("[tools] warning: {error}");
            }
            render_list(&catalog, all);
            println!(
                "exposed {}/{} tools; schema bytes {}/{}",
                catalog.exposed_count(),
                catalog.entries().len(),
                catalog.exposed_schema_bytes(),
                config.tools.max_tool_schema_bytes
            );
            Ok(())
        }
        ToolsCommand::Inspect { name } => {
            let entry = catalog
                .find(&name)
                .ok_or(CatalogError::UnknownTool { name })?;
            render_inspect(entry);
            Ok(())
        }
    }
}

fn render_list(catalog: &ToolCatalog, all: bool) {
    let name_width = catalog
        .entries()
        .iter()
        .map(|entry| entry.descriptor.name.len())
        .max()
        .unwrap_or(4)
        .max(4);
    if all {
        println!(
            "{:<name_width$}  {:<10}  {:<7}  {:<45}  DIGEST",
            "NAME", "SOURCE", "EXPOSED", "REASON"
        );
        for entry in catalog.entries() {
            println!(
                "{:<name_width$}  {:<10}  {:<7}  {:<45}  {}",
                entry.descriptor.name,
                entry.descriptor.source.as_str(),
                if entry.is_exposed() { "yes" } else { "no" },
                entry.hidden_reason_text(),
                entry.descriptor.schema_digest
            );
        }
    } else {
        println!(
            "{:<name_width$}  {:<10}  {:<10}  DIGEST",
            "NAME", "SOURCE", "TIMEOUT_MS"
        );
        for entry in catalog.exposed() {
            println!(
                "{:<name_width$}  {:<10}  {:<10}  {}",
                entry.descriptor.name,
                entry.descriptor.source.as_str(),
                entry.descriptor.limits.timeout_ms,
                entry.descriptor.schema_digest
            );
        }
    }
}

fn render_inspect(entry: &CatalogEntry) {
    let descriptor = &entry.descriptor;
    println!("name: {}", descriptor.name);
    println!("source: {}", descriptor.source.as_str());
    println!("version: {}", descriptor.version);
    println!("exposed: {}", yes_no(entry.is_exposed()));
    if !entry.is_exposed() {
        println!("hidden: {}", entry.hidden_reason_text());
    }
    println!("approval: {}", descriptor.approval.as_str());
    println!("result_format: {}", descriptor.result_format.as_str());
    println!("capabilities:");
    println!("  read_only: {}", yes_no(descriptor.capabilities.read_only));
    println!(
        "  writes_files: {}",
        yes_no(descriptor.capabilities.writes_files)
    );
    println!("  executes: {}", yes_no(descriptor.capabilities.executes));
    println!("  network: {}", yes_no(descriptor.capabilities.network));
    println!(
        "  destructive: {}",
        yes_no(descriptor.capabilities.destructive)
    );
    println!(
        "  parallel_safe: {}",
        yes_no(descriptor.capabilities.parallel_safe)
    );
    println!("limits:");
    println!("  timeout_ms: {}", descriptor.limits.timeout_ms);
    println!(
        "  max_args_bytes: {}",
        optional_bytes(descriptor.limits.max_args_bytes)
    );
    println!("  max_inline_bytes: {}", descriptor.limits.max_inline_bytes);
    println!(
        "  max_artifact_bytes: {}",
        descriptor
            .limits
            .max_artifact_bytes
            .map(|bytes| bytes.to_string())
            .unwrap_or_else(|| "unsupported".to_owned())
    );
    println!(
        "  max_output_lines: {}",
        optional_bytes(descriptor.limits.max_output_lines)
    );
    println!("policy_hints:");
    println!(
        "  path_field: {}",
        descriptor.policy_hints.path_field.as_deref().unwrap_or("-")
    );
    println!(
        "  command_field: {}",
        descriptor
            .policy_hints
            .command_field
            .as_deref()
            .unwrap_or("-")
    );
    println!(
        "  protected_paths: {}",
        if descriptor.policy_hints.protected_paths.is_empty() {
            "-".to_owned()
        } else {
            descriptor.policy_hints.protected_paths.join(", ")
        }
    );
    println!("schema_digest: {}", descriptor.schema_digest);
    println!("schema_bytes: {}", descriptor.schema_bytes);
    println!("schema:");
    println!("{:#}", descriptor.schema);
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn optional_bytes(value: Option<usize>) -> String {
    value
        .map(|bytes| bytes.to_string())
        .unwrap_or_else(|| "unlimited".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_candidates_are_the_three_native_tools_in_any_order() {
        let candidates = native_candidates(Path::new("."));
        let mut names: Vec<&str> = candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect();
        names.sort_unstable();
        assert_eq!(names, ["bash", "read_file", "write_file"]);
        assert!(
            candidates.iter().all(|candidate| candidate.tool.is_some()),
            "native candidates must be executable"
        );
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.source == ToolSource::Native)
        );
    }

    #[test]
    fn delegation_inspection_candidate_uses_the_runtime_schema_without_a_tool() {
        let candidate = delegation_candidate();
        assert_eq!(candidate.name, DELEGATE_TASK_TOOL_NAME);
        assert_eq!(candidate.schema, delegate_task_schema());
        assert_eq!(candidate.source, ToolSource::Delegation);
        assert!(candidate.tool.is_none());
    }
}
