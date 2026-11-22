//! Tool catalog assembly and the `jessica tools` inspection commands.

mod catalog;
mod descriptor;
mod runner;

use std::path::Path;

use jessica_core::tool::Tool;
use jessica_runtime::{DELEGATE_TASK_TOOL_NAME, delegate_task_schema};
use jessica_tools::{
    ApplyPatchTool, BashTool, CommandRunner, EditFileTool, ExecTool, GitOperation, GitTool,
    GrepOptions, GrepTool, ListDirOptions, ListDirTool, ReadFileTool, WriteFileTool,
};

use crate::app::{CliError, load_wasm_tools};
use crate::artifacts::ReadArtifactTool;
use crate::config::HarnessConfig;

pub(crate) use catalog::{CatalogCandidate, CatalogEntry, CatalogError, ToolCatalog};
pub(crate) use descriptor::{
    ApprovalPolicy, Capabilities, ResultFormat, ToolDescriptor, ToolSource,
};

use std::sync::Arc;

use runner::HarnessCommandRunner;

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
/// The tools are bound to the turn's canonical workspace root and to the
/// supervisor settings in `config`. Constructing them has no side effects;
/// `tools list` uses the same candidates as a turn.
///
/// The bash candidate is always constructed so inspection can explain its
/// state, but the catalog hides it unless `tools.bash.enabled` is true.
pub(crate) fn native_candidates(root: &Path, config: &HarnessConfig) -> Vec<CatalogCandidate> {
    let supervisor = config.resolved_supervisor();
    let runner: Arc<dyn CommandRunner> = Arc::new(HarnessCommandRunner);
    let bash_timeout = config
        .tools
        .timeout_for("bash", config.budget.tool_timeout());
    let exec_timeout = config
        .tools
        .timeout_for("exec", config.budget.tool_timeout());
    let git_status_timeout = config
        .tools
        .timeout_for("git_status", config.budget.tool_timeout());
    let git_diff_timeout = config
        .tools
        .timeout_for("git_diff", config.budget.tool_timeout());
    let read: Arc<dyn Tool> = Arc::new(ReadFileTool::new(root.to_path_buf()));
    let write: Arc<dyn Tool> = Arc::new(WriteFileTool::new(root.to_path_buf()));
    let apply_patch: Arc<dyn Tool> = Arc::new(ApplyPatchTool::new(root.to_path_buf()));
    let edit_file: Arc<dyn Tool> = Arc::new(EditFileTool::new(root.to_path_buf()));
    let read_artifact: Arc<dyn Tool> = Arc::new(ReadArtifactTool::from_config(root, config));
    let list_dir: Arc<dyn Tool> = Arc::new(ListDirTool::new(
        root.to_path_buf(),
        ListDirOptions {
            ignore_dirs: config.tools.list_dir.ignore_dirs.clone(),
            max_entries: config.tools.list_dir.max_entries,
        },
    ));
    let grep: Arc<dyn Tool> = Arc::new(GrepTool::new(
        root.to_path_buf(),
        GrepOptions {
            ignore_dirs: config.tools.grep.ignore_dirs.clone(),
            max_files: config.tools.grep.max_files,
            max_file_bytes: config.tools.grep.max_file_bytes,
            max_matches: config.tools.grep.max_matches,
            max_output_bytes: config.tools.grep.max_output_bytes,
        },
    ));
    let git_status: Arc<dyn Tool> = Arc::new(
        GitTool::new(
            root.to_path_buf(),
            GitOperation::Status,
            Arc::clone(&runner),
            git_status_timeout,
            supervisor.max_output_bytes,
        )
        .with_env_allowlist(supervisor.env_allowlist.clone()),
    );
    let git_diff: Arc<dyn Tool> = Arc::new(
        GitTool::new(
            root.to_path_buf(),
            GitOperation::Diff,
            Arc::clone(&runner),
            git_diff_timeout,
            supervisor.max_output_bytes,
        )
        .with_env_allowlist(supervisor.env_allowlist.clone()),
    );
    let exec: Arc<dyn Tool> = Arc::new(
        ExecTool::new(
            root.to_path_buf(),
            Arc::clone(&runner),
            exec_timeout,
            supervisor.max_output_bytes,
        )
        .with_env_allowlist(supervisor.env_allowlist.clone()),
    );
    let bash: Arc<dyn Tool> = Arc::new(
        BashTool::with_limits(bash_timeout, supervisor.max_output_bytes)
            .with_content_limit(supervisor.max_output_bytes)
            .with_env_allowlist(supervisor.env_allowlist)
            .with_current_dir(root.to_path_buf()),
    );
    vec![
        CatalogCandidate::from_tool(read, ToolSource::Native),
        CatalogCandidate::from_tool(write, ToolSource::Native),
        CatalogCandidate::from_tool(apply_patch, ToolSource::Native),
        CatalogCandidate::from_tool(edit_file, ToolSource::Native),
        CatalogCandidate::from_tool(read_artifact, ToolSource::Native),
        CatalogCandidate::from_tool(list_dir, ToolSource::Native),
        CatalogCandidate::from_tool(grep, ToolSource::Native),
        CatalogCandidate::from_tool(git_status, ToolSource::Native),
        CatalogCandidate::from_tool(git_diff, ToolSource::Native),
        CatalogCandidate::from_tool(exec, ToolSource::Native),
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
pub(crate) async fn inspection_catalog(
    config: &HarnessConfig,
    root: &Path,
) -> Result<ToolCatalog, CliError> {
    let mut candidates = native_candidates(root, config);
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
    fn native_candidates_are_the_configured_native_tools_in_any_order() {
        let config = HarnessConfig::default();
        let candidates = native_candidates(Path::new("."), &config);
        let mut names: Vec<&str> = candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "apply_patch",
                "bash",
                "edit_file",
                "exec",
                "git_diff",
                "git_status",
                "grep",
                "list_dir",
                "read_artifact",
                "read_file",
                "write_file"
            ]
        );
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
