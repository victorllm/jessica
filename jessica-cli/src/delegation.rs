//! CLI-side assembly for delegated child agents.
//!
//! A child agent shares the parent's filesystem boundary: [`ReadFileTool`] and
//! [`WriteFileTool`] are rooted at the same workspace directory, and
//! [`BashTool`] starts commands in that directory. The child gets no sandbox of
//! its own.
//!
//! Child command tools inherit the turn's resolved supervisor settings rather
//! than bare defaults: `bash` and `exec` use the same environment allowlist,
//! deadlines, and per-stream output caps as the parent, and `exec` runs through
//! the same [`HarnessCommandRunner`] as native `exec`. Every child result passes
//! through the parent turn's [`ResultManager`], so oversized output becomes a
//! workspace artifact instead of entering the child transcript verbatim.
//!
//! The only intentionally shared mutable state is the parent's provider token
//! budget and result manager: the CLI passes the same counting provider to the
//! executor, so child calls charge the parent guard and can cancel the parent
//! turn. Child tools are rebuilt for every delegation and are never shared
//! across children. Child events use a private runtime channel and are not
//! persisted to the parent event store.
//!
//! This factory supports exactly the four native tools below. It never
//! constructs a WASM tool and never returns `delegate_task`, so child agents
//! cannot nest. A parent can narrow the child only to names it already exposes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use jessica_engine::ToolResultManager;
use jessica_runtime::{ChildToolFactory, DelegationError};
use jessica_tools::{BashTool, CommandRunner, ExecTool, ReadFileTool, WriteFileTool};
use serde_json::Value;

use crate::config::{HarnessConfig, ResolvedSupervisor};
use crate::tools::{HarnessCommandRunner, ResultFormat, ToolSource, result_format_for};

/// Builds fresh native tools for a delegated child.
pub(crate) struct NativeChildToolFactory {
    /// Workspace root shared with the parent file tools.
    root: PathBuf,
    /// Resolved parent supervisor settings.
    supervisor: ResolvedSupervisor,
    /// Effective child `bash` deadline.
    bash_timeout: Duration,
    /// Effective child `exec` deadline.
    exec_timeout: Duration,
    /// Parent turn result manager.
    result_manager: Arc<dyn ToolResultManager>,
}

impl NativeChildToolFactory {
    /// Creates a factory bound to the parent workspace and turn settings.
    pub(crate) fn new(
        root: PathBuf,
        config: &HarnessConfig,
        result_manager: Arc<dyn ToolResultManager>,
    ) -> Self {
        Self {
            supervisor: config.resolved_supervisor(),
            bash_timeout: config
                .tools
                .timeout_for("bash", config.budget.tool_timeout()),
            exec_timeout: config
                .tools
                .timeout_for("exec", config.budget.tool_timeout()),
            root,
            result_manager,
        }
    }

    /// Builds one supported child tool.
    fn build_one(&self, name: &str) -> Result<Arc<dyn Tool>, DelegationError> {
        let tool: Arc<dyn Tool> = match name {
            "read_file" => Arc::new(ReadFileTool::new(self.root.clone())),
            "write_file" => Arc::new(WriteFileTool::new(self.root.clone())),
            "bash" => Arc::new(
                BashTool::with_limits(self.bash_timeout, self.supervisor.max_output_bytes)
                    .with_content_limit(self.supervisor.max_output_bytes)
                    .with_env_allowlist(self.supervisor.env_allowlist.clone())
                    .with_current_dir(self.root.clone()),
            ),
            "exec" => {
                let runner: Arc<dyn CommandRunner> = Arc::new(HarnessCommandRunner);
                Arc::new(
                    ExecTool::new(
                        self.root.clone(),
                        runner,
                        self.exec_timeout,
                        self.supervisor.max_output_bytes,
                    )
                    .with_env_allowlist(self.supervisor.env_allowlist.clone()),
                )
            }
            other => {
                return Err(DelegationError::ToolFactory {
                    message: format!(
                        "child tool `{other}` is not one of the supported native tools"
                    ),
                });
            }
        };
        Ok(Arc::new(ManagedChildTool {
            inner: tool,
            manager: Arc::clone(&self.result_manager),
            format: result_format_for(ToolSource::Native, name),
        }))
    }
}

impl ChildToolFactory for NativeChildToolFactory {
    fn build(&self, names: &[String]) -> Result<Vec<Arc<dyn Tool>>, DelegationError> {
        names.iter().map(|name| self.build_one(name)).collect()
    }
}

/// Applies the parent turn's result manager to one child tool result.
struct ManagedChildTool {
    /// Executable child tool.
    inner: Arc<dyn Tool>,
    /// Parent turn result manager.
    manager: Arc<dyn ToolResultManager>,
    /// Result shape used when rendering an artifact envelope.
    format: ResultFormat,
}

#[async_trait]
impl Tool for ManagedChildTool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema(&self) -> ToolSchema {
        self.inner.schema()
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let name = self.inner.name().to_owned();
        let result = self.inner.execute(args).await;
        self.manager.manage(&name, self.format, result).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    const EXPECTED: &[&str] = &["bash", "exec", "read_file", "write_file"];

    /// Unique temporary directory removed on drop.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "jessica-child-tools-{}-{label}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temporary directory must be created");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Builds the engine result manager for one test configuration.
    fn result_manager(root: &Path, config: &HarnessConfig) -> Arc<dyn ToolResultManager> {
        let result_config = config
            .tools
            .to_result_config(config.budget.max_result_bytes);
        Arc::new(jessica_engine::ResultManager::new(
            root,
            &result_config,
            config.budget.max_result_bytes,
        ))
    }

    fn factory(root: &Path) -> NativeChildToolFactory {
        let config = HarnessConfig::default();
        let manager = result_manager(root, &config);
        NativeChildToolFactory::new(root.to_path_buf(), &config, manager)
    }

    fn build(
        factory: &NativeChildToolFactory,
        names: &[&str],
    ) -> Result<Vec<Arc<dyn Tool>>, DelegationError> {
        let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        factory.build(&names)
    }

    #[test]
    fn factory_builds_exactly_the_supported_native_tools() {
        let temp = TempDir::new("supported");
        let factory = factory(&temp.path);
        let tools = build(&factory, EXPECTED).expect("native tools should build");
        let names: Vec<&str> = tools.iter().map(|tool| tool.name()).collect();

        assert_eq!(names, EXPECTED);
        assert!(!names.contains(&"delegate_task"));
        assert!(
            tools
                .iter()
                .all(|tool| tool.schema()["function"]["name"].is_string())
        );
    }

    #[test]
    fn factory_rebuilds_fresh_instances_for_every_child() {
        let temp = TempDir::new("fresh");
        let factory = factory(&temp.path);
        let first = build(&factory, &["read_file"]).expect("tool should build");
        let second = build(&factory, &["read_file"]).expect("tool should build");

        assert_eq!(first[0].name(), second[0].name());
        assert!(
            !Arc::ptr_eq(&first[0], &second[0]),
            "child tools must not be shared across delegations"
        );
    }

    #[test]
    fn factory_rejects_unknown_wasm_and_delegate_names() {
        let temp = TempDir::new("rejected");
        let factory = factory(&temp.path);

        for name in ["echo_json", "wasm_tool", "delegate_task"] {
            let error = build(&factory, &[name])
                .err()
                .expect("name must be rejected");
            assert!(
                error.to_string().contains(name),
                "error should name `{name}`: {error}"
            );
        }
    }

    #[test]
    fn factory_returns_no_tools_for_an_empty_request() {
        let temp = TempDir::new("empty");
        let factory = factory(&temp.path);
        assert!(build(&factory, &[]).expect("empty build").is_empty());
    }

    #[tokio::test]
    async fn child_bash_uses_the_configured_deadline_and_output_cap() {
        let temp = TempDir::new("bash-limits");
        let config = HarnessConfig::from_toml_str(
            "[budget]\ntool_timeout_ms = 2000\nmax_result_bytes = 4096\n\
             [tools.bash]\nenabled = true\ntimeout_ms = 250\n\
             [tools.supervisor]\nmax_output_bytes = 128\n",
        )
        .expect("test config must parse");
        let manager = result_manager(&temp.path, &config);
        let factory = NativeChildToolFactory::new(temp.path.clone(), &config, manager);
        let tools = build(&factory, &["bash"]).expect("bash must build");

        let result = tools[0]
            .execute(serde_json::json!({"command": "printf 'x%.0s' {1..1000}"}))
            .await;

        assert!(
            tools[0].schema()["function"]["description"]
                .as_str()
                .is_some_and(|description| description.contains("each capped at 128 bytes"))
                && result.content.contains("[output truncated"),
            "child bash must use the supervisor output cap: {}",
            result.content
        );

        let timed_out = tools[0]
            .execute(serde_json::json!({"command": "sleep 2"}))
            .await;
        assert!(timed_out.is_error);
        assert!(
            timed_out.content.contains("timed out"),
            "child bash must be killed by the configured deadline: {}",
            timed_out.content
        );
    }

    #[tokio::test]
    async fn oversized_child_results_are_stored_as_artifacts() {
        let temp = TempDir::new("artifact");
        let config = HarnessConfig::from_toml_str(
            "[budget]\ntool_timeout_ms = 2000\nmax_result_bytes = 4096\n\
             [tools]\ninline_threshold_bytes = 64\nmax_inline_bytes = 128\nmax_artifact_bytes = 4096\n\
             [tools.bash]\nenabled = true\n",
        )
        .expect("test config must parse");
        let manager = result_manager(&temp.path, &config);
        let factory = NativeChildToolFactory::new(temp.path.clone(), &config, manager);
        let tools = build(&factory, &["bash"]).expect("bash must build");

        let result = tools[0]
            .execute(serde_json::json!({"command": "printf 'y%.0s' {1..400}"}))
            .await;

        assert!(!result.is_error, "child command failed: {}", result.content);
        assert!(
            result.content.contains("[result artifact]")
                && result.content.contains("digest: sha256:"),
            "the child result must be replaced by an artifact envelope: {}",
            result.content
        );
        let artifacts = temp.path.join(".jessica").join("artifacts");
        assert!(
            artifacts.is_dir(),
            "the artifact must live under the workspace: {}",
            artifacts.display()
        );
    }

    #[tokio::test]
    async fn child_exec_runs_through_the_supervisor_with_a_cleared_environment() {
        let temp = TempDir::new("exec");
        let config = HarnessConfig::default();
        let manager = result_manager(&temp.path, &config);
        let factory = NativeChildToolFactory::new(temp.path.clone(), &config, manager);
        let tools = build(&factory, &["exec"]).expect("exec must build");

        let result = tools[0]
            .execute(serde_json::json!({
                "program": "sh",
                "args": ["-c", "printf '%s' \"$OPENAI_API_KEY\""]
            }))
            .await;

        let value: serde_json::Value =
            serde_json::from_str(&result.content).expect("exec result must be JSON");
        assert_eq!(value["exit_code"], 0);
        assert_eq!(
            value["stdout"], "",
            "the supervisor must clear inherited environment values"
        );
    }
}
