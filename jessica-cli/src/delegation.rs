//! CLI-side assembly for delegated child agents.
//!
//! A child agent shares the parent's filesystem boundary: [`ReadFileTool`] and
//! [`WriteFileTool`] are rooted at the same workspace directory, and
//! [`BashTool`] is the same native implementation the parent uses. The child
//! gets no sandbox of its own.
//!
//! The only intentionally shared mutable state is the parent's provider token
//! budget: the CLI passes the same counting provider to the executor, so child
//! calls charge the parent guard and can cancel the parent turn. Child tools
//! are rebuilt for every delegation and are never shared across children.
//! Child events use a private runtime channel and are not persisted to the
//! parent event store.
//!
//! This factory supports exactly the three native tools. It never constructs a
//! WASM tool and never returns `delegate_task`, so child agents cannot nest.

use std::path::PathBuf;
use std::sync::Arc;

use jessica_core::tool::Tool;
use jessica_runtime::{ChildToolFactory, DelegationError};
use jessica_tools::{BashTool, ReadFileTool, WriteFileTool};

/// Builds fresh native tools for a delegated child.
pub(crate) struct NativeChildToolFactory {
    /// Workspace root shared with the parent file tools.
    root: PathBuf,
}

impl NativeChildToolFactory {
    /// Creates a factory bound to the parent workspace root.
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

impl ChildToolFactory for NativeChildToolFactory {
    fn build(&self, names: &[String]) -> Result<Vec<Arc<dyn Tool>>, DelegationError> {
        names
            .iter()
            .map(|name| {
                let tool: Arc<dyn Tool> = match name.as_str() {
                    "read_file" => Arc::new(ReadFileTool::new(self.root.clone())),
                    "write_file" => Arc::new(WriteFileTool::new(self.root.clone())),
                    "bash" => Arc::new(BashTool::new()),
                    other => {
                        return Err(DelegationError::ToolFactory {
                            message: format!(
                                "child tool `{other}` is not one of the supported native tools"
                            ),
                        });
                    }
                };
                Ok(tool)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use jessica_core::tool::Tool;

    const EXPECTED: &[&str] = &["bash", "read_file", "write_file"];

    fn build(
        factory: &NativeChildToolFactory,
        names: &[&str],
    ) -> Result<Vec<Arc<dyn Tool>>, DelegationError> {
        let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        factory.build(&names)
    }

    #[test]
    fn factory_builds_exactly_the_supported_native_tools() {
        let factory = NativeChildToolFactory::new(PathBuf::from("/workspace"));
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
        let factory = NativeChildToolFactory::new(PathBuf::from("/workspace"));
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
        let factory = NativeChildToolFactory::new(PathBuf::from("/workspace"));

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
        let factory = NativeChildToolFactory::new(PathBuf::from("/workspace"));
        assert!(build(&factory, &[]).expect("empty build").is_empty());
    }
}
