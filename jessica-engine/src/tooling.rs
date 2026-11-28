//! Host-owned tool catalog metadata.
//!
//! A tool knows how to execute itself; it does not decide whether it is
//! sensitive, destructive, or allowed. [`ToolDescriptor`] carries those facts,
//! and [`ToolCatalog`] is the engine-side boundary between a host-provided
//! tool set and the governed execution path.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use jessica_core::tool::{Tool, ToolSchema};

use crate::error::EngineError;

/// Where a catalog tool came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ToolSource {
    /// A tool compiled into the host.
    Native,
    /// A sandboxed WebAssembly plugin.
    Wasm,
    /// A tool discovered from a session-scoped MCP server.
    Mcp,
    /// The parent-side `delegate_task` tool.
    Delegation,
}

impl ToolSource {
    /// Returns the stable lowercase name used in diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Wasm => "wasm",
            Self::Mcp => "mcp",
            Self::Delegation => "delegation",
        }
    }
}

/// Capability classes assigned by the host.
///
/// Every flag is conservative: it is set when the tool can plausibly exercise
/// the capability, even if another layer currently restricts that action.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// Reads files or other host state without mutating it.
    pub read_only: bool,
    /// Can create or modify files.
    pub writes_files: bool,
    /// Can execute commands or arbitrary server-side operations.
    pub executes: bool,
    /// Can reach the network.
    pub network: bool,
    /// Can destroy or overwrite existing state.
    pub destructive: bool,
    /// Safe to run concurrently with other tool calls.
    pub parallel_safe: bool,
}

/// Host approval policy for one tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApprovalPolicy {
    /// The policy check is the only pre-execution gate.
    NotRequired,
    /// The host asks its client for one-time approval when a gate exists.
    Interactive,
}

impl ApprovalPolicy {
    /// Returns the stable lowercase name used in diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotRequired => "not_required",
            Self::Interactive => "interactive",
        }
    }
}

/// Shape of a tool result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResultFormat {
    /// Free-form text.
    Text,
    /// A JSON document.
    Json,
}

impl ResultFormat {
    /// Returns the stable lowercase name used in diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

/// Per-tool execution limits.
///
/// A `None` value inherits the engine's tool-execution configuration. Hosts
/// set a per-tool limit only when the tool has a narrower safe bound.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ToolLimits {
    /// Optional wall-clock limit for this tool.
    pub timeout: Option<Duration>,
    /// Optional serialized-argument limit for this tool.
    pub max_args_bytes: Option<usize>,
}

/// Host-side governance metadata for one tool.
///
/// A descriptor is assigned by the host from the tool's source and name. Tools
/// cannot claim their own capabilities or approval class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolDescriptor {
    /// Name exposed to the model.
    pub name: String,
    /// Host-assigned source.
    pub source: ToolSource,
    /// Host-assigned capabilities.
    pub capabilities: Capabilities,
    /// Host approval policy.
    pub approval: ApprovalPolicy,
    /// Shape of the result.
    pub result_format: ResultFormat,
    /// Argument field carrying a filesystem path, when the tool has one.
    pub path_field: Option<String>,
    /// Per-tool execution limits.
    pub limits: ToolLimits,
}

/// One executable tool together with its host-owned descriptor.
#[derive(Clone)]
pub struct ToolEntry {
    /// Executable tool implementation.
    pub tool: Arc<dyn Tool>,
    /// Governance metadata for the tool.
    pub descriptor: ToolDescriptor,
}

impl std::fmt::Debug for ToolEntry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolEntry")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

/// Deterministic, name-indexed set of tools for one turn.
///
/// The catalog rejects duplicate names and descriptors that disagree with the
/// executable tool. Iteration and schema order are sorted by tool name, so the
/// model-visible tool list is stable across runs.
#[derive(Debug, Clone, Default)]
pub struct ToolCatalog {
    entries: BTreeMap<String, ToolEntry>,
}

impl ToolCatalog {
    /// Creates an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one executable tool and its descriptor.
    ///
    /// Returns [`EngineError::ToolNameMismatch`] when the descriptor and tool
    /// disagree, and [`EngineError::DuplicateTool`] when the name is already
    /// present. In either failure the catalog is unchanged.
    pub fn insert(&mut self, entry: ToolEntry) -> Result<(), EngineError> {
        let tool_name = entry.tool.name().to_owned();
        if entry.descriptor.name != tool_name {
            return Err(EngineError::ToolNameMismatch {
                descriptor: entry.descriptor.name,
                tool: tool_name,
            });
        }
        if self.entries.contains_key(&tool_name) {
            return Err(EngineError::DuplicateTool { name: tool_name });
        }
        self.entries.insert(tool_name, entry);
        Ok(())
    }

    /// Returns whether the catalog contains no tools.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of tools in the catalog.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Looks up a tool by its model-visible name.
    pub fn get(&self, name: &str) -> Option<&ToolEntry> {
        self.entries.get(name)
    }

    /// Iterates over entries in sorted tool-name order.
    pub fn iter(&self) -> impl Iterator<Item = &ToolEntry> {
        self.entries.values()
    }

    /// Returns tool names in sorted order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    /// Returns provider-facing schemas in sorted tool-name order.
    pub fn schemas(&self) -> Vec<ToolSchema> {
        self.entries
            .values()
            .map(|entry| entry.tool.schema())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use jessica_core::tool::ToolSchema;
    use jessica_core::types::ToolResult;
    use serde_json::{Value, json};

    use super::*;

    struct MockTool {
        name: &'static str,
    }

    #[async_trait]
    impl Tool for MockTool {
        fn name(&self) -> &str {
            self.name
        }

        fn schema(&self) -> ToolSchema {
            json!({
                "type": "function",
                "function": {"name": self.name, "parameters": {"type": "object"}}
            })
        }

        async fn execute(&self, _args: Value) -> ToolResult {
            ToolResult::success("call-1", self.name)
        }
    }

    fn descriptor(name: &str) -> ToolDescriptor {
        ToolDescriptor {
            name: name.to_owned(),
            source: ToolSource::Native,
            capabilities: Capabilities::default(),
            approval: ApprovalPolicy::NotRequired,
            result_format: ResultFormat::Text,
            path_field: None,
            limits: ToolLimits::default(),
        }
    }

    fn entry(name: &'static str) -> ToolEntry {
        ToolEntry {
            tool: Arc::new(MockTool { name }),
            descriptor: descriptor(name),
        }
    }

    #[test]
    fn a_descriptor_that_disagrees_with_the_tool_is_rejected() {
        let mut catalog = ToolCatalog::new();
        let error = catalog
            .insert(ToolEntry {
                tool: Arc::new(MockTool { name: "actual" }),
                descriptor: descriptor("claimed"),
            })
            .expect_err("a mismatched descriptor must not enter the catalog");

        assert!(matches!(
            error,
            EngineError::ToolNameMismatch {
                descriptor,
                tool
            } if descriptor == "claimed" && tool == "actual"
        ));
        assert!(catalog.is_empty());
    }

    #[test]
    fn duplicate_tool_names_are_rejected_without_replacing_the_first_entry() {
        let mut catalog = ToolCatalog::new();
        catalog.insert(entry("alpha")).expect("first insert");

        let error = catalog
            .insert(entry("alpha"))
            .expect_err("a duplicate name must not overwrite the catalog entry");

        assert!(matches!(
            error,
            EngineError::DuplicateTool { name } if name == "alpha"
        ));
        assert_eq!(catalog.len(), 1);
    }

    #[test]
    fn names_and_schemas_are_sorted_independently_of_insertion_order() {
        let mut catalog = ToolCatalog::new();
        catalog.insert(entry("gamma")).expect("gamma insert");
        catalog.insert(entry("alpha")).expect("alpha insert");
        catalog.insert(entry("beta")).expect("beta insert");

        assert_eq!(
            catalog.names().collect::<Vec<_>>(),
            vec!["alpha", "beta", "gamma"]
        );
        let schema_names = catalog
            .schemas()
            .into_iter()
            .map(|schema| {
                schema["function"]["name"]
                    .as_str()
                    .expect("schema name must be a string")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(schema_names, vec!["alpha", "beta", "gamma"]);
    }
}
