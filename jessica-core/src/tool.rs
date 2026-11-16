//! Tool contract and registry.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

use crate::types::{ToolCall, ToolResult};

/// JSON schema describing a tool, in the format expected by the LLM provider.
pub type ToolSchema = Value;

/// A callable capability exposed to the agent loop.
#[async_trait]
pub trait Tool: Send + Sync {
    /// Stable name used by the model to invoke this tool.
    fn name(&self) -> &str;

    /// Provider-facing schema describing the tool and its arguments.
    fn schema(&self) -> ToolSchema;

    /// Executes the tool with JSON `args`.
    ///
    /// Failures are reported through [`ToolResult::is_error`] instead of a
    /// returned error so the model can observe and recover from them.
    async fn execute(&self, args: Value) -> ToolResult;

    /// Executes the tool with the provider's full call metadata.
    ///
    /// The default implementation forwards the arguments to
    /// [`Tool::execute`], discarding the call id and name. Tools and wrappers
    /// that need to preserve or inspect the originating [`ToolCall`] should
    /// override this method.
    ///
    /// Failures are reported through [`ToolResult::is_error`], matching
    /// [`Tool::execute`].
    async fn execute_with_call(&self, call: &ToolCall) -> ToolResult {
        self.execute(call.arguments.clone()).await
    }
}

/// Error returned when a tool cannot be registered.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ToolRegistryError {
    /// A tool with the same name is already registered.
    #[error("tool `{name}` is already registered")]
    Duplicate {
        /// Name of the conflicting tool.
        name: String,
    },
}

/// Deterministic, name-indexed collection of tools.
///
/// The registry uses a sorted map so iteration order (and therefore the order
/// in which schemas are presented to the model) is stable across runs.
#[derive(Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `tool`, keyed by its name.
    ///
    /// Returns [`ToolRegistryError::Duplicate`] when the name is already taken.
    pub fn register(&mut self, tool: Arc<dyn Tool>) -> Result<(), ToolRegistryError> {
        let name = tool.name().to_owned();
        if self.tools.contains_key(&name) {
            return Err(ToolRegistryError::Duplicate { name });
        }
        self.tools.insert(name, tool);
        Ok(())
    }

    /// Looks up a tool by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).cloned()
    }

    /// Returns whether a tool with `name` is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    /// Returns the number of registered tools.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Returns whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// Iterates over registered tool names in sorted order.
    pub fn names(&self) -> impl Iterator<Item = &str> + '_ {
        self.tools.keys().map(String::as_str)
    }

    /// Returns the schemas of all registered tools in sorted name order.
    pub fn schemas(&self) -> Vec<ToolSchema> {
        self.tools.values().map(|tool| tool.schema()).collect()
    }
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolRegistry")
            .field("tools", &self.names().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct MockTool {
        name: &'static str,
    }

    impl MockTool {
        fn new(name: &'static str) -> Self {
            Self { name }
        }
    }

    #[async_trait]
    impl Tool for MockTool {
        fn name(&self) -> &str {
            self.name
        }

        fn schema(&self) -> ToolSchema {
            json!({
                "type": "function",
                "function": {
                    "name": self.name,
                    "parameters": {"type": "object"}
                }
            })
        }

        async fn execute(&self, args: Value) -> ToolResult {
            ToolResult::success("call-1", args.to_string())
        }
    }

    #[test]
    fn register_lookup_and_duplicate_detection() {
        let mut registry = ToolRegistry::new();
        assert!(registry.is_empty());

        registry
            .register(Arc::new(MockTool::new("alpha")))
            .expect("first registration should succeed");
        assert_eq!(registry.len(), 1);
        assert!(registry.contains("alpha"));
        assert!(registry.get("alpha").is_some());
        assert!(registry.get("missing").is_none());

        let duplicate = registry
            .register(Arc::new(MockTool::new("alpha")))
            .expect_err("duplicate registration must fail");
        assert_eq!(
            duplicate,
            ToolRegistryError::Duplicate {
                name: "alpha".into()
            }
        );
        assert!(duplicate.to_string().contains("alpha"));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn names_and_schemas_are_sorted_deterministically() {
        let mut registry = ToolRegistry::new();
        for name in ["beta", "alpha", "gamma"] {
            registry
                .register(Arc::new(MockTool::new(name)))
                .expect("unique registration should succeed");
        }

        let names: Vec<_> = registry.names().collect();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);

        let schemas = registry.schemas();
        let schema_names: Vec<_> = schemas
            .iter()
            .map(|schema| schema["function"]["name"].as_str().unwrap())
            .collect();
        assert_eq!(schema_names, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn registered_tool_executes_through_dyn_dispatch() {
        let mut registry = ToolRegistry::new();
        registry
            .register(Arc::new(MockTool::new("echo")))
            .expect("registration should succeed");

        let tool = registry.get("echo").expect("tool should exist");
        let result = futures::executor::block_on(tool.execute(json!({"value": 7})));
        assert_eq!(result.tool_call_id, "call-1");
        assert_eq!(result.content, "{\"value\":7}");
        assert!(!result.is_error);
    }

    #[test]
    fn default_execute_with_call_forwards_arguments() {
        let tool = MockTool::new("echo");
        let call = ToolCall::new("call-9", "echo", json!({"value": 11}));
        let result = futures::executor::block_on(tool.execute_with_call(&call));

        // The default implementation has no call-id behavior of its own; it
        // forwards the exact arguments to `execute`.
        assert_eq!(result.tool_call_id, "call-1");
        assert_eq!(result.content, "{\"value\":11}");
        assert!(!result.is_error);
    }

    /// Tool that observes the full call context instead of only its arguments.
    struct CallAwareTool;

    #[async_trait]
    impl Tool for CallAwareTool {
        fn name(&self) -> &str {
            "call-aware"
        }

        fn schema(&self) -> ToolSchema {
            json!({"type": "function", "function": {"name": "call-aware"}})
        }

        async fn execute(&self, _args: Value) -> ToolResult {
            ToolResult::error("unused", "execute must not be called")
        }

        async fn execute_with_call(&self, call: &ToolCall) -> ToolResult {
            ToolResult::success(call.id.clone(), format!("{}:{}", call.name, call.arguments))
        }
    }

    #[test]
    fn execute_with_call_override_receives_the_original_call() {
        let tool: Arc<dyn Tool> = Arc::new(CallAwareTool);
        let call = ToolCall::new("call-42", "call-aware", json!({"key": "value"}));
        let result = futures::executor::block_on(tool.execute_with_call(&call));

        assert_eq!(result.tool_call_id, "call-42");
        assert_eq!(result.content, "call-aware:{\"key\":\"value\"}");
        assert!(!result.is_error);
    }

    #[test]
    fn debug_output_lists_tool_names() {
        let mut registry = ToolRegistry::new();
        registry
            .register(Arc::new(MockTool::new("inspect")))
            .expect("registration should succeed");
        let rendered = format!("{registry:?}");
        assert!(rendered.contains("inspect"));
    }
}
