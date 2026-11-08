//! Native tools for Jessica agents.
//!
//! File tools are confined to a workspace root. [`jessica_core::tool::Tool::execute`]
//! only receives JSON arguments, so tools cannot know the originating call id;
//! results carry an empty `tool_call_id` and the agent loop stamps it before
//! emitting the event or feeding the result back to the model.

#![deny(missing_docs)]

mod bash;
mod file;

pub use bash::BashTool;
pub use file::{ReadFileTool, WriteFileTool};

use jessica_core::types::ToolResult;

/// Builds a successful result whose call id will be stamped by the loop.
pub(crate) fn success(content: impl Into<String>) -> ToolResult {
    ToolResult::success(String::new(), content)
}

/// Builds a failed result whose call id will be stamped by the loop.
pub(crate) fn failure(content: impl Into<String>) -> ToolResult {
    ToolResult::error(String::new(), content)
}
