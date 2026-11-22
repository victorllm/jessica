//! Tools for Jessica agents: native workspace tools and sandboxed WebAssembly
//! plugins.
//!
//! File tools are confined to a workspace root. [`jessica_core::tool::Tool::execute`]
//! only receives JSON arguments, so tools cannot know the originating call id;
//! results carry an empty `tool_call_id` and the agent loop stamps it before
//! emitting the event or feeding the result back to the model.
//!
//! [`WasmTool`] adds manifest-driven WebAssembly tools. Native and wasm tools
//! share one [`jessica_core::tool::ToolRegistry`]; [`register_wasm_tool`] is the
//! registration entry point for the latter.

#![deny(missing_docs)]

mod bash;
mod file;
mod wasm;

pub use bash::{BashTool, DEFAULT_BASH_TIMEOUT};
pub use file::{ReadFileTool, WriteFileTool};
pub use wasm::{WasmTool, WasmToolError, register_wasm_tool};

use jessica_core::types::ToolResult;

/// Builds a successful result whose call id will be stamped by the loop.
pub(crate) fn success(content: impl Into<String>) -> ToolResult {
    ToolResult::success(String::new(), content)
}

/// Builds a failed result whose call id will be stamped by the loop.
pub(crate) fn failure(content: impl Into<String>) -> ToolResult {
    ToolResult::error(String::new(), content)
}
