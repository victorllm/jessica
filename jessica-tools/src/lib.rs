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
//!
//! Command-backed native tools ([`ExecTool`] and [`GitTool`]) accept a
//! [`CommandRunner`]. The host supplies the execution supervisor; this crate
//! only validates arguments and renders bounded results, so it never depends on
//! `jessica-harness`.

#![deny(missing_docs)]

mod bash;
mod command;
mod exec;
mod file;
mod git;
mod grep;
mod list_dir;
#[cfg(test)]
mod test_support;
mod wasm;
mod workspace;

pub use bash::{BashTool, DEFAULT_BASH_TIMEOUT};
pub use command::{CommandError, CommandOutput, CommandRequest, CommandRunner, CommandStream};
pub use exec::ExecTool;
pub use file::{
    DEFAULT_READ_FILE_LINES, MAX_READ_FILE_BYTES, MAX_READ_FILE_LINES, ReadFileTool, WriteFileTool,
};
pub use git::{GitOperation, GitTool};
pub use grep::{
    DEFAULT_GREP_FILE_BYTES, DEFAULT_GREP_FILES, DEFAULT_GREP_IGNORES, DEFAULT_GREP_MATCHES,
    DEFAULT_GREP_OUTPUT_BYTES, GrepOptions, GrepTool,
};
pub use list_dir::{
    DEFAULT_LIST_DIR_ENTRIES, DEFAULT_LIST_DIR_IGNORES, ListDirOptions, ListDirTool,
};
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
