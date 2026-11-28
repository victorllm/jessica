//! CLI-facing tool descriptor metadata.
//!
//! The descriptor model is owned by `jessica-engine` so the host and the
//! embeddable engine classify tools from the same source, schema, and provider
//! annotations. This module only adds the CLI package version used as the
//! default label for native and delegation tools.

pub(crate) use jessica_engine::{
    ApprovalPolicy, Capabilities, ResultFormat, ToolAnnotations, ToolDescriptor, ToolLimits,
    ToolMetadata, ToolSource, result_format_for,
};

/// Returns the default version label for a tool source.
pub(crate) fn version_for(source: ToolSource) -> &'static str {
    match source {
        ToolSource::Native | ToolSource::Delegation => env!("CARGO_PKG_VERSION"),
        ToolSource::Wasm => "manifest-v1",
        ToolSource::Mcp => "mcp-2025-06-18",
    }
}
