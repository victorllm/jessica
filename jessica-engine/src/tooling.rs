//! Host-owned tool catalog metadata.
//!
//! A tool knows how to execute itself; it does not decide whether it is
//! sensitive, destructive, or allowed. [`ToolDescriptor`] carries those facts,
//! and [`ToolCatalog`] is the engine-side boundary between a host-provided
//! tool set and the governed execution path.
//!
//! Descriptors are derived from the source, the tool name, and the provider
//! metadata. The host supplies the resolved limits and approval policy; it
//! cannot ask a tool to classify itself. Schema metadata is canonicalized so
//! two catalogs built from equivalent JSON objects produce the same digest.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use jessica_core::tool::{Tool, ToolSchema};
use sha2::{Digest, Sha256};

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

/// Advisory metadata supplied by a tool provider.
///
/// MCP annotations and WASM manifest annotations are untrusted
/// self-descriptions. The host uses them only to narrow or widen capability
/// classification conservatively; it never treats a claim as proof of safety.
/// `None` means the provider did not state a value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ToolAnnotations {
    /// Provider claims the tool does not mutate state.
    pub read_only: Option<bool>,
    /// Provider claims the tool can destroy existing state.
    pub destructive: Option<bool>,
    /// Provider claims the tool can interact with external entities.
    pub open_world: Option<bool>,
}

impl ToolAnnotations {
    /// Returns annotations with no provider claim.
    pub const fn none() -> Self {
        Self {
            read_only: None,
            destructive: None,
            open_world: None,
        }
    }

    /// Returns whether no provider claimed any field.
    pub fn is_empty(self) -> bool {
        self.read_only.is_none() && self.destructive.is_none() && self.open_world.is_none()
    }
}

/// Provider metadata attached to one catalog candidate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolMetadata {
    /// Explicit source version, such as `manifest-v2`.
    pub version: Option<String>,
    /// Provider annotations.
    pub annotations: ToolAnnotations,
}

/// Capability classes assigned by the host.
///
/// Every flag is conservative: it is set when the tool can plausibly exercise
/// the capability, even if another layer currently restricts that action.
/// `parallel_safe` is false for every tool in this phase because the host does
/// not yet schedule tools concurrently.
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

impl Capabilities {
    /// Assigns capabilities from the tool source and native tool name.
    pub fn for_tool(source: ToolSource, name: &str) -> Self {
        match (source, name) {
            (
                ToolSource::Native,
                "read_file" | "read_artifact" | "list_dir" | "grep" | "git_status" | "git_diff",
            ) => Self {
                read_only: true,
                ..Self::none()
            },
            (ToolSource::Native, "write_file" | "apply_patch" | "edit_file") => Self {
                writes_files: true,
                destructive: true,
                ..Self::none()
            },
            (ToolSource::Native, "bash" | "exec") => Self {
                executes: true,
                network: true,
                destructive: true,
                ..Self::none()
            },
            // Wasm runs with read-only preopens, no network, and no write
            // capability. It still executes guest code, but not host commands.
            (ToolSource::Wasm, _) => Self {
                read_only: true,
                ..Self::none()
            },
            // MCP tools run outside the host's sandbox. The host cannot prove
            // they are read-only, so classify them conservatively.
            (ToolSource::Mcp, _) => Self {
                executes: true,
                network: true,
                destructive: true,
                ..Self::none()
            },
            (ToolSource::Delegation, _) => Self {
                writes_files: true,
                executes: true,
                network: true,
                destructive: true,
                ..Self::none()
            },
            // Unknown native tool: do not understate its capabilities.
            (ToolSource::Native, _) => Self {
                writes_files: true,
                executes: true,
                network: true,
                destructive: true,
                ..Self::none()
            },
        }
    }

    /// Returns a capability set with every flag disabled.
    fn none() -> Self {
        Self {
            read_only: false,
            writes_files: false,
            executes: false,
            network: false,
            destructive: false,
            parallel_safe: false,
        }
    }

    /// Assigns capabilities using provider annotations where they exist.
    ///
    /// Native and delegation classification never consult annotations: those
    /// tools are compiled into the host and reviewed here. WASM and MCP
    /// classification is conservative:
    ///
    /// - MCP is read-only only when `readOnlyHint` is true and
    ///   `destructiveHint` is explicitly false; a missing or conflicting
    ///   destructive hint keeps the destructive baseline. A missing
    ///   `openWorldHint` keeps the network baseline.
    /// - WASM keeps the v1 sandbox baseline (read-only, no network) when no
    ///   annotations are present. Declared destructive or open-world behavior
    ///   escalates the descriptor; annotations can never grant more privilege
    ///   than the sandbox already enforces.
    pub fn for_metadata(source: ToolSource, name: &str, annotations: ToolAnnotations) -> Self {
        if annotations.is_empty() {
            return Self::for_tool(source, name);
        }
        match source {
            ToolSource::Native | ToolSource::Delegation => Self::for_tool(source, name),
            ToolSource::Mcp => {
                let read_only =
                    annotations.read_only == Some(true) && annotations.destructive == Some(false);
                if read_only {
                    Self {
                        read_only: true,
                        network: annotations.open_world.unwrap_or(true),
                        ..Self::none()
                    }
                } else {
                    Self {
                        executes: true,
                        network: annotations.open_world.unwrap_or(true),
                        destructive: annotations.destructive.unwrap_or(true),
                        writes_files: annotations.destructive.unwrap_or(true),
                        ..Self::none()
                    }
                }
            }
            ToolSource::Wasm => {
                let destructive = annotations.destructive.unwrap_or(false);
                let network = annotations.open_world.unwrap_or(false);
                let read_only = !destructive && !network;
                Self {
                    read_only,
                    writes_files: !read_only,
                    executes: destructive,
                    network,
                    destructive,
                    parallel_safe: false,
                }
            }
        }
    }
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

/// Advisory argument-field hints for policy rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicyHints {
    /// Argument field naming a filesystem path.
    pub path_field: Option<String>,
    /// Argument field naming a shell command.
    pub command_field: Option<String>,
    /// Paths a policy should treat as protected. Empty until policy v2.
    pub protected_paths: Vec<String>,
}

/// Per-tool execution and result limits.
///
/// A `None` value inherits the engine's tool-execution configuration. Hosts
/// set a per-tool limit only when the tool has a narrower safe bound.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ToolLimits {
    /// Optional wall-clock limit for this tool.
    pub timeout: Option<Duration>,
    /// Optional serialized-argument limit for this tool.
    pub max_args_bytes: Option<usize>,
    /// Maximum inline result bytes entering the model context.
    pub max_inline_bytes: usize,
    /// Maximum artifact bytes, or `None` when artifacts are unsupported.
    pub max_artifact_bytes: Option<usize>,
    /// Maximum output lines, or `None` when not limited.
    pub max_output_lines: Option<usize>,
}

impl ToolLimits {
    /// Returns the effective timeout in whole milliseconds, if configured.
    pub fn timeout_ms(&self) -> Option<u64> {
        self.timeout
            .map(|timeout| u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX))
    }
}

/// Host-side governance metadata for one tool.
///
/// A descriptor is assigned by the host from the tool's source, name, schema,
/// and provider metadata. Tools cannot claim their own capabilities, approval
/// class, or schema digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolDescriptor {
    /// Name exposed to the model.
    pub name: String,
    /// Host-assigned source.
    pub source: ToolSource,
    /// Source or schema version string.
    pub version: String,
    /// Provider-facing schema.
    pub schema: ToolSchema,
    /// `sha256:<64 lowercase hex>` over canonical schema bytes.
    pub schema_digest: String,
    /// Canonical schema byte length.
    pub schema_bytes: usize,
    /// Host-assigned capabilities.
    pub capabilities: Capabilities,
    /// Effective limits.
    pub limits: ToolLimits,
    /// Host approval policy.
    pub approval: ApprovalPolicy,
    /// Advisory policy hints.
    pub policy_hints: PolicyHints,
    /// Shape of the result.
    pub result_format: ResultFormat,
}

impl ToolDescriptor {
    /// Builds a descriptor and computes its canonical schema digest.
    ///
    /// `default_version` labels the source when [`ToolMetadata::version`] does
    /// not name one. The schema is retained so inspection and policy matching
    /// do not need to call the executable tool again.
    pub fn new(
        name: String,
        source: ToolSource,
        schema: ToolSchema,
        default_version: impl Into<String>,
        limits: ToolLimits,
        approval: ApprovalPolicy,
        metadata: &ToolMetadata,
    ) -> Result<Self, serde_json::Error> {
        let (schema_digest, schema_bytes) = digest_schema(&schema)?;
        let version = metadata
            .version
            .clone()
            .unwrap_or_else(|| default_version.into());
        let capabilities = Capabilities::for_metadata(source, &name, metadata.annotations);
        let policy_hints = policy_hints_for(source, &name);
        let result_format = result_format_for(source, &name);
        Ok(Self {
            name,
            source,
            version,
            schema,
            schema_digest,
            schema_bytes,
            capabilities,
            limits,
            approval,
            policy_hints,
            result_format,
        })
    }
}

/// Returns the result shape for a tool source and name.
///
/// `exec` is the one native tool whose result is a JSON document: callers need
/// a machine-readable `exit_code` and stream metadata even though a non-zero
/// exit is a successful call. Other native tools and MCP stay free-form text.
pub fn result_format_for(source: ToolSource, name: &str) -> ResultFormat {
    match (source, name) {
        (ToolSource::Native, "exec") => ResultFormat::Json,
        (ToolSource::Native | ToolSource::Mcp, _) => ResultFormat::Text,
        (ToolSource::Wasm | ToolSource::Delegation, _) => ResultFormat::Json,
    }
}

/// Returns advisory policy hints for a tool source and name.
pub fn policy_hints_for(source: ToolSource, name: &str) -> PolicyHints {
    match (source, name) {
        (ToolSource::Native, "read_file" | "write_file" | "edit_file" | "list_dir" | "grep") => {
            PolicyHints {
                path_field: Some("path".to_owned()),
                ..PolicyHints::default()
            }
        }
        (ToolSource::Native, "apply_patch") => PolicyHints {
            path_field: Some("patch".to_owned()),
            ..PolicyHints::default()
        },
        (ToolSource::Native, "bash") => PolicyHints {
            command_field: Some("command".to_owned()),
            ..PolicyHints::default()
        },
        (ToolSource::Native, "exec") => PolicyHints {
            command_field: Some("program".to_owned()),
            ..PolicyHints::default()
        },
        _ => PolicyHints::default(),
    }
}

/// Computes the canonical schema digest and byte length.
///
/// Object keys are sorted recursively before serialization, so two schemas
/// whose maps were built in different orders produce the same digest. The
/// digest is data, not a signature: it detects changes and supports
/// reproducible catalog output.
pub fn digest_schema(schema: &ToolSchema) -> Result<(String, usize), serde_json::Error> {
    let mut bytes = Vec::new();
    write_canonical(schema, &mut bytes)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let digest = hasher.finalize();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut rendered = String::with_capacity(7 + 64);
    rendered.push_str("sha256:");
    for byte in digest {
        rendered.push(HEX[(byte >> 4) as usize] as char);
        rendered.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok((rendered, bytes.len()))
}

/// Writes compact JSON with object keys sorted at every depth.
fn write_canonical(value: &serde_json::Value, out: &mut Vec<u8>) -> Result<(), serde_json::Error> {
    match value {
        serde_json::Value::Object(map) => {
            out.push(b'{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(&serde_json::to_vec(&serde_json::Value::String(
                    key.clone(),
                ))?);
                out.push(b':');
                if let Some(child) = map.get(key) {
                    write_canonical(child, out)?;
                }
            }
            out.push(b'}');
        }
        serde_json::Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out)?;
            }
            out.push(b']');
        }
        other => out.extend_from_slice(&serde_json::to_vec(other)?),
    }
    Ok(())
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
        ToolDescriptor::new(
            name.to_owned(),
            ToolSource::Native,
            json!({
                "type": "function",
                "function": {"name": name, "parameters": {"type": "object"}}
            }),
            "test-version",
            ToolLimits::default(),
            ApprovalPolicy::NotRequired,
            &ToolMetadata::default(),
        )
        .expect("test schema must serialize")
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

    #[test]
    fn catalog_preserves_descriptor_metadata_and_per_tool_limits() {
        let descriptor = ToolDescriptor::new(
            "apply_patch".to_owned(),
            ToolSource::Native,
            json!({
                "type": "function",
                "function": {"name": "apply_patch", "parameters": {"type": "object"}}
            }),
            "cli-version",
            ToolLimits {
                timeout: Some(Duration::from_millis(2_500)),
                max_args_bytes: Some(8_192),
                max_inline_bytes: 4_096,
                max_artifact_bytes: Some(16_384),
                max_output_lines: Some(200),
            },
            ApprovalPolicy::Interactive,
            &ToolMetadata {
                version: None,
                annotations: ToolAnnotations::none(),
            },
        )
        .expect("schema must serialize");
        let expected_digest = descriptor.schema_digest.clone();
        let mut catalog = ToolCatalog::new();
        catalog
            .insert(ToolEntry {
                tool: Arc::new(MockTool {
                    name: "apply_patch",
                }),
                descriptor,
            })
            .expect("catalog insert");

        let entry = catalog.get("apply_patch").expect("entry");
        assert_eq!(entry.descriptor.version, "cli-version");
        assert_eq!(entry.descriptor.schema_digest, expected_digest);
        assert_eq!(entry.descriptor.limits.timeout_ms(), Some(2_500));
        assert_eq!(entry.descriptor.limits.max_args_bytes, Some(8_192));
        assert_eq!(entry.descriptor.limits.max_inline_bytes, 4_096);
        assert_eq!(entry.descriptor.limits.max_artifact_bytes, Some(16_384));
        assert_eq!(entry.descriptor.limits.max_output_lines, Some(200));
        assert_eq!(
            entry.descriptor.policy_hints.path_field.as_deref(),
            Some("patch")
        );
        assert_eq!(entry.descriptor.approval, ApprovalPolicy::Interactive);
    }

    #[test]
    fn digest_is_stable_and_key_order_independent() {
        let first = json!({
            "type": "function",
            "function": {"name": "sample", "parameters": {"b": 2, "a": 1}}
        });
        let second = json!({
            "function": {"parameters": {"a": 1, "b": 2}, "name": "sample"},
            "type": "function"
        });
        let (first_digest, first_bytes) = digest_schema(&first).expect("schema must serialize");
        let (second_digest, second_bytes) = digest_schema(&second).expect("schema must serialize");
        assert_eq!(first_digest, second_digest);
        assert_eq!(first_bytes, second_bytes);
        assert!(
            first_digest.starts_with("sha256:"),
            "digest must be namespaced: {first_digest}"
        );
        assert_eq!(first_digest.len(), 7 + 64);
        assert!(
            first_digest[7..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "digest must be lowercase hex: {first_digest}"
        );
    }

    #[test]
    fn a_schema_change_changes_the_digest() {
        let first = json!({"type": "function", "function": {"name": "sample"}});
        let second =
            json!({"type": "function", "function": {"name": "sample", "description": "different"}});
        assert_ne!(
            digest_schema(&first).expect("schema must serialize").0,
            digest_schema(&second).expect("schema must serialize").0
        );
    }

    #[test]
    fn capabilities_are_assigned_by_source_not_by_tool_claims() {
        let read = Capabilities::for_tool(ToolSource::Native, "read_file");
        assert!(read.read_only);
        assert!(!read.writes_files && !read.executes && !read.network && !read.destructive);

        let read_artifact = Capabilities::for_tool(ToolSource::Native, "read_artifact");
        assert!(read_artifact.read_only);
        assert!(
            !read_artifact.writes_files
                && !read_artifact.executes
                && !read_artifact.network
                && !read_artifact.destructive
        );

        for name in ["list_dir", "grep", "git_status", "git_diff"] {
            let read_only = Capabilities::for_tool(ToolSource::Native, name);
            assert!(read_only.read_only, "{name} must be read-only");
            assert!(
                !read_only.writes_files
                    && !read_only.executes
                    && !read_only.network
                    && !read_only.destructive,
                "{name} must not carry write, exec, network, or destructive capabilities"
            );
        }

        let exec = Capabilities::for_tool(ToolSource::Native, "exec");
        assert!(exec.executes && exec.network && exec.destructive);
        for name in ["write_file", "apply_patch", "edit_file"] {
            let write = Capabilities::for_tool(ToolSource::Native, name);
            assert!(
                write.writes_files && write.destructive,
                "{name} must be destructive"
            );
            assert!(!write.executes && !write.network);
        }

        let bash = Capabilities::for_tool(ToolSource::Native, "bash");
        assert!(bash.executes && bash.network && bash.destructive);

        let wasm = Capabilities::for_tool(ToolSource::Wasm, "echo");
        assert!(wasm.read_only && !wasm.writes_files && !wasm.network);

        let delegation = Capabilities::for_tool(ToolSource::Delegation, "delegate_task");
        assert!(
            delegation.writes_files
                && delegation.executes
                && delegation.network
                && delegation.destructive
        );
    }

    #[test]
    fn descriptors_carry_stable_source_versions_and_hints() {
        let descriptor = descriptor("apply_patch");
        assert_eq!(descriptor.version, "test-version");
        assert_eq!(descriptor.policy_hints.path_field.as_deref(), Some("patch"));
        assert_eq!(descriptor.result_format, ResultFormat::Text);
        assert!(descriptor.schema_digest.starts_with("sha256:"));
        assert_eq!(descriptor.schema_digest.len(), 71);
        assert_eq!(
            descriptor.schema_bytes,
            serde_json::to_vec(&descriptor.schema)
                .expect("canonical schema must serialize")
                .len()
        );
    }

    #[test]
    fn mcp_annotations_narrow_capabilities_only_when_fully_consistent() {
        let read_only = Capabilities::for_metadata(
            ToolSource::Mcp,
            "mcp__srv__search",
            ToolAnnotations {
                read_only: Some(true),
                destructive: Some(false),
                open_world: Some(false),
            },
        );
        assert!(read_only.read_only);
        assert!(!read_only.destructive && !read_only.network);

        let conflicting = Capabilities::for_metadata(
            ToolSource::Mcp,
            "mcp__srv__search",
            ToolAnnotations {
                read_only: Some(true),
                destructive: Some(true),
                open_world: Some(false),
            },
        );
        assert!(conflicting.destructive && !conflicting.read_only);

        let missing_open_world = Capabilities::for_metadata(
            ToolSource::Mcp,
            "mcp__srv__search",
            ToolAnnotations {
                read_only: Some(true),
                destructive: Some(false),
                open_world: None,
            },
        );
        assert!(
            missing_open_world.network,
            "a missing open-world claim must keep the network baseline"
        );
    }

    #[test]
    fn wasm_annotations_escalate_but_never_widen_the_sandbox() {
        let v1 = Capabilities::for_metadata(ToolSource::Wasm, "echo_json", ToolAnnotations::none());
        assert!(v1.read_only && !v1.writes_files && !v1.network && !v1.destructive);

        let destructive = Capabilities::for_metadata(
            ToolSource::Wasm,
            "echo_json",
            ToolAnnotations {
                read_only: Some(false),
                destructive: Some(true),
                open_world: Some(false),
            },
        );
        assert!(destructive.destructive && destructive.writes_files && !destructive.read_only);

        let open_world = Capabilities::for_metadata(
            ToolSource::Wasm,
            "echo_json",
            ToolAnnotations {
                read_only: Some(false),
                destructive: Some(false),
                open_world: Some(true),
            },
        );
        assert!(open_world.network && !open_world.read_only);
    }

    #[test]
    fn descriptor_version_uses_explicit_metadata_when_present() {
        let descriptor = ToolDescriptor::new(
            "echo_json".to_owned(),
            ToolSource::Wasm,
            json!({"type": "function", "function": {"name": "echo_json"}}),
            "manifest-v1",
            ToolLimits {
                timeout: Some(Duration::from_millis(1_000)),
                max_args_bytes: Some(4_096),
                max_inline_bytes: 1_024,
                max_artifact_bytes: Some(8_192),
                max_output_lines: None,
            },
            ApprovalPolicy::NotRequired,
            &ToolMetadata {
                version: Some("manifest-v2".to_owned()),
                annotations: ToolAnnotations::none(),
            },
        )
        .expect("schema must serialize");

        assert_eq!(descriptor.version, "manifest-v2");
        assert_eq!(descriptor.limits.timeout_ms(), Some(1_000));
        assert_eq!(descriptor.limits.max_args_bytes, Some(4_096));
        assert_eq!(descriptor.result_format, ResultFormat::Json);
    }
}
