//! Host-side descriptors for tools in the catalog.
//!
//! A descriptor is governance metadata owned by the host. Tools do not report
//! their own capabilities, approval policy, or limits: those are assigned here
//! from the tool source, the tool name, and the resolved configuration. The
//! descriptor never enters `jessica-core`.

use jessica_core::tool::ToolSchema;
use sha2::{Digest, Sha256};

/// Where a catalog tool came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ToolSource {
    /// A tool compiled into `jessica-tools`.
    Native,
    /// A sandboxed WebAssembly plugin declared by a manifest.
    Wasm,
    /// A tool discovered from a session-scoped MCP server.
    Mcp,
    /// The parent-side `delegate_task` tool.
    Delegation,
}

impl ToolSource {
    /// Returns the stable lowercase name used in command output.
    pub(crate) fn as_str(self) -> &'static str {
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
/// MCP `annotations` and WASM manifest v2 annotations are untrusted
/// self-descriptions. The host uses them only to narrow or widen capability
/// classification conservatively; it never treats a claim as proof of safety.
/// `None` means the provider did not state a value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ToolAnnotations {
    /// Provider claims the tool does not mutate state.
    pub read_only: Option<bool>,
    /// Provider claims the tool can destroy existing state.
    pub destructive: Option<bool>,
    /// Provider claims the tool can interact with external entities.
    pub open_world: Option<bool>,
}

impl ToolAnnotations {
    /// Returns annotations with no provider claim.
    pub(crate) const fn none() -> Self {
        Self {
            read_only: None,
            destructive: None,
            open_world: None,
        }
    }

    /// Returns whether no provider claimed any field.
    pub(crate) fn is_empty(self) -> bool {
        self.read_only.is_none() && self.destructive.is_none() && self.open_world.is_none()
    }
}

/// Provider metadata attached to one catalog candidate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ToolMetadata {
    /// Explicit source version, such as `manifest-v2`.
    pub version: Option<String>,
    /// Provider annotations.
    pub annotations: ToolAnnotations,
}

/// Capability classes assigned by the host.
///
/// Every flag is conservative: a capability is set when the tool can plausibly
/// exercise it, even if the current sandbox restricts it. `parallel_safe` is
/// false for every tool in this phase because the host does not yet schedule
/// tools concurrently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Capabilities {
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
    pub(crate) fn for_tool(source: ToolSource, name: &str) -> Self {
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
    pub(crate) fn for_metadata(
        source: ToolSource,
        name: &str,
        annotations: ToolAnnotations,
    ) -> Self {
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

/// Limits reported for one tool.
///
/// `timeout_ms` and `max_args_bytes` are enforced by the CLI governance
/// wrapper. `max_inline_bytes` is the effective result-manager cap; the
/// configured cap is additionally bounded by `budget.max_result_bytes`.
/// `max_artifact_bytes` reports the host artifact limit. Artifact storage is
/// shared by all tools, so `read_artifact` reports the same limits even though
/// its own output never creates another artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolLimits {
    /// Effective wall-clock timeout in milliseconds.
    pub timeout_ms: u64,
    /// Maximum serialized argument bytes, or `None` when unlimited.
    pub max_args_bytes: Option<usize>,
    /// Maximum inline result bytes entering the model context.
    pub max_inline_bytes: usize,
    /// Maximum artifact bytes, or `None` when artifacts are unsupported.
    pub max_artifact_bytes: Option<usize>,
    /// Maximum output lines, or `None` when not limited.
    pub max_output_lines: Option<usize>,
}

/// Host approval policy for a tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApprovalPolicy {
    /// The policy check is the only pre-execution gate.
    NotRequired,
    /// The host asks its client for one-time approval when a permission gate is
    /// installed (currently ACP). CLI turns have no gate.
    Interactive,
}

impl ApprovalPolicy {
    /// Returns the stable lowercase name used in command output.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotRequired => "not_required",
            Self::Interactive => "interactive",
        }
    }
}

/// Advisory argument-field hints for future policy rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PolicyHints {
    /// Argument field naming a filesystem path.
    pub path_field: Option<String>,
    /// Argument field naming a shell command.
    pub command_field: Option<String>,
    /// Paths a policy should treat as protected. Empty until policy v2.
    pub protected_paths: Vec<String>,
}

/// Shape of a tool result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultFormat {
    /// Free-form text.
    Text,
    /// A JSON document.
    Json,
}

impl ResultFormat {
    /// Returns the stable lowercase name used in command output.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

/// Complete host-side descriptor for one catalog tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolDescriptor {
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
    /// Result shape.
    pub result_format: ResultFormat,
}

impl ToolDescriptor {
    /// Builds a descriptor and computes its canonical schema digest.
    pub(crate) fn new(
        name: String,
        source: ToolSource,
        schema: ToolSchema,
        limits: ToolLimits,
        approval: ApprovalPolicy,
        metadata: &ToolMetadata,
    ) -> Result<Self, serde_json::Error> {
        let (schema_digest, schema_bytes) = digest_schema(&schema)?;
        Ok(Self {
            name: name.clone(),
            source,
            version: metadata
                .version
                .clone()
                .unwrap_or_else(|| version_for(source).to_owned()),
            schema,
            schema_digest,
            schema_bytes,
            capabilities: Capabilities::for_metadata(source, &name, metadata.annotations),
            limits,
            approval,
            policy_hints: policy_hints_for(source, &name),
            result_format: result_format_for(source, &name),
        })
    }
}

/// Returns the version label for a tool source.
pub(crate) fn version_for(source: ToolSource) -> &'static str {
    match source {
        ToolSource::Native | ToolSource::Delegation => env!("CARGO_PKG_VERSION"),
        ToolSource::Wasm => "manifest-v1",
        ToolSource::Mcp => "mcp-2025-06-18",
    }
}

/// Returns the result shape for a tool source and name.
///
/// `exec` is the one native tool whose result is a JSON document: callers need
/// a machine-readable `exit_code` and stream metadata even though a non-zero
/// exit is a successful call. Other native tools and MCP stay free-form text.
pub(crate) fn result_format_for(source: ToolSource, name: &str) -> ResultFormat {
    match (source, name) {
        (ToolSource::Native, "exec") => ResultFormat::Json,
        (ToolSource::Native | ToolSource::Mcp, _) => ResultFormat::Text,
        (ToolSource::Wasm | ToolSource::Delegation, _) => ResultFormat::Json,
    }
}

fn policy_hints_for(source: ToolSource, name: &str) -> PolicyHints {
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
pub(crate) fn digest_schema(schema: &ToolSchema) -> Result<(String, usize), serde_json::Error> {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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
                "{name} must not claim side effects"
            );
        }

        let exec = Capabilities::for_tool(ToolSource::Native, "exec");
        assert!(exec.executes && exec.network && exec.destructive);
        assert!(!exec.read_only && !exec.writes_files);

        for name in ["write_file", "apply_patch", "edit_file"] {
            let write = Capabilities::for_tool(ToolSource::Native, name);
            assert!(
                write.writes_files && write.destructive,
                "{name} must be a destructive writer"
            );
            assert!(!write.read_only && !write.executes);
        }
        let write = Capabilities::for_tool(ToolSource::Native, "write_file");

        let bash = Capabilities::for_tool(ToolSource::Native, "bash");
        assert!(bash.executes && bash.network && bash.destructive);

        let wasm = Capabilities::for_tool(ToolSource::Wasm, "echo");
        assert!(wasm.read_only);
        assert!(!wasm.writes_files && !wasm.network && !wasm.destructive);

        let delegation = Capabilities::for_tool(
            ToolSource::Delegation,
            jessica_runtime::DELEGATE_TASK_TOOL_NAME,
        );
        assert!(
            delegation.destructive && delegation.writes_files && delegation.executes,
            "delegation runs a child agent and must be treated as destructive"
        );
        assert!(!delegation.read_only);

        for capabilities in [read, read_artifact, exec, write, bash, wasm, delegation] {
            assert!(
                !capabilities.parallel_safe,
                "no tool is parallel-safe in this phase"
            );
        }
    }

    #[test]
    fn descriptors_carry_stable_source_versions_and_hints() {
        let schema = json!({"type": "function", "function": {"name": "bash"}});
        let limits = ToolLimits {
            timeout_ms: 10_000,
            max_args_bytes: None,
            max_inline_bytes: 65_536,
            max_artifact_bytes: None,
            max_output_lines: None,
        };
        let descriptor = ToolDescriptor::new(
            "bash".to_owned(),
            ToolSource::Native,
            schema,
            limits,
            ApprovalPolicy::Interactive,
            &ToolMetadata::default(),
        )
        .expect("descriptor must build");
        assert_eq!(descriptor.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            descriptor.policy_hints.command_field.as_deref(),
            Some("command")
        );
        assert_eq!(descriptor.result_format, ResultFormat::Text);
        assert_eq!(descriptor.approval, ApprovalPolicy::Interactive);

        assert_eq!(
            result_format_for(ToolSource::Native, "exec"),
            ResultFormat::Json,
            "exec results carry structured exit metadata"
        );
        assert_eq!(
            result_format_for(ToolSource::Native, "grep"),
            ResultFormat::Text
        );
        assert_eq!(
            policy_hints_for(ToolSource::Native, "grep")
                .path_field
                .as_deref(),
            Some("path")
        );
        assert_eq!(
            policy_hints_for(ToolSource::Native, "exec")
                .command_field
                .as_deref(),
            Some("program")
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
        assert!(!read_only.destructive && !read_only.network && !read_only.writes_files);

        let open_world = Capabilities::for_metadata(
            ToolSource::Mcp,
            "mcp__srv__search",
            ToolAnnotations {
                read_only: Some(true),
                destructive: Some(false),
                open_world: Some(true),
            },
        );
        assert!(
            open_world.read_only && open_world.network && !open_world.destructive,
            "a read-only tool may still reach the open world: {open_world:?}"
        );

        for annotations in [
            ToolAnnotations::none(),
            ToolAnnotations {
                read_only: Some(true),
                destructive: None,
                open_world: Some(false),
            },
            ToolAnnotations {
                read_only: Some(true),
                destructive: Some(true),
                open_world: Some(false),
            },
            ToolAnnotations {
                read_only: Some(false),
                destructive: Some(false),
                open_world: Some(false),
            },
        ] {
            let capabilities =
                Capabilities::for_metadata(ToolSource::Mcp, "mcp__srv__tool", annotations);
            assert!(
                !capabilities.read_only,
                "missing or conflicting hints must not be treated as read-only: {annotations:?}"
            );
        }

        let missing_open_world = Capabilities::for_metadata(
            ToolSource::Mcp,
            "mcp__srv__tool",
            ToolAnnotations {
                read_only: Some(true),
                destructive: Some(false),
                open_world: None,
            },
        );
        assert!(
            missing_open_world.network,
            "a missing open-world hint must keep the conservative network classification"
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
        assert!(
            destructive.destructive && destructive.writes_files && !destructive.read_only,
            "declared destructive wasm must be classified conservatively: {destructive:?}"
        );

        let open_world = Capabilities::for_metadata(
            ToolSource::Wasm,
            "echo_json",
            ToolAnnotations {
                read_only: Some(true),
                destructive: Some(false),
                open_world: Some(true),
            },
        );
        assert!(
            open_world.network && !open_world.destructive,
            "declared open-world wasm must expose the network capability: {open_world:?}"
        );

        let native = Capabilities::for_metadata(
            ToolSource::Native,
            "read_file",
            ToolAnnotations {
                read_only: Some(false),
                destructive: Some(true),
                open_world: Some(true),
            },
        );
        assert!(
            native.read_only && !native.destructive,
            "native classification must ignore provider annotations"
        );
    }

    #[test]
    fn descriptor_version_uses_explicit_metadata_when_present() {
        let descriptor = ToolDescriptor::new(
            "echo_json".to_owned(),
            ToolSource::Wasm,
            json!({"type": "function", "function": {"name": "echo_json"}}),
            ToolLimits {
                timeout_ms: 1_000,
                max_args_bytes: None,
                max_inline_bytes: 1_024,
                max_artifact_bytes: Some(4_096),
                max_output_lines: None,
            },
            ApprovalPolicy::NotRequired,
            &ToolMetadata {
                version: Some("manifest-v2".to_owned()),
                annotations: ToolAnnotations::none(),
            },
        )
        .expect("descriptor must build");
        assert_eq!(descriptor.version, "manifest-v2");
    }
}
