//! The per-turn tool catalog.
//!
//! The catalog is the single place that decides which configured tools are
//! exposed. Hidden tools are never registered, so they cannot be sent to the
//! model and a hallucinated call receives `unknown tool`. Filtering is the
//! intersection of three host decisions:
//!
//! 1. the source (native, WASM, MCP, or delegation) is available for this turn;
//! 2. `policy.allowed_tools` contains the name;
//! 3. `tools.profile` contains the source/name pair.
//!
//! Exposure budgets are checked separately from construction so `tools list`
//! can report an over-budget catalog instead of failing to display it.

use std::collections::BTreeMap;
use std::sync::Arc;

use jessica_core::tool::{Tool, ToolSchema};
use thiserror::Error;

use crate::config::{HarnessConfig, ToolProfile};
use crate::governance::permission_required;

use super::descriptor::{
    ApprovalPolicy, Capabilities, ToolDescriptor, ToolLimits, ToolMetadata, ToolSource,
};

/// Error raised while building or querying the catalog.
#[derive(Debug, Error)]
pub(crate) enum CatalogError {
    /// Two configured tools claim the same model-visible name.
    #[error("tool `{name}` is declared more than once in the catalog")]
    Duplicate {
        /// Duplicated tool name.
        name: String,
    },
    /// A tool schema could not be serialized canonically.
    #[error("tool `{name}` schema could not be serialized: {source}")]
    Schema {
        /// Tool whose schema failed.
        name: String,
        /// Serialization error.
        #[source]
        source: serde_json::Error,
    },
    /// The exposed tool count exceeds the configured budget.
    #[error(
        "tool catalog exposes {exposed} tools but tools.max_exposed_tools is {limit}; raise tools.max_exposed_tools or narrow tools.profile, policy.allowed_tools, WASM manifests, or MCP tools"
    )]
    TooManyTools {
        /// Number of exposed tools.
        exposed: usize,
        /// Configured limit.
        limit: usize,
    },
    /// The exposed schema bytes exceed the configured budget.
    #[error(
        "exposed tool schemas total {bytes} bytes but tools.max_tool_schema_bytes is {limit}; raise tools.max_tool_schema_bytes or narrow the exposed tool set"
    )]
    SchemaBytes {
        /// Total canonical schema bytes.
        bytes: usize,
        /// Configured limit.
        limit: usize,
    },
    /// An entry marked exposed has no executable implementation.
    #[error("tool `{name}` is marked exposed but has no executable implementation")]
    MissingImplementation {
        /// Tool name.
        name: String,
    },
    /// The requested tool is not configured at all.
    #[error("tool `{name}` is not present in the configured catalog")]
    UnknownTool {
        /// Requested tool name.
        name: String,
    },
}

/// Why a configured tool is not exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum HiddenReason {
    /// The name is absent from `policy.allowed_tools`.
    NotInPolicy,
    /// The source/name pair is excluded by `tools.profile`.
    NotInProfile,
    /// The native bash tool is disabled by `tools.bash.enabled`.
    BashDisabled,
}

impl HiddenReason {
    /// Returns the stable reason text used in command output.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotInPolicy => "not in policy.allowed_tools",
            Self::NotInProfile => "not in tools.profile",
            Self::BashDisabled => "disabled; set [tools.bash].enabled = true",
        }
    }
}

/// A configured tool before the catalog applies policy and profile filtering.
#[derive(Clone)]
pub(crate) struct CatalogCandidate {
    /// Name exposed to the model.
    pub name: String,
    /// Provider-facing schema.
    pub schema: ToolSchema,
    /// Host-assigned source.
    pub source: ToolSource,
    /// Executable implementation.
    ///
    /// `None` is allowed only for descriptor-only commands such as
    /// `tools list`; a runtime turn fails closed if such an entry is exposed.
    pub tool: Option<Arc<dyn Tool>>,
    /// Provider-supplied version and annotations.
    pub metadata: ToolMetadata,
}

impl CatalogCandidate {
    /// Builds a candidate from an executable tool.
    pub(crate) fn from_tool(tool: Arc<dyn Tool>, source: ToolSource) -> Self {
        let name = tool.name().to_owned();
        let schema = tool.schema();
        Self {
            name,
            schema,
            source,
            tool: Some(tool),
            metadata: ToolMetadata::default(),
        }
    }

    /// Builds a candidate without constructing its executable implementation.
    pub(crate) fn descriptor_only(
        name: impl Into<String>,
        schema: ToolSchema,
        source: ToolSource,
    ) -> Self {
        Self {
            name: name.into(),
            schema,
            source,
            tool: None,
            metadata: ToolMetadata::default(),
        }
    }

    /// Attaches provider metadata to this candidate.
    ///
    /// Metadata is advisory. The catalog still owns the final capability and
    /// approval classification.
    pub(crate) fn with_metadata(mut self, metadata: ToolMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// One catalog entry after filtering.
#[derive(Clone)]
pub(crate) struct CatalogEntry {
    /// Host-side descriptor.
    pub descriptor: ToolDescriptor,
    /// Executable implementation, when one exists.
    pub tool: Option<Arc<dyn Tool>>,
    /// Empty when exposed; otherwise every reason the tool is hidden.
    pub hidden_reasons: Vec<HiddenReason>,
}

impl CatalogEntry {
    /// Returns whether this tool is exposed to the model.
    pub(crate) fn is_exposed(&self) -> bool {
        self.hidden_reasons.is_empty()
    }

    /// Returns a stable comma-separated hidden-reason list.
    pub(crate) fn hidden_reason_text(&self) -> String {
        self.hidden_reasons
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// All configured tools for one turn, sorted by name.
#[derive(Clone)]
pub(crate) struct ToolCatalog {
    entries: Vec<CatalogEntry>,
}

impl ToolCatalog {
    /// Builds a catalog from every available source.
    ///
    /// Duplicate names fail closed before the turn starts. Filtering records
    /// hidden reasons but does not drop entries, so `tools list --all` and
    /// `tools inspect` can explain every configured tool.
    pub(crate) fn build(
        candidates: Vec<CatalogCandidate>,
        config: &HarnessConfig,
    ) -> Result<Self, CatalogError> {
        let mut entries = BTreeMap::new();
        for candidate in candidates {
            if entries.contains_key(&candidate.name) {
                return Err(CatalogError::Duplicate {
                    name: candidate.name,
                });
            }
            let timeout = config
                .tools
                .timeout_for(&candidate.name, config.budget.tool_timeout());
            let limits = ToolLimits {
                timeout: Some(timeout),
                max_args_bytes: config.tools.max_args_bytes(&candidate.name),
                max_inline_bytes: max_inline_bytes(candidate.source, config),
                max_artifact_bytes: Some(config.tools.max_artifact_bytes),
                max_output_lines: max_output_lines(&candidate.name, config),
            };
            let capabilities = Capabilities::for_metadata(
                candidate.source,
                &candidate.name,
                candidate.metadata.annotations,
            );
            // External tools run outside the host's sandbox. A destructive or
            // open-world classification always needs one-time approval when a
            // gate exists, even without an explicit `[approval]` section.
            // Read-only annotated MCP/WASM tools stay on the allowlist path.
            let external_requires_approval =
                matches!(candidate.source, ToolSource::Mcp | ToolSource::Wasm)
                    && (capabilities.destructive || capabilities.network);
            let approval = if permission_required(&candidate.name) || external_requires_approval {
                ApprovalPolicy::Interactive
            } else {
                ApprovalPolicy::NotRequired
            };
            let descriptor = ToolDescriptor::new(
                candidate.name.clone(),
                candidate.source,
                candidate.schema,
                super::descriptor::version_for(candidate.source),
                limits,
                approval,
                &candidate.metadata,
            )
            .map_err(|source| CatalogError::Schema {
                name: candidate.name.clone(),
                source,
            })?;

            let mut hidden_reasons = Vec::new();
            if !config
                .policy
                .allowed_tools
                .iter()
                .any(|name| name == &candidate.name)
            {
                hidden_reasons.push(HiddenReason::NotInPolicy);
            }
            if !profile_allows(config.tools.profile, candidate.source, &candidate.name) {
                hidden_reasons.push(HiddenReason::NotInProfile);
            }
            if candidate.source == ToolSource::Native
                && candidate.name == "bash"
                && !config.tools.bash.enabled
            {
                hidden_reasons.push(HiddenReason::BashDisabled);
            }

            entries.insert(
                candidate.name,
                CatalogEntry {
                    descriptor,
                    tool: candidate.tool,
                    hidden_reasons,
                },
            );
        }
        Ok(Self {
            entries: entries.into_values().collect(),
        })
    }

    /// Returns every configured entry, including hidden tools.
    pub(crate) fn entries(&self) -> &[CatalogEntry] {
        &self.entries
    }

    /// Returns the entry with `name`, exposed or hidden.
    pub(crate) fn find(&self, name: &str) -> Option<&CatalogEntry> {
        self.entries
            .iter()
            .find(|entry| entry.descriptor.name == name)
    }

    /// Returns only exposed entries, in sorted order.
    pub(crate) fn exposed(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries.iter().filter(|entry| entry.is_exposed())
    }

    /// Returns the number of exposed tools.
    pub(crate) fn exposed_count(&self) -> usize {
        self.exposed().count()
    }

    /// Returns the total canonical schema bytes of exposed tools.
    pub(crate) fn exposed_schema_bytes(&self) -> usize {
        self.exposed()
            .map(|entry| entry.descriptor.schema_bytes)
            .sum()
    }

    /// Returns the sorted names of exposed tools.
    #[cfg(test)]
    pub(crate) fn exposed_names(&self) -> Vec<String> {
        self.exposed()
            .map(|entry| entry.descriptor.name.clone())
            .collect()
    }

    /// Enforces the configured exposure budgets.
    ///
    /// Turn startup calls this before constructing the provider request. The
    /// `tools list` command reports the same error as a warning so an
    /// over-budget catalog can still be inspected.
    pub(crate) fn enforce_budget(&self, config: &HarnessConfig) -> Result<(), CatalogError> {
        let exposed = self.exposed_count();
        if exposed > config.tools.max_exposed_tools {
            return Err(CatalogError::TooManyTools {
                exposed,
                limit: config.tools.max_exposed_tools,
            });
        }
        let bytes = self.exposed_schema_bytes();
        if bytes > config.tools.max_tool_schema_bytes {
            return Err(CatalogError::SchemaBytes {
                bytes,
                limit: config.tools.max_tool_schema_bytes,
            });
        }
        Ok(())
    }
}

/// Returns whether a profile exposes a source/name pair.
///
/// Third-party tools and delegation are exposed only by `full`; the narrower
/// profiles name the native tools they include. Native tools added in later
/// phases must be added here explicitly so a new capability is never exposed by
/// accident under a narrow profile.
fn profile_allows(profile: ToolProfile, source: ToolSource, name: &str) -> bool {
    if source != ToolSource::Native {
        return profile == ToolProfile::Full;
    }
    match profile {
        ToolProfile::Full => true,
        ToolProfile::Readonly => is_read_only_native(name),
        ToolProfile::Coding => {
            is_read_only_native(name)
                || matches!(name, "write_file" | "apply_patch" | "edit_file" | "exec")
        }
        ToolProfile::Ops => is_read_only_native(name) || matches!(name, "bash" | "exec"),
    }
}

/// Returns whether a native tool has no host side effects.
///
/// The list is explicit so a newly added native tool is never exposed by a
/// narrow profile until its capabilities have been reviewed here.
fn is_read_only_native(name: &str) -> bool {
    matches!(
        name,
        "read_file" | "read_artifact" | "list_dir" | "grep" | "git_status" | "git_diff"
    )
}

/// Returns the descriptor's output-line cap for tools that bound rendered lines.
fn max_output_lines(name: &str, config: &HarnessConfig) -> Option<usize> {
    match name {
        "read_file" => {
            Some(usize::try_from(jessica_tools::MAX_READ_FILE_LINES).unwrap_or(usize::MAX))
        }
        "list_dir" => Some(config.tools.list_dir.max_entries),
        "grep" => Some(config.tools.grep.max_matches),
        _ => None,
    }
}

fn max_inline_bytes(source: ToolSource, config: &HarnessConfig) -> usize {
    let configured = config
        .tools
        .effective_max_inline_bytes(config.budget.max_result_bytes);
    if source == ToolSource::Delegation {
        configured.min(config.delegation.max_result_bytes)
    } else {
        configured
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use jessica_core::tool::ToolSchema;
    use jessica_core::types::ToolResult;
    use serde_json::{Value, json};

    use super::*;
    use crate::config::HarnessConfig;

    struct StubTool {
        name: String,
        schema: ToolSchema,
    }

    impl StubTool {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_owned(),
                schema: json!({
                    "type": "function",
                    "function": {"name": name, "parameters": {"type": "object"}}
                }),
            }
        }
    }

    #[async_trait]
    impl Tool for StubTool {
        fn name(&self) -> &str {
            &self.name
        }

        fn schema(&self) -> ToolSchema {
            self.schema.clone()
        }

        async fn execute(&self, _args: Value) -> ToolResult {
            ToolResult::success(String::new(), "ok")
        }
    }

    fn candidate(name: &str, source: ToolSource) -> CatalogCandidate {
        CatalogCandidate::from_tool(Arc::new(StubTool::new(name)), source)
    }

    fn parse_config(toml: &str) -> HarnessConfig {
        HarnessConfig::from_toml_str(toml).expect("test configuration must parse")
    }

    #[test]
    fn full_profile_exposes_allowlisted_sources_in_sorted_order() {
        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\", \"bash\", \"mcp__s__tool\"]\n\
             [tools]\nprofile = \"full\"\n\
             [tools.bash]\nenabled = true\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                candidate("bash", ToolSource::Native),
                candidate("read_file", ToolSource::Native),
                candidate("mcp__s__tool", ToolSource::Mcp),
            ],
            &config,
        )
        .expect("catalog must build");
        assert_eq!(
            catalog.exposed_names(),
            ["bash", "mcp__s__tool", "read_file"]
        );
        assert_eq!(catalog.exposed_count(), 3);
        assert!(catalog.entries().iter().all(CatalogEntry::is_exposed));
    }

    #[test]
    fn hidden_tools_report_every_reason_and_are_not_exposed() {
        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\", \"bash\"]\n\
             [tools]\nprofile = \"readonly\"\n\
             [tools.bash]\nenabled = true\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                candidate("read_file", ToolSource::Native),
                candidate("bash", ToolSource::Native),
                candidate("write_file", ToolSource::Native),
            ],
            &config,
        )
        .expect("catalog must build");

        assert_eq!(catalog.exposed_names(), ["read_file"]);
        let bash = catalog.find("bash").expect("bash must be configured");
        assert_eq!(
            bash.hidden_reasons,
            vec![HiddenReason::NotInProfile],
            "bash is allowed but excluded by the readonly profile"
        );
        let write = catalog
            .find("write_file")
            .expect("write must be configured");
        assert_eq!(
            write.hidden_reasons,
            vec![HiddenReason::NotInPolicy, HiddenReason::NotInProfile],
            "the reason order must be stable"
        );
        assert_eq!(
            write.hidden_reason_text(),
            "not in policy.allowed_tools, not in tools.profile"
        );
    }

    #[test]
    fn disabled_bash_is_hidden_even_when_policy_and_profile_allow_it() {
        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\", \"bash\"]\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                candidate("read_file", ToolSource::Native),
                candidate("bash", ToolSource::Native),
            ],
            &config,
        )
        .expect("catalog must build");

        assert_eq!(catalog.exposed_names(), ["read_file"]);
        let bash = catalog.find("bash").expect("bash must stay inspectable");
        assert_eq!(
            bash.hidden_reasons,
            vec![HiddenReason::BashDisabled],
            "the enable gate must hide bash even when policy allows it"
        );
        assert_eq!(
            bash.hidden_reason_text(),
            "disabled; set [tools.bash].enabled = true"
        );

        let enabled = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"bash\"]\n\
             [tools.bash]\nenabled = true\n",
        );
        let catalog = ToolCatalog::build(vec![candidate("bash", ToolSource::Native)], &enabled)
            .expect("catalog must build");
        assert_eq!(catalog.exposed_names(), ["bash"]);
    }

    #[test]
    fn narrow_profiles_never_expose_wasm_mcp_or_delegation() {
        for profile in ["readonly", "coding", "ops"] {
            let config = parse_config(&format!(
                "[profile]\ntrace = \"off\"\n\
                 [policy]\nallowed_tools = [\"read_file\", \"write_file\", \"bash\", \"echo\", \"mcp__s__tool\", \"delegate_task\"]\n\
                 [tools]\nprofile = \"{profile}\"\n"
            ));
            let catalog = ToolCatalog::build(
                vec![
                    candidate("read_file", ToolSource::Native),
                    candidate("write_file", ToolSource::Native),
                    candidate("bash", ToolSource::Native),
                    candidate("echo", ToolSource::Wasm),
                    candidate("mcp__s__tool", ToolSource::Mcp),
                    candidate("delegate_task", ToolSource::Delegation),
                ],
                &config,
            )
            .expect("catalog must build");
            assert!(
                catalog
                    .exposed()
                    .all(|entry| entry.descriptor.source == ToolSource::Native),
                "{profile} must not expose third-party tools"
            );
        }
    }

    #[test]
    fn readonly_profile_exposes_read_only_native_tools_but_never_exec() {
        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\", \"read_artifact\", \"list_dir\", \"grep\", \"git_status\", \"git_diff\", \"exec\", \"bash\"]\n\
             [tools]\nprofile = \"readonly\"\n\
             [tools.bash]\nenabled = true\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                candidate("read_file", ToolSource::Native),
                candidate("read_artifact", ToolSource::Native),
                candidate("list_dir", ToolSource::Native),
                candidate("grep", ToolSource::Native),
                candidate("git_status", ToolSource::Native),
                candidate("git_diff", ToolSource::Native),
                candidate("exec", ToolSource::Native),
                candidate("bash", ToolSource::Native),
            ],
            &config,
        )
        .expect("catalog must build");

        assert_eq!(
            catalog.exposed_names(),
            [
                "git_diff",
                "git_status",
                "grep",
                "list_dir",
                "read_artifact",
                "read_file"
            ]
        );
        assert_eq!(
            catalog
                .find("exec")
                .expect("exec must be configured")
                .hidden_reasons,
            vec![HiddenReason::NotInProfile],
            "readonly must hide the command tool"
        );
    }

    #[test]
    fn exec_is_exposed_only_by_command_profiles_and_still_needs_the_allowlist() {
        for (profile, expected) in [
            ("readonly", Vec::new()),
            ("coding", vec!["exec"]),
            ("ops", vec!["exec"]),
            ("full", vec!["exec"]),
        ] {
            let config = parse_config(&format!(
                "[profile]\ntrace = \"off\"\n\
                 [policy]\nallowed_tools = [\"exec\", \"bash\"]\n\
                 [tools]\nprofile = \"{profile}\"\n\
                 [tools.bash]\nenabled = false\n"
            ));
            let catalog = ToolCatalog::build(
                vec![
                    candidate("exec", ToolSource::Native),
                    candidate("bash", ToolSource::Native),
                ],
                &config,
            )
            .expect("catalog must build");
            assert_eq!(catalog.exposed_names(), expected, "profile {profile}");

            let not_allowlisted = parse_config(&format!(
                "[profile]\ntrace = \"off\"\n\
                 [policy]\nallowed_tools = [\"read_file\"]\n\
                 [tools]\nprofile = \"{profile}\"\n"
            ));
            let catalog = ToolCatalog::build(
                vec![candidate("exec", ToolSource::Native)],
                &not_allowlisted,
            )
            .expect("catalog must build");
            assert!(
                catalog
                    .find("exec")
                    .expect("exec must exist")
                    .hidden_reasons
                    .contains(&HiddenReason::NotInPolicy),
                "{profile} must still require an allowlist entry for exec"
            );
        }
    }

    #[test]
    fn new_native_tools_are_hidden_unless_the_allowlist_names_them() {
        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\"]\n\
             [tools]\nprofile = \"readonly\"\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                candidate("read_file", ToolSource::Native),
                candidate("list_dir", ToolSource::Native),
                candidate("grep", ToolSource::Native),
                candidate("git_status", ToolSource::Native),
                candidate("git_diff", ToolSource::Native),
            ],
            &config,
        )
        .expect("catalog must build");

        assert_eq!(catalog.exposed_names(), ["read_file"]);
        for name in ["list_dir", "grep", "git_status", "git_diff"] {
            assert_eq!(
                catalog
                    .find(name)
                    .expect("tool must be configured")
                    .hidden_reasons,
                vec![HiddenReason::NotInPolicy],
                "`{name}` must be hidden without an allowlist entry"
            );
        }
    }

    #[test]
    fn output_line_caps_report_the_configured_or_schema_limits() {
        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\", \"list_dir\", \"grep\", \"exec\"]\n\
             [tools]\nprofile = \"readonly\"\n\
             [tools.list_dir]\nmax_entries = 7\n\
             [tools.grep]\nmax_matches = 9\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                candidate("read_file", ToolSource::Native),
                candidate("list_dir", ToolSource::Native),
                candidate("grep", ToolSource::Native),
                candidate("exec", ToolSource::Native),
            ],
            &config,
        )
        .expect("catalog must build");

        assert_eq!(
            catalog
                .find("read_file")
                .expect("read_file must exist")
                .descriptor
                .limits
                .max_output_lines,
            Some(10_000)
        );
        assert_eq!(
            catalog
                .find("list_dir")
                .expect("list_dir must exist")
                .descriptor
                .limits
                .max_output_lines,
            Some(7)
        );
        assert_eq!(
            catalog
                .find("grep")
                .expect("grep must exist")
                .descriptor
                .limits
                .max_output_lines,
            Some(9)
        );
        assert_eq!(
            catalog
                .find("exec")
                .expect("exec must exist")
                .descriptor
                .limits
                .max_output_lines,
            None,
            "exec output is byte-capped, not line-capped"
        );
    }

    #[test]
    fn duplicate_names_fail_before_registration() {
        let config = parse_config("[profile]\ntrace = \"off\"\n");
        let error = ToolCatalog::build(
            vec![
                candidate("read_file", ToolSource::Native),
                candidate("read_file", ToolSource::Wasm),
            ],
            &config,
        )
        .err()
        .expect("duplicates must be rejected");
        assert!(
            matches!(&error, CatalogError::Duplicate { name } if name == "read_file"),
            "duplicate error must name the tool: {error}"
        );
    }

    #[test]
    fn exposure_budgets_count_only_exposed_tools_and_schemas() {
        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\", \"bash\"]\n\
             [tools]\nprofile = \"readonly\"\nmax_exposed_tools = 1\nmax_tool_schema_bytes = 100000\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                candidate("read_file", ToolSource::Native),
                candidate("bash", ToolSource::Native),
            ],
            &config,
        )
        .expect("catalog must build");
        assert!(
            catalog.enforce_budget(&config).is_ok(),
            "hidden bash must not consume the tool budget"
        );

        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\", \"bash\"]\n\
             [tools]\nprofile = \"full\"\nmax_exposed_tools = 1\n\
             [tools.bash]\nenabled = true\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                candidate("read_file", ToolSource::Native),
                candidate("bash", ToolSource::Native),
            ],
            &config,
        )
        .expect("catalog must build");
        assert!(matches!(
            catalog.enforce_budget(&config),
            Err(CatalogError::TooManyTools {
                exposed: 2,
                limit: 1
            })
        ));

        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\"]\n\
             [tools]\nmax_tool_schema_bytes = 1\n",
        );
        let catalog = ToolCatalog::build(vec![candidate("read_file", ToolSource::Native)], &config)
            .expect("catalog must build");
        assert!(matches!(
            catalog.enforce_budget(&config),
            Err(CatalogError::SchemaBytes { limit: 1, .. })
        ));
    }

    #[test]
    fn per_tool_limits_and_approval_come_from_configuration_and_host_policy() {
        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [budget]\ntool_timeout_ms = 20000\n\
             [policy]\nallowed_tools = [\"read_file\", \"bash\", \"delegate_task\"]\n\
             [delegation]\nenabled = true\n\
             [tools.read_file]\ntimeout_ms = 500\nmax_args_bytes = 2048\n\
             [tools.bash]\nenabled = true\ntimeout_ms = 3000\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                candidate("read_file", ToolSource::Native),
                candidate("bash", ToolSource::Native),
                CatalogCandidate::descriptor_only(
                    "delegate_task",
                    json!({"type": "function", "function": {"name": "delegate_task"}}),
                    ToolSource::Delegation,
                ),
            ],
            &config,
        )
        .expect("catalog must build");

        let read = catalog.find("read_file").expect("read must exist");
        assert_eq!(read.descriptor.limits.timeout_ms(), Some(500));
        assert_eq!(read.descriptor.limits.max_args_bytes, Some(2048));
        assert_eq!(read.descriptor.approval, ApprovalPolicy::NotRequired);
        assert_eq!(read.descriptor.limits.max_inline_bytes, 64 * 1024);

        let bash = catalog.find("bash").expect("bash must exist");
        assert_eq!(bash.descriptor.limits.timeout_ms(), Some(3000));
        assert_eq!(bash.descriptor.approval, ApprovalPolicy::Interactive);

        let delegate = catalog.find("delegate_task").expect("delegate must exist");
        assert_eq!(
            delegate.descriptor.limits.max_inline_bytes,
            32 * 1024,
            "delegation tightens the global inline result cap"
        );
        assert_eq!(
            delegate.descriptor.result_format,
            super::super::descriptor::ResultFormat::Json
        );
    }

    #[test]
    fn digest_and_schema_bytes_are_reproducible_across_builds() {
        let config = parse_config("[profile]\ntrace = \"off\"\n");
        let first = ToolCatalog::build(vec![candidate("read_file", ToolSource::Native)], &config)
            .expect("catalog must build");
        let second = ToolCatalog::build(vec![candidate("read_file", ToolSource::Native)], &config)
            .expect("catalog must build");
        let first = &first.entries()[0].descriptor;
        let second = &second.entries()[0].descriptor;
        assert_eq!(first.schema_digest, second.schema_digest);
        assert_eq!(first.schema_bytes, second.schema_bytes);
        assert_eq!(first.schema_digest.len(), 71);
    }

    fn annotated(
        name: &str,
        source: ToolSource,
        annotations: super::super::descriptor::ToolAnnotations,
    ) -> CatalogCandidate {
        candidate(name, source).with_metadata(super::super::descriptor::ToolMetadata {
            version: None,
            annotations,
        })
    }

    fn wasm_v2(
        name: &str,
        annotations: super::super::descriptor::ToolAnnotations,
    ) -> CatalogCandidate {
        annotated(name, ToolSource::Wasm, annotations).with_metadata(
            super::super::descriptor::ToolMetadata {
                version: Some("manifest-v2".to_owned()),
                annotations,
            },
        )
    }

    #[test]
    fn mcp_annotations_drive_capabilities_version_and_approval() {
        use super::super::descriptor::ToolAnnotations;

        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"mcp__srv__search\", \"mcp__srv__wipe\", \"mcp__srv__raw\"]\n\
             [tools]\nprofile = \"full\"\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                annotated(
                    "mcp__srv__search",
                    ToolSource::Mcp,
                    ToolAnnotations {
                        read_only: Some(true),
                        destructive: Some(false),
                        open_world: Some(false),
                    },
                ),
                annotated(
                    "mcp__srv__wipe",
                    ToolSource::Mcp,
                    ToolAnnotations {
                        read_only: Some(false),
                        destructive: Some(true),
                        open_world: Some(false),
                    },
                ),
                candidate("mcp__srv__raw", ToolSource::Mcp),
            ],
            &config,
        )
        .expect("catalog must build");

        let search = catalog
            .find("mcp__srv__search")
            .expect("search must exist")
            .descriptor
            .clone();
        assert!(search.capabilities.read_only);
        assert!(!search.capabilities.destructive && !search.capabilities.network);
        assert_eq!(search.approval, ApprovalPolicy::NotRequired);
        assert_eq!(search.version, "mcp-2025-06-18");

        let wipe = catalog.find("mcp__srv__wipe").expect("wipe must exist");
        assert!(
            wipe.descriptor.capabilities.destructive,
            "destructiveHint must be honored"
        );
        assert_eq!(
            wipe.descriptor.approval,
            ApprovalPolicy::Interactive,
            "destructive MCP tools must always require one-time approval"
        );

        let raw = catalog.find("mcp__srv__raw").expect("raw must exist");
        assert!(
            raw.descriptor.capabilities.destructive
                && raw.descriptor.capabilities.network
                && !raw.descriptor.capabilities.read_only,
            "unannotated MCP tools must keep the conservative baseline"
        );
        assert_eq!(raw.descriptor.approval, ApprovalPolicy::Interactive);
    }

    #[test]
    fn wasm_annotations_and_manifest_version_are_visible() {
        use super::super::descriptor::ToolAnnotations;

        let config = parse_config(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"echo_json\", \"wipe_json\"]\n\
             [tools]\nprofile = \"full\"\n",
        );
        let catalog = ToolCatalog::build(
            vec![
                wasm_v2(
                    "echo_json",
                    ToolAnnotations {
                        read_only: Some(true),
                        destructive: Some(false),
                        open_world: Some(false),
                    },
                ),
                wasm_v2(
                    "wipe_json",
                    ToolAnnotations {
                        read_only: Some(false),
                        destructive: Some(true),
                        open_world: Some(false),
                    },
                ),
            ],
            &config,
        )
        .expect("catalog must build");

        let echo = catalog.find("echo_json").expect("echo must exist");
        assert!(echo.descriptor.capabilities.read_only);
        assert_eq!(echo.descriptor.version, "manifest-v2");
        assert_eq!(echo.descriptor.approval, ApprovalPolicy::NotRequired);

        let wipe = catalog.find("wipe_json").expect("wipe must exist");
        assert!(wipe.descriptor.capabilities.destructive);
        assert_eq!(
            wipe.descriptor.approval,
            ApprovalPolicy::Interactive,
            "a destructive wasm declaration must require approval"
        );
    }

    #[test]
    fn descriptor_only_candidates_keep_the_default_metadata() {
        let candidate = CatalogCandidate::descriptor_only(
            "delegate_task",
            json!({"type": "function", "function": {"name": "delegate_task"}}),
            ToolSource::Delegation,
        );
        assert!(candidate.metadata.version.is_none());
        assert!(candidate.metadata.annotations.is_empty());
    }
}
