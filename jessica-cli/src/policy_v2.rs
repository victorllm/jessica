//! Host-side policy v2 evaluation.
//!
//! The frozen `jessica-core` policy trait can only express allow or deny, while
//! policy v2 needs a three-way decision (`deny`, `ask`, `allow`). This module
//! therefore evaluates rules in the CLI before the harness allowlist policy and
//! returns a host-only verdict. It never enters `jessica-core`.
//!
//! Evaluation order is fixed:
//!
//! 1. every matching `deny` rule, in configuration order;
//! 2. protected paths for the editor tools, unless an `allow` rule matched;
//! 3. every matching `ask` rule, in configuration order;
//! 4. every matching `allow` rule, in configuration order;
//! 5. the approval mode for sensitive tools;
//! 6. otherwise the existing allowlist decision stands.
//!
//! Paths are canonicalized before matching. A canonical target outside the
//! workspace root, a dangling symlink, or an uncanonicalizable path is a
//! structured policy error instead of a silent non-match.

use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

use jessica_core::types::ToolCall;
use serde_json::Value;
use thiserror::Error;

use crate::config::{ApprovalMode, HarnessConfig, PolicyRuleConfig, RuleAction};
use crate::tools::{Capabilities, ToolDescriptor, ToolSource};

/// Maximum number of missing trailing components accepted while resolving a
/// write target. This is a guard against pathological paths, not a semantic
/// limit for normal workspace paths.
const MAX_MISSING_COMPONENTS: usize = 64;

/// Error produced while matching a policy rule.
#[derive(Debug, Error)]
pub(crate) enum PolicyError {
    /// The workspace root itself could not be canonicalized.
    #[error("policy workspace root `{root}` could not be canonicalized: {source}")]
    Root {
        /// Root that failed.
        root: String,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// A path argument was empty.
    #[error("policy path must not be empty")]
    EmptyPath,
    /// A path resolved outside the workspace root.
    #[error("policy path `{path}` escapes the workspace root")]
    Escape {
        /// Rejected path.
        path: String,
    },
    /// A path that policy matching requires to exist does not exist.
    #[error("policy path `{path}` does not exist")]
    NotFound {
        /// Missing path.
        path: String,
    },
    /// A path could not be inspected.
    #[error("policy path `{path}` could not be canonicalized: {source}")]
    Io {
        /// Path that failed.
        path: String,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// A resolved path or glob was not valid UTF-8.
    #[error("policy path `{path}` is not valid UTF-8")]
    NonUtf8 {
        /// Path that failed.
        path: String,
    },
    /// A rule contained an unsupported glob.
    #[error("policy glob `{glob}` is invalid: {reason}")]
    InvalidGlob {
        /// Rejected glob.
        glob: String,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// The `apply_patch` input could not be parsed for path matching.
    #[error("invalid apply_patch input: {reason}")]
    InvalidPatch {
        /// Parser explanation.
        reason: String,
    },
}

/// Host-side facts a policy rule may inspect.
///
/// Descriptors assign these facts from the tool source and name; tools cannot
/// claim their own capabilities or approval class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PolicyTool {
    /// Model-visible name.
    pub name: String,
    /// Host-assigned source.
    pub source: ToolSource,
    /// Host-assigned capabilities.
    pub capabilities: Capabilities,
    /// Argument field carrying a filesystem path, when the tool has one.
    pub path_field: Option<String>,
    /// Whether the pre-v2 host required one-time approval for this tool.
    pub legacy_sensitive: bool,
}

impl PolicyTool {
    /// Builds the policy facts from a host descriptor.
    pub(crate) fn from_descriptor(descriptor: &ToolDescriptor) -> Self {
        Self {
            name: descriptor.name.clone(),
            source: descriptor.source,
            capabilities: descriptor.capabilities,
            path_field: descriptor.policy_hints.path_field.clone(),
            legacy_sensitive: descriptor.approval == crate::tools::ApprovalPolicy::Interactive,
        }
    }

    /// Returns whether approval v2 treats this tool as sensitive.
    ///
    /// Legacy-sensitive native tools stay sensitive. With an explicit
    /// `[approval]` section, any descriptor marked destructive is also
    /// sensitive, which covers MCP tools conservatively. WASM tools currently
    /// report read-only capabilities and therefore are not widened.
    fn sensitive(&self, approval_explicit: bool) -> bool {
        self.legacy_sensitive || (approval_explicit && self.capabilities.destructive)
    }
}

/// A matched rule and its configured explanation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuleMatch {
    /// Stable rule id.
    pub rule_id: String,
    /// Explanation shown in errors and simulate output.
    pub reason: String,
}

/// Final decision exposed by `policy simulate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalDecision {
    /// The call may execute.
    Allow,
    /// One-time approval is required.
    Ask,
    /// The call is denied.
    Deny,
}

impl FinalDecision {
    /// Returns the stable lowercase name.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Ask => "ask",
            Self::Deny => "deny",
        }
    }
}

/// Approval disposition that accompanies a final simulate decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApprovalDisposition {
    /// No approval is involved.
    NotRequired,
    /// The host may ask when a permission gate is installed.
    Interactive,
    /// The host will ask before execution.
    Ask,
    /// Approval mode auto-approves and records the event.
    AutoApproved,
    /// Approval mode or a rule denies the call.
    Denied,
}

impl ApprovalDisposition {
    /// Returns the stable lowercase name.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotRequired => "not_required",
            Self::Interactive => "interactive",
            Self::Ask => "ask",
            Self::AutoApproved => "auto_approved",
            Self::Denied => "denied",
        }
    }
}

/// Verdict returned by [`PolicyV2::evaluate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PolicyVerdict {
    /// No v2 rule matched and the tool is not sensitive.
    Allow {
        /// Why the call is allowed.
        reason: String,
        /// Matched rule, if an explicit allow rule decided it.
        rule: Option<RuleMatch>,
    },
    /// No v2 rule matched; legacy approval behavior applies.
    Legacy {
        /// Whether the legacy approval gate applies.
        sensitive: bool,
    },
    /// Approval mode auto-approves a sensitive tool or an `ask` rule.
    AutoApprove {
        /// Rule that triggered approval, if any.
        rule: Option<RuleMatch>,
        /// Explanation.
        reason: String,
    },
    /// One-time approval is required.
    Ask {
        /// Rule that required approval, if any.
        rule: Option<RuleMatch>,
        /// Explanation.
        reason: String,
    },
    /// The call is denied.
    Deny {
        /// Rule that denied the call, if any.
        rule: Option<RuleMatch>,
        /// Explanation.
        reason: String,
    },
}

impl PolicyVerdict {
    /// Returns the matched rule id, if a rule decided the outcome.
    pub(crate) fn rule_id(&self) -> Option<&str> {
        match self {
            Self::Allow { rule, .. }
            | Self::AutoApprove { rule, .. }
            | Self::Ask { rule, .. }
            | Self::Deny { rule, .. } => rule.as_ref().map(|rule| rule.rule_id.as_str()),
            Self::Legacy { .. } => None,
        }
    }

    /// Returns the explanation attached to this verdict.
    pub(crate) fn reason(&self) -> &str {
        match self {
            Self::Allow { reason, .. }
            | Self::AutoApprove { reason, .. }
            | Self::Ask { reason, .. }
            | Self::Deny { reason, .. } => reason,
            Self::Legacy { sensitive: true } => {
                "sensitive tool; the host may request one-time approval"
            }
            Self::Legacy { sensitive: false } => "allowlisted; no policy rule matched",
        }
    }

    /// Produces the decision and approval disposition reported by simulate.
    pub(crate) fn explain(&self) -> (FinalDecision, ApprovalDisposition) {
        match self {
            Self::Allow { .. } => (FinalDecision::Allow, ApprovalDisposition::NotRequired),
            Self::Legacy { sensitive: true } => {
                (FinalDecision::Allow, ApprovalDisposition::Interactive)
            }
            Self::Legacy { sensitive: false } => {
                (FinalDecision::Allow, ApprovalDisposition::NotRequired)
            }
            Self::AutoApprove { .. } => (FinalDecision::Allow, ApprovalDisposition::AutoApproved),
            Self::Ask { .. } => (FinalDecision::Ask, ApprovalDisposition::Ask),
            Self::Deny { .. } => (FinalDecision::Deny, ApprovalDisposition::Denied),
        }
    }
}

/// Effective policy v2 evaluator for one configuration snapshot.
#[derive(Debug, Clone)]
pub(crate) struct PolicyV2 {
    rules: Vec<PolicyRuleConfig>,
    approval: ApprovalMode,
    approval_explicit: bool,
    canonical_root: PathBuf,
}

impl PolicyV2 {
    /// Builds an evaluator rooted at `root`.
    ///
    /// The root is canonicalized once so every path comparison is against the
    /// same resolved directory.
    pub(crate) async fn new(root: &Path, config: &HarnessConfig) -> Result<Self, PolicyError> {
        let canonical_root =
            tokio::fs::canonicalize(root)
                .await
                .map_err(|source| PolicyError::Root {
                    root: root.display().to_string(),
                    source,
                })?;
        Ok(Self {
            rules: config.policy.rules.clone(),
            approval: config.approval.mode,
            approval_explicit: config.approval.explicit,
            canonical_root,
        })
    }

    /// Evaluates one call against the configured rules and approval mode.
    pub(crate) async fn evaluate(
        &self,
        tool: &PolicyTool,
        call: &ToolCall,
    ) -> Result<PolicyVerdict, PolicyError> {
        if let Some(rule) = self.first_rule(RuleAction::Deny, tool, call).await? {
            return Ok(PolicyVerdict::Deny {
                reason: rule.reason.clone(),
                rule: Some(rule),
            });
        }

        // The protected-path default is a deny, so it must run before an `ask`
        // rule could turn it into a prompt (and before `approval.mode = "allow"`
        // could auto-approve it). Only an explicit `allow` rule overrides it.
        if matches!(tool.name.as_str(), "apply_patch" | "edit_file")
            && let Some(path) = self.matching_path(tool, call).await?
            && is_protected_path(&path)
            && self
                .first_rule(RuleAction::Allow, tool, call)
                .await?
                .is_none()
        {
            return Ok(PolicyVerdict::Deny {
                reason: format!(
                    "`{path}` is a protected path; add an explicit allow rule to edit it"
                ),
                rule: None,
            });
        }

        if let Some(rule) = self.first_rule(RuleAction::Ask, tool, call).await? {
            return Ok(match self.approval {
                ApprovalMode::Deny => PolicyVerdict::Deny {
                    reason: format!(
                        "approval mode `deny` denies rule `{}`: {}",
                        rule.rule_id, rule.reason
                    ),
                    rule: Some(rule),
                },
                ApprovalMode::Allow => PolicyVerdict::AutoApprove {
                    reason: format!(
                        "approval mode `allow` auto-approved rule `{}`: {}",
                        rule.rule_id, rule.reason
                    ),
                    rule: Some(rule),
                },
                ApprovalMode::Ask => PolicyVerdict::Ask {
                    reason: rule.reason.clone(),
                    rule: Some(rule),
                },
            });
        }

        if let Some(rule) = self.first_rule(RuleAction::Allow, tool, call).await? {
            return Ok(PolicyVerdict::Allow {
                reason: rule.reason.clone(),
                rule: Some(rule),
            });
        }

        if !tool.sensitive(self.approval_explicit) {
            return Ok(PolicyVerdict::Allow {
                reason: "allowlisted; no policy rule matched".to_owned(),
                rule: None,
            });
        }

        // No explicit `[approval]` section keeps the pre-v2 split: ACP asks
        // through its gate, while a CLI turn without a gate runs the tool.
        if !self.approval_explicit {
            return Ok(PolicyVerdict::Legacy { sensitive: true });
        }

        Ok(match self.approval {
            ApprovalMode::Ask => PolicyVerdict::Ask {
                reason: format!("sensitive tool `{}` requires one-time approval", tool.name),
                rule: None,
            },
            ApprovalMode::Deny => PolicyVerdict::Deny {
                reason: format!(
                    "approval mode `deny` denies sensitive tool `{}` without an explicit allow rule",
                    tool.name
                ),
                rule: None,
            },
            ApprovalMode::Allow => PolicyVerdict::AutoApprove {
                reason: format!(
                    "approval mode `allow` auto-approved sensitive tool `{}`",
                    tool.name
                ),
                rule: None,
            },
        })
    }

    /// Returns the first rule with `action` whose conditions all match.
    async fn first_rule(
        &self,
        action: RuleAction,
        tool: &PolicyTool,
        call: &ToolCall,
    ) -> Result<Option<RuleMatch>, PolicyError> {
        for rule in self.rules.iter().filter(|rule| rule.action == action) {
            if self.rule_matches(rule, tool, call).await? {
                return Ok(Some(RuleMatch {
                    rule_id: rule.id.clone(),
                    reason: rule
                        .reason
                        .clone()
                        .unwrap_or_else(|| format!("policy rule `{}` matched", rule.id)),
                }));
            }
        }
        Ok(None)
    }

    /// Returns whether every condition in `rule` matches.
    async fn rule_matches(
        &self,
        rule: &PolicyRuleConfig,
        tool: &PolicyTool,
        call: &ToolCall,
    ) -> Result<bool, PolicyError> {
        if let Some(selector) = &rule.tool
            && !selector.matches(&tool.name)
        {
            return Ok(false);
        }

        if let Some(program) = &rule.program {
            if tool.name != "exec" {
                return Ok(false);
            }
            match call.arguments.get("program").and_then(Value::as_str) {
                Some(actual) if actual == program => {}
                _ => return Ok(false),
            }
        }

        if let Some(prefix) = &rule.args_prefix {
            if tool.name != "exec" {
                return Ok(false);
            }
            let empty = Vec::new();
            let actual = call
                .arguments
                .get("args")
                .and_then(Value::as_array)
                .unwrap_or(&empty);
            if actual.len() < prefix.len() {
                return Ok(false);
            }
            for (expected, actual) in prefix.iter().zip(actual) {
                if actual.as_str() != Some(expected.as_str()) {
                    return Ok(false);
                }
            }
        }

        if let Some(glob) = &rule.path_glob {
            let Some(path) = self.matching_path(tool, call).await? else {
                return Ok(false);
            };
            if !glob_matches(glob, &path)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Extracts and canonicalizes the path a rule should match.
    ///
    /// File tools use their descriptor path field. `exec` matches its `cwd`.
    /// `bash` deliberately returns `None`: its command string is never parsed.
    async fn matching_path(
        &self,
        tool: &PolicyTool,
        call: &ToolCall,
    ) -> Result<Option<String>, PolicyError> {
        if tool.source != ToolSource::Native {
            return Ok(None);
        }
        if tool.name == "bash" {
            return Ok(None);
        }
        if tool.name == "exec" {
            let raw = call
                .arguments
                .get("cwd")
                .and_then(Value::as_str)
                .unwrap_or(".");
            return self.resolve_relative(raw, false).await.map(Some);
        }
        if tool.name == "apply_patch" {
            let Some(patch) = call.arguments.get("patch").and_then(Value::as_str) else {
                return Ok(None);
            };
            let summary = jessica_tools::summarize_patch(patch)
                .map_err(|reason| PolicyError::InvalidPatch { reason })?;
            return self.resolve_relative(&summary.path, false).await.map(Some);
        }
        let Some(field) = &tool.path_field else {
            return Ok(None);
        };
        let Some(raw) = call.arguments.get(field).and_then(Value::as_str) else {
            return Ok(None);
        };
        let allow_missing = tool.name == "write_file";
        self.resolve_relative(raw, allow_missing).await.map(Some)
    }

    /// Resolves `raw` against the workspace root and returns a slash-separated
    /// path relative to that root.
    async fn resolve_relative(
        &self,
        raw: &str,
        allow_missing: bool,
    ) -> Result<String, PolicyError> {
        if raw.trim().is_empty() {
            return Err(PolicyError::EmptyPath);
        }
        let candidate = lexical_absolute(&self.canonical_root, raw)?;
        let resolved = self
            .canonicalize_candidate(candidate, allow_missing)
            .await?;
        let relative =
            resolved
                .strip_prefix(&self.canonical_root)
                .map_err(|_| PolicyError::Escape {
                    path: raw.to_owned(),
                })?;
        if relative.as_os_str().is_empty() {
            return Ok(".".to_owned());
        }
        let mut parts = Vec::new();
        for component in relative.components() {
            match component {
                Component::Normal(part) => parts.push(
                    part.to_str()
                        .ok_or_else(|| PolicyError::NonUtf8 {
                            path: raw.to_owned(),
                        })?
                        .to_owned(),
                ),
                Component::CurDir => {}
                _ => {
                    return Err(PolicyError::Escape {
                        path: raw.to_owned(),
                    });
                }
            }
        }
        Ok(parts.join("/"))
    }

    /// Canonicalizes `candidate`, allowing missing trailing components when
    /// `allow_missing` is true.
    async fn canonicalize_candidate(
        &self,
        candidate: PathBuf,
        allow_missing: bool,
    ) -> Result<PathBuf, PolicyError> {
        let display = candidate.display().to_string();
        let mut current = candidate;
        let mut suffix: Vec<OsString> = Vec::new();

        loop {
            match tokio::fs::canonicalize(&current).await {
                Ok(canonical) => {
                    if !canonical.starts_with(&self.canonical_root) {
                        return Err(PolicyError::Escape { path: display });
                    }
                    let mut resolved = canonical;
                    for part in suffix.iter().rev() {
                        resolved.push(part);
                    }
                    if !resolved.starts_with(&self.canonical_root) {
                        return Err(PolicyError::Escape { path: display });
                    }
                    return Ok(resolved);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if !allow_missing || suffix.len() >= MAX_MISSING_COMPONENTS {
                        return Err(PolicyError::NotFound { path: display });
                    }
                    // A dangling symlink is reported as NotFound by
                    // canonicalize. Resolve it without following so it cannot
                    // later redirect a write outside the workspace.
                    match tokio::fs::symlink_metadata(&current).await {
                        Ok(metadata) if metadata.file_type().is_symlink() => {
                            return Err(PolicyError::Escape { path: display });
                        }
                        Ok(_) => return Err(PolicyError::NotFound { path: display }),
                        Err(inner) if inner.kind() == std::io::ErrorKind::NotFound => {}
                        Err(inner) => {
                            return Err(PolicyError::Io {
                                path: display,
                                source: inner,
                            });
                        }
                    }
                    let Some(name) = current.file_name().map(std::ffi::OsStr::to_owned) else {
                        return Err(PolicyError::Escape { path: display });
                    };
                    suffix.push(name);
                    let Some(parent) = current.parent() else {
                        return Err(PolicyError::Escape { path: display });
                    };
                    if parent.as_os_str().is_empty() {
                        return Err(PolicyError::Escape { path: display });
                    }
                    current = parent.to_path_buf();
                }
                Err(error) => {
                    return Err(PolicyError::Io {
                        path: display,
                        source: error,
                    });
                }
            }
        }
    }
}

/// Builds an absolute, lexically normalized candidate path.
///
/// `..` is resolved before any filesystem access. A path that walks above the
/// filesystem root or above an absolute input root is rejected as an escape.
/// Returns whether an edit target is protected by default.
///
/// The editor tools refuse to modify repository metadata, Jessica's own state,
/// environment files, SSH material, and common private-key files unless an
/// explicit `allow` rule matches the call. `write_file` keeps its pre-9.7
/// behavior and is not covered here.
fn is_protected_path(path: &str) -> bool {
    for component in path.split('/') {
        if matches!(component, ".git" | ".jessica" | ".ssh") {
            return true;
        }
    }
    let name = path.rsplit('/').next().unwrap_or(path);
    let lower = name.to_ascii_lowercase();
    if lower == ".env" || lower.starts_with(".env.") {
        return true;
    }
    if matches!(
        lower.as_str(),
        "id_rsa" | "id_dsa" | "id_ecdsa" | "id_ed25519"
    ) {
        return true;
    }
    [".pem", ".key", ".p12", ".pfx"]
        .iter()
        .any(|extension| lower.ends_with(extension))
}

fn lexical_absolute(root: &Path, raw: &str) -> Result<PathBuf, PolicyError> {
    let raw_path = Path::new(raw);
    let mut output = if raw_path.is_absolute() {
        PathBuf::new()
    } else {
        root.to_path_buf()
    };
    for component in raw_path.components() {
        match component {
            Component::Prefix(prefix) => output.push(prefix.as_os_str()),
            Component::RootDir => output.push(std::path::MAIN_SEPARATOR.to_string()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !output.pop() {
                    return Err(PolicyError::Escape {
                        path: raw.to_owned(),
                    });
                }
            }
            Component::Normal(part) => output.push(part),
        }
    }
    Ok(output)
}

/// Returns whether `path` matches the documented minimal glob subset.
///
/// `**` is recognized only when it is a complete path segment and matches zero
/// or more segments. Within one segment, `*` matches zero or more characters
/// and `?` matches exactly one character. Character classes, brace expansion,
/// and backslash escapes are not implemented; those bytes are literal.
pub(crate) fn glob_matches(pattern: &str, path: &str) -> Result<bool, PolicyError> {
    if pattern.is_empty() {
        return Err(PolicyError::InvalidGlob {
            glob: pattern.to_owned(),
            reason: "pattern must not be empty",
        });
    }
    let pattern_segments: Vec<&str> = pattern.split('/').collect();
    let path_segments: Vec<&str> = path.split('/').collect();
    let mut memo = vec![vec![None; path_segments.len() + 1]; pattern_segments.len() + 1];

    fn matches(
        pattern: &[&str],
        path: &[&str],
        pattern_index: usize,
        path_index: usize,
        memo: &mut Vec<Vec<Option<bool>>>,
    ) -> bool {
        if let Some(cached) = memo[pattern_index][path_index] {
            return cached;
        }
        let result = if pattern_index == pattern.len() {
            path_index == path.len()
        } else if pattern[pattern_index] == "**" {
            matches(pattern, path, pattern_index + 1, path_index, memo)
                || (path_index < path.len()
                    && matches(pattern, path, pattern_index, path_index + 1, memo))
        } else if path_index == path.len() {
            false
        } else {
            segment_matches(pattern[pattern_index], path[path_index])
                && matches(pattern, path, pattern_index + 1, path_index + 1, memo)
        };
        memo[pattern_index][path_index] = Some(result);
        result
    }

    Ok(matches(&pattern_segments, &path_segments, 0, 0, &mut memo))
}

/// Matches one glob segment without crossing `/`.
fn segment_matches(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let mut memo = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    memo[0][0] = true;
    for pattern_index in 1..=pattern.len() {
        for text_index in 0..=text.len() {
            memo[pattern_index][text_index] = match pattern[pattern_index - 1] {
                '*' => {
                    memo[pattern_index - 1][text_index]
                        || (text_index > 0 && memo[pattern_index][text_index - 1])
                }
                '?' => text_index > 0 && memo[pattern_index - 1][text_index - 1],
                expected => {
                    text_index > 0
                        && memo[pattern_index - 1][text_index - 1]
                        && text[text_index - 1] == expected
                }
            };
        }
    }
    memo[pattern.len()][text.len()]
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::json;

    use super::*;
    use crate::config::{ApprovalConfig, PolicyConfig, PolicyRuleConfig, RuleAction, ToolSelector};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Creates a unique directory and removes it on drop.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "jessica-policy-{label}-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temp dir must be created");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn rule(
        id: &str,
        action: RuleAction,
        tool: Option<ToolSelector>,
        path_glob: Option<&str>,
    ) -> PolicyRuleConfig {
        PolicyRuleConfig {
            id: id.to_owned(),
            tool,
            path_glob: path_glob.map(str::to_owned),
            program: None,
            args_prefix: None,
            action,
            reason: Some(format!("reason for {id}")),
        }
    }

    fn config(rules: Vec<PolicyRuleConfig>, approval: ApprovalConfig) -> HarnessConfig {
        HarnessConfig {
            policy: PolicyConfig {
                allowed_tools: vec![
                    "read_file".to_owned(),
                    "write_file".to_owned(),
                    "exec".to_owned(),
                    "bash".to_owned(),
                    "delegate_task".to_owned(),
                ],
                rules,
            },
            approval,
            ..HarnessConfig::default()
        }
    }

    fn approval(mode: ApprovalMode, explicit: bool) -> ApprovalConfig {
        ApprovalConfig { mode, explicit }
    }

    fn file_tool(name: &str) -> PolicyTool {
        PolicyTool {
            name: name.to_owned(),
            source: ToolSource::Native,
            capabilities: Capabilities::for_tool(ToolSource::Native, name),
            path_field: Some(if name == "apply_patch" {
                "patch".to_owned()
            } else {
                "path".to_owned()
            }),
            legacy_sensitive: matches!(name, "write_file" | "apply_patch" | "edit_file"),
        }
    }

    fn exec_tool() -> PolicyTool {
        PolicyTool {
            name: "exec".to_owned(),
            source: ToolSource::Native,
            capabilities: Capabilities::for_tool(ToolSource::Native, "exec"),
            path_field: None,
            legacy_sensitive: true,
        }
    }

    fn bash_tool() -> PolicyTool {
        PolicyTool {
            name: "bash".to_owned(),
            source: ToolSource::Native,
            capabilities: Capabilities::for_tool(ToolSource::Native, "bash"),
            path_field: None,
            legacy_sensitive: true,
        }
    }

    fn mcp_tool() -> PolicyTool {
        PolicyTool {
            name: "mcp__srv__wipe".to_owned(),
            source: ToolSource::Mcp,
            capabilities: Capabilities::for_tool(ToolSource::Mcp, "wipe"),
            path_field: None,
            legacy_sensitive: false,
        }
    }

    fn call(name: &str, arguments: Value) -> ToolCall {
        ToolCall::new("call-1", name, arguments)
    }

    async fn evaluator(root: &Path, config: &HarnessConfig) -> PolicyV2 {
        PolicyV2::new(root, config)
            .await
            .expect("policy must build")
    }

    #[test]
    fn minimal_glob_supports_star_globstar_and_question_mark_only() {
        assert!(glob_matches("src/*.rs", "src/main.rs").expect("glob must parse"));
        assert!(!glob_matches("src/*.rs", "src/nested/main.rs").expect("glob must parse"));
        assert!(glob_matches("src/**/*.rs", "src/main.rs").expect("glob must parse"));
        assert!(glob_matches("src/**/*.rs", "src/a/b/main.rs").expect("glob must parse"));
        assert!(glob_matches("src/**", "src/a/b").expect("glob must parse"));
        assert!(glob_matches("?.txt", "a.txt").expect("glob must parse"));
        assert!(!glob_matches("?.txt", "ab.txt").expect("glob must parse"));
        assert!(!glob_matches("?.txt", ".txt").expect("glob must parse"));
        assert!(
            glob_matches("[abc].txt", "[abc].txt").expect("brackets are literal"),
            "character classes are not implemented"
        );
        assert!(!glob_matches("[abc].txt", "a.txt").expect("brackets are literal"));
        assert!(glob_matches("a\\*b", "a\\*b").expect("backslash is literal"));
        assert!(glob_matches("secrets/**", "secrets/keys/id").expect("glob must parse"));
        assert!(
            glob_matches("secrets/**", "secrets").expect("glob must parse"),
            "** matches zero or more segments"
        );
        assert!(glob_matches("**", "a/b").expect("glob must parse"));
        assert!(glob_matches("**", ".").expect("glob must parse"));
    }

    #[tokio::test]
    async fn deny_rules_win_over_ask_and_allow_regardless_of_configuration_order() {
        let temp = TempDir::new("priority");
        let config = config(
            vec![
                rule(
                    "allow-first",
                    RuleAction::Allow,
                    Some(ToolSelector::One("read_file".to_owned())),
                    None,
                ),
                rule(
                    "ask-second",
                    RuleAction::Ask,
                    Some(ToolSelector::One("read_file".to_owned())),
                    None,
                ),
                rule(
                    "deny-third",
                    RuleAction::Deny,
                    Some(ToolSelector::One("read_file".to_owned())),
                    None,
                ),
            ],
            approval(ApprovalMode::Ask, false),
        );
        let policy = evaluator(&temp.path, &config).await;

        let verdict = policy
            .evaluate(
                &file_tool("read_file"),
                &call("read_file", json!({"path": "a"})),
            )
            .await
            .expect("policy must evaluate");

        assert!(matches!(
            verdict,
            PolicyVerdict::Deny { ref rule, .. }
                if rule.as_ref().map(|rule| rule.rule_id.as_str()) == Some("deny-third")
        ));
    }

    #[tokio::test]
    async fn ask_rules_win_over_allow_rules_and_the_first_ask_match_wins() {
        let temp = TempDir::new("ask-priority");
        let config = config(
            vec![
                rule(
                    "first-ask",
                    RuleAction::Ask,
                    Some(ToolSelector::One("read_file".to_owned())),
                    None,
                ),
                rule(
                    "second-ask",
                    RuleAction::Ask,
                    Some(ToolSelector::One("read_file".to_owned())),
                    None,
                ),
                rule(
                    "allow",
                    RuleAction::Allow,
                    Some(ToolSelector::One("read_file".to_owned())),
                    None,
                ),
            ],
            approval(ApprovalMode::Ask, false),
        );
        let policy = evaluator(&temp.path, &config).await;

        let verdict = policy
            .evaluate(
                &file_tool("read_file"),
                &call("read_file", json!({"path": "a"})),
            )
            .await
            .expect("policy must evaluate");

        assert_eq!(verdict.rule_id(), Some("first-ask"));
        assert!(matches!(verdict, PolicyVerdict::Ask { .. }));
    }

    #[tokio::test]
    async fn path_rules_match_canonical_workspace_relative_paths() {
        let temp = TempDir::new("paths");
        std::fs::create_dir_all(temp.path.join("secrets")).expect("directory must exist");
        std::fs::write(temp.path.join("secrets/token.txt"), "secret").expect("file must exist");
        std::fs::create_dir_all(temp.path.join("public")).expect("directory must exist");
        std::fs::write(temp.path.join("public/note.txt"), "note").expect("file must exist");
        let config = config(
            vec![rule(
                "protect-secrets",
                RuleAction::Deny,
                Some(ToolSelector::One("read_file".to_owned())),
                Some("secrets/**"),
            )],
            approval(ApprovalMode::Ask, false),
        );
        let policy = evaluator(&temp.path, &config).await;

        let denied = policy
            .evaluate(
                &file_tool("read_file"),
                &call("read_file", json!({"path": "secrets/token.txt"})),
            )
            .await
            .expect("policy must evaluate");
        assert_eq!(denied.rule_id(), Some("protect-secrets"));

        let allowed = policy
            .evaluate(
                &file_tool("read_file"),
                &call("read_file", json!({"path": "./public/../public/note.txt"})),
            )
            .await
            .expect("policy must evaluate");
        assert!(matches!(allowed, PolicyVerdict::Allow { rule: None, .. }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_escape_is_a_structured_path_error() {
        let temp = TempDir::new("escape-root");
        let outside = TempDir::new("escape-outside");
        std::fs::write(outside.path.join("secret.txt"), "outside").expect("file must exist");
        std::os::unix::fs::symlink(&outside.path, temp.path.join("link"))
            .expect("symlink must be created");
        let config = config(
            vec![rule(
                "protect",
                RuleAction::Deny,
                Some(ToolSelector::One("read_file".to_owned())),
                Some("**/secret.txt"),
            )],
            approval(ApprovalMode::Ask, false),
        );
        let policy = evaluator(&temp.path, &config).await;

        let error = policy
            .evaluate(
                &file_tool("read_file"),
                &call("read_file", json!({"path": "link/secret.txt"})),
            )
            .await
            .expect_err("a symlink escape must fail closed");
        assert!(
            matches!(error, PolicyError::Escape { .. }),
            "unexpected error: {error}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_dangling_symlink_cannot_be_used_as_a_write_target() {
        let temp = TempDir::new("dangling-root");
        let outside = TempDir::new("dangling-outside");
        std::os::unix::fs::symlink(outside.path.join("created.txt"), temp.path.join("dangling"))
            .expect("symlink must be created");
        let config = config(
            vec![rule(
                "protect",
                RuleAction::Deny,
                Some(ToolSelector::One("write_file".to_owned())),
                Some("**"),
            )],
            approval(ApprovalMode::Ask, false),
        );
        let policy = evaluator(&temp.path, &config).await;

        let error = policy
            .evaluate(
                &file_tool("write_file"),
                &call(
                    "write_file",
                    json!({"path": "dangling", "content": "escape"}),
                ),
            )
            .await
            .expect_err("a dangling symlink must not be followed for writes");
        assert!(
            matches!(error, PolicyError::Escape { .. }),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn exec_rules_match_program_argument_prefix_and_cwd() {
        let temp = TempDir::new("exec-match");
        std::fs::create_dir_all(temp.path.join("sub")).expect("cwd must exist");
        let mut protect = rule(
            "protect-git-push",
            RuleAction::Deny,
            Some(ToolSelector::One("exec".to_owned())),
            Some("sub/**"),
        );
        protect.program = Some("git".to_owned());
        protect.args_prefix = Some(vec!["push".to_owned(), "--force".to_owned()]);
        let config = config(vec![protect], approval(ApprovalMode::Ask, false));
        let policy = evaluator(&temp.path, &config).await;

        let denied = policy
            .evaluate(
                &exec_tool(),
                &call(
                    "exec",
                    json!({"program": "git", "args": ["push", "--force", "origin"], "cwd": "sub"}),
                ),
            )
            .await
            .expect("policy must evaluate");
        assert_eq!(denied.rule_id(), Some("protect-git-push"));

        for arguments in [
            json!({"program": "git", "args": ["push", "origin"], "cwd": "sub"}),
            json!({"program": "git", "args": ["status"], "cwd": "sub"}),
            json!({"program": "git", "args": ["push", "--force"], "cwd": "."}),
        ] {
            let allowed = policy
                .evaluate(&exec_tool(), &call("exec", arguments))
                .await
                .expect("policy must evaluate");
            assert!(
                matches!(allowed, PolicyVerdict::Legacy { sensitive: true }),
                "non-matching exec must fall back to legacy approval: {allowed:?}"
            );
        }
    }

    #[tokio::test]
    async fn bash_commands_are_never_parsed_as_program_or_paths() {
        let temp = TempDir::new("bash-opaque");
        std::fs::create_dir_all(temp.path.join("secrets")).expect("directory must exist");
        let mut dangerous = rule(
            "no-rm",
            RuleAction::Deny,
            Some(ToolSelector::One("bash".to_owned())),
            None,
        );
        dangerous.program = Some("rm".to_owned());
        dangerous.args_prefix = Some(vec!["-rf".to_owned()]);
        let config = config(vec![dangerous], approval(ApprovalMode::Ask, false));
        let policy = evaluator(&temp.path, &config).await;

        let verdict = policy
            .evaluate(
                &bash_tool(),
                &call("bash", json!({"command": "rm -rf secrets"})),
            )
            .await
            .expect("policy must evaluate");

        assert!(
            matches!(verdict, PolicyVerdict::Legacy { sensitive: true }),
            "bash text must not satisfy program/args conditions: {verdict:?}"
        );
    }

    #[tokio::test]
    async fn explicit_approval_modes_are_observable_for_sensitive_and_mcp_tools() {
        let temp = TempDir::new("approval-modes");

        let deny = config(vec![], approval(ApprovalMode::Deny, true));
        let policy = evaluator(&temp.path, &deny).await;
        let verdict = policy
            .evaluate(&exec_tool(), &call("exec", json!({"program": "true"})))
            .await
            .expect("policy must evaluate");
        assert!(matches!(verdict, PolicyVerdict::Deny { .. }));

        let allow = config(vec![], approval(ApprovalMode::Allow, true));
        let policy = evaluator(&temp.path, &allow).await;
        let verdict = policy
            .evaluate(&exec_tool(), &call("exec", json!({"program": "true"})))
            .await
            .expect("policy must evaluate");
        assert!(matches!(verdict, PolicyVerdict::AutoApprove { .. }));

        let ask = config(vec![], approval(ApprovalMode::Ask, true));
        let policy = evaluator(&temp.path, &ask).await;
        let verdict = policy
            .evaluate(&mcp_tool(), &call("mcp__srv__wipe", json!({})))
            .await
            .expect("policy must evaluate");
        assert!(
            matches!(verdict, PolicyVerdict::Ask { .. }),
            "explicit approval must treat destructive MCP tools conservatively: {verdict:?}"
        );

        let no_approval = config(vec![], approval(ApprovalMode::Ask, false));
        let policy = evaluator(&temp.path, &no_approval).await;
        let verdict = policy
            .evaluate(&mcp_tool(), &call("mcp__srv__wipe", json!({})))
            .await
            .expect("policy must evaluate");
        assert!(
            matches!(verdict, PolicyVerdict::Allow { .. }),
            "without an explicit [approval] section MCP behavior must stay unchanged: {verdict:?}"
        );
    }

    #[tokio::test]
    async fn an_allow_rule_overrides_explicit_deny_mode_and_records_no_auto_approval() {
        let temp = TempDir::new("allow-override");
        let config = config(
            vec![rule(
                "allow-status",
                RuleAction::Allow,
                Some(ToolSelector::One("exec".to_owned())),
                None,
            )],
            approval(ApprovalMode::Deny, true),
        );
        let policy = evaluator(&temp.path, &config).await;

        let verdict = policy
            .evaluate(&exec_tool(), &call("exec", json!({"program": "git"})))
            .await
            .expect("policy must evaluate");

        assert_eq!(verdict.rule_id(), Some("allow-status"));
        assert!(matches!(
            verdict.explain(),
            (FinalDecision::Allow, ApprovalDisposition::NotRequired)
        ));
    }

    #[tokio::test]
    async fn editor_tools_use_path_rules_and_apply_is_parsed_for_matching() {
        let temp = TempDir::new("editor-rules");
        std::fs::create_dir_all(temp.path.join("src")).expect("source dir must exist");
        std::fs::write(temp.path.join("src/app.txt"), "alpha\n").expect("file must exist");
        let patch = "--- a/src/app.txt\n+++ b/src/app.txt\n@@ -1 +1 @@\n-alpha\n+beta\n";
        let config = config(
            vec![
                rule(
                    "deny-edit",
                    RuleAction::Deny,
                    Some(ToolSelector::One("edit_file".to_owned())),
                    Some("src/**"),
                ),
                rule(
                    "ask-patch",
                    RuleAction::Ask,
                    Some(ToolSelector::One("apply_patch".to_owned())),
                    Some("src/**"),
                ),
            ],
            approval(ApprovalMode::Ask, false),
        );
        let policy = evaluator(&temp.path, &config).await;

        let edit = policy
            .evaluate(
                &file_tool("edit_file"),
                &call(
                    "edit_file",
                    json!({"path": "src/app.txt", "old": "alpha", "new": "beta"}),
                ),
            )
            .await
            .expect("policy must evaluate");
        assert_eq!(edit.rule_id(), Some("deny-edit"));

        let patch_verdict = policy
            .evaluate(
                &file_tool("apply_patch"),
                &call("apply_patch", json!({"patch": patch})),
            )
            .await
            .expect("policy must evaluate");
        assert_eq!(patch_verdict.rule_id(), Some("ask-patch"));
        assert!(matches!(patch_verdict, PolicyVerdict::Ask { .. }));
    }

    #[tokio::test]
    async fn protected_paths_are_denied_unless_an_allow_rule_matches() {
        let temp = TempDir::new("protected");
        std::fs::write(temp.path.join(".env"), "SECRET=value\n").expect("file must exist");
        std::fs::create_dir_all(temp.path.join(".git")).expect("git dir must exist");
        std::fs::write(temp.path.join(".git/config"), "[core]\n").expect("config must exist");
        let edit = file_tool("edit_file");

        let no_rules = config(vec![], approval(ApprovalMode::Allow, true));
        let policy = evaluator(&temp.path, &no_rules).await;
        for path in [".env", ".git/config"] {
            let verdict = policy
                .evaluate(
                    &edit,
                    &call("edit_file", json!({"path": path, "old": "a", "new": "b"})),
                )
                .await
                .expect("policy must evaluate");
            match verdict {
                PolicyVerdict::Deny { reason, .. } => assert!(
                    reason.contains("protected path"),
                    "the denial must explain the protection: {reason}"
                ),
                other => panic!("protected path `{path}` must be denied, got {other:?}"),
            }
        }

        let allowed = config(
            vec![rule(
                "allow-env",
                RuleAction::Allow,
                Some(ToolSelector::One("edit_file".to_owned())),
                Some(".env"),
            )],
            approval(ApprovalMode::Allow, true),
        );
        let policy = evaluator(&temp.path, &allowed).await;
        let verdict = policy
            .evaluate(
                &edit,
                &call(
                    "edit_file",
                    json!({"path": ".env", "old": "SECRET", "new": "PUBLIC"}),
                ),
            )
            .await
            .expect("policy must evaluate");
        assert_eq!(verdict.rule_id(), Some("allow-env"));
        assert!(matches!(verdict, PolicyVerdict::Allow { .. }));
    }

    #[tokio::test]
    async fn an_ask_rule_does_not_turn_a_protected_default_denial_into_a_prompt() {
        let temp = TempDir::new("protected-ask");
        std::fs::write(temp.path.join(".env"), "SECRET=value\n").expect("file must exist");
        let policy = evaluator(
            &temp.path,
            &config(
                vec![rule(
                    "review-env",
                    RuleAction::Ask,
                    Some(ToolSelector::One("edit_file".to_owned())),
                    Some(".env"),
                )],
                approval(ApprovalMode::Allow, true),
            ),
        )
        .await;

        let verdict = policy
            .evaluate(
                &file_tool("edit_file"),
                &call(
                    "edit_file",
                    json!({"path": ".env", "old": "SECRET", "new": "PUBLIC"}),
                ),
            )
            .await
            .expect("policy must evaluate");

        match verdict {
            PolicyVerdict::Deny { reason, .. } => assert!(
                reason.contains("protected path"),
                "the protected default must run before ask rules: {reason}"
            ),
            other => panic!("an ask rule must not bypass the protected default denial: {other:?}"),
        }
    }

    #[tokio::test]
    async fn write_file_keeps_its_pre_9_7_protected_path_behavior() {
        let temp = TempDir::new("write-protected");
        std::fs::write(temp.path.join(".env"), "SECRET=value\n").expect("file must exist");
        let policy = evaluator(
            &temp.path,
            &config(vec![], approval(ApprovalMode::Ask, false)),
        )
        .await;

        let verdict = policy
            .evaluate(
                &file_tool("write_file"),
                &call("write_file", json!({"path": ".env", "content": "x"})),
            )
            .await
            .expect("policy must evaluate");

        assert!(
            matches!(verdict, PolicyVerdict::Legacy { sensitive: true }),
            "write_file must retain its legacy approval path: {verdict:?}"
        );
    }

    #[tokio::test]
    async fn a_malformed_apply_patch_is_a_structured_policy_error() {
        let temp = TempDir::new("bad-patch");
        let policy = evaluator(
            &temp.path,
            &config(vec![], approval(ApprovalMode::Ask, true)),
        )
        .await;

        let error = policy
            .evaluate(
                &file_tool("apply_patch"),
                &call("apply_patch", json!({"patch": "not a unified diff"})),
            )
            .await
            .expect_err("a malformed patch must not receive a verdict");

        assert!(
            matches!(error, PolicyError::InvalidPatch { .. }),
            "the failure must be structured: {error:?}"
        );
    }

    #[test]
    fn protected_path_classification_covers_metadata_environment_and_keys() {
        for path in [
            ".git/config",
            "nested/.jessica/events.db",
            ".env",
            ".env.local",
            "keys/server.pem",
            "keys/server.key",
            "id_rsa",
            "home/.ssh/id_ed25519",
        ] {
            assert!(is_protected_path(path), "`{path}` must be protected");
        }
        for path in [
            "src/main.rs",
            ".environment",
            "id_rsa.pub",
            "docs/private-key.md",
        ] {
            assert!(!is_protected_path(path), "`{path}` must not be protected");
        }
    }
}
