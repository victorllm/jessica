//! Engine configuration types.
//!
//! These types express governance decisions owned by the engine. Adapters such
//! as the CLI map their own file-format configuration into them; the engine
//! never parses TOML or constructs providers.

use std::path::PathBuf;
use std::time::Duration;

use jessica_core::types::Budget;
use jessica_runtime::DelegationLimits;

/// Default wall-clock timeout for one governed tool execution.
const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(30);
/// Default hard cap on a tool result entering the model context.
const DEFAULT_MAX_RESULT_BYTES: usize = 64 * 1024;
/// Default size at or below which a result is returned without management.
const DEFAULT_INLINE_THRESHOLD_BYTES: usize = 8 * 1024;
/// Default hard cap on a managed inline result.
const DEFAULT_MAX_INLINE_BYTES: usize = 64 * 1024;
/// Default maximum bytes stored in one artifact.
const DEFAULT_MAX_ARTIFACT_BYTES: usize = 1024 * 1024;
/// Default artifact retention period.
const DEFAULT_ARTIFACT_RETENTION_DAYS: u64 = 7;
/// Default total byte budget for retained artifacts.
const DEFAULT_ARTIFACT_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Default number of consecutive stalled rounds before the stuck guard trips.
const DEFAULT_MAX_STUCK_ROUNDS: u32 = 3;
/// Default maximum number of tools exposed to the model in one turn.
const DEFAULT_MAX_EXPOSED_TOOLS: usize = 64;
/// Default maximum total canonical schema bytes exposed in one turn.
const DEFAULT_MAX_TOOL_SCHEMA_BYTES: usize = 128 * 1024;

/// Durability policy for the turn trace.
///
/// [`TracePolicy::Required`] is the default. An embedder must opt out
/// explicitly with [`TracePolicy::Disabled`]; the engine never silently
/// downgrades a required trace, and a required turn only succeeds after its
/// terminal `turn_ended` marker is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TracePolicy {
    /// Do not persist a turn trace.
    ///
    /// Only for embedders that explicitly provide their own evidence path.
    Disabled,
    /// Persist a turn trace and fail the turn if it cannot be opened,
    /// recorded, or closed.
    #[default]
    Required,
}

/// Approval mode for tools that are classified as sensitive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ApprovalMode {
    /// Ask the host for one-time approval when a gate is installed.
    #[default]
    Ask,
    /// Auto-approve a sensitive tool permitted by policy and record the
    /// decision before execution.
    Allow,
    /// Deny a sensitive tool that no explicit allow rule permits.
    Deny,
}

/// Action selected by a parameter-level policy rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleAction {
    /// Deny the call. Deny rules are evaluated first.
    Deny,
    /// Require one-time approval. Ask rules are evaluated after denies.
    Ask,
    /// Allow the call. Allow rules are evaluated after asks.
    Allow,
}

/// Tool selector accepted by a policy rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolSelector {
    /// One tool name.
    One(String),
    /// Multiple tool names.
    Many(Vec<String>),
}

impl ToolSelector {
    /// Returns whether the selector contains `name`.
    pub fn matches(&self, name: &str) -> bool {
        match self {
            Self::One(tool) => tool == name,
            Self::Many(tools) => tools.iter().any(|tool| tool == name),
        }
    }
}

/// One ordered parameter-level policy rule.
///
/// All present conditions must match. The engine evaluates every matching deny
/// rule first, then ask rules, then allow rules, preserving configured order
/// inside each action class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRule {
    /// Stable, non-empty rule identifier reported in denials and traces.
    pub id: String,
    /// Tool name or names this rule applies to.
    pub tool: Option<ToolSelector>,
    /// Workspace-relative path glob, when the rule is path-based.
    pub path_glob: Option<String>,
    /// Exact `exec` program, when the rule is program-based.
    pub program: Option<String>,
    /// Required prefix of the `exec` argument array.
    pub args_prefix: Option<Vec<String>>,
    /// Decision when every condition matches.
    pub action: RuleAction,
    /// Optional human-readable explanation surfaced in errors.
    pub reason: Option<String>,
}

/// Tool policy settings.
///
/// The allowlist is evaluated before parameter-level rules. Rules only make
/// decisions for tools that are already exposed; a hidden tool remains
/// unknown to the model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicyConfig {
    /// Tool names the model may execute. An empty list denies every call.
    pub allowed_tools: Vec<String>,
    /// Ordered parameter-level rules.
    pub rules: Vec<PolicyRule>,
}

/// Approval settings for sensitive tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalConfig {
    /// Mode applied to sensitive tools and `ask` rules.
    pub mode: ApprovalMode,
    /// Whether the adapter explicitly configured approval.
    ///
    /// When false, the legacy behavior applies: an `ask` mode alone does not
    /// force a permission gate for otherwise-sensitive tools.
    pub explicit: bool,
}

impl Default for ApprovalConfig {
    fn default() -> Self {
        Self {
            mode: ApprovalMode::Ask,
            explicit: false,
        }
    }
}

/// Acceptance-check retry policy for one turn.
///
/// The engine only decides when the configured [`AcceptanceCheck`] runs and how
/// many retries are allowed. The check itself, including its command or
/// protocol, is supplied by an adapter.
///
/// [`AcceptanceCheck`]: jessica_core::acceptance::AcceptanceCheck
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptanceConfig {
    max_retries: u32,
}

impl AcceptanceConfig {
    /// Creates a policy that retries at most `max_retries` times after the
    /// first failed check.
    pub fn new(max_retries: u32) -> Self {
        Self { max_retries }
    }

    /// Returns the configured retry budget.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

impl Default for AcceptanceConfig {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Configuration for one governed tool execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExecutionConfig {
    /// Wall-clock limit for one tool execution, measured after approval.
    pub timeout: Duration,
    /// Maximum serialized argument bytes, or `None` when unlimited.
    pub max_args_bytes: Option<usize>,
    /// Maximum result bytes allowed to enter the model context.
    pub max_result_bytes: usize,
}

impl Default for ToolExecutionConfig {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TOOL_TIMEOUT,
            max_args_bytes: None,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
        }
    }
}

/// Exposure budgets for the model-visible tool catalog.
///
/// The engine checks the catalog returned by the host's
/// [`ToolProvider`](crate::ToolProvider) after the allowlist and any
/// delegation tool have been applied. An over-budget catalog fails turn
/// startup before the model is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogLimits {
    /// Maximum number of exposed tools.
    pub max_exposed_tools: usize,
    /// Maximum total canonical schema bytes of exposed tools.
    pub max_tool_schema_bytes: usize,
}

impl Default for CatalogLimits {
    fn default() -> Self {
        Self {
            max_exposed_tools: DEFAULT_MAX_EXPOSED_TOOLS,
            max_tool_schema_bytes: DEFAULT_MAX_TOOL_SCHEMA_BYTES,
        }
    }
}

/// Configuration for result management and content-addressed artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultConfig {
    /// Results at or below this byte length are returned unchanged.
    pub inline_threshold_bytes: usize,
    /// Hard cap on a managed result entering the model context.
    pub max_inline_bytes: usize,
    /// Maximum bytes stored in one artifact.
    pub max_artifact_bytes: usize,
    /// Artifact directory, relative to the workspace root.
    pub artifact_dir: PathBuf,
    /// Number of days an artifact is retained.
    pub artifact_retention_days: u64,
    /// Total byte budget for retained artifacts.
    pub artifact_max_bytes: u64,
}

impl Default for ResultConfig {
    fn default() -> Self {
        Self {
            inline_threshold_bytes: DEFAULT_INLINE_THRESHOLD_BYTES,
            max_inline_bytes: DEFAULT_MAX_INLINE_BYTES,
            max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
            artifact_dir: PathBuf::from(".jessica/artifacts"),
            artifact_retention_days: DEFAULT_ARTIFACT_RETENTION_DAYS,
            artifact_max_bytes: DEFAULT_ARTIFACT_MAX_BYTES,
        }
    }
}

/// One immutable configuration snapshot for a turn.
///
/// The engine takes this snapshot once at the start of a turn. Configuration
/// changes made by an adapter after the snapshot are observed only by the next
/// turn.
#[derive(Debug, Clone, PartialEq)]
pub struct EngineConfig {
    budget: Budget,
    max_tool_calls: Option<u32>,
    max_stuck_rounds: u32,
    policy: PolicyConfig,
    approval: ApprovalConfig,
    tools: ToolExecutionConfig,
    acceptance: Option<AcceptanceConfig>,
    result: ResultConfig,
    catalog_limits: CatalogLimits,
    delegation: Option<DelegationLimits>,
    trace_policy: TracePolicy,
}

impl EngineConfig {
    /// Creates a configuration with an explicit trace policy and fail-closed
    /// defaults for every other setting.
    ///
    /// The default budget allows no tokens; callers must opt into a positive
    /// token budget with [`Self::with_budget`]. A required trace needs a
    /// [`TraceStore`](crate::TraceStore) at assembly time.
    pub fn new(trace_policy: TracePolicy) -> Self {
        Self {
            budget: Budget::new(0, DEFAULT_TOOL_TIMEOUT, 32),
            max_tool_calls: None,
            max_stuck_rounds: DEFAULT_MAX_STUCK_ROUNDS,
            policy: PolicyConfig::default(),
            approval: ApprovalConfig::default(),
            tools: ToolExecutionConfig::default(),
            acceptance: None,
            result: ResultConfig::default(),
            catalog_limits: CatalogLimits::default(),
            delegation: None,
            trace_policy,
        }
    }

    /// Replaces the session budget.
    pub fn with_budget(mut self, budget: Budget) -> Self {
        self.budget = budget;
        self
    }

    /// Replaces the tool-call limit for one turn.
    pub fn with_max_tool_calls(mut self, max_tool_calls: Option<u32>) -> Self {
        self.max_tool_calls = max_tool_calls;
        self
    }

    /// Replaces the number of consecutive stalled rounds allowed before the
    /// stuck guard trips.
    ///
    /// Values below one are treated as one by the guard.
    pub fn with_max_stuck_rounds(mut self, max_stuck_rounds: u32) -> Self {
        self.max_stuck_rounds = max_stuck_rounds;
        self
    }

    /// Replaces the acceptance-check policy.
    ///
    /// `None` runs one attempt without an external acceptance check.
    pub fn with_acceptance(mut self, acceptance: Option<AcceptanceConfig>) -> Self {
        self.acceptance = acceptance;
        self
    }

    /// Replaces the tool policy.
    pub fn with_policy(mut self, policy: PolicyConfig) -> Self {
        self.policy = policy;
        self
    }

    /// Replaces the approval settings.
    pub fn with_approval(mut self, approval: ApprovalConfig) -> Self {
        self.approval = approval;
        self
    }

    /// Replaces the tool execution settings.
    pub fn with_tools(mut self, tools: ToolExecutionConfig) -> Self {
        self.tools = tools;
        self
    }

    /// Replaces the result-management and artifact settings.
    pub fn with_result(mut self, result: ResultConfig) -> Self {
        self.result = result;
        self
    }

    /// Replaces the tool-catalog exposure budgets.
    pub fn with_catalog_limits(mut self, catalog_limits: CatalogLimits) -> Self {
        self.catalog_limits = catalog_limits;
        self
    }

    /// Enables delegation for fresh turns with `delegation` limits.
    ///
    /// `None` disables delegation. The engine requires a
    /// [`ChildToolFactory`](crate::ChildToolFactory) at build time when limits
    /// are configured. Resume never exposes `delegate_task`, regardless of
    /// this setting.
    pub fn with_delegation(mut self, delegation: Option<DelegationLimits>) -> Self {
        self.delegation = delegation;
        self
    }

    /// Returns the session budget.
    pub fn budget(&self) -> &Budget {
        &self.budget
    }

    /// Returns the per-turn tool-call limit.
    pub fn max_tool_calls(&self) -> Option<u32> {
        self.max_tool_calls
    }

    /// Returns the stalled-round limit enforced by the stuck guard.
    pub fn max_stuck_rounds(&self) -> u32 {
        self.max_stuck_rounds
    }

    /// Returns the acceptance-check policy, when configured.
    pub fn acceptance(&self) -> Option<AcceptanceConfig> {
        self.acceptance
    }

    /// Returns the tool policy.
    pub fn policy(&self) -> &PolicyConfig {
        &self.policy
    }

    /// Returns the approval settings.
    pub fn approval(&self) -> &ApprovalConfig {
        &self.approval
    }

    /// Returns the tool execution settings.
    pub fn tools(&self) -> &ToolExecutionConfig {
        &self.tools
    }

    /// Returns the result-management and artifact settings.
    pub fn result(&self) -> &ResultConfig {
        &self.result
    }

    /// Returns the tool-catalog exposure budgets.
    pub fn catalog_limits(&self) -> CatalogLimits {
        self.catalog_limits
    }

    /// Returns the delegation limits, or `None` when delegation is disabled.
    pub fn delegation(&self) -> Option<DelegationLimits> {
        self.delegation
    }

    /// Returns the configured trace policy.
    pub fn trace_policy(&self) -> TracePolicy {
        self.trace_policy
    }

    /// Returns whether the configuration can require a permission gate.
    ///
    /// An ask rule always needs a gate. An explicit `ask` approval mode needs
    /// one even when no rule asks; the legacy default does not force one.
    pub(crate) fn requires_approval_gate(&self) -> bool {
        self.policy
            .rules
            .iter()
            .any(|rule| rule.action == RuleAction::Ask)
            || (self.approval.explicit && self.approval.mode == ApprovalMode::Ask)
    }
}

impl Default for EngineConfig {
    /// Returns a configuration whose trace policy is [`TracePolicy::Required`]
    /// and whose budgets fail closed.
    fn default() -> Self {
        Self::new(TracePolicy::Required)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_required_trace_is_the_default_policy() {
        let config = EngineConfig::default();

        assert_eq!(config.trace_policy(), TracePolicy::Required);
        assert_eq!(config.budget().max_tokens, 0);
    }

    #[test]
    fn the_default_budget_fails_closed_until_a_caller_opts_in() {
        let config = EngineConfig::new(TracePolicy::Disabled);

        assert_eq!(config.budget().max_tokens, 0);
        assert_eq!(config.trace_policy(), TracePolicy::Disabled);
    }

    #[test]
    fn an_ask_rule_requires_a_gate_but_a_plain_allowlist_does_not() {
        let ask_policy = PolicyConfig {
            allowed_tools: vec!["read_file".to_owned()],
            rules: vec![PolicyRule {
                id: "ask-read".to_owned(),
                tool: Some(ToolSelector::One("read_file".to_owned())),
                path_glob: None,
                program: None,
                args_prefix: None,
                action: RuleAction::Ask,
                reason: Some("test ask".to_owned()),
            }],
        };
        let config = EngineConfig::new(TracePolicy::Disabled).with_policy(ask_policy);
        assert!(config.requires_approval_gate());

        let plain = EngineConfig::new(TracePolicy::Disabled);
        assert!(!plain.requires_approval_gate());
    }

    #[test]
    fn explicit_ask_mode_requires_a_gate_but_legacy_ask_does_not() {
        let explicit = EngineConfig::new(TracePolicy::Disabled).with_approval(ApprovalConfig {
            mode: ApprovalMode::Ask,
            explicit: true,
        });
        assert!(explicit.requires_approval_gate());

        let legacy = EngineConfig::new(TracePolicy::Disabled).with_approval(ApprovalConfig {
            mode: ApprovalMode::Ask,
            explicit: false,
        });
        assert!(!legacy.requires_approval_gate());
    }
}
