//! TOML configuration for the governed CLI.
//!
//! Every setting that governs a session is explicit in the file: budgets,
//! the tool allowlist, the post-tool command sensor, the acceptance check, and
//! layered memory. Missing optional sections disable that component; the policy
//! default is the same allowlist the CLI has always exposed natively.

use std::path::{Path, PathBuf};
use std::time::Duration;

use jessica_core::types::Budget;
use jessica_runtime::{
    CHILD_TOOL_TIMEOUT, DELEGATE_TASK_TOOL_NAME, DelegationLimits, MIN_RESULT_BYTES,
};
use serde::Deserialize;
use thiserror::Error;

/// Tools registered by the CLI and allowed when no policy section is given.
const NATIVE_TOOLS: &[&str] = &["bash", "read_file", "write_file"];
/// Native tools a delegated child may be granted.
pub(crate) const NATIVE_CHILD_TOOLS: &[&str] = &["bash", "read_file", "write_file"];

/// Default token budget for a session (prompt plus completion tokens).
const DEFAULT_MAX_TOKENS: u64 = 1_000_000;
/// Default per-tool wall-clock timeout.
const DEFAULT_TOOL_TIMEOUT_MS: u64 = 10_000;
/// Default number of model rounds in one attempt.
const DEFAULT_MAX_ROUNDS: u32 = 16;
/// Default number of consecutive no-progress rounds before stopping.
const DEFAULT_MAX_STUCK_ROUNDS: u32 = 3;
/// Default maximum size of one tool result.
const DEFAULT_MAX_RESULT_BYTES: usize = 64 * 1024;
/// Default additional acceptance attempts after the first failure.
const DEFAULT_MAX_RETRIES: u32 = 2;
/// Default memory database path, resolved against the CLI workspace root.
const DEFAULT_MEMORY_PATH: &str = ".jessica/memory.db";
/// Default number of memory entries requested by one recall.
const DEFAULT_RECALL_LIMIT: usize = 5;
/// Default byte budget for the injected memory system message.
const DEFAULT_MAX_INJECT_BYTES: usize = 4096;
/// Default skill database path, resolved against the CLI workspace root.
const DEFAULT_SKILLS_PATH: &str = ".jessica/skills.db";
/// Default minimum tool calls required to distill a turn.
const DEFAULT_SKILL_MIN_TOOL_CALLS: usize = 5;
/// Default minimum assistant rounds required to distill a turn.
const DEFAULT_SKILL_MIN_ROUNDS: usize = 2;
/// Default byte budget for the escaped distillation source content.
const DEFAULT_SKILL_MAX_SOURCE_BYTES: usize = 16 * 1024;
/// Default number of skills requested by one recall.
const DEFAULT_SKILL_RECALL_LIMIT: usize = 3;
/// Default byte budget for the injected skill system message.
const DEFAULT_SKILL_MAX_INJECT_BYTES: usize = 2048;
/// Default skill relevance floor; zero disables score filtering.
const DEFAULT_SKILL_MIN_SCORE: f64 = 0.0;
/// Default wall-clock timeout for one controlled distillation call.
const DEFAULT_SKILL_DISTILL_TIMEOUT_MS: u64 = 30_000;
/// Default grace period for draining the skill worker before exit.
const DEFAULT_SKILL_DRAIN_TIMEOUT_MS: u64 = 30_000;
/// Default attempts per distillation job, including the first.
const DEFAULT_SKILL_MAX_ATTEMPTS: usize = 2;
/// Default number of model rounds for one delegated child agent.
const DEFAULT_DELEGATION_MAX_ROUNDS: u32 = 4;
/// Default wall-clock limit for one delegated child agent.
const DEFAULT_DELEGATION_TIMEOUT_MS: u64 = 8_000;
/// Default maximum UTF-8 byte length of a delegated task.
const DEFAULT_DELEGATION_MAX_TASK_BYTES: usize = 8_192;
/// Default maximum byte length of a delegated result.
const DEFAULT_DELEGATION_MAX_RESULT_BYTES: usize = 32_768;
/// Default maximum number of tool calls a delegated child may request.
const DEFAULT_DELEGATION_MAX_CHILD_TOOL_CALLS: u32 = 20;

/// Complete harness configuration loaded from a TOML file.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HarnessConfig {
    /// Session budgets enforced by the runtime and harness guards.
    pub budget: BudgetConfig,
    /// Tool allowlist.
    pub policy: PolicyConfig,
    /// Optional child-agent delegation.
    pub delegation: DelegationConfig,
    /// Optional WebAssembly tool plugins registered next to native tools.
    pub wasm: WasmConfig,
    /// Optional command run after every tool result.
    pub sensor: Option<SensorConfig>,
    /// Optional external acceptance check with retry orchestration.
    pub acceptance: Option<AcceptanceConfig>,
    /// Optional persistent event log used by `--resume`.
    pub events: EventsConfig,
    /// Optional layered memory used for context injection and turn writes.
    pub memory: MemoryConfig,
    /// Optional skill distillation, retrieval, and manual review.
    pub skills: SkillsConfig,
}

impl HarnessConfig {
    /// Loads and validates a configuration file.
    pub async fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = tokio::fs::read_to_string(path)
            .await
            .map_err(|source| ConfigError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        Self::from_toml_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source: Box::new(source),
        })
    }

    /// Parses and validates configuration from an in-memory TOML document.
    pub fn from_toml_str(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text).map_err(|source| ConfigError::Toml { source })?;
        config.validate()?;
        Ok(config)
    }

    /// Rejects values that would make the assembly ambiguous or unusable.
    ///
    /// A zero token budget and a zero result-size limit are accepted: they are
    /// strict but well-defined boundaries. Round, stuck, and timeout limits
    /// must be positive.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.budget.max_rounds == 0 {
            return Err(invalid("budget.max_rounds must be at least 1"));
        }
        if self.budget.tool_timeout_ms == 0 {
            return Err(invalid("budget.tool_timeout_ms must be at least 1"));
        }
        if self.budget.max_stuck_rounds == 0 {
            return Err(invalid("budget.max_stuck_rounds must be at least 1"));
        }
        self.delegation.validate()?;
        if self.delegation.enabled {
            if !self
                .policy
                .allowed_tools
                .iter()
                .any(|name| name == DELEGATE_TASK_TOOL_NAME)
            {
                return Err(invalid(format!(
                    "policy.allowed_tools must include `{DELEGATE_TASK_TOOL_NAME}` when delegation.enabled is true"
                )));
            }
            if self.delegation.timeout() <= CHILD_TOOL_TIMEOUT {
                return Err(invalid(format!(
                    "delegation.timeout_ms must be greater than {} (the fixed child tool timeout)",
                    CHILD_TOOL_TIMEOUT.as_millis()
                )));
            }
            if self.delegation.timeout() >= self.budget.tool_timeout() {
                return Err(invalid(
                    "delegation.timeout_ms must be less than budget.tool_timeout_ms",
                ));
            }
            if self.delegation.max_result_bytes > self.budget.max_result_bytes {
                return Err(invalid(
                    "delegation.max_result_bytes must not exceed budget.max_result_bytes",
                ));
            }
        }
        if let Some(sensor) = &self.sensor {
            validate_command("sensor", &sensor.name, &sensor.command)?;
            validate_working_dir("sensor.working_dir", sensor.working_dir.as_deref())?;
        }
        for manifest in &self.wasm.manifests {
            if manifest.as_os_str().is_empty() {
                return Err(invalid("wasm.manifests entries must not be empty"));
            }
        }
        if let Some(acceptance) = &self.acceptance {
            validate_command("acceptance", &acceptance.name, &acceptance.command)?;
            validate_working_dir("acceptance.working_dir", acceptance.working_dir.as_deref())?;
        }
        if self
            .events
            .path
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(invalid("events.path must not be empty when present"));
        }
        if self.memory.path.as_os_str().is_empty() {
            return Err(invalid("memory.path must not be empty"));
        }
        self.skills.validate()?;
        Ok(())
    }
}

/// Session budget settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BudgetConfig {
    /// Maximum prompt plus completion tokens across the session.
    pub max_tokens: u64,
    /// Maximum wall-clock time for one tool execution, in milliseconds.
    pub tool_timeout_ms: u64,
    /// Maximum number of model rounds in one agent attempt.
    pub max_rounds: u32,
    /// Maximum consecutive rounds without observable progress.
    pub max_stuck_rounds: u32,
    /// Maximum byte size of one tool result.
    pub max_result_bytes: usize,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            max_tokens: DEFAULT_MAX_TOKENS,
            tool_timeout_ms: DEFAULT_TOOL_TIMEOUT_MS,
            max_rounds: DEFAULT_MAX_ROUNDS,
            max_stuck_rounds: DEFAULT_MAX_STUCK_ROUNDS,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
        }
    }
}

impl BudgetConfig {
    /// Converts the subset understood by the runtime into a [`Budget`].
    pub fn to_budget(&self) -> Budget {
        Budget::new(
            self.max_tokens,
            Duration::from_millis(self.tool_timeout_ms),
            self.max_rounds,
        )
    }

    /// Returns the per-tool timeout as a duration.
    pub fn tool_timeout(&self) -> Duration {
        Duration::from_millis(self.tool_timeout_ms)
    }
}

/// Tool policy settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolicyConfig {
    /// Tool names the model may execute. An empty list denies every call.
    pub allowed_tools: Vec<String>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            allowed_tools: NATIVE_TOOLS.iter().map(|name| (*name).to_owned()).collect(),
        }
    }
}

/// Child-agent delegation settings.
///
/// Delegation is disabled by default. When disabled, the CLI neither registers
/// `delegate_task` nor constructs a child factory or executor, and the parent
/// allowlist does not need to mention the tool. When enabled, the parent
/// allowlist must contain `delegate_task` and the child timeout is fixed at
/// [`CHILD_TOOL_TIMEOUT`]; there is no per-child timeout setting.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DelegationConfig {
    /// Whether `delegate_task` is registered and child agents may run.
    pub enabled: bool,
    /// Maximum number of model rounds for one child agent.
    pub max_rounds: u32,
    /// Wall-clock limit for one child agent, in milliseconds.
    ///
    /// When enabled this must be greater than [`CHILD_TOOL_TIMEOUT`] and
    /// strictly less than `budget.tool_timeout_ms`.
    pub timeout_ms: u64,
    /// Maximum UTF-8 byte length of the delegated task.
    pub max_task_bytes: usize,
    /// Maximum byte length of the serialized delegation result.
    ///
    /// When enabled this must not exceed `budget.max_result_bytes`.
    pub max_result_bytes: usize,
    /// Maximum number of tool calls one child agent may request.
    pub max_child_tool_calls: u32,
}

impl Default for DelegationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_rounds: DEFAULT_DELEGATION_MAX_ROUNDS,
            timeout_ms: DEFAULT_DELEGATION_TIMEOUT_MS,
            max_task_bytes: DEFAULT_DELEGATION_MAX_TASK_BYTES,
            max_result_bytes: DEFAULT_DELEGATION_MAX_RESULT_BYTES,
            max_child_tool_calls: DEFAULT_DELEGATION_MAX_CHILD_TOOL_CALLS,
        }
    }
}

impl DelegationConfig {
    /// Rejects values that are invalid even when delegation is disabled.
    ///
    /// The allowlist, parent timeout, and parent result-size checks are
    /// cross-field constraints applied only when delegation is enabled.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_rounds == 0 {
            return Err(invalid("delegation.max_rounds must be at least 1"));
        }
        if self.max_child_tool_calls == 0 {
            return Err(invalid(
                "delegation.max_child_tool_calls must be at least 1",
            ));
        }
        if self.max_task_bytes == 0 {
            return Err(invalid("delegation.max_task_bytes must be at least 1"));
        }
        if self.max_result_bytes < MIN_RESULT_BYTES {
            return Err(invalid(format!(
                "delegation.max_result_bytes must be at least {MIN_RESULT_BYTES}"
            )));
        }
        Ok(())
    }

    /// Returns the child wall-clock limit as a duration.
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// Converts the settings into runtime delegation limits.
    pub fn to_limits(&self) -> DelegationLimits {
        DelegationLimits {
            max_rounds: self.max_rounds,
            timeout: self.timeout(),
            max_task_bytes: self.max_task_bytes,
            max_result_bytes: self.max_result_bytes,
            max_child_tool_calls: self.max_child_tool_calls,
        }
    }
}

/// Persistent event log settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EventsConfig {
    /// SQLite database path. Relative paths resolve against the workspace root.
    ///
    /// When omitted, events are not persisted and `--resume` is unavailable.
    pub path: Option<PathBuf>,
}

/// Layered memory settings.
///
/// Memory is disabled by default. When disabled, the CLI neither creates nor
/// opens the configured database file, and no memory is injected or written.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryConfig {
    /// Whether layered memory is enabled for this run.
    pub enabled: bool,
    /// SQLite database path. Relative paths resolve against the workspace root.
    pub path: PathBuf,
    /// Maximum number of entries requested by one recall.
    pub recall_limit: usize,
    /// Maximum bytes in the injected memory system message.
    pub max_inject_bytes: usize,
    /// Embedder used for the long-term vector path.
    pub embedder: MemoryEmbedderKind,
    /// Policy governing automatic writes after a completed turn.
    pub write_policy: MemoryWritePolicy,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from(DEFAULT_MEMORY_PATH),
            recall_limit: DEFAULT_RECALL_LIMIT,
            max_inject_bytes: DEFAULT_MAX_INJECT_BYTES,
            embedder: MemoryEmbedderKind::default(),
            write_policy: MemoryWritePolicy::default(),
        }
    }
}

/// Embedder selected for long-term memory recall.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryEmbedderKind {
    /// Reproducible, non-semantic byte n-gram embeddings.
    #[default]
    Deterministic,
    /// Disable the vector path and rely on short-term and keyword recall.
    None,
}

/// Policy for automatic memory writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryWritePolicy {
    /// Write one `kind=turn` entry after a turn finishes with `Completed`.
    #[default]
    Completed,
    /// Never write automatically; an explicit entry point is not implemented.
    Explicit,
}

/// Skill distillation, retrieval, and manual review settings.
///
/// Skills are disabled by default. When disabled, the CLI neither creates nor
/// opens the configured database, and no skill job is enqueued or injected.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SkillsConfig {
    /// Whether skill triggering, distillation, retrieval, and review are enabled.
    pub enabled: bool,
    /// SQLite database path. Relative paths resolve against the workspace root.
    pub path: PathBuf,
    /// Minimum tool calls in the final attempt required to trigger distillation.
    pub min_tool_calls: usize,
    /// Minimum assistant rounds in the final attempt required to trigger.
    pub min_rounds: usize,
    /// Maximum byte budget for escaped source content.
    pub max_source_bytes: usize,
    /// Maximum number of active skills requested by one recall.
    pub recall_limit: usize,
    /// Maximum bytes in the injected skill system message.
    pub max_inject_bytes: usize,
    /// Minimum relevance score; `0.0` disables score filtering.
    pub min_score: f64,
    /// Timeout for one controlled distillation call, in milliseconds.
    pub distill_timeout_ms: u64,
    /// Maximum wait for the worker to drain before exit, in milliseconds.
    pub drain_timeout_ms: u64,
    /// Attempts per distillation job, including the first.
    pub max_attempts: usize,
    /// Whether bounded tool-output excerpts are included in the source.
    ///
    /// Full tool outputs are never included. The default is `false`.
    pub include_tool_outputs: bool,
    /// Whether a newly written skill starts active instead of as a draft.
    pub auto_activate: bool,
}

impl Default for SkillsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from(DEFAULT_SKILLS_PATH),
            min_tool_calls: DEFAULT_SKILL_MIN_TOOL_CALLS,
            min_rounds: DEFAULT_SKILL_MIN_ROUNDS,
            max_source_bytes: DEFAULT_SKILL_MAX_SOURCE_BYTES,
            recall_limit: DEFAULT_SKILL_RECALL_LIMIT,
            max_inject_bytes: DEFAULT_SKILL_MAX_INJECT_BYTES,
            min_score: DEFAULT_SKILL_MIN_SCORE,
            distill_timeout_ms: DEFAULT_SKILL_DISTILL_TIMEOUT_MS,
            drain_timeout_ms: DEFAULT_SKILL_DRAIN_TIMEOUT_MS,
            max_attempts: DEFAULT_SKILL_MAX_ATTEMPTS,
            include_tool_outputs: false,
            auto_activate: false,
        }
    }
}

impl SkillsConfig {
    /// Rejects values that would make skill processing ambiguous or unusable.
    ///
    /// Zero trigger thresholds and a zero recall or injection budget are
    /// accepted as explicit boundaries. Source budgets, timeouts, and attempts
    /// must be positive, and the relevance floor must be finite and
    /// non-negative.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.path.as_os_str().is_empty() {
            return Err(invalid("skills.path must not be empty"));
        }
        if self.max_source_bytes == 0 {
            return Err(invalid("skills.max_source_bytes must be at least 1"));
        }
        if !self.min_score.is_finite() || self.min_score < 0.0 {
            return Err(invalid(
                "skills.min_score must be a finite non-negative value",
            ));
        }
        if self.distill_timeout_ms == 0 {
            return Err(invalid("skills.distill_timeout_ms must be at least 1"));
        }
        if self.drain_timeout_ms == 0 {
            return Err(invalid("skills.drain_timeout_ms must be at least 1"));
        }
        if self.max_attempts == 0 {
            return Err(invalid("skills.max_attempts must be at least 1"));
        }
        Ok(())
    }
}

/// WebAssembly tool plugin settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WasmConfig {
    /// Manifest paths. Relative paths resolve against the workspace root.
    ///
    /// Names declared by these manifests are still subject to
    /// [`PolicyConfig::allowed_tools`]; registering a plugin does not grant it
    /// execution permission.
    pub manifests: Vec<PathBuf>,
}

/// One command sensor run after a tool result.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SensorConfig {
    /// Name shown in sensor diagnostics.
    pub name: String,
    /// Program to execute directly, without a shell.
    pub command: String,
    /// Program arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory; relative paths resolve against the workspace root.
    #[serde(default)]
    pub working_dir: Option<PathBuf>,
}

/// External acceptance check with retry orchestration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceConfig {
    /// Name shown in acceptance diagnostics.
    pub name: String,
    /// Program to execute directly, without a shell.
    pub command: String,
    /// Program arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory; relative paths resolve against the workspace root.
    #[serde(default)]
    pub working_dir: Option<PathBuf>,
    /// Additional attempts allowed after the first failed check.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

fn default_max_retries() -> u32 {
    DEFAULT_MAX_RETRIES
}

fn validate_command(section: &str, name: &str, command: &str) -> Result<(), ConfigError> {
    if name.trim().is_empty() {
        return Err(invalid(format!("{section}.name must not be empty")));
    }
    if command.trim().is_empty() {
        return Err(invalid(format!("{section}.command must not be empty")));
    }
    Ok(())
}

fn validate_working_dir(field: &str, working_dir: Option<&Path>) -> Result<(), ConfigError> {
    if working_dir.is_some_and(|path| path.as_os_str().is_empty()) {
        return Err(invalid(format!("{field} must not be empty when present")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        message: message.into(),
    }
}

/// Errors produced while loading or validating configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The configuration file could not be read.
    #[error("failed to read config `{path}`: {source}")]
    Read {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The configuration file was not valid TOML.
    #[error("failed to parse config: {source}")]
    Toml {
        /// Underlying TOML parser error.
        #[source]
        source: toml::de::Error,
    },
    /// The configuration parsed but failed validation.
    #[error("invalid config: {message}")]
    Invalid {
        /// Human-readable validation failure.
        message: String,
    },
    /// A file loaded successfully was rejected by validation.
    #[error("invalid config `{path}`: {source}")]
    Parse {
        /// Path of the rejected file.
        path: PathBuf,
        /// Validation or parse failure.
        #[source]
        source: Box<ConfigError>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = include_str!("../../examples/harness.toml");

    #[test]
    fn example_config_parses_with_expected_governance() {
        let config = HarnessConfig::from_toml_str(EXAMPLE).expect("example config must parse");

        assert_eq!(config.budget.max_rounds, 12);
        assert_eq!(config.budget.max_tokens, 20_000);
        assert_eq!(config.budget.tool_timeout(), Duration::from_secs(10));
        assert_eq!(
            config.policy.allowed_tools,
            vec!["read_file".to_owned(), "write_file".to_owned()]
        );

        let sensor = config.sensor.expect("example must configure a sensor");
        assert_eq!(sensor.name, "diff-check");
        assert_eq!(sensor.command, "git");
        assert_eq!(sensor.args, vec!["diff", "--check"]);

        let acceptance = config
            .acceptance
            .expect("example must configure acceptance");
        assert_eq!(acceptance.name, "readme-spelling");
        assert_eq!(acceptance.command, "sh");
        assert_eq!(acceptance.max_retries, 2);
    }

    #[test]
    fn omitted_policy_defaults_to_the_native_tool_allowlist() {
        let config = HarnessConfig::from_toml_str("[budget]\nmax_rounds = 3\n")
            .expect("minimal config must parse");
        assert_eq!(
            config.policy.allowed_tools,
            vec!["bash", "read_file", "write_file"]
        );
        assert!(config.sensor.is_none());
        assert!(config.acceptance.is_none());
    }

    #[test]
    fn explicit_empty_allowlist_denies_everything() {
        let config = HarnessConfig::from_toml_str("[policy]\nallowed_tools = []\n")
            .expect("empty allowlist is valid");
        assert!(config.policy.allowed_tools.is_empty());
    }

    #[test]
    fn acceptance_retries_default_to_two() {
        let config =
            HarnessConfig::from_toml_str("[acceptance]\nname = \"check\"\ncommand = \"true\"\n")
                .expect("acceptance without retries must parse");
        assert_eq!(
            config
                .acceptance
                .expect("acceptance is present")
                .max_retries,
            2
        );
    }

    #[test]
    fn unknown_fields_are_rejected_instead_of_silently_ignored() {
        let error = HarnessConfig::from_toml_str("[budget]\nmax_roundz = 4\n")
            .expect_err("misspelled fields must fail");
        assert!(error.to_string().contains("max_roundz"));
    }

    #[test]
    fn zero_rounds_or_zero_timeout_is_rejected() {
        let rounds = HarnessConfig::from_toml_str("[budget]\nmax_rounds = 0\n")
            .expect_err("zero rounds must fail");
        assert!(rounds.to_string().contains("max_rounds"));

        let timeout = HarnessConfig::from_toml_str("[budget]\ntool_timeout_ms = 0\n")
            .expect_err("zero timeout must fail");
        assert!(timeout.to_string().contains("tool_timeout_ms"));
    }

    #[test]
    fn wasm_manifests_are_parsed_and_remain_subject_to_the_allowlist() {
        let config = HarnessConfig::from_toml_str(
            "[policy]\nallowed_tools = [\"echo_json\"]\n\
             [wasm]\nmanifests = [\"examples/plugins/echo/manifest.json\"]\n",
        )
        .expect("wasm section must parse");

        assert_eq!(
            config.wasm.manifests,
            vec![PathBuf::from("examples/plugins/echo/manifest.json")]
        );
        assert_eq!(config.policy.allowed_tools, vec!["echo_json"]);
    }

    #[test]
    fn blank_wasm_manifest_paths_are_rejected() {
        let error = HarnessConfig::from_toml_str("[wasm]\nmanifests = [\"\"]\n")
            .expect_err("blank manifest path must fail");
        assert!(error.to_string().contains("wasm.manifests"));
    }

    #[test]
    fn unknown_wasm_fields_are_rejected() {
        let error = HarnessConfig::from_toml_str("[wasm]\nplugins = [\"plugin.json\"]\n")
            .expect_err("unknown wasm fields must fail");
        assert!(error.to_string().contains("plugins"));
    }

    #[test]
    fn event_store_path_is_optional_and_parsed() {
        let config = HarnessConfig::from_toml_str("[events]\npath = \"state/events.sqlite\"\n")
            .expect("events section must parse");
        assert_eq!(
            config.events.path,
            Some(PathBuf::from("state/events.sqlite"))
        );

        let default = HarnessConfig::from_toml_str("").expect("empty config must parse");
        assert_eq!(default.events.path, None);
    }

    #[test]
    fn memory_defaults_are_disabled_with_the_documented_values() {
        let config = HarnessConfig::from_toml_str("").expect("empty config must parse");
        assert!(!config.memory.enabled);
        assert_eq!(config.memory.path, PathBuf::from(".jessica/memory.db"));
        assert_eq!(config.memory.recall_limit, 5);
        assert_eq!(config.memory.max_inject_bytes, 4096);
        assert_eq!(config.memory.embedder, MemoryEmbedderKind::Deterministic);
        assert_eq!(config.memory.write_policy, MemoryWritePolicy::Completed);
    }

    #[test]
    fn memory_section_parses_every_documented_setting() {
        let config = HarnessConfig::from_toml_str(
            "[memory]\n\
             enabled = true\n\
             path = \"state/memory.db\"\n\
             recall_limit = 9\n\
             max_inject_bytes = 128\n\
             embedder = \"none\"\n\
             write_policy = \"explicit\"\n",
        )
        .expect("memory section must parse");

        assert!(config.memory.enabled);
        assert_eq!(config.memory.path, PathBuf::from("state/memory.db"));
        assert_eq!(config.memory.recall_limit, 9);
        assert_eq!(config.memory.max_inject_bytes, 128);
        assert_eq!(config.memory.embedder, MemoryEmbedderKind::None);
        assert_eq!(config.memory.write_policy, MemoryWritePolicy::Explicit);
    }

    #[test]
    fn invalid_memory_enum_values_report_the_offending_field() {
        let embedder = HarnessConfig::from_toml_str("[memory]\nembedder = \"http\"\n")
            .expect_err("unknown embedders must fail");
        assert!(embedder.to_string().contains("embedder"));

        let policy = HarnessConfig::from_toml_str("[memory]\nwrite_policy = \"always\"\n")
            .expect_err("unknown write policies must fail");
        assert!(policy.to_string().contains("write_policy"));
    }

    #[test]
    fn blank_memory_paths_and_unknown_fields_are_rejected() {
        let blank = HarnessConfig::from_toml_str("[memory]\npath = \"\"\n")
            .expect_err("blank memory path must fail");
        assert!(blank.to_string().contains("memory.path"));

        let unknown = HarnessConfig::from_toml_str("[memory]\ndatabase = \"memory.db\"\n")
            .expect_err("unknown memory fields must fail");
        assert!(unknown.to_string().contains("database"));
    }

    #[test]
    fn blank_event_store_paths_and_unknown_fields_are_rejected() {
        let blank = HarnessConfig::from_toml_str("[events]\npath = \"\"\n")
            .expect_err("blank event path must fail");
        assert!(blank.to_string().contains("events.path"));

        let unknown = HarnessConfig::from_toml_str("[events]\ndatabase = \"events.sqlite\"\n")
            .expect_err("unknown events fields must fail");
        assert!(unknown.to_string().contains("database"));
    }

    #[test]
    fn skills_defaults_are_disabled_with_the_documented_values() {
        let config = HarnessConfig::from_toml_str("").expect("empty config must parse");
        let skills = &config.skills;

        assert!(!skills.enabled);
        assert_eq!(skills.path, PathBuf::from(".jessica/skills.db"));
        assert_eq!(skills.min_tool_calls, 5);
        assert_eq!(skills.min_rounds, 2);
        assert_eq!(skills.max_source_bytes, 16 * 1024);
        assert_eq!(skills.recall_limit, 3);
        assert_eq!(skills.max_inject_bytes, 2048);
        assert_eq!(skills.min_score, 0.0);
        assert_eq!(skills.distill_timeout_ms, 30_000);
        assert_eq!(skills.drain_timeout_ms, 30_000);
        assert_eq!(skills.max_attempts, 2);
        assert!(!skills.include_tool_outputs);
        assert!(!skills.auto_activate);
    }

    #[test]
    fn skills_section_parses_every_documented_setting() {
        let config = HarnessConfig::from_toml_str(
            "[skills]\n\
             enabled = true\n\
             path = \"state/skills.db\"\n\
             min_tool_calls = 1\n\
             min_rounds = 0\n\
             max_source_bytes = 2048\n\
             recall_limit = 7\n\
             max_inject_bytes = 512\n\
             min_score = 0.25\n\
             distill_timeout_ms = 1500\n\
             drain_timeout_ms = 2500\n\
             max_attempts = 3\n\
             include_tool_outputs = true\n\
             auto_activate = true\n",
        )
        .expect("skills section must parse");

        assert!(config.skills.enabled);
        assert_eq!(config.skills.path, PathBuf::from("state/skills.db"));
        assert_eq!(config.skills.min_tool_calls, 1);
        assert_eq!(config.skills.min_rounds, 0);
        assert_eq!(config.skills.max_source_bytes, 2048);
        assert_eq!(config.skills.recall_limit, 7);
        assert_eq!(config.skills.max_inject_bytes, 512);
        assert_eq!(config.skills.min_score, 0.25);
        assert_eq!(config.skills.distill_timeout_ms, 1500);
        assert_eq!(config.skills.drain_timeout_ms, 2500);
        assert_eq!(config.skills.max_attempts, 3);
        assert!(config.skills.include_tool_outputs);
        assert!(config.skills.auto_activate);
    }

    #[test]
    fn invalid_skill_values_are_rejected_at_load_time() {
        for (document, field) in [
            ("[skills]\npath = \"\"\n", "skills.path"),
            (
                "[skills]\nmax_source_bytes = 0\n",
                "skills.max_source_bytes",
            ),
            ("[skills]\nmin_score = -0.5\n", "skills.min_score"),
            ("[skills]\nmin_score = inf\n", "skills.min_score"),
            (
                "[skills]\ndistill_timeout_ms = 0\n",
                "skills.distill_timeout_ms",
            ),
            (
                "[skills]\ndrain_timeout_ms = 0\n",
                "skills.drain_timeout_ms",
            ),
            ("[skills]\nmax_attempts = 0\n", "skills.max_attempts"),
        ] {
            let error = HarnessConfig::from_toml_str(document)
                .expect_err("invalid skills values must fail at load time");
            assert!(
                error.to_string().contains(field),
                "error for {document:?} should name {field}: {error}"
            );
        }
    }

    #[test]
    fn zero_skill_boundaries_and_unknown_fields_are_handled_explicitly() {
        let config = HarnessConfig::from_toml_str(
            "[skills]\nmin_tool_calls = 0\nmin_rounds = 0\nrecall_limit = 0\nmax_inject_bytes = 0\n",
        )
        .expect("zero thresholds and budgets are explicit boundaries");
        assert_eq!(config.skills.min_tool_calls, 0);
        assert_eq!(config.skills.min_rounds, 0);
        assert_eq!(config.skills.recall_limit, 0);
        assert_eq!(config.skills.max_inject_bytes, 0);

        let unknown = HarnessConfig::from_toml_str("[skills]\ndatabase = \"skills.db\"\n")
            .expect_err("unknown skills fields must fail");
        assert!(unknown.to_string().contains("database"));
    }

    #[test]
    fn delegation_defaults_are_disabled_with_the_documented_values() {
        let config = HarnessConfig::from_toml_str("").expect("empty config must parse");
        let delegation = &config.delegation;

        assert!(!delegation.enabled);
        assert_eq!(delegation.max_rounds, 4);
        assert_eq!(delegation.timeout_ms, 8_000);
        assert_eq!(delegation.max_task_bytes, 8_192);
        assert_eq!(delegation.max_result_bytes, 32_768);
        assert_eq!(delegation.max_child_tool_calls, 20);
    }

    #[test]
    fn delegation_section_parses_every_documented_setting() {
        let config = HarnessConfig::from_toml_str(
            "[delegation]\n\
             enabled = true\n\
             max_rounds = 2\n\
             timeout_ms = 6000\n\
             max_task_bytes = 1024\n\
             max_result_bytes = 128\n\
             max_child_tool_calls = 3\n\
             [policy]\n\
             allowed_tools = [\"delegate_task\"]\n",
        )
        .expect("delegation section must parse");

        assert!(config.delegation.enabled);
        assert_eq!(config.delegation.max_rounds, 2);
        assert_eq!(config.delegation.timeout_ms, 6_000);
        assert_eq!(config.delegation.max_task_bytes, 1_024);
        assert_eq!(config.delegation.max_result_bytes, 128);
        assert_eq!(config.delegation.max_child_tool_calls, 3);
        assert_eq!(
            config.delegation.to_limits(),
            DelegationLimits {
                max_rounds: 2,
                timeout: Duration::from_millis(6_000),
                max_task_bytes: 1_024,
                max_result_bytes: 128,
                max_child_tool_calls: 3,
            }
        );
    }

    #[test]
    fn delegation_basic_boundaries_are_rejected_even_when_disabled() {
        for (field, document) in [
            ("delegation.max_rounds", "[delegation]\nmax_rounds = 0\n"),
            (
                "delegation.max_child_tool_calls",
                "[delegation]\nmax_child_tool_calls = 0\n",
            ),
            (
                "delegation.max_task_bytes",
                "[delegation]\nmax_task_bytes = 0\n",
            ),
            (
                "delegation.max_result_bytes",
                "[delegation]\nmax_result_bytes = 127\n",
            ),
        ] {
            let error = HarnessConfig::from_toml_str(document)
                .expect_err("invalid delegation boundary must fail at load time");
            assert!(
                error.to_string().contains(field),
                "error for {document:?} should name {field}: {error}"
            );
        }
    }

    #[test]
    fn enabling_delegation_requires_the_tool_in_the_allowlist() {
        let error = HarnessConfig::from_toml_str(
            "[delegation]\nenabled = true\n\
             [policy]\nallowed_tools = [\"bash\"]\n",
        )
        .expect_err("enabled delegation without the tool must fail");

        assert!(
            error.to_string().contains("policy.allowed_tools"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("delegate_task"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn delegation_timeout_must_exceed_the_fixed_child_tool_timeout() {
        let error = HarnessConfig::from_toml_str(
            "[delegation]\nenabled = true\ntimeout_ms = 5000\n\
             [policy]\nallowed_tools = [\"delegate_task\"]\n",
        )
        .expect_err("timeout equal to the child tool timeout must fail");

        assert!(
            error.to_string().contains("delegation.timeout_ms"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("5000"),
            "unexpected error: {error}"
        );

        HarnessConfig::from_toml_str(
            "[delegation]\nenabled = true\ntimeout_ms = 5001\n\
             [policy]\nallowed_tools = [\"delegate_task\"]\n",
        )
        .expect("one millisecond above the child tool timeout must be valid");
    }

    #[test]
    fn delegation_timeout_must_stay_below_the_parent_tool_timeout() {
        let error = HarnessConfig::from_toml_str(
            "[budget]\ntool_timeout_ms = 8000\n\
             [delegation]\nenabled = true\ntimeout_ms = 8000\n\
             [policy]\nallowed_tools = [\"delegate_task\"]\n",
        )
        .expect_err("timeout equal to the parent tool timeout must fail");

        assert!(
            error.to_string().contains("delegation.timeout_ms"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("budget.tool_timeout_ms"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn delegation_result_budget_must_not_exceed_the_parent_budget() {
        let error = HarnessConfig::from_toml_str(
            "[budget]\nmax_result_bytes = 1000\n\
             [delegation]\nenabled = true\nmax_result_bytes = 1001\n\
             [policy]\nallowed_tools = [\"delegate_task\"]\n",
        )
        .expect_err("delegation result budget above the parent budget must fail");

        assert!(
            error.to_string().contains("delegation.max_result_bytes"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("budget.max_result_bytes"),
            "unexpected error: {error}"
        );

        HarnessConfig::from_toml_str(
            "[budget]\nmax_result_bytes = 1000\n\
             [delegation]\nenabled = true\nmax_result_bytes = 1000\n\
             [policy]\nallowed_tools = [\"delegate_task\"]\n",
        )
        .expect("equal result budgets must be valid");
    }

    #[test]
    fn disabled_delegation_skips_cross_field_checks_but_keeps_basic_ones() {
        let config = HarnessConfig::from_toml_str(
            "[budget]\ntool_timeout_ms = 1\nmax_result_bytes = 128\n\
             [policy]\nallowed_tools = []\n\
             [delegation]\nenabled = false\ntimeout_ms = 8000\nmax_result_bytes = 32768\n",
        )
        .expect("disabled delegation must not enforce cross-field constraints");

        assert!(!config.delegation.enabled);
        assert_eq!(config.budget.tool_timeout_ms, 1);
        assert_eq!(config.delegation.timeout_ms, 8_000);
        assert_eq!(config.delegation.max_result_bytes, 32_768);
    }

    #[test]
    fn delegation_rejects_the_unprovided_child_tool_timeout_field() {
        let error = HarnessConfig::from_toml_str("[delegation]\nchild_tool_timeout_ms = 5000\n")
            .expect_err("child tool timeout is fixed and has no config field");

        assert!(
            error.to_string().contains("child_tool_timeout_ms"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn blank_commands_and_working_dirs_are_rejected() {
        let blank_name =
            HarnessConfig::from_toml_str("[acceptance]\nname = \"  \"\ncommand = \"true\"\n")
                .expect_err("blank acceptance name must fail");
        assert!(blank_name.to_string().contains("acceptance.name"));

        let blank_dir = HarnessConfig::from_toml_str(
            "[sensor]\nname = \"check\"\ncommand = \"true\"\nworking_dir = \"\"\n",
        )
        .expect_err("blank working directory must fail");
        assert!(blank_dir.to_string().contains("working_dir"));
    }
}
