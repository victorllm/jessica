//! TOML configuration for the governed CLI.
//!
//! Every setting that governs a session is explicit in the file: budgets,
//! the tool allowlist, the post-tool command sensor, the acceptance check, and
//! layered memory. Missing optional sections disable that component; the policy
//! default is the same allowlist the CLI has always exposed natively.

use std::path::{Path, PathBuf};
use std::time::Duration;

use jessica_core::types::Budget;
use serde::Deserialize;
use thiserror::Error;

/// Tools registered by the CLI and allowed when no policy section is given.
const NATIVE_TOOLS: &[&str] = &["bash", "read_file", "write_file"];

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

/// Complete harness configuration loaded from a TOML file.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HarnessConfig {
    /// Session budgets enforced by the runtime and harness guards.
    pub budget: BudgetConfig,
    /// Tool allowlist.
    pub policy: PolicyConfig,
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
