//! Assembly of the governed CLI runtime and one-turn execution.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use jessica_core::llm::{LlmError, LlmProvider};
use jessica_core::tool::Tool;
use jessica_core::types::{Message, SessionEndReason, SessionId};
use jessica_engine::replay::ReplayError;
use jessica_engine::{EventRecorder, RecorderError, UpdateSink};
use jessica_runtime::{DelegationError, recover_session};
use jessica_tools::WasmTool;
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::acp_store::AcpStoreError;
use crate::config::{ConfigError, HarnessConfig};
use crate::engine_ports::build_engine;
use crate::governance::ToolPermissionGate;
use crate::memory::MemoryAssembly;
use crate::policy_v2::PolicyError;
use crate::shutdown::ShutdownController;
use crate::skills::{SkillCommandError, SkillsAssembly};
use crate::tools::{CatalogCandidate, CatalogError, ToolAnnotations, ToolMetadata};

pub(crate) use jessica_engine::TurnOutcome;

/// Workspace and cancellation context for one turn.
///
/// The canonical root is the boundary for every per-turn file tool, bash
/// command, sensor, acceptance check, and delegation child. The startup root
/// owned by [`CliRuntime`] remains responsible for process-level resources
/// such as memory, skills, wasm plugins, and the event recorder.
pub(crate) struct TurnScope {
    /// Canonical workspace root for this turn.
    root: PathBuf,
    /// Token whose cancellation ends this turn.
    cancel_parent: CancellationToken,
}

impl TurnScope {
    /// Canonicalizes `root` and binds the turn to `cancel_parent`.
    ///
    /// Canonicalization runs on the blocking pool because a network or FUSE
    /// workspace can block. A path that does not exist is a structured error;
    /// the turn never falls back to an uncanonicalized root.
    pub(crate) async fn new(
        root: PathBuf,
        cancel_parent: CancellationToken,
    ) -> Result<Self, CliError> {
        let requested = root.display().to_string();
        let root = tokio::task::spawn_blocking(move || std::fs::canonicalize(root))
            .await
            .map_err(CliError::CanonicalizeRootTask)?
            .map_err(|source| CliError::CanonicalizeRoot {
                path: requested,
                source,
            })?;
        Ok(Self {
            root,
            cancel_parent,
        })
    }

    /// Returns the canonical workspace root.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the token whose cancellation ends this turn.
    pub(crate) fn cancel_parent(&self) -> &CancellationToken {
        &self.cancel_parent
    }
}

/// Inputs for one reusable turn execution.
pub(crate) struct TurnRequest {
    /// Workspace and cancellation context for this turn.
    pub(crate) scope: TurnScope,
    /// Conversation established before this turn's user input.
    ///
    /// The CLI passes an empty slice because it starts one session per turn.
    /// A multi-turn client carries earlier messages here. History is placed
    /// after the system and guide messages and before the new user input.
    pub(crate) history: Vec<Message>,
    /// New user input for this turn.
    pub(crate) user_input: String,
    /// Programmatic event consumer.
    ///
    /// When present the engine runs the pump silently and publishes every
    /// event to this sink in pump order. When absent the pump keeps the
    /// existing human-console rendering.
    pub(crate) update_sink: Option<Arc<dyn UpdateSink>>,
    /// Per-turn override for the configured acceptance retry budget.
    ///
    /// `None` uses `config.acceptance.max_retries`. `Some(0)` runs the check
    /// once without retrying even when the configuration allows retries.
    pub(crate) acceptance_retries: Option<u32>,
    /// Session id to use for the first attempt instead of a generated CLI id.
    ///
    /// ACP supplies its process-unique turn id so events, persistence, and
    /// memory all share the protocol's identifier. `None` keeps the existing
    /// per-turn `cli-{pid}-{counter}` id. Acceptance retries still allocate
    /// their own CLI ids.
    pub(crate) session_id: Option<SessionId>,
    /// Session-scoped tools registered in addition to the built-in and WASM
    /// tools.
    ///
    /// ACP uses this for MCP tools discovered for one session. Candidates keep
    /// the server's advisory annotations so the catalog can classify
    /// destructive or open-world tools conservatively. The engine wraps every
    /// executable tool, so policy, timeout, approval, and result-size rules
    /// apply exactly as they do to native tools. The CLI and resume paths pass
    /// an empty vector.
    pub(crate) extra_tools: Vec<CatalogCandidate>,
    /// Optional approval gate for sensitive native tools.
    ///
    /// ACP supplies a gate that asks the client through
    /// `session/request_permission`. The CLI and resume paths pass `None`,
    /// leaving sensitive tools governed by policy alone. The fixed native
    /// sensitive set plus destructive or open-world WASM/MCP descriptors are
    /// classified by the catalog; read-only annotated external tools do not
    /// request permission.
    pub(crate) permission_gate: Option<Arc<dyn ToolPermissionGate>>,
}

/// A compiled WASM tool plus the manifest metadata used by the catalog.
///
/// The executable handle is process-wide because compilation is expensive.
/// Metadata is copied into a descriptor-only view for each turn; annotations
/// never grant capabilities beyond the sandbox policy.
pub(crate) struct LoadedWasmTool {
    /// Compiled tool handle.
    pub(crate) tool: Arc<dyn Tool>,
    /// Manifest version and advisory annotations.
    pub(crate) metadata: ToolMetadata,
}

/// Owns the provider, configuration snapshot source, and workspace.
pub(crate) struct CliRuntime {
    provider: Arc<dyn LlmProvider>,
    /// Source of the configuration snapshot used by the next turn.
    ///
    /// A REPL-only watcher publishes a new snapshot after a valid reload. A
    /// one-shot or resume run has no watcher, so the receiver keeps returning
    /// the startup configuration.
    config: watch::Receiver<Arc<HarnessConfig>>,
    root: PathBuf,
    wasm_tools: Arc<Vec<LoadedWasmTool>>,
    recorder: Option<Arc<EventRecorder>>,
    memory: Option<Arc<MemoryAssembly>>,
    skills: Option<Arc<SkillsAssembly>>,
    /// Process-level shutdown state shared by every turn and the REPL.
    shutdown: Arc<ShutdownController>,
}

impl CliRuntime {
    /// Creates a runtime for the current workspace.
    ///
    /// `config` must already contain the fully validated startup configuration;
    /// the same value is cloned at the start of every turn so the engine can
    /// build the whole turn from one immutable snapshot.
    /// `wasm_tools` contains plugins already loaded by [`load_wasm_tools`].
    /// Compilation happens once at startup rather than on every turn.
    /// `recorder` is present when the configuration enables event persistence.
    /// `memory` is present only when enabled and opened successfully.
    /// `skills` is present only when enabled and opened successfully.
    /// `shutdown` is the process-level controller; every turn derives a child
    /// token from it instead of installing its own signal listener.
    ///
    /// The constructor is the single assembly point for independently owned
    /// handles; grouping them into another struct would only rename the same
    /// fields.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        provider: Arc<dyn LlmProvider>,
        config: watch::Receiver<Arc<HarnessConfig>>,
        root: PathBuf,
        wasm_tools: Vec<LoadedWasmTool>,
        recorder: Option<Arc<EventRecorder>>,
        memory: Option<MemoryAssembly>,
        skills: Option<SkillsAssembly>,
        shutdown: Arc<ShutdownController>,
    ) -> Self {
        Self {
            provider,
            config,
            root,
            wasm_tools: Arc::new(wasm_tools),
            recorder,
            memory: memory.map(Arc::new),
            skills: skills.map(Arc::new),
            shutdown,
        }
    }

    /// Returns the immutable configuration snapshot for the next turn.
    ///
    /// Each turn clones exactly one `Arc`. Components rebuilt inside that turn
    /// (system prompt, tool schema, guards, policy, delegation, sensors, and
    /// acceptance) all read from this same snapshot. The watcher may publish a
    /// newer snapshot while the turn runs; that value is observed only by the
    /// next turn.
    fn config_snapshot(&self) -> Arc<HarnessConfig> {
        self.config.borrow().clone()
    }

    /// Drains the skill worker before the process exits.
    ///
    /// The drain is bounded by `drain_timeout_ms`; failures only warn and never
    /// change the process exit code.
    pub(crate) async fn shutdown(&self) {
        if let Some(skills) = &self.skills {
            skills.drain().await;
        }
    }

    /// Runs one user request with the configured governance.
    ///
    /// This is the CLI entry point: it binds the turn to the startup workspace
    /// root, starts without history, and uses the configured acceptance retry
    /// budget. REPL, one-shot, and resume behavior is unchanged; the reusable
    /// path is [`CliRuntime::run_turn_request`].
    pub(crate) async fn run_turn(&self, input: String) -> Result<SessionEndReason, CliError> {
        let scope = TurnScope::new(self.root.clone(), self.shutdown.child_token()).await?;
        let outcome = self
            .run_turn_request(TurnRequest {
                scope,
                history: Vec::new(),
                user_input: input,
                update_sink: None,
                acceptance_retries: None,
                session_id: None,
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await?;
        Ok(outcome.reason)
    }

    /// Executes one turn with an explicit workspace scope and history.
    ///
    /// All ordering is delegated to `jessica-engine`. The CLI contributes its
    /// configuration snapshot, native/WASM/MCP tools, event journal, sensors,
    /// acceptance check, memory and skills, system prompt, and process-local
    /// identifiers through host ports. Memory and skills are recalled once by
    /// the engine before the first attempt; acceptance retries reuse the same
    /// prefix, and post-turn work runs only for a completed, untripped turn.
    pub(crate) async fn run_turn_request(
        &self,
        request: TurnRequest,
    ) -> Result<TurnOutcome, CliError> {
        let TurnRequest {
            scope,
            history,
            user_input,
            update_sink,
            acceptance_retries,
            session_id,
            extra_tools,
            permission_gate,
        } = request;

        // One snapshot for the whole turn: the tool provider, prompt, sensors,
        // acceptance check, policy, and budgets all read this same value.
        let config = self.config_snapshot();
        let engine = build_engine(
            Arc::clone(&self.provider),
            config,
            scope.root(),
            self.recorder.as_ref(),
            &self.wasm_tools,
            self.memory.as_ref(),
            self.skills.as_ref(),
            &extra_tools,
            permission_gate,
            true,
        )?;

        let mut engine_request = jessica_engine::TurnRequest::new(user_input)
            .with_workspace_root(scope.root().to_path_buf())
            .with_history(history)
            .with_cancellation(scope.cancel_parent().clone());
        if let Some(session_id) = session_id {
            engine_request = engine_request.with_session_id(session_id);
        }
        if let Some(retries) = acceptance_retries {
            engine_request = engine_request.with_acceptance_retries(retries);
        }
        if let Some(sink) = update_sink {
            engine_request = engine_request.with_observer(sink);
        }

        engine
            .run_turn_request(engine_request)
            .await
            .map_err(CliError::from)
    }

    /// Continues the interrupted loop recorded for `session_id`.
    ///
    /// The CLI keeps the read-only pre-check so an already-ended session
    /// reports its recorded reason without assembling an engine. An unfinished
    /// session is handed to the engine, which reloads the same evidence,
    /// resumes from the first missing model round, and never exposes
    /// `delegate_task`.
    pub(crate) async fn resume_turn(
        &self,
        session_id: SessionId,
    ) -> Result<SessionEndReason, CliError> {
        let recorder = self
            .recorder
            .as_ref()
            .ok_or(CliError::ResumeRequiresEventStore)?;
        let stored =
            recorder
                .load(session_id.clone())
                .await?
                .ok_or_else(|| CliError::UnknownSession {
                    session_id: session_id.to_string(),
                })?;
        let checkpoint =
            recover_session(&session_id, stored.initial_messages, &stored.events, None)?;

        if let Some(reason) = checkpoint.end_reason() {
            println!("[resume] session {session_id} already ended: {reason:?}");
            return Ok(reason);
        }

        let config = self.config_snapshot();
        let scope = TurnScope::new(self.root.clone(), self.shutdown.child_token()).await?;
        let engine = build_engine(
            Arc::clone(&self.provider),
            config,
            scope.root(),
            self.recorder.as_ref(),
            &self.wasm_tools,
            self.memory.as_ref(),
            self.skills.as_ref(),
            &[],
            None,
            false,
        )?;
        let request = jessica_engine::TurnRequest::new(String::new())
            .with_workspace_root(scope.root().to_path_buf())
            .with_cancellation(scope.cancel_parent().clone());
        let outcome = engine
            .resume_turn(session_id, request)
            .await
            .map_err(CliError::from)?;
        Ok(outcome.reason)
    }
}

/// Loads every wasm tool manifest declared in `config`.
///
/// Manifest reading and wasm compilation are blocking, so all manifests are
/// loaded in one `spawn_blocking` task. Relative manifest paths resolve
/// against the workspace root. The returned tools are shared across turns;
/// each turn still wraps them in fresh governance. Manifest version and
/// annotations are captured once alongside the compiled handle.
pub(crate) async fn load_wasm_tools(
    config: &HarnessConfig,
    root: &Path,
) -> Result<Vec<LoadedWasmTool>, CliError> {
    if config.wasm.manifests.is_empty() {
        return Ok(Vec::new());
    }

    let paths: Vec<PathBuf> = config
        .wasm
        .manifests
        .iter()
        .map(|path| {
            if path.is_absolute() {
                path.clone()
            } else {
                root.join(path)
            }
        })
        .collect();

    tokio::task::spawn_blocking(move || {
        paths
            .iter()
            .map(|path| {
                WasmTool::load(path).map(|tool| LoadedWasmTool {
                    metadata: wasm_metadata(&tool),
                    tool: Arc::new(tool) as Arc<dyn Tool>,
                })
            })
            .collect::<Result<Vec<_>, _>>()
    })
    .await
    .map_err(CliError::WasmToolTask)?
    .map_err(CliError::WasmTool)
}

/// Converts manifest metadata into catalog metadata.
fn wasm_metadata(tool: &WasmTool) -> ToolMetadata {
    ToolMetadata {
        version: Some(format!("manifest-v{}", tool.manifest_version())),
        annotations: tool
            .annotations()
            .map_or_else(ToolAnnotations::none, |annotations| ToolAnnotations {
                read_only: Some(annotations.read_only),
                destructive: Some(annotations.destructive),
                open_world: Some(annotations.open_world),
            }),
    }
}

/// Errors produced while assembling or running a CLI turn.
#[derive(Debug, Error)]
pub(crate) enum CliError {
    /// The LLM provider could not be configured.
    #[error("provider setup failed: {0}")]
    Provider(#[from] LlmError),
    /// `JESSICA_PROVIDER` named an unsupported provider.
    #[error("unsupported JESSICA_PROVIDER `{name}`; expected `openai` or `anthropic`")]
    InvalidProvider {
        /// Unsupported value read from the environment.
        name: String,
    },
    /// The harness configuration could not be loaded or validated.
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),
    /// The tool catalog could not be built or queried.
    #[error("tool catalog error: {0}")]
    ToolCatalog(#[from] CatalogError),
    /// Policy v2 could not be initialized or evaluated.
    #[error("policy error: {0}")]
    PolicyV2(#[from] PolicyError),
    /// A `tools` subcommand argument was missing, unknown, or malformed.
    #[error("invalid tools command: {message}")]
    InvalidToolsCommand {
        /// Human-readable parse failure.
        message: String,
    },
    /// A `policy` subcommand argument was missing, unknown, or malformed.
    #[error("invalid policy command: {message}")]
    InvalidPolicyCommand {
        /// Human-readable parse failure.
        message: String,
    },
    /// `policy simulate` could not evaluate the requested call.
    #[error("policy simulation failed: {message}")]
    PolicySimulation {
        /// Structured reason such as hidden tool, invalid JSON, or path error.
        message: String,
    },
    /// A wasm tool manifest or module could not be loaded.
    #[error("failed to load wasm tools: {0}")]
    WasmTool(#[from] jessica_tools::WasmToolError),
    /// The blocking wasm tool loading task failed.
    #[error("wasm tool loading task failed: {0}")]
    WasmToolTask(#[source] tokio::task::JoinError),
    /// `--config` was given without a path.
    #[error("`--config` requires a path")]
    MissingConfigPath,
    /// `--config` was given more than once.
    #[error("`--config` may be given only once")]
    DuplicateConfig,
    /// `config` was given an unexpected argument.
    #[error("unexpected config argument `{argument}`")]
    InvalidConfigCommand {
        /// Unrecognized argument.
        argument: String,
    },
    /// The effective configuration could not be serialized.
    #[error("failed to serialize the effective configuration: {0}")]
    ConfigSerialization(#[source] serde_json::Error),
    /// `--resume` was given without a session id.
    #[error("`--resume` requires a session id")]
    MissingResumeSessionId,
    /// `--resume` was given more than once.
    #[error("`--resume` may be given only once")]
    DuplicateResume,
    /// A prompt and `--resume` cannot be used together.
    #[error("`--resume` cannot be combined with a prompt")]
    ResumeWithPrompt,
    /// `--resume` was requested while trace persistence was disabled.
    #[error("`--resume` requires trace persistence (`profile.trace = \"strict\"`)")]
    ResumeRequiresEventStore,
    /// `replay` was requested while trace persistence was disabled.
    #[error("`replay` requires trace persistence (`profile.trace = \"strict\"`)")]
    ReplayRequiresEventStore,
    /// A `replay` argument was not recognized.
    #[error("unexpected replay argument `{argument}`")]
    UnknownReplayArgument {
        /// Unrecognized argument.
        argument: String,
    },
    /// `replay --session` was given without a session id.
    #[error("`replay --session` requires a session id")]
    MissingReplaySession,
    /// `replay --session` was given more than once.
    #[error("`replay --session` may be given only once")]
    DuplicateReplaySession,
    /// `replay --out` was given without a path.
    #[error("`replay --out` requires a path")]
    MissingReplayOut,
    /// `replay --out` was given more than once.
    #[error("`replay --out` may be given only once")]
    DuplicateReplayOut,
    /// Offline replay failed.
    #[error("replay failed: {0}")]
    Replay(#[from] ReplayError),
    /// `trace` was requested while trace persistence was disabled.
    #[error("`trace` requires trace persistence (`profile.trace = \"strict\"`)")]
    TraceRequiresEventStore,
    /// A `trace` argument was missing, unknown, or malformed.
    #[error("invalid trace command: {message}")]
    InvalidTraceCommand {
        /// Human-readable parse failure.
        message: String,
    },
    /// `trace --turn` was given without a turn id.
    #[error("`trace --turn` requires a turn id")]
    MissingTraceTurn,
    /// `trace --turn` was given more than once.
    #[error("`trace --turn` may be given only once")]
    DuplicateTraceTurn,
    /// `trace verify`/`trace digest` was given neither `--turn` nor `--all`.
    #[error("`trace` requires `--turn <turn_id>` or `--all`")]
    MissingTraceSelection,
    /// `trace verify`/`trace digest` was given both `--turn` and `--all`.
    #[error("`trace --turn` cannot be combined with `--all`")]
    TraceSelectionConflict,
    /// `trace export --out` was given without a path.
    #[error("`trace export --out` requires a path")]
    MissingTraceExportOut,
    /// `trace export --out` was given more than once.
    #[error("`trace export --out` may be given only once")]
    DuplicateTraceExportOut,
    /// `trace verify-bundle --bundle` was given without a path.
    #[error("`trace verify-bundle --bundle` requires a path")]
    MissingTraceBundle,
    /// `trace verify-bundle --bundle` was given more than once.
    #[error("`trace verify-bundle --bundle` may be given only once")]
    DuplicateTraceBundle,
    /// The requested turn trace does not exist.
    #[error("turn `{turn_id}` was not found in the event store")]
    UnknownTurn {
        /// Turn id that could not be found.
        turn_id: String,
    },
    /// The read-only event store could not be opened or read.
    #[error("trace command could not access the event store: {0}")]
    TraceStore(#[from] jessica_memory::EventStoreError),
    /// The blocking trace task failed.
    #[error("trace task failed: {0}")]
    TraceTask(#[source] tokio::task::JoinError),
    /// The trace report could not be serialized.
    #[error("failed to serialize the trace report: {0}")]
    TraceReport(#[source] serde_json::Error),
    /// A turn's digest could not be computed from trustworthy evidence.
    #[error("cannot compute a trace digest for turn `{turn_id}`: {message}")]
    TraceDigestFailed {
        /// Turn whose chain could not be validated.
        turn_id: String,
        /// Description of the evidence failure.
        message: String,
    },
    /// One or more traces failed verification.
    #[error("trace verification failed: {summary}")]
    TraceVerificationFailed {
        /// One-line summary of the report.
        summary: String,
    },
    /// The trace is valid evidence but the replay engine cannot exercise it.
    #[error("turn `{turn_id}` cannot be replay-verified: {reason}")]
    TraceReplayUnsupported {
        /// Turn whose parity check could not run.
        turn_id: String,
        /// Why the trace is outside the replayable subset.
        reason: String,
    },
    /// Multiple valid traces cannot be exercised by the replay engine.
    #[error("{count} turn(s) cannot be replay-verified: {turns}")]
    TraceReplayUnsupportedBatch {
        /// Number of affected turns.
        count: usize,
        /// Comma-separated affected turn ids.
        turns: String,
    },
    /// A trace could not be exported as portable evidence.
    #[error("trace export failed: {summary}")]
    TraceExportFailed {
        /// One-line reason for the refusal.
        summary: String,
    },
    /// A bundle file could not be inspected or read.
    #[error("failed to read trace bundle `{path}`: {source}")]
    TraceBundleRead {
        /// Bundle path that could not be read.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// A bundle exceeded the offline verifier's size limit.
    #[error("trace bundle `{path}` is {bytes} bytes, exceeding the {limit}-byte limit")]
    TraceBundleTooLarge {
        /// Bundle path that was rejected.
        path: PathBuf,
        /// Observed file size.
        bytes: u64,
        /// Maximum accepted size.
        limit: u64,
    },
    /// A bundle file was not valid JSON for the bundle schema.
    #[error("failed to decode trace bundle `{path}`: {source}")]
    TraceBundleDecode {
        /// Bundle path that could not be decoded.
        path: PathBuf,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// A bundle used an unsupported structure version.
    #[error("trace bundle version {found} is unsupported; this build supports version {supported}")]
    TraceBundleVersion {
        /// Version declared by the bundle.
        found: u32,
        /// Version supported by this build.
        supported: u32,
    },
    /// A bundle used an unrecognized format identifier.
    #[error("trace bundle format `{found}` is unsupported")]
    TraceBundleFormat {
        /// Format identifier declared by the bundle.
        found: String,
    },
    /// A bundle listed the same turn more than once.
    #[error("trace bundle contains turn `{turn_id}` more than once")]
    TraceBundleDuplicateTurn {
        /// Turn that was repeated.
        turn_id: String,
    },
    /// A bundle digest field was not canonical lowercase SHA-256 hex.
    #[error("trace bundle {field} is not 64 lowercase hexadecimal characters: `{value}`")]
    TraceBundleDigest {
        /// Field that failed validation.
        field: &'static str,
        /// Rejected value.
        value: String,
    },
    /// A bundle could not be written.
    #[error("failed to write trace bundle `{path}`: {source}")]
    TraceBundleWrite {
        /// Destination path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// The requested session is absent from the event store.
    #[error("session `{session_id}` was not found in the event store")]
    UnknownSession {
        /// Session id that could not be loaded.
        session_id: String,
    },
    /// Recorded session state could not be recovered.
    #[error("failed to recover session state: {0}")]
    Recovery(#[from] jessica_runtime::RecoveryError),
    /// Recording or loading events failed.
    #[error("event store failure: {0}")]
    Events(#[from] RecorderError),
    /// The turn trace could not be completed or closed.
    ///
    /// The trace intentionally has no `turn_ended` marker; callers must not
    /// present the turn as complete.
    #[error("turn `{turn_id}` trace is incomplete: {cause}")]
    TraceIncomplete {
        /// Turn whose trace is incomplete.
        turn_id: String,
        /// Failure that prevented completion.
        cause: String,
    },
    /// ACP session recovery was configured while trace persistence was
    /// disabled.
    #[error("ACP session recovery requires trace persistence (`profile.trace = \"strict\"`)")]
    AcpPersistenceWithoutEvents,
    /// The ACP session index could not be opened.
    #[error("ACP session store failure: {0}")]
    AcpStore(#[from] AcpStoreError),
    /// The current directory could not be determined.
    #[error("failed to determine the current directory: {0}")]
    CurrentDir(#[source] io::Error),
    /// The blocking current-directory task failed.
    #[error("current-directory task failed: {0}")]
    CurrentDirTask(#[source] tokio::task::JoinError),
    /// The per-turn workspace root could not be canonicalized.
    #[error("failed to canonicalize the turn workspace root `{path}`: {source}")]
    CanonicalizeRoot {
        /// Root path exactly as requested by the caller.
        path: String,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// The blocking canonicalization task failed.
    #[error("workspace root canonicalization task failed: {0}")]
    CanonicalizeRootTask(#[source] tokio::task::JoinError),
    /// A `skills` argument was missing, unknown, or malformed.
    #[error("invalid skills command: {message}")]
    InvalidSkillsCommand {
        /// Human-readable parse failure.
        message: String,
    },
    /// A `skills` subcommand failed.
    #[error("skill command failed: {0}")]
    SkillsCommand(#[from] SkillCommandError),
    /// The delegation executor could not be assembled.
    #[error("delegation setup failed: {0}")]
    Delegation(#[from] DelegationError),
    /// The provider reliability wrapper could not be assembled.
    #[error("provider reliability setup failed: {0}")]
    Reliability(#[from] jessica_providers::ReliabilityConfigError),
    /// The ACP transport or protocol layer failed.
    #[error("ACP transport failed: {0}")]
    Acp(#[from] agent_client_protocol::Error),
    /// An `acp` argument was not recognized.
    #[error("unexpected acp argument `{argument}`")]
    UnknownAcpArgument {
        /// Unrecognized argument.
        argument: String,
    },
    /// The governed engine failed with an error the CLI does not reclassify.
    #[error("governed engine failed: {0}")]
    Engine(#[source] Box<jessica_engine::EngineError>),
}

impl From<jessica_engine::EngineError> for CliError {
    fn from(error: jessica_engine::EngineError) -> Self {
        use jessica_engine::EngineError as Engine;
        match error {
            Engine::TraceIncomplete { turn_id, cause } => Self::TraceIncomplete { turn_id, cause },
            Engine::UnknownSession { session_id } => Self::UnknownSession { session_id },
            Engine::Recorder(error) => Self::Events(error),
            Engine::Recovery(error) => Self::Recovery(error),
            Engine::Delegation(error) => Self::Delegation(error),
            Engine::Policy(error) => Self::PolicyV2(cli_policy_error(error)),
            Engine::Workspace { root, source } => Self::CanonicalizeRoot { path: root, source },
            Engine::CatalogTooLarge { exposed, limit } => {
                Self::ToolCatalog(CatalogError::TooManyTools { exposed, limit })
            }
            Engine::SchemaBudgetExceeded { bytes, limit } => {
                Self::ToolCatalog(CatalogError::SchemaBytes { bytes, limit })
            }
            Engine::DuplicateTool { name } => Self::ToolCatalog(CatalogError::Duplicate { name }),
            Engine::MissingToolImplementation { name } => {
                Self::ToolCatalog(CatalogError::MissingImplementation { name })
            }
            other => Self::Engine(Box::new(other)),
        }
    }
}

/// Maps an engine policy error back to the CLI's structural policy error.
fn cli_policy_error(error: jessica_engine::PolicyError) -> PolicyError {
    use jessica_engine::PolicyError as Engine;
    match error {
        Engine::Root { root, source } => PolicyError::Root { root, source },
        Engine::EmptyPath => PolicyError::EmptyPath,
        Engine::Escape { path } => PolicyError::Escape { path },
        Engine::NotFound { path } => PolicyError::NotFound { path },
        Engine::Io { path, source } => PolicyError::Io { path, source },
        Engine::NonUtf8 { path } => PolicyError::NonUtf8 { path },
        Engine::InvalidGlob { glob, reason } => PolicyError::InvalidGlob { glob, reason },
        Engine::InvalidPatch { reason } => PolicyError::InvalidPatch { reason },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use futures::StreamExt;
    use jessica_core::llm::{LlmEvent, LlmStream};
    use jessica_core::tool::ToolSchema;
    use jessica_core::trace::{TraceEvent, TracePayload, TraceTurnMode};
    use jessica_core::types::{AgentEvent, Role, TokenUsage, ToolCall};
    use jessica_memory::{
        EventStore, MemoryLog, MemoryRecord, SqliteMemoryStore, StoredTraceTurn, Timestamp,
    };
    use jessica_runtime::{CHILD_AGENT_SYSTEM_PROMPT, DELEGATE_TASK_TOOL_NAME};
    use serde_json::json;

    use crate::governance::ToolPermissionDecision;
    use crate::shutdown::ShutdownSignal;

    /// One captured provider call.
    #[derive(Clone)]
    struct ProviderCall {
        /// Messages the loop or executor sent.
        messages: Vec<Message>,
        /// Schemas the provider exposed.
        tools: Vec<ToolSchema>,
    }

    /// A config publication deferred until the next provider call.
    type PendingReload = Option<(watch::Sender<Arc<HarnessConfig>>, Arc<HarnessConfig>)>;

    /// Provider that replays scripted event batches and records every call.
    struct ScriptedProvider {
        /// Responses in call order.
        responses: Mutex<VecDeque<Vec<LlmEvent>>>,
        /// Calls observed so far.
        calls: Mutex<Vec<ProviderCall>>,
        /// Config snapshot to publish during the next provider call.
        reload: Mutex<PendingReload>,
    }

    impl ScriptedProvider {
        /// Creates a provider from response batches.
        fn new(responses: Vec<Vec<LlmEvent>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                calls: Mutex::new(Vec::new()),
                reload: Mutex::new(None),
            }
        }

        /// Publishes `config` while the next model call is in flight.
        ///
        /// This simulates the REPL watcher firing mid-turn: the turn already
        /// cloned its snapshot, so only a later turn may observe the value.
        fn reload_during_next_call(
            &self,
            sender: watch::Sender<Arc<HarnessConfig>>,
            config: HarnessConfig,
        ) {
            *self.reload.lock().expect("reload mutex poisoned") = Some((sender, Arc::new(config)));
        }

        /// Returns a snapshot of the calls observed so far.
        fn calls(&self) -> Vec<ProviderCall> {
            self.calls
                .lock()
                .expect("provider calls mutex poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl LlmProvider for ScriptedProvider {
        async fn stream_chat(
            &self,
            messages: &[Message],
            tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            self.calls
                .lock()
                .expect("provider calls mutex poisoned")
                .push(ProviderCall {
                    messages: messages.to_vec(),
                    tools: tools.to_vec(),
                });
            if let Some((sender, config)) =
                self.reload.lock().expect("reload mutex poisoned").take()
            {
                let _ = sender.send(config);
            }
            let events = self
                .responses
                .lock()
                .expect("provider responses mutex poisoned")
                .pop_front()
                .expect("scripted provider ran out of responses");
            Ok(futures::stream::iter(events).boxed())
        }
    }

    /// Provider whose model stream never produces an event.
    struct PendingProvider {
        /// Notified as soon as the provider is entered.
        started: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl LlmProvider for PendingProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            self.started.notify_one();
            Ok(futures::stream::pending().boxed())
        }
    }

    /// Builds a text response that reports `tokens` total usage.
    fn done_text(text: &str, tokens: u64) -> Vec<LlmEvent> {
        vec![
            LlmEvent::TextDelta {
                text: text.to_owned(),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(tokens / 2, tokens - tokens / 2)),
            },
        ]
    }

    /// Builds a parent response that requests one delegation.
    fn delegate_request() -> Vec<LlmEvent> {
        delegate_request_for(&["read_file"])
    }

    /// Builds a delegation request naming exactly `tools`.
    fn delegate_request_for(tools: &[&str]) -> Vec<LlmEvent> {
        vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new(
                    "parent-call",
                    DELEGATE_TASK_TOOL_NAME,
                    json!({"task": "child task", "tools": tools}),
                ),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(5, 5)),
            },
        ]
    }

    /// Builds a parent response that requests one unknown tool.
    ///
    /// The loop reports the unknown tool as an error result and spends another
    /// round; no real tool executes.
    fn unknown_tool_request(id: &str) -> Vec<LlmEvent> {
        vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new(id, "unknown_tool", json!({})),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(1, 1)),
            },
        ]
    }

    /// Builds a parent response that requests one bash command.
    fn bash_request(id: &str, command: &str) -> Vec<LlmEvent> {
        vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new(id, "bash", json!({"command": command})),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(1, 1)),
            },
        ]
    }

    /// Builds a response that asks `read_file` for one path.
    fn read_request(id: &str, path: &str) -> Vec<LlmEvent> {
        vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new(id, "read_file", json!({ "path": path })),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(1, 1)),
            },
        ]
    }

    /// Builds a response that asks `write_file` for one path and content.
    fn write_request(id: &str, path: &str, content: &str) -> Vec<LlmEvent> {
        vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new(
                    id,
                    "write_file",
                    json!({ "path": path, "content": content }),
                ),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(1, 1)),
            },
        ]
    }

    /// Temporary directory removed when the test ends.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        /// Creates a unique canonical temporary directory.
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "jessica-app-{label}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("the clock must be after the epoch")
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).expect("temporary directory must be creatable");
            let path = std::fs::canonicalize(&path).expect("temporary directory must resolve");
            Self { path }
        }

        /// Returns the canonical path.
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Sink double that records every event published by the pump.
    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<AgentEvent>>,
    }

    impl RecordingSink {
        /// Returns a snapshot of the events observed so far.
        fn events(&self) -> Vec<AgentEvent> {
            self.events
                .lock()
                .expect("sink events mutex poisoned")
                .clone()
        }
    }

    impl UpdateSink for RecordingSink {
        fn publish(&self, event: &AgentEvent) {
            self.events
                .lock()
                .expect("sink events mutex poisoned")
                .push(event.clone());
        }
    }

    /// Extracts schema names in registry order.
    fn schema_names(tools: &[ToolSchema]) -> Vec<&str> {
        tools
            .iter()
            .map(|schema| {
                schema["function"]["name"]
                    .as_str()
                    .expect("tool schema must have a name")
            })
            .collect()
    }

    /// Builds an enabled delegation config with the required allowlist.
    fn enabled_config() -> HarnessConfig {
        HarnessConfig::from_toml_str(
            "[delegation]\nenabled = true\n\
             [policy]\n\
             allowed_tools = [\"bash\", \"read_file\", \"write_file\", \"delegate_task\"]\n\
             [tools.bash]\nenabled = true\n",
        )
        .expect("enabled delegation config must parse")
    }

    /// Builds a runtime over the given provider and config.
    fn runtime_with(provider: Arc<ScriptedProvider>, config: HarnessConfig) -> CliRuntime {
        runtime_at(provider, config, PathBuf::from("."))
    }

    /// Builds a runtime with an explicit workspace root.
    fn runtime_at(
        provider: Arc<ScriptedProvider>,
        config: HarnessConfig,
        root: PathBuf,
    ) -> CliRuntime {
        let (_sender, receiver) = watch::channel(Arc::new(config));
        CliRuntime::new(
            provider,
            receiver,
            root,
            Vec::new(),
            None,
            None,
            None,
            Arc::new(ShutdownController::new()),
        )
    }

    /// Builds a runtime with an already-opened memory assembly.
    fn runtime_with_memory(
        provider: Arc<ScriptedProvider>,
        config: HarnessConfig,
        root: PathBuf,
        memory: MemoryAssembly,
    ) -> CliRuntime {
        let (_sender, receiver) = watch::channel(Arc::new(config));
        CliRuntime::new(
            provider,
            receiver,
            root,
            Vec::new(),
            None,
            Some(memory),
            None,
            Arc::new(ShutdownController::new()),
        )
    }

    /// Creates a turn scope rooted at `root`.
    async fn test_scope_at(root: PathBuf) -> TurnScope {
        TurnScope::new(root, CancellationToken::new())
            .await
            .expect("the test workspace must canonicalize")
    }

    /// Creates a turn scope rooted at the process current directory.
    async fn test_scope() -> TurnScope {
        test_scope_at(PathBuf::from(".")).await
    }

    /// Reads the live memory entries written for `session`.
    fn memory_entries(database: &Path, root: &Path, session: &SessionId) -> Vec<MemoryRecord> {
        let store =
            SqliteMemoryStore::open(database, root, None).expect("memory store must reopen");
        let log = MemoryLog::open(database).expect("memory log must reopen");
        log.recent_for_session(session, store.workspace_scope(), 10)
            .expect("memory entries must load")
    }

    /// Gate that records every approval request and allows it.
    #[derive(Default)]
    struct AllowOnceGate {
        calls: Mutex<Vec<ToolCall>>,
    }

    impl AllowOnceGate {
        /// Returns the tool calls that requested approval.
        fn calls(&self) -> Vec<ToolCall> {
            self.calls
                .lock()
                .expect("gate calls mutex poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl ToolPermissionGate for AllowOnceGate {
        async fn request_permission(&self, call: &ToolCall) -> ToolPermissionDecision {
            self.calls
                .lock()
                .expect("gate calls mutex poisoned")
                .push(call.clone());
            ToolPermissionDecision::Allow
        }
    }

    /// Builds a runtime over the given provider, config, and event database.
    async fn runtime_with_recorder(
        provider: Arc<ScriptedProvider>,
        config: HarnessConfig,
        database: PathBuf,
    ) -> CliRuntime {
        let recorder = Arc::new(
            EventRecorder::open(database)
                .await
                .expect("event recorder must open"),
        );
        let (_sender, receiver) = watch::channel(Arc::new(config));
        CliRuntime::new(
            provider,
            receiver,
            PathBuf::from("."),
            Vec::new(),
            Some(recorder),
            None,
            None,
            Arc::new(ShutdownController::new()),
        )
    }

    /// Reads the only turn trace in `database`.
    async fn load_trace(database: PathBuf) -> StoredTraceTurn {
        tokio::task::spawn_blocking(move || {
            let store = EventStore::open(&database).expect("event store must open");
            let summaries = store.trace_turns(10).expect("turn summaries must read");
            assert_eq!(
                summaries.len(),
                1,
                "exactly one turn must be recorded: {summaries:?}"
            );
            store
                .trace_turn(summaries[0].turn_id())
                .expect("turn trace must read")
                .expect("the summarized turn must have a trace")
        })
        .await
        .expect("trace read task must finish")
    }

    /// Reads the legacy event log for one session.
    async fn load_legacy_events(database: PathBuf, session: SessionId) -> Vec<AgentEvent> {
        tokio::task::spawn_blocking(move || {
            EventStore::open(&database)
                .expect("event store must open")
                .events(&session)
                .expect("legacy events must read")
                .into_iter()
                .map(|stored| stored.event().clone())
                .collect()
        })
        .await
        .expect("legacy read task must finish")
    }

    /// Returns the system prompt the provider observed for one call.
    fn system_prompt_of(call: &ProviderCall) -> &str {
        call.messages
            .first()
            .expect("the call must include a system message")
            .content
            .as_str()
    }

    #[tokio::test]
    async fn delegate_tool_is_present_only_on_enabled_fresh_turns() {
        let enabled_provider = Arc::new(ScriptedProvider::new(vec![done_text("ok", 1)]));
        let enabled_runtime = runtime_with(Arc::clone(&enabled_provider), enabled_config());

        assert_eq!(
            enabled_runtime
                .run_turn("hi".to_owned())
                .await
                .expect("the enabled turn must finish"),
            SessionEndReason::Completed
        );

        let enabled_calls = enabled_provider.calls();
        assert_eq!(
            schema_names(&enabled_calls[0].tools),
            vec!["bash", "delegate_task", "read_file", "write_file"]
        );

        let disabled_provider = Arc::new(ScriptedProvider::new(vec![done_text("ok", 1)]));
        let disabled_runtime =
            runtime_with(Arc::clone(&disabled_provider), HarnessConfig::default());

        assert_eq!(
            disabled_runtime
                .run_turn("hi".to_owned())
                .await
                .expect("the disabled turn must finish"),
            SessionEndReason::Completed
        );

        assert_eq!(
            schema_names(&disabled_provider.calls()[0].tools),
            vec!["read_file", "write_file"],
            "bash is disabled by default even though the default allowlist names it, \
             and delegation must stay off unless configured"
        );
    }

    #[tokio::test]
    async fn readonly_profile_omits_mutating_tools_and_unknown_calls_do_not_write() {
        let workspace = TempDir::new("readonly-profile");
        let provider = Arc::new(ScriptedProvider::new(vec![
            write_request("call_write", "escaped.txt", "must not be written"),
            done_text("adapted", 1),
        ]));
        let config = HarnessConfig::from_toml_str("[tools]\nprofile = \"readonly\"\n")
            .expect("readonly config must parse");
        let runtime = runtime_at(
            Arc::clone(&provider),
            config,
            workspace.path().to_path_buf(),
        );

        assert_eq!(
            runtime
                .run_turn("write a file".to_owned())
                .await
                .expect("the turn must finish"),
            SessionEndReason::Completed
        );

        let calls = provider.calls();
        assert_eq!(
            schema_names(&calls[0].tools),
            vec!["read_file"],
            "the readonly profile must hide write_file and bash"
        );
        assert_eq!(calls.len(), 2);
        assert!(
            calls[1]
                .messages
                .last()
                .expect("the unknown tool must produce a result")
                .content
                .contains("unknown tool `write_file`"),
            "a hidden tool must be unknown: {:?}",
            calls[1].messages.last()
        );
        assert!(
            !workspace.path().join("escaped.txt").exists(),
            "a hidden write_file call must have no filesystem effect"
        );
    }

    #[tokio::test]
    async fn bash_is_hidden_until_enabled_and_unknown_calls_do_not_execute() {
        let workspace = TempDir::new("bash-disabled");
        let disabled_provider = Arc::new(ScriptedProvider::new(vec![
            bash_request("call-disabled-bash", "printf ran > disabled-marker.txt"),
            done_text("adapted", 1),
        ]));
        let disabled_runtime = runtime_at(
            Arc::clone(&disabled_provider),
            HarnessConfig::default(),
            workspace.path().to_path_buf(),
        );

        assert_eq!(
            disabled_runtime
                .run_turn("run bash".to_owned())
                .await
                .expect("the turn must finish"),
            SessionEndReason::Completed
        );

        let disabled_calls = disabled_provider.calls();
        assert_eq!(
            schema_names(&disabled_calls[0].tools),
            vec!["read_file", "write_file"],
            "disabled bash must not appear in the provider schema"
        );
        let rejection = disabled_calls[1]
            .messages
            .last()
            .expect("the unknown bash call must produce a result")
            .content
            .clone();
        assert!(
            rejection.contains("unknown tool `bash`"),
            "a hallucinated call to disabled bash must be unknown: {rejection}"
        );
        assert!(
            !workspace.path().join("disabled-marker.txt").exists(),
            "a disabled bash call must have no filesystem effect"
        );

        let enabled_provider = Arc::new(ScriptedProvider::new(vec![
            bash_request("call-enabled-bash", "printf ran > enabled-marker.txt"),
            done_text("done", 1),
        ]));
        let enabled_config = HarnessConfig::from_toml_str("[tools.bash]\nenabled = true\n")
            .expect("an explicit bash enable must parse");
        let enabled_runtime = runtime_at(
            Arc::clone(&enabled_provider),
            enabled_config,
            workspace.path().to_path_buf(),
        );
        assert_eq!(
            enabled_runtime
                .run_turn("run bash".to_owned())
                .await
                .expect("the enabled turn must finish"),
            SessionEndReason::Completed
        );

        let enabled_calls = enabled_provider.calls();
        assert!(
            schema_names(&enabled_calls[0].tools).contains(&"bash"),
            "explicitly enabled bash must be exposed"
        );
        assert!(
            workspace.path().join("enabled-marker.txt").exists(),
            "an enabled bash call must execute"
        );
    }

    #[tokio::test]
    async fn exposure_budgets_fail_turn_startup_before_the_model_is_called() {
        for (toml, expected) in [
            (
                "[tools]\nmax_exposed_tools = 2\n[tools.bash]\nenabled = true\n",
                "tools.max_exposed_tools",
            ),
            (
                "[tools]\nmax_tool_schema_bytes = 1\n[tools.bash]\nenabled = true\n",
                "tools.max_tool_schema_bytes",
            ),
        ] {
            let provider = Arc::new(ScriptedProvider::new(vec![done_text("unused", 1)]));
            let config = HarnessConfig::from_toml_str(toml).expect("budgets must parse");
            let runtime = runtime_with(Arc::clone(&provider), config);
            let error = runtime
                .run_turn("hi".to_owned())
                .await
                .expect_err("an over-budget catalog must fail the turn");
            assert!(
                matches!(error, CliError::ToolCatalog(_)),
                "expected a catalog error, got {error:?}"
            );
            assert!(
                error.to_string().contains(expected),
                "the error must name the exceeded budget: {error}"
            );
            assert!(
                provider.calls().is_empty(),
                "turn startup must fail before any model request"
            );
        }
    }

    #[tokio::test]
    async fn per_tool_max_args_bytes_blocks_execution_before_side_effects() {
        let workspace = TempDir::new("max-args");
        let provider = Arc::new(ScriptedProvider::new(vec![
            write_request("call_write", "blocked.txt", "must not be written"),
            done_text("adapted", 1),
        ]));
        let config = HarnessConfig::from_toml_str("[tools.write_file]\nmax_args_bytes = 1\n")
            .expect("per-tool limits must parse");
        let runtime = runtime_at(
            Arc::clone(&provider),
            config,
            workspace.path().to_path_buf(),
        );

        runtime
            .run_turn("write a file".to_owned())
            .await
            .expect("the turn must finish");

        let calls = provider.calls();
        assert_eq!(calls.len(), 2);
        assert!(
            calls[1]
                .messages
                .last()
                .expect("the rejected call must produce a result")
                .content
                .contains("max_args_bytes"),
            "the argument limit must reach the model: {:?}",
            calls[1].messages.last()
        );
        assert!(
            !workspace.path().join("blocked.txt").exists(),
            "oversized arguments must be rejected before execution"
        );
    }

    #[tokio::test]
    async fn a_per_tool_timeout_overrides_the_global_timeout() {
        let provider = Arc::new(ScriptedProvider::new(vec![bash_request(
            "slow-command",
            "sleep 2",
        )]));
        let config = HarnessConfig::from_toml_str("[tools.bash]\nenabled = true\ntimeout_ms = 1\n")
            .expect("per-tool timeout must parse");
        let runtime = runtime_with(Arc::clone(&provider), config);

        let started = std::time::Instant::now();
        assert_eq!(
            runtime
                .run_turn("run a slow command".to_owned())
                .await
                .expect("the turn must finish"),
            SessionEndReason::BudgetExceeded,
            "the per-tool timeout must abort the turn"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the 1 ms per-tool timeout must win over the 10 s global timeout"
        );
        assert_eq!(
            provider.calls().len(),
            1,
            "a timed-out tool must cancel the turn before another model request"
        );
    }

    /// Extracts the digest header from a managed result.
    fn managed_digest(content: &str) -> String {
        content
            .lines()
            .find_map(|line| line.strip_prefix("digest: "))
            .expect("a managed result must carry a digest")
            .to_owned()
    }

    /// Resolves the content-addressed artifact path under `root`.
    fn artifact_path(root: &Path, digest: &str) -> PathBuf {
        let hex = digest
            .strip_prefix("sha256:")
            .expect("digest must be namespaced");
        root.join(".jessica/artifacts").join(&hex[..2]).join(hex)
    }

    #[tokio::test]
    async fn a_large_read_file_result_becomes_a_preview_and_artifact() {
        let workspace = TempDir::new("large-read");
        let content = "r".repeat(100 * 1024);
        let read_output = format!("1\t{content}\n");
        std::fs::write(workspace.path().join("big.txt"), &content)
            .expect("large file must be written");
        let provider = Arc::new(ScriptedProvider::new(vec![
            read_request("call_read", "big.txt"),
            done_text("read complete", 1),
        ]));
        let config = HarnessConfig::from_toml_str(
            "[profile]\ntrace = \"off\"\n\
             [policy]\nallowed_tools = [\"read_file\"]\n",
        )
        .expect("read config must parse");
        let runtime = runtime_at(
            Arc::clone(&provider),
            config,
            workspace.path().to_path_buf(),
        );

        assert_eq!(
            runtime
                .run_turn("read the large file".to_owned())
                .await
                .expect("the turn must finish"),
            SessionEndReason::Completed
        );

        let calls = provider.calls();
        assert_eq!(calls.len(), 2);
        let tool_result = calls[1]
            .messages
            .last()
            .expect("the read result must reach the model")
            .content
            .clone();
        assert!(tool_result.starts_with("[result artifact]"));
        assert!(tool_result.contains(&format!("original_bytes: {}", read_output.len())));
        assert!(tool_result.contains("truncated: false"));
        assert!(
            tool_result.len() <= 64 * 1024,
            "only a bounded preview may enter the model context: {}",
            tool_result.len()
        );
        assert_ne!(
            tool_result, read_output,
            "the full result must not be inline"
        );

        let digest = managed_digest(&tool_result);
        let artifact = artifact_path(workspace.path(), &digest);
        assert_eq!(
            std::fs::read(&artifact).expect("artifact must exist"),
            read_output.as_bytes()
        );
    }

    #[tokio::test]
    async fn a_large_bash_result_does_not_trip_the_result_size_guard() {
        let workspace = TempDir::new("large-bash");
        let command = "i=0; while [ $i -lt 20000 ]; do printf 'abcdefghij'; i=$((i+1)); done";
        let provider = Arc::new(ScriptedProvider::new(vec![
            bash_request("call_bash", command),
            done_text("bash complete", 1),
        ]));
        let config = HarnessConfig::from_toml_str(
            "[profile]\ntrace = \"off\"\n\
             [budget]\nmax_result_bytes = 300000\n\
             [policy]\nallowed_tools = [\"bash\"]\n\
             [tools.bash]\nenabled = true\n\
             [tools.supervisor]\n\
             command_timeout_ms = 5000\n\
             max_output_bytes = 250000\n",
        )
        .expect("bash config must parse");
        let runtime = runtime_at(
            Arc::clone(&provider),
            config,
            workspace.path().to_path_buf(),
        );

        assert_eq!(
            runtime
                .run_turn("produce a large command result".to_owned())
                .await
                .expect("the turn must finish"),
            SessionEndReason::Completed,
            "a managed large result must not trip the result-size guard"
        );

        let calls = provider.calls();
        assert_eq!(calls.len(), 2);
        let tool_result = calls[1]
            .messages
            .last()
            .expect("the bash result must reach the model")
            .content
            .clone();
        assert!(tool_result.starts_with("[result artifact]"));
        assert!(tool_result.contains("digest: sha256:"));
        assert!(tool_result.contains("next_offset: 32768"));
        assert!(
            tool_result.len() <= 64 * 1024,
            "the managed preview must stay within the inline cap: {}",
            tool_result.len()
        );
        assert!(
            tool_result.matches("abcdefghij").count() < 20_000,
            "the full 200 KiB command output must not enter the model context"
        );

        let digest = managed_digest(&tool_result);
        let artifact = artifact_path(workspace.path(), &digest);
        let stored = std::fs::read_to_string(&artifact).expect("artifact must exist");
        assert!(
            stored.matches("abcdefghij").count() >= 20_000,
            "the artifact must retain the full command output"
        );
    }

    #[tokio::test]
    async fn a_narrow_profile_hides_delegation_even_when_policy_allows_it() {
        let provider = Arc::new(ScriptedProvider::new(vec![done_text("ok", 1)]));
        let config = HarnessConfig::from_toml_str(
            "[delegation]\nenabled = true\n\
             [policy]\nallowed_tools = [\"bash\", \"read_file\", \"write_file\", \"delegate_task\"]\n\
             [tools]\nprofile = \"coding\"\n",
        )
        .expect("coding config must parse");
        let runtime = runtime_with(Arc::clone(&provider), config);

        runtime
            .run_turn("hi".to_owned())
            .await
            .expect("the turn must finish");

        assert_eq!(
            schema_names(&provider.calls()[0].tools),
            vec!["read_file", "write_file"],
            "the coding profile must hide bash and delegate_task"
        );
    }

    #[tokio::test]
    async fn delegation_cannot_grant_a_tool_the_parent_catalog_hides() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            delegate_request_for(&["write_file"]),
            done_text("adapted", 1),
        ]));
        let config = HarnessConfig::from_toml_str(
            "[delegation]\nenabled = true\n\
             [policy]\nallowed_tools = [\"read_file\", \"delegate_task\"]\n",
        )
        .expect("delegation config must parse");
        let runtime = runtime_with(Arc::clone(&provider), config);

        runtime
            .run_turn("delegate".to_owned())
            .await
            .expect("the turn must finish");

        let calls = provider.calls();
        assert_eq!(
            schema_names(&calls[0].tools),
            vec!["delegate_task", "read_file"],
            "hidden native tools must not be advertised"
        );
        assert_eq!(
            calls.len(),
            2,
            "a rejected delegation must not start a child model call"
        );
        let result = calls[1]
            .messages
            .last()
            .expect("the rejected delegation must produce a result")
            .content
            .clone();
        assert!(
            result.contains("not permitted"),
            "the child tool grant must be rejected: {result}"
        );
    }

    #[tokio::test]
    async fn child_calls_charge_the_parent_token_budget_through_the_shared_wrapper() {
        // The parent alone spends 20 tokens (two calls), the child spends 10
        // more. A limit of 25 therefore completes only if the child usage is
        // invisible to the parent's budget.
        let provider = Arc::new(ScriptedProvider::new(vec![
            delegate_request(),
            done_text("child answer", 10),
            done_text("parent done", 10),
        ]));
        let config = HarnessConfig::from_toml_str(
            "[budget]\nmax_tokens = 25\n\
             [delegation]\nenabled = true\n\
             [policy]\n\
             allowed_tools = [\"bash\", \"read_file\", \"write_file\", \"delegate_task\"]\n\
             [tools.bash]\nenabled = true\n",
        )
        .expect("delegation and budget config must parse");
        let runtime = runtime_with(Arc::clone(&provider), config);

        let reason = runtime
            .run_turn("parent task".to_owned())
            .await
            .expect("the turn must finish");

        assert_eq!(
            reason,
            SessionEndReason::BudgetExceeded,
            "the child's 10 tokens must be charged to the parent's budget"
        );

        let calls = provider.calls();
        assert_eq!(
            calls.len(),
            3,
            "one parent call, one child call, one parent answer"
        );
        assert!(
            schema_names(&calls[0].tools).contains(&DELEGATE_TASK_TOOL_NAME),
            "the parent must see delegate_task"
        );
        assert_eq!(calls[1].messages.len(), 2);
        assert_eq!(
            calls[1].messages[0],
            Message::system(CHILD_AGENT_SYSTEM_PROMPT)
        );
        assert_eq!(calls[1].messages[1], Message::user("child task"));
        assert_eq!(schema_names(&calls[1].tools), vec!["read_file"]);

        let tool_message = calls[2].messages.last().expect("delegate result message");
        assert_eq!(tool_message.role, Role::Tool);
        let result: serde_json::Value =
            serde_json::from_str(&tool_message.content).expect("delegate result must be JSON");
        assert_eq!(result["status"], json!("ok"));
        assert_eq!(result["answer"], json!("child answer"));
    }

    /// Builds a runtime whose config can be reloaded by the test.
    fn runtime_with_reload(
        provider: Arc<ScriptedProvider>,
        config: HarnessConfig,
    ) -> (CliRuntime, watch::Sender<Arc<HarnessConfig>>) {
        let (sender, receiver) = watch::channel(Arc::new(config));
        let runtime = CliRuntime::new(
            provider,
            receiver,
            PathBuf::from("."),
            Vec::new(),
            None,
            None,
            None,
            Arc::new(ShutdownController::new()),
        );
        (runtime, sender)
    }

    #[tokio::test]
    async fn a_published_config_is_used_by_the_next_turn() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            done_text("first answer", 1),
            done_text("second answer", 1),
        ]));
        let (runtime, sender) =
            runtime_with_reload(Arc::clone(&provider), HarnessConfig::default());

        runtime
            .run_turn("first question".to_owned())
            .await
            .expect("first turn should complete");
        sender
            .send(Arc::new(enabled_config()))
            .expect("runtime must keep the receiver open");
        runtime
            .run_turn("second question".to_owned())
            .await
            .expect("second turn should complete");

        let calls = provider.calls();
        assert_eq!(calls.len(), 2);
        assert!(
            !system_prompt_of(&calls[0]).contains("delegate_task"),
            "the first turn must keep the startup snapshot"
        );
        assert_eq!(
            schema_names(&calls[0].tools),
            vec!["read_file", "write_file"]
        );
        assert!(
            !system_prompt_of(&calls[0]).contains("bash"),
            "the first prompt must not advertise disabled bash"
        );
        assert!(
            system_prompt_of(&calls[1]).contains("delegate_task"),
            "the second turn's system prompt must use the reloaded allowlist"
        );
        assert_eq!(
            schema_names(&calls[1].tools),
            vec!["bash", "delegate_task", "read_file", "write_file"],
            "the second turn's tool schema must include the newly enabled tool"
        );
        assert_eq!(
            calls[1]
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            vec![Role::System, Role::User],
            "hot reload must not add or reorder messages"
        );
        assert_eq!(calls[1].messages[1].content, "second question");
    }

    #[tokio::test]
    async fn enabling_bash_is_reloadable_for_the_next_turn() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            done_text("first answer", 1),
            done_text("second answer", 1),
        ]));
        let (runtime, sender) =
            runtime_with_reload(Arc::clone(&provider), HarnessConfig::default());

        runtime
            .run_turn("first question".to_owned())
            .await
            .expect("first turn should complete");
        let enabled = HarnessConfig::from_toml_str("[tools.bash]\nenabled = true\n")
            .expect("the enable gate must parse");
        sender
            .send(Arc::new(enabled))
            .expect("runtime must keep the receiver open");
        runtime
            .run_turn("second question".to_owned())
            .await
            .expect("second turn should complete");

        let calls = provider.calls();
        assert_eq!(
            schema_names(&calls[0].tools),
            vec!["read_file", "write_file"],
            "the first turn must keep bash disabled"
        );
        assert_eq!(
            schema_names(&calls[1].tools),
            vec!["bash", "read_file", "write_file"],
            "the next turn must expose the reloaded bash gate"
        );
    }

    #[tokio::test]
    async fn a_reload_during_a_turn_is_observed_only_by_the_next_turn() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            done_text("first answer", 1),
            done_text("second answer", 1),
        ]));
        let (runtime, sender) =
            runtime_with_reload(Arc::clone(&provider), HarnessConfig::default());
        provider.reload_during_next_call(sender, enabled_config());

        runtime
            .run_turn("first question".to_owned())
            .await
            .expect("first turn should complete");
        runtime
            .run_turn("second question".to_owned())
            .await
            .expect("second turn should complete");

        let calls = provider.calls();
        assert_eq!(calls.len(), 2);
        assert!(
            !system_prompt_of(&calls[0]).contains("delegate_task"),
            "the in-flight turn must finish with its original snapshot"
        );
        assert_eq!(
            schema_names(&calls[0].tools),
            vec!["read_file", "write_file"]
        );
        assert!(
            !system_prompt_of(&calls[0]).contains("bash"),
            "the in-flight prompt must not advertise disabled bash"
        );
        assert!(
            system_prompt_of(&calls[1]).contains("delegate_task"),
            "the next turn must observe the value published mid-turn"
        );
        assert_eq!(
            schema_names(&calls[1].tools),
            vec!["bash", "delegate_task", "read_file", "write_file"]
        );
    }

    #[tokio::test]
    async fn a_reloaded_round_budget_applies_to_the_next_turn() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            unknown_tool_request("first-a"),
            unknown_tool_request("first-b"),
            done_text("first answer", 1),
            unknown_tool_request("second-a"),
        ]));
        let (runtime, sender) =
            runtime_with_reload(Arc::clone(&provider), HarnessConfig::default());

        assert_eq!(
            runtime
                .run_turn("first question".to_owned())
                .await
                .expect("first turn should run"),
            SessionEndReason::Completed,
            "the default round budget allows both tool rounds"
        );
        let reloaded = HarnessConfig::from_toml_str("[budget]\nmax_rounds = 1\n")
            .expect("budget should parse");
        sender
            .send(Arc::new(reloaded))
            .expect("runtime must keep the receiver open");

        assert_eq!(
            runtime
                .run_turn("second question".to_owned())
                .await
                .expect("second turn should run"),
            SessionEndReason::BudgetExceeded,
            "the reloaded max_rounds must stop the second turn after one model call"
        );
        assert_eq!(
            provider.calls().len(),
            4,
            "three calls for the first turn and exactly one for the second"
        );
    }

    #[tokio::test]
    async fn a_reloaded_tool_timeout_applies_to_the_next_turn() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            done_text("first answer", 1),
            bash_request("slow-command", "sleep 0.2"),
            done_text("second answer", 1),
        ]));
        let (runtime, sender) =
            runtime_with_reload(Arc::clone(&provider), HarnessConfig::default());

        assert_eq!(
            runtime
                .run_turn("first question".to_owned())
                .await
                .expect("first turn should run"),
            SessionEndReason::Completed
        );
        let reloaded = HarnessConfig::from_toml_str(
            "[budget]\ntool_timeout_ms = 1\n[tools.bash]\nenabled = true\n",
        )
        .expect("budget should parse");
        sender
            .send(Arc::new(reloaded))
            .expect("runtime must keep the receiver open");

        assert_eq!(
            runtime
                .run_turn("second question".to_owned())
                .await
                .expect("second turn should run"),
            SessionEndReason::BudgetExceeded,
            "the reloaded 1ms tool timeout must abort the 200ms command"
        );
        assert_eq!(
            provider.calls().len(),
            2,
            "a timed-out tool cancels the second turn before another model call"
        );
    }

    #[tokio::test]
    async fn a_shutdown_before_a_turn_prevents_any_model_request() {
        let provider = Arc::new(ScriptedProvider::new(Vec::new()));
        let llm: Arc<dyn LlmProvider> = provider.clone();
        let (_sender, receiver) = watch::channel(Arc::new(HarnessConfig::default()));
        let shutdown = Arc::new(ShutdownController::new());
        let runtime = CliRuntime::new(
            llm,
            receiver,
            PathBuf::from("."),
            Vec::new(),
            None,
            None,
            None,
            Arc::clone(&shutdown),
        );
        assert!(shutdown.request_shutdown(ShutdownSignal::SigTerm));

        let reason = runtime
            .run_turn("must not run".to_owned())
            .await
            .expect("a shutdown must end the turn cleanly");

        assert_eq!(reason, SessionEndReason::Cancelled);
        assert!(
            provider.calls().is_empty(),
            "a turn whose child token is already cancelled must not call the provider"
        );
    }

    #[tokio::test]
    async fn a_turn_completing_does_not_cancel_the_process_or_the_next_turn() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            done_text("first answer", 1),
            done_text("second answer", 1),
        ]));
        let llm: Arc<dyn LlmProvider> = provider.clone();
        let (_sender, receiver) = watch::channel(Arc::new(HarnessConfig::default()));
        let shutdown = Arc::new(ShutdownController::new());
        let runtime = CliRuntime::new(
            llm,
            receiver,
            PathBuf::from("."),
            Vec::new(),
            None,
            None,
            None,
            Arc::clone(&shutdown),
        );

        assert_eq!(
            runtime
                .run_turn("first".to_owned())
                .await
                .expect("first turn must run"),
            SessionEndReason::Completed
        );
        assert!(!shutdown.is_shutting_down());
        assert_eq!(
            runtime
                .run_turn("second".to_owned())
                .await
                .expect("second turn must run"),
            SessionEndReason::Completed
        );
        assert_eq!(
            provider.calls().len(),
            2,
            "each turn must acquire an independent child token"
        );
    }

    #[tokio::test]
    async fn turn_scopes_isolate_file_tools_and_reject_out_of_bound_paths() {
        let base = TempDir::new("turn-scopes");
        let root_a = base.path().join("a");
        let root_b = base.path().join("b");
        std::fs::create_dir_all(&root_a).expect("root a must be creatable");
        std::fs::create_dir_all(&root_b).expect("root b must be creatable");
        std::fs::write(root_a.join("inside.txt"), "alpha").expect("file a must be writable");
        std::fs::write(root_b.join("inside.txt"), "beta").expect("file b must be writable");
        std::fs::write(base.path().join("outside.txt"), "secret")
            .expect("outside file must be writable");
        let canonical_a = std::fs::canonicalize(&root_a).expect("root a must resolve");
        let canonical_b = std::fs::canonicalize(&root_b).expect("root b must resolve");

        let provider = Arc::new(ScriptedProvider::new(vec![
            read_request("a-read", "inside.txt"),
            write_request("a-write", "written.txt", "from-a"),
            done_text("a done", 1),
            read_request("b-read", "inside.txt"),
            write_request("b-write", "written.txt", "from-b"),
            done_text("b done", 1),
            read_request("escape", "../outside.txt"),
            done_text("escape done", 1),
        ]));
        let runtime = runtime_with(Arc::clone(&provider), HarnessConfig::default());

        for (root, input) in [(&root_a, "work in a"), (&root_b, "work in b")] {
            let outcome = runtime
                .run_turn_request(TurnRequest {
                    scope: TurnScope::new(root.clone(), CancellationToken::new())
                        .await
                        .expect("turn root must canonicalize"),
                    history: Vec::new(),
                    user_input: input.to_owned(),
                    update_sink: None,
                    acceptance_retries: None,
                    session_id: None,
                    extra_tools: Vec::new(),
                    permission_gate: None,
                })
                .await
                .expect("turn must run");
            assert_eq!(outcome.reason, SessionEndReason::Completed);
        }
        let escape = runtime
            .run_turn_request(TurnRequest {
                scope: TurnScope::new(root_a.clone(), CancellationToken::new())
                    .await
                    .expect("turn root must canonicalize"),
                history: Vec::new(),
                user_input: "escape".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: None,
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("turn must run");
        assert_eq!(escape.reason, SessionEndReason::Completed);

        let calls = provider.calls();
        assert_eq!(calls.len(), 8);
        assert!(
            system_prompt_of(&calls[0])
                .contains(canonical_a.to_str().expect("temporary paths must be UTF-8")),
            "the first turn must render the first root into its prompt"
        );
        assert_eq!(
            calls[1].messages.last().expect("tool result").content,
            "1\talpha\n",
            "the first turn must read from its own root"
        );
        assert!(
            system_prompt_of(&calls[3])
                .contains(canonical_b.to_str().expect("temporary paths must be UTF-8")),
            "the second turn must render the second root into its prompt"
        );
        assert_eq!(
            calls[4].messages.last().expect("tool result").content,
            "1\tbeta\n",
            "the second turn must read from its own root"
        );
        assert!(
            system_prompt_of(&calls[6])
                .contains(canonical_a.to_str().expect("temporary paths must be UTF-8")),
            "later turns must return to the root supplied by their scope"
        );
        assert!(
            calls[7]
                .messages
                .last()
                .expect("tool result")
                .content
                .contains("outside the workspace"),
            "a path above the turn root must be rejected: {}",
            calls[7].messages.last().expect("tool result").content
        );

        assert_eq!(
            std::fs::read_to_string(canonical_a.join("written.txt"))
                .expect("turn a must write its file"),
            "from-a"
        );
        assert_eq!(
            std::fs::read_to_string(canonical_b.join("written.txt"))
                .expect("turn b must write its file"),
            "from-b"
        );
        assert!(
            !canonical_a.join("outside.txt").exists() && !canonical_b.join("outside.txt").exists(),
            "out-of-bound reads must not copy the file into a turn root"
        );
    }

    #[tokio::test]
    async fn cancelling_one_scope_parent_does_not_cancel_another_turn() {
        let started = Arc::new(tokio::sync::Notify::new());
        let provider: Arc<dyn LlmProvider> = Arc::new(PendingProvider {
            started: Arc::clone(&started),
        });
        let (_sender, receiver) = watch::channel(Arc::new(HarnessConfig::default()));
        let runtime = CliRuntime::new(
            provider,
            receiver,
            PathBuf::from("."),
            Vec::new(),
            None,
            None,
            None,
            Arc::new(ShutdownController::new()),
        );
        let parent_a = CancellationToken::new();
        let parent_b = CancellationToken::new();
        let scope_b = TurnScope::new(PathBuf::from("."), parent_b.clone())
            .await
            .expect("scope b must build");
        let turn_b = runtime.run_turn_request(TurnRequest {
            scope: scope_b,
            history: Vec::new(),
            user_input: "hang".to_owned(),
            update_sink: None,
            acceptance_retries: None,
            session_id: None,
            extra_tools: Vec::new(),
            permission_gate: None,
        });
        tokio::pin!(turn_b);

        tokio::select! {
            _ = started.notified() => {}
            outcome = &mut turn_b => {
                panic!("turn b ended before the provider was entered: {outcome:?}");
            }
        }

        parent_a.cancel();
        assert!(
            !parent_b.is_cancelled(),
            "an unrelated scope's parent must stay uncancelled"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut turn_b)
                .await
                .is_err(),
            "cancelling another scope's parent must not end this turn"
        );

        parent_b.cancel();
        let outcome = turn_b.await.expect("turn b must return an outcome");
        assert_eq!(
            outcome.reason,
            SessionEndReason::Cancelled,
            "the turn must only end when its own scope parent is cancelled"
        );
    }

    #[tokio::test]
    async fn cancelling_a_scope_parent_stops_an_in_flight_turn() {
        let started = Arc::new(tokio::sync::Notify::new());
        let provider: Arc<dyn LlmProvider> = Arc::new(PendingProvider {
            started: Arc::clone(&started),
        });
        let (_sender, receiver) = watch::channel(Arc::new(HarnessConfig::default()));
        let runtime = CliRuntime::new(
            provider,
            receiver,
            PathBuf::from("."),
            Vec::new(),
            None,
            None,
            None,
            Arc::new(ShutdownController::new()),
        );
        let parent = CancellationToken::new();
        let scope = TurnScope::new(PathBuf::from("."), parent.clone())
            .await
            .expect("scope must build");
        let turn = runtime.run_turn_request(TurnRequest {
            scope,
            history: Vec::new(),
            user_input: "hang".to_owned(),
            update_sink: None,
            acceptance_retries: None,
            session_id: None,
            extra_tools: Vec::new(),
            permission_gate: None,
        });
        let canceller = async move {
            started.notified().await;
            parent.cancel();
        };

        let (outcome, ()) = tokio::join!(turn, canceller);

        let outcome = outcome.expect("a cancelled turn must return an outcome");
        assert_eq!(
            outcome.reason,
            SessionEndReason::Cancelled,
            "the provider was entered and its pending stream must be cancelled by the parent token"
        );
        assert_eq!(
            outcome.messages.last(),
            Some(&Message::user("hang")),
            "cancellation must preserve the initial transcript"
        );
    }

    #[tokio::test]
    async fn a_turn_scope_rejects_a_missing_workspace_root() {
        let missing = std::env::temp_dir().join(format!(
            "jessica-missing-root-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("the clock must be after the epoch")
                .as_nanos()
        ));

        let error = TurnScope::new(missing.clone(), CancellationToken::new())
            .await
            .err()
            .expect("a missing root must not produce a scope");

        assert!(
            error.to_string().contains("canonicalize"),
            "the error must name the failed canonicalization: {error}"
        );
        assert!(
            error.to_string().contains(&missing.display().to_string()),
            "the error must name the requested path: {error}"
        );
    }

    #[tokio::test]
    async fn history_is_replayed_before_the_new_input_and_excluded_from_counts() {
        let provider = Arc::new(ScriptedProvider::new(vec![done_text("new answer", 1)]));
        let runtime = runtime_with(Arc::clone(&provider), HarnessConfig::default());
        let history = vec![
            Message::user("old question"),
            Message::assistant_with_tool_calls(
                "old",
                vec![ToolCall::new(
                    "old-call",
                    "read_file",
                    json!({"path": "old"}),
                )],
            ),
            Message::tool("old-call", "old output"),
            Message::assistant("old answer"),
        ];

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope().await,
                history: history.clone(),
                user_input: "new question".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: None,
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("turn must run");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        let calls = provider.calls();
        assert_eq!(calls.len(), 1);
        let observed = &calls[0].messages;
        assert_eq!(
            observed.len(),
            1 + history.len() + 1,
            "the prompt must be system, history, then the new input"
        );
        assert_eq!(observed[0].role, Role::System);
        assert_eq!(&observed[1..=history.len()], history.as_slice());
        assert_eq!(observed.last(), Some(&Message::user("new question")));

        assert_eq!(
            outcome.turn_start,
            observed.len(),
            "the outcome boundary must be the initial conversation, not the history"
        );
        let this_turn = &outcome.messages[outcome.turn_start..];
        let tool_calls: usize = this_turn
            .iter()
            .map(|message| message.tool_calls.len())
            .sum();
        assert_eq!(
            tool_calls, 0,
            "tool calls replayed from history must not count toward this turn"
        );
        assert_eq!(
            this_turn
                .iter()
                .filter(|message| message.role == Role::Assistant)
                .count(),
            1,
            "only the new answer belongs to this turn"
        );
    }

    #[tokio::test]
    async fn turn_outcome_matches_the_event_rebuilt_transcript() {
        let workspace = TempDir::new("outcome-transcript");
        std::fs::write(workspace.path().join("README.md"), "contents")
            .expect("workspace file must be writable");
        let provider = Arc::new(ScriptedProvider::new(vec![
            read_request("read-1", "README.md"),
            done_text("finished", 1),
        ]));
        let runtime = runtime_with(provider, HarnessConfig::default());

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: TurnScope::new(workspace.path().to_path_buf(), CancellationToken::new())
                    .await
                    .expect("workspace root must canonicalize"),
                history: Vec::new(),
                user_input: "inspect".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: None,
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("turn must run");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(
            outcome.turn_start, 2,
            "the system prompt and user input are the initial conversation"
        );
        assert!(!outcome.lagged, "a healthy pump must not mark the outcome");
        assert_eq!(outcome.messages.len(), 5);
        assert_eq!(outcome.messages[0].role, Role::System);
        assert_eq!(outcome.messages[1], Message::user("inspect"));
        assert_eq!(outcome.messages[2].role, Role::Assistant);
        assert_eq!(outcome.messages[2].tool_calls.len(), 1);
        assert_eq!(outcome.messages[2].tool_calls[0].id, "read-1");
        assert_eq!(outcome.messages[2].tool_calls[0].name, "read_file");
        assert_eq!(
            outcome.messages[3],
            Message::tool("read-1", "1\tcontents\n"),
            "the transcript must contain the same tool result the event stream carried"
        );
        assert_eq!(outcome.messages[4], Message::assistant("finished"));
    }

    #[tokio::test]
    async fn a_programmatic_turn_forwards_every_event_to_its_sink() {
        let provider = Arc::new(ScriptedProvider::new(vec![done_text("answer", 1)]));
        let runtime = runtime_with(provider, HarnessConfig::default());
        let sink = Arc::new(RecordingSink::default());

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope().await,
                history: Vec::new(),
                user_input: "question".to_owned(),
                update_sink: Some(Arc::clone(&sink) as Arc<dyn UpdateSink>),
                acceptance_retries: None,
                session_id: None,
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("turn must run");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        let observed = sink.events();
        assert!(
            matches!(observed.first(), Some(AgentEvent::SessionStarted { .. })),
            "the sink must observe the session start first"
        );
        assert!(
            observed
                .iter()
                .any(|event| matches!(event, AgentEvent::ModelDelta { text } if text == "answer")),
            "the sink must observe streamed model text"
        );
        assert!(
            matches!(
                observed.last(),
                Some(AgentEvent::SessionEnded {
                    reason: SessionEndReason::Completed,
                    ..
                })
            ),
            "the sink must observe the final terminal event"
        );
    }

    #[tokio::test]
    async fn a_turn_request_can_run_acceptance_once_without_retries() {
        let provider = Arc::new(ScriptedProvider::new(vec![done_text("answer", 1)]));
        let config = HarnessConfig::from_toml_str(
            "[acceptance]\n\
             name = \"always-fails\"\n\
             command = \"false\"\n\
             max_retries = 2\n",
        )
        .expect("acceptance config must parse");
        let runtime = runtime_with(Arc::clone(&provider), config);

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope().await,
                history: Vec::new(),
                user_input: "question".to_owned(),
                update_sink: None,
                acceptance_retries: Some(0),
                session_id: None,
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("turn must run");

        assert_eq!(outcome.reason, SessionEndReason::AcceptanceFailed);
        assert_eq!(
            provider.calls().len(),
            1,
            "a per-turn zero retry budget must override the configured retries"
        );
    }

    #[tokio::test]
    async fn a_completed_turn_writes_one_closed_trace_with_mirrored_legacy_events() {
        let temp = TempDir::new("trace-completed");
        let database = temp.path().join("events.sqlite");
        let session = SessionId::new("trace-completed-session");
        let provider = Arc::new(ScriptedProvider::new(vec![done_text("answer", 1)]));
        let runtime = runtime_with_recorder(
            Arc::clone(&provider),
            HarnessConfig::default(),
            database.clone(),
        )
        .await;

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope().await,
                history: Vec::new(),
                user_input: "question".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: Some(session.clone()),
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("turn must run");
        assert_eq!(outcome.reason, SessionEndReason::Completed);
        drop(runtime);

        let trace = load_trace(database.clone()).await;
        assert!(trace.closed(), "a completed turn must close its trace");
        let events = trace.events();
        assert_eq!(
            events.len(),
            7,
            "turn start, five agent events, and turn end: {events:?}"
        );
        assert_eq!(events[0].seq(), 1);
        assert!(
            matches!(
                &events[0].event().payload,
                TracePayload::TurnStarted {
                    mode: TraceTurnMode::Fresh,
                    messages,
                } if messages.first().is_some_and(|message| message.role == Role::System)
                    && messages.last().is_some_and(|message| message.content == "question")
            ),
            "the trace must start with the supplied conversation"
        );
        assert_eq!(events[0].event().attempt, None);
        assert_eq!(events[0].event().session_id.as_ref(), Some(&session));

        let agent_payloads: Vec<AgentEvent> = events
            .iter()
            .filter_map(|stored| match &stored.event().payload {
                TracePayload::Agent { event } => Some(event.clone()),
                _ => None,
            })
            .collect();
        for stored in &events[1..events.len() - 1] {
            assert_eq!(
                stored.event().attempt,
                Some(1),
                "every attempt event must name its attempt"
            );
            assert_eq!(
                stored.event().session_id.as_ref(),
                Some(&session),
                "every attempt event must name its session"
            );
        }

        let end = events.last().expect("the trace must have a terminal entry");
        assert!(matches!(
            &end.event().payload,
            TracePayload::TurnEnded {
                reason: SessionEndReason::Completed
            }
        ));
        assert_eq!(end.event().attempt, None);
        assert_eq!(end.event().session_id, None);

        let legacy = load_legacy_events(database, session.clone()).await;
        assert_eq!(
            legacy, agent_payloads,
            "the legacy log must mirror the agent payloads exactly"
        );
        assert!(
            matches!(
                legacy.last(),
                Some(AgentEvent::SessionEnded {
                    reason: SessionEndReason::Completed,
                    session_id,
                }) if session_id == &session
            ),
            "the legacy log must end with the attempt's terminal event: {legacy:?}"
        );
    }

    #[tokio::test]
    async fn an_acceptance_retry_trace_keeps_failures_trace_only_and_attributes_each_attempt() {
        let temp = TempDir::new("trace-acceptance");
        let database = temp.path().join("events.sqlite");
        let first_session = SessionId::new("trace-acceptance-first");
        let config = HarnessConfig::from_toml_str(
            "[acceptance]\n\
             name = \"always-fails\"\n\
             command = \"false\"\n\
             max_retries = 1\n",
        )
        .expect("acceptance config must parse");
        let provider = Arc::new(ScriptedProvider::new(vec![
            done_text("first answer", 1),
            done_text("second answer", 1),
        ]));
        let runtime = runtime_with_recorder(Arc::clone(&provider), config, database.clone()).await;

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope().await,
                history: Vec::new(),
                user_input: "question".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: Some(first_session.clone()),
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("turn must run");
        assert_eq!(outcome.reason, SessionEndReason::AcceptanceFailed);
        assert_eq!(
            provider.calls().len(),
            2,
            "one retry must run one more attempt"
        );
        drop(runtime);

        let trace = load_trace(database.clone()).await;
        assert!(trace.closed(), "an exhausted acceptance turn still closes");
        let events = trace.events();
        assert_eq!(
            events.len(),
            16,
            "start + two complete attempts (with rounds) + their failures + outer session + end: {events:?}"
        );
        assert!(matches!(
            &events[0].event().payload,
            TracePayload::TurnStarted {
                mode: TraceTurnMode::Fresh,
                ..
            }
        ));
        let end = events.last().expect("the trace must have a terminal entry");
        assert!(matches!(
            &end.event().payload,
            TracePayload::TurnEnded {
                reason: SessionEndReason::AcceptanceFailed
            }
        ));
        assert_eq!(end.event().attempt, None);
        assert_eq!(end.event().session_id, None);

        let second_session = events
            .iter()
            .find_map(|stored| match &stored.event().payload {
                TracePayload::Agent {
                    event: AgentEvent::SessionStarted { session_id, .. },
                } if stored.event().attempt == Some(2) => Some(session_id.clone()),
                _ => None,
            })
            .expect("the retry must have its own session");
        assert_ne!(
            second_session, first_session,
            "an acceptance retry must not reuse the judged session"
        );

        let failures: Vec<(&TraceEvent, u32)> = events
            .iter()
            .filter_map(|stored| match &stored.event().payload {
                TracePayload::Agent {
                    event: AgentEvent::AcceptanceFailed { attempt, .. },
                } => Some((stored.event(), *attempt)),
                _ => None,
            })
            .collect();
        assert_eq!(
            failures.len(),
            2,
            "each failed attempt must contribute its own verdict"
        );
        for (stored, judged_attempt) in &failures {
            assert_eq!(
                stored.attempt,
                Some(*judged_attempt),
                "the failure must be attributed to the attempt it judged"
            );
        }
        assert_eq!(failures[0].0.session_id.as_ref(), Some(&first_session));
        assert_eq!(failures[1].0.session_id.as_ref(), Some(&second_session));

        for (session, expected_text) in [
            (&first_session, "first answer"),
            (&second_session, "second answer"),
        ] {
            let legacy = load_legacy_events(database.clone(), session.clone()).await;
            assert!(
                !legacy
                    .iter()
                    .any(|event| matches!(event, AgentEvent::AcceptanceFailed { .. })),
                "acceptance failures must stay trace-only for {session}: {legacy:?}"
            );
            let deltas: Vec<&str> = legacy
                .iter()
                .filter_map(|event| match event {
                    AgentEvent::ModelDelta { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                deltas,
                vec![expected_text],
                "each attempt must persist only its own output"
            );
            assert!(
                matches!(
                    legacy.last(),
                    Some(AgentEvent::SessionEnded {
                        reason: SessionEndReason::Completed,
                        ..
                    })
                ),
                "the attempt session must end before the acceptance verdict: {legacy:?}"
            );
        }

        let outer_sessions: Vec<SessionId> = events
            .iter()
            .filter_map(|stored| match &stored.event().payload {
                TracePayload::Agent {
                    event: AgentEvent::SessionStarted { session_id, .. },
                } if stored.event().attempt.is_none() => Some(session_id.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            outer_sessions.len(),
            1,
            "the governed verdict needs exactly one outer session: {events:?}"
        );
        assert_ne!(outer_sessions[0], first_session);
        assert_ne!(outer_sessions[0], second_session);
        let outer = load_legacy_events(database, outer_sessions[0].clone()).await;
        assert!(
            matches!(
                outer.last(),
                Some(AgentEvent::SessionEnded {
                    reason: SessionEndReason::AcceptanceFailed,
                    ..
                })
            ),
            "the outer session must carry the governed verdict: {outer:?}"
        );
    }

    #[tokio::test]
    async fn a_session_metadata_conflict_fails_closed_without_agent_events() {
        let temp = TempDir::new("trace-metadata-conflict");
        let database = temp.path().join("events.sqlite");
        let session = SessionId::new("trace-conflicting-session");
        {
            let mut store = EventStore::open(&database).expect("event store must open");
            store
                .record_session(
                    &session,
                    &[Message::system("a different bootstrap")],
                    Timestamp::from_millis(1),
                )
                .expect("the conflicting session must be seeded");
        }
        let provider = Arc::new(ScriptedProvider::new(Vec::new()));
        let runtime = runtime_with_recorder(
            Arc::clone(&provider),
            HarnessConfig::default(),
            database.clone(),
        )
        .await;

        let error = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope().await,
                history: Vec::new(),
                user_input: "question".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: Some(session.clone()),
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect_err("a metadata conflict must fail the turn");
        assert!(
            matches!(&error, CliError::TraceIncomplete { cause, .. } if cause.contains("metadata")),
            "the failure must name the metadata write: {error}"
        );
        assert!(
            provider.calls().is_empty(),
            "a turn that cannot record its bootstrap must not call the model"
        );
        drop(runtime);

        let trace = load_trace(database.clone()).await;
        assert!(
            !trace.closed(),
            "a failed turn must be observable as incomplete"
        );
        assert_eq!(
            trace.events().len(),
            1,
            "only turn_started may exist: {:?}",
            trace.events()
        );
        assert!(matches!(
            &trace.events()[0].event().payload,
            TracePayload::TurnStarted { .. }
        ));
        assert!(
            load_legacy_events(database, session).await.is_empty(),
            "no event may be written for a session whose metadata failed"
        );
    }

    #[tokio::test]
    async fn a_token_budget_trip_overrides_completed_and_skips_memory_writes() {
        let workspace = TempDir::new("budget-memory");
        let config = HarnessConfig::from_toml_str(
            "[budget]\nmax_tokens = 1\n\
             [memory]\nenabled = true\npath = \"memory.db\"\nembedder = \"none\"\n",
        )
        .expect("budget and memory config must parse");
        let session = SessionId::new("budget-memory-session");
        let memory = MemoryAssembly::open(&config.memory, workspace.path())
            .await
            .expect("memory must open");
        let provider = Arc::new(ScriptedProvider::new(vec![done_text("answer", 10)]));
        let runtime = runtime_with_memory(
            Arc::clone(&provider),
            config,
            workspace.path().to_path_buf(),
            memory,
        );

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope_at(workspace.path().to_path_buf()).await,
                history: Vec::new(),
                user_input: "question".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: Some(session.clone()),
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("the turn must finish");
        assert_eq!(
            outcome.reason,
            SessionEndReason::BudgetExceeded,
            "a charged token trip must override the runtime's Completed result"
        );
        assert_eq!(
            provider.calls().len(),
            1,
            "the cancelled turn must not make another model call"
        );
        drop(runtime);

        let entries = memory_entries(
            &workspace.path().join("memory.db"),
            workspace.path(),
            &session,
        );
        assert!(
            entries.is_empty(),
            "a tripped turn must not be written to memory: {entries:?}"
        );
    }

    #[tokio::test]
    async fn an_acceptance_failed_turn_does_not_write_memory() {
        let workspace = TempDir::new("acceptance-memory");
        let config = HarnessConfig::from_toml_str(
            "[acceptance]\n\
             name = \"always-fails\"\n\
             command = \"false\"\n\
             max_retries = 0\n\
             [memory]\nenabled = true\npath = \"memory.db\"\nembedder = \"none\"\n",
        )
        .expect("acceptance and memory config must parse");
        let session = SessionId::new("acceptance-memory-session");
        let memory = MemoryAssembly::open(&config.memory, workspace.path())
            .await
            .expect("memory must open");
        let provider = Arc::new(ScriptedProvider::new(vec![done_text("answer", 1)]));
        let runtime = runtime_with_memory(
            Arc::clone(&provider),
            config,
            workspace.path().to_path_buf(),
            memory,
        );

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope_at(workspace.path().to_path_buf()).await,
                history: Vec::new(),
                user_input: "question".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: Some(session.clone()),
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("the turn must finish");
        assert_eq!(outcome.reason, SessionEndReason::AcceptanceFailed);
        assert_eq!(
            provider.calls().len(),
            1,
            "the zero retry budget must stop after the first failed check"
        );
        drop(runtime);

        let entries = memory_entries(
            &workspace.path().join("memory.db"),
            workspace.path(),
            &session,
        );
        assert!(
            entries.is_empty(),
            "an acceptance-failed turn must not be remembered: {entries:?}"
        );
    }

    #[tokio::test]
    async fn an_acceptance_retry_reuses_the_initial_prefix_and_recalls_memory_once() {
        let workspace = TempDir::new("acceptance-prefix");
        let session = SessionId::new("acceptance-prefix-session");
        let config = HarnessConfig::from_toml_str(
            "[acceptance]\n\
             name = \"always-fails\"\n\
             command = \"false\"\n\
             max_retries = 1\n\
             [memory]\nenabled = true\npath = \"memory.db\"\nembedder = \"none\"\n",
        )
        .expect("acceptance and memory config must parse");
        let memory = MemoryAssembly::open(&config.memory, workspace.path())
            .await
            .expect("memory must open");
        memory
            .record_completed_turn(
                &session,
                "the build command is cargo test",
                "MEMORY_SENTINEL_ANSWER",
                0,
                0,
            )
            .await
            .expect("the seeded turn must be written");
        let provider = Arc::new(ScriptedProvider::new(vec![
            done_text("first answer", 1),
            done_text("second answer", 1),
        ]));
        let runtime = runtime_with_memory(
            Arc::clone(&provider),
            config,
            workspace.path().to_path_buf(),
            memory,
        );

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope_at(workspace.path().to_path_buf()).await,
                history: Vec::new(),
                user_input: "what is the build command?".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: Some(session),
                extra_tools: Vec::new(),
                permission_gate: None,
            })
            .await
            .expect("the turn must finish");
        assert_eq!(outcome.reason, SessionEndReason::AcceptanceFailed);

        let calls = provider.calls();
        assert_eq!(calls.len(), 2, "one failed check must run one retry");
        let initial = &calls[0].messages;
        assert_eq!(
            initial.len(),
            3,
            "the first attempt must receive system, memory, and user messages: {initial:?}"
        );
        assert_eq!(
            outcome.turn_start,
            initial.len(),
            "the outcome must point at the first attempt's initial boundary"
        );
        assert!(!outcome.lagged, "a healthy pump must not mark the outcome");
        assert_eq!(initial[0].role, Role::System);
        assert_eq!(initial[1].role, Role::System);
        assert!(
            initial[1].content.contains("MEMORY_SENTINEL_ANSWER"),
            "the recalled memory must be the second system message: {initial:?}"
        );
        assert_eq!(
            initial[2],
            Message::user("what is the build command?"),
            "the user input must follow the guides"
        );
        assert_eq!(
            &calls[1].messages[..initial.len()],
            initial.as_slice(),
            "the retry must reuse the first attempt's prefix unchanged"
        );
        assert_eq!(
            calls[1]
                .messages
                .iter()
                .filter(|message| message.content.contains("MEMORY_SENTINEL_ANSWER"))
                .count(),
            1,
            "memory must be recalled once, not re-injected into the retry: {:?}",
            calls[1].messages
        );
        assert_eq!(
            calls[1].messages.get(initial.len()),
            Some(&Message::assistant("first answer")),
            "the retry must continue from the first attempt instead of restarting"
        );
    }

    #[tokio::test]
    async fn top_level_delegate_approval_covers_the_child_run() {
        let workspace = TempDir::new("delegate-approval");
        let config = HarnessConfig::from_toml_str(
            "[delegation]\nenabled = true\n\
             [approval]\nmode = \"ask\"\n\
             [policy]\nallowed_tools = [\"bash\", \"delegate_task\"]\n\
             [tools.bash]\nenabled = true\n",
        )
        .expect("delegation and approval config must parse");
        let provider = Arc::new(ScriptedProvider::new(vec![
            delegate_request_for(&["bash"]),
            bash_request("child-bash", "printf child > child.txt"),
            done_text("child answer", 1),
            done_text("parent answer", 1),
        ]));
        let gate = Arc::new(AllowOnceGate::default());
        let runtime = runtime_at(
            Arc::clone(&provider),
            config,
            workspace.path().to_path_buf(),
        );

        let outcome = runtime
            .run_turn_request(TurnRequest {
                scope: test_scope_at(workspace.path().to_path_buf()).await,
                history: Vec::new(),
                user_input: "delegate".to_owned(),
                update_sink: None,
                acceptance_retries: None,
                session_id: None,
                extra_tools: Vec::new(),
                permission_gate: Some(Arc::clone(&gate) as Arc<dyn ToolPermissionGate>),
            })
            .await
            .expect("the turn must finish");
        assert_eq!(outcome.reason, SessionEndReason::Completed);

        let decisions = gate.calls();
        assert_eq!(
            decisions.len(),
            1,
            "only the top-level delegate_task may request approval: {decisions:?}"
        );
        assert_eq!(decisions[0].name.as_str(), DELEGATE_TASK_TOOL_NAME);
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("child.txt"))
                .expect("the child command must create its file"),
            "child",
            "the approved delegation must execute the child's tool call"
        );

        let calls = provider.calls();
        assert_eq!(
            calls.len(),
            4,
            "one parent call, one child tool round, one child answer, and one parent answer"
        );
        assert_eq!(
            schema_names(&calls[1].tools),
            vec!["bash"],
            "the child must see only the granted parent tool"
        );
    }
}
