//! Assembly of the governed CLI runtime and one-turn execution.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jessica_core::acceptance::AcceptanceCheck;
use jessica_core::guard::Guard;
use jessica_core::llm::{LlmError, LlmProvider};
use jessica_core::policy::Policy;
use jessica_core::sensor::Sensor;
use jessica_core::tool::{Tool, ToolRegistry, ToolRegistryError};
use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId};
use jessica_harness::{
    AcceptanceRunner, AllowlistPolicy, CommandSensor, DEFAULT_EVENT_CAPACITY, Harness,
    ResultSizeGuard, SessionAttempt, StuckDetectionGuard, TokenBudgetGuard, ToolTimeoutGuard,
};
use jessica_runtime::{AgentLoop, SessionCheckpoint, recover_session};
use jessica_tools::{BashTool, ReadFileTool, WasmTool, WriteFileTool};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::acceptance_check::CommandAcceptanceCheck;
use crate::config::{ConfigError, HarnessConfig};
use crate::events::{EventRecorder, RecorderError};
use crate::governance::{GovernanceState, GovernedTool, TokenCountingProvider};
use crate::pump::{EventPump, SessionSnapshot};
use crate::replay::ReplayError;

/// Maximum time to wait for the event pump to observe a terminal event.
const PUMP_TIMEOUT: Duration = Duration::from_secs(30);

/// How a new attempt enters the agent loop.
enum AttemptStart {
    /// A new session with a fresh conversation.
    Fresh {
        /// Messages the session starts with.
        messages: Vec<Message>,
    },
    /// A recovered session continuing an interrupted loop.
    Continue {
        /// Checkpoint rebuilt from the recorded events.
        checkpoint: SessionCheckpoint,
    },
}

/// Per-turn middleware, agent, and pump wiring.
struct TurnSetup {
    /// Harness owning the events, guards, policy, and sensors.
    harness: Arc<Harness>,
    /// Event channel shared by the harness, agent, and governance.
    events: broadcast::Sender<AgentEvent>,
    /// Cancellation token for the turn.
    cancel: CancellationToken,
    /// Budget and policy state observed by the wrappers and pump.
    governance: Arc<GovernanceState>,
    /// Agent loop for this turn.
    agent: Arc<AgentLoop>,
    /// Transcript renderer and guard checkpoint observer.
    pump: Arc<EventPump>,
    /// Background task draining the event channel into the pump.
    pump_task: tokio::task::JoinHandle<()>,
    /// Background task forwarding Ctrl+C to the cancellation token.
    signal_task: tokio::task::JoinHandle<()>,
    /// Stateful stuck guard reset per attempt.
    stuck_guard: Arc<StuckDetectionGuard>,
}

impl TurnSetup {
    /// Stops the background tasks owned by this turn.
    fn shutdown(self) {
        self.signal_task.abort();
        self.pump_task.abort();
    }
}

/// Handles shared by every attempt in one turn.
#[derive(Clone)]
struct AttemptContext {
    /// Agent loop for this turn.
    agent: Arc<AgentLoop>,
    /// Harness owning middleware and the event channel.
    harness: Arc<Harness>,
    /// Transcript renderer and guard checkpoint observer.
    pump: Arc<EventPump>,
    /// Budget and policy state observed by the wrappers and pump.
    governance: Arc<GovernanceState>,
    /// Stateful stuck guard reset per attempt.
    stuck_guard: Arc<StuckDetectionGuard>,
    /// Event recorder, when persistence is enabled.
    recorder: Option<Arc<EventRecorder>>,
    /// Cancellation token for the turn.
    cancel: CancellationToken,
}

impl AttemptContext {
    /// Clones the per-turn handles into an attempt context.
    fn from_setup(setup: &TurnSetup, recorder: Option<Arc<EventRecorder>>) -> Self {
        Self {
            agent: Arc::clone(&setup.agent),
            harness: Arc::clone(&setup.harness),
            pump: Arc::clone(&setup.pump),
            governance: Arc::clone(&setup.governance),
            stuck_guard: Arc::clone(&setup.stuck_guard),
            recorder,
            cancel: setup.cancel.clone(),
        }
    }
}

/// Owns the provider, configuration, and workspace shared by every turn.
pub(crate) struct CliRuntime {
    provider: Arc<dyn LlmProvider>,
    config: Arc<HarnessConfig>,
    root: PathBuf,
    system_prompt: String,
    wasm_tools: Vec<Arc<dyn Tool>>,
    recorder: Option<Arc<EventRecorder>>,
}

impl CliRuntime {
    /// Creates a runtime for the current workspace.
    ///
    /// `wasm_tools` contains plugins already loaded by [`load_wasm_tools`].
    /// Compilation happens once at startup rather than on every turn.
    /// `recorder` is present when the configuration enables event persistence.
    pub(crate) fn new(
        provider: Arc<dyn LlmProvider>,
        config: HarnessConfig,
        root: PathBuf,
        wasm_tools: Vec<Arc<dyn Tool>>,
        recorder: Option<Arc<EventRecorder>>,
    ) -> Self {
        let system_prompt = system_prompt(&root, &config);
        Self {
            provider,
            config: Arc::new(config),
            root,
            system_prompt,
            wasm_tools,
            recorder,
        }
    }

    /// Runs one user request with the configured governance.
    pub(crate) async fn run_turn(&self, input: String) -> Result<SessionEndReason, CliError> {
        let setup = self.build_turn()?;
        let initial = vec![
            Message::system(self.system_prompt.clone()),
            Message::user(input),
        ];

        let context = AttemptContext::from_setup(&setup, self.recorder.clone());
        let (reason, emit_outer) = if let Some(acceptance) = &self.config.acceptance {
            let check = CommandAcceptanceCheck::new(
                acceptance.name.clone(),
                acceptance.command.clone(),
                acceptance.args.clone(),
                resolve_dir(&self.root, acceptance.working_dir.as_deref()),
            );
            let runner = AcceptanceRunner::new(
                Arc::new(check) as Arc<dyn AcceptanceCheck>,
                acceptance.max_retries,
                setup.events.clone(),
            );
            let outcome = runner
                .run(initial, |messages| {
                    let context = context.clone();
                    async move { run_attempt(context, AttemptStart::Fresh { messages }).await }
                })
                .await;

            (outcome.reason, true)
        } else {
            let attempt = run_attempt(context, AttemptStart::Fresh { messages: initial }).await;
            (attempt.reason, setup.governance.tripped())
        };

        // A budget trip overrides the runtime's Completed/Cancelled reason.
        // Emit an outer session event so the final observable reason is the
        // governed one rather than the last attempt's reason.
        if emit_outer {
            emit_outer_session(&setup.pump, &setup.events, self.recorder.as_deref(), reason).await;
        }

        setup.shutdown();
        Ok(reason)
    }

    /// Continues the interrupted loop recorded for `session_id`.
    ///
    /// Recovery is read-only: the checkpoint supplies the messages and round
    /// count, and [`AgentLoop::resume`] continues from there without calling
    /// the model or any tool for work already in the log. An already-ended
    /// session returns its recorded reason without starting a loop.
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

        let setup = self.build_turn()?;
        let context = AttemptContext::from_setup(&setup, self.recorder.clone());
        let attempt = run_attempt(context, AttemptStart::Continue { checkpoint }).await;
        let reason = attempt.reason;
        if setup.governance.tripped() {
            emit_outer_session(&setup.pump, &setup.events, self.recorder.as_deref(), reason).await;
        }

        setup.shutdown();
        Ok(reason)
    }

    /// Builds the harness, governance wrappers, agent, and event pump for one
    /// turn.
    fn build_turn(&self) -> Result<TurnSetup, CliError> {
        let timeout = self.config.budget.tool_timeout();
        let token_guard = Arc::new(TokenBudgetGuard::new(self.config.budget.max_tokens));
        let timeout_guard = Arc::new(ToolTimeoutGuard::new(timeout));
        let stuck_guard = Arc::new(StuckDetectionGuard::new(
            self.config.budget.max_stuck_rounds,
        ));
        let result_guard = Arc::new(ResultSizeGuard::new(self.config.budget.max_result_bytes));

        let guards: Vec<Arc<dyn Guard>> = vec![
            Arc::clone(&token_guard) as Arc<dyn Guard>,
            Arc::clone(&timeout_guard) as Arc<dyn Guard>,
            Arc::clone(&stuck_guard) as Arc<dyn Guard>,
            Arc::clone(&result_guard) as Arc<dyn Guard>,
        ];
        let policy: Arc<dyn Policy> = Arc::new(AllowlistPolicy::new(
            self.config.policy.allowed_tools.clone(),
        ));
        let sensors = self.build_sensors();
        let harness = Arc::new(
            Harness::with_event_capacity(Vec::new(), DEFAULT_EVENT_CAPACITY)
                .with_guards(guards)
                .with_policy(policy)
                .with_sensors(sensors),
        );

        let events = harness.events();
        let cancel = CancellationToken::new();
        let governance = Arc::new(GovernanceState::new(events.clone(), cancel.clone()));
        let provider: Arc<dyn LlmProvider> = Arc::new(TokenCountingProvider::new(
            Arc::clone(&self.provider),
            Arc::clone(&governance),
            Arc::clone(&token_guard),
        ));

        let registry = self.build_tool_registry(
            Arc::clone(&harness),
            Arc::clone(&governance),
            timeout,
            Arc::clone(&timeout_guard),
            Arc::clone(&result_guard),
        )?;
        let agent = Arc::new(AgentLoop::new(
            provider,
            Arc::new(registry),
            self.config.budget.to_budget(),
            events.clone(),
        ));
        let pump = Arc::new(EventPump::new(
            Arc::clone(&harness),
            Arc::clone(&governance),
            self.recorder.clone(),
        ));
        let pump_task = tokio::spawn(Arc::clone(&pump).run(harness.subscribe()));
        let signal_task = spawn_ctrl_c_listener(cancel.clone());

        Ok(TurnSetup {
            harness,
            events,
            cancel,
            governance,
            agent,
            pump,
            pump_task,
            signal_task,
            stuck_guard,
        })
    }

    fn build_sensors(&self) -> Vec<Arc<dyn Sensor>> {
        let Some(sensor) = &self.config.sensor else {
            return Vec::new();
        };
        let command = CommandSensor::new(&sensor.name, &sensor.command)
            .args(sensor.args.clone())
            .with_current_dir(resolve_dir(&self.root, sensor.working_dir.as_deref()));
        vec![Arc::new(command) as Arc<dyn Sensor>]
    }

    fn build_tool_registry(
        &self,
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        timeout: Duration,
        timeout_guard: Arc<ToolTimeoutGuard>,
        result_guard: Arc<ResultSizeGuard>,
    ) -> Result<ToolRegistry, CliError> {
        let mut registry = ToolRegistry::new();

        let read: Arc<dyn Tool> = Arc::new(ReadFileTool::new(self.root.clone()));
        let read = GovernedTool::new(
            read,
            Arc::clone(&harness),
            Arc::clone(&governance),
            timeout,
            Arc::clone(&timeout_guard),
            Arc::clone(&result_guard),
        );
        registry
            .register(Arc::new(read))
            .map_err(|source| CliError::RegisterTool {
                name: "read_file".to_owned(),
                source,
            })?;

        let write: Arc<dyn Tool> = Arc::new(WriteFileTool::new(self.root.clone()));
        let write = GovernedTool::new(
            write,
            Arc::clone(&harness),
            Arc::clone(&governance),
            timeout,
            Arc::clone(&timeout_guard),
            Arc::clone(&result_guard),
        );
        registry
            .register(Arc::new(write))
            .map_err(|source| CliError::RegisterTool {
                name: "write_file".to_owned(),
                source,
            })?;

        for tool in &self.wasm_tools {
            let name = tool.name().to_owned();
            let governed = GovernedTool::new(
                Arc::clone(tool),
                Arc::clone(&harness),
                Arc::clone(&governance),
                timeout,
                Arc::clone(&timeout_guard),
                Arc::clone(&result_guard),
            );
            registry
                .register(Arc::new(governed))
                .map_err(|source| CliError::RegisterTool { name, source })?;
        }

        let bash: Arc<dyn Tool> = Arc::new(BashTool::new());
        let bash = GovernedTool::new(
            bash,
            harness,
            governance,
            timeout,
            timeout_guard,
            result_guard,
        );
        registry
            .register(Arc::new(bash))
            .map_err(|source| CliError::RegisterTool {
                name: "bash".to_owned(),
                source,
            })?;

        Ok(registry)
    }
}

/// Loads every wasm tool manifest declared in `config`.
///
/// Manifest reading and wasm compilation are blocking, so all manifests are
/// loaded in one `spawn_blocking` task. Relative manifest paths resolve
/// against the workspace root. The returned tools are shared across turns;
/// each turn still wraps them in fresh governance.
pub(crate) async fn load_wasm_tools(
    config: &HarnessConfig,
    root: &Path,
) -> Result<Vec<Arc<dyn Tool>>, CliError> {
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
            .map(|path| WasmTool::load(path).map(|tool| Arc::new(tool) as Arc<dyn Tool>))
            .collect::<Result<Vec<_>, _>>()
    })
    .await
    .map_err(CliError::WasmToolTask)?
    .map_err(CliError::WasmTool)
}

/// Runs one agent attempt and waits until its transcript is complete.
///
/// The stuck guard is stateful per session, so it is reset before every
/// acceptance retry; token usage remains cumulative across the whole turn.
async fn run_attempt(context: AttemptContext, start: AttemptStart) -> SessionAttempt {
    context.stuck_guard.reset();
    let (session_id, initial_messages) = match &start {
        AttemptStart::Fresh { messages } => (next_session_id(), messages.clone()),
        AttemptStart::Continue { checkpoint } => (
            checkpoint.session_id().clone(),
            checkpoint.messages().to_vec(),
        ),
    };

    // A fresh session needs its immutable bootstrap messages before any event
    // is appended. A resumed session already has them in the store.
    if let AttemptStart::Fresh { messages } = &start
        && let Some(recorder) = &context.recorder
        && let Err(error) = recorder.record_session_start(&session_id, messages).await
    {
        eprintln!("[warn] failed to record session metadata for {session_id}: {error}");
    }

    context
        .pump
        .register(session_id.clone(), initial_messages.clone());
    let fallback_messages = initial_messages;

    let raw_reason = match start {
        AttemptStart::Fresh { messages } => {
            context
                .harness
                .run(
                    &session_id,
                    context
                        .agent
                        .run(session_id.clone(), messages, context.cancel.clone()),
                )
                .await
        }
        AttemptStart::Continue { checkpoint } => {
            context
                .harness
                .run(
                    &session_id,
                    context.agent.resume(checkpoint, context.cancel.clone()),
                )
                .await
        }
    };
    let reason = if context.governance.tripped() && raw_reason != SessionEndReason::BudgetExceeded {
        SessionEndReason::BudgetExceeded
    } else {
        raw_reason
    };

    let (snapshot, pump_failed) =
        match tokio::time::timeout(PUMP_TIMEOUT, context.pump.wait_for(&session_id)).await {
            Ok(Some(snapshot)) => (snapshot, false),
            Ok(None) => {
                eprintln!("[warn] event pump lost session {session_id}");
                (
                    SessionSnapshot {
                        messages: fallback_messages,
                        last_output: String::new(),
                    },
                    true,
                )
            }
            Err(_) => {
                eprintln!("[warn] event pump timed out for session {session_id}");
                (
                    SessionSnapshot {
                        messages: fallback_messages,
                        last_output: String::new(),
                    },
                    true,
                )
            }
        };

    SessionAttempt {
        messages: snapshot.messages,
        last_output: snapshot.last_output,
        reason: if pump_failed {
            SessionEndReason::Error
        } else {
            reason
        },
    }
}

/// Emits a final outer session event after acceptance orchestration.
///
/// Individual attempts already emitted their own terminal events. The outer
/// event carries the governed turn's final reason, so a retry-exhausted turn is
/// observable as [`SessionEndReason::AcceptanceFailed`] rather than ending with
/// the last attempt's [`SessionEndReason::Completed`].
async fn emit_outer_session(
    pump: &EventPump,
    events: &broadcast::Sender<AgentEvent>,
    recorder: Option<&EventRecorder>,
    reason: SessionEndReason,
) {
    let session_id = next_session_id();
    if let Some(recorder) = recorder
        && let Err(error) = recorder.record_session_start(&session_id, &[]).await
    {
        eprintln!("[warn] failed to record session metadata for {session_id}: {error}");
    }
    pump.register(session_id.clone(), Vec::new());
    let _ = events.send(AgentEvent::SessionStarted {
        session_id: session_id.clone(),
    });
    let _ = events.send(AgentEvent::SessionEnded {
        session_id: session_id.clone(),
        reason,
    });

    match tokio::time::timeout(PUMP_TIMEOUT, pump.wait_for(&session_id)).await {
        Ok(Some(_)) => {}
        Ok(None) => eprintln!("[warn] outer session was not registered with the event pump"),
        Err(_) => eprintln!("[warn] timed out while flushing the outer session event"),
    }
}

fn spawn_ctrl_c_listener(cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                break;
            }
            cancel.cancel();
        }
    })
}

fn resolve_dir(root: &Path, configured: Option<&Path>) -> PathBuf {
    match configured {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => root.join(path),
        None => root.to_path_buf(),
    }
}

fn system_prompt(root: &Path, config: &HarnessConfig) -> String {
    let allowed = if config.policy.allowed_tools.is_empty() {
        "none".to_owned()
    } else {
        config.policy.allowed_tools.join(", ")
    };
    format!(
        "You are Jessica, a deterministic coding agent. The workspace root is {}. \
         Use read_file and write_file with paths relative to the workspace root; bash is also \
         available when allowed. Tool calls are subject to this allowlist: {allowed}. A denied \
         call returns an error result, so adapt instead of repeating it. A command sensor runs \
         after every tool result. If an acceptance check fails, its feedback is injected as a \
         user message: fix the underlying problem and retry. Verify your work with tools instead \
         of claiming success. When the task is complete, reply with a concise summary.",
        root.display()
    )
}

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a process-local session id.
fn next_session_id() -> SessionId {
    let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    SessionId::new(format!("cli-{}-{counter}", std::process::id()))
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
    /// A tool could not be registered.
    #[error("failed to register the `{name}` tool: {source}")]
    RegisterTool {
        /// Name of the tool that could not be registered.
        name: String,
        /// Underlying registry error.
        #[source]
        source: ToolRegistryError,
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
    /// `--resume` was given without a session id.
    #[error("`--resume` requires a session id")]
    MissingResumeSessionId,
    /// `--resume` was given more than once.
    #[error("`--resume` may be given only once")]
    DuplicateResume,
    /// A prompt and `--resume` cannot be used together.
    #[error("`--resume` cannot be combined with a prompt")]
    ResumeWithPrompt,
    /// `--resume` was requested without a configured event store.
    #[error("`--resume` requires an `[events]` path in the configuration")]
    ResumeRequiresEventStore,
    /// `replay` was requested without a configured event store.
    #[error("`replay` requires an `[events]` path in the configuration")]
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
    /// The current directory could not be determined.
    #[error("failed to determine the current directory: {0}")]
    CurrentDir(#[source] io::Error),
    /// The blocking current-directory task failed.
    #[error("current-directory task failed: {0}")]
    CurrentDirTask(#[source] tokio::task::JoinError),
}
