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
use jessica_core::types::{AgentEvent, Message, Role, SessionEndReason, SessionId};
use jessica_harness::{
    AcceptanceOutcome, AcceptanceRunner, AllowlistPolicy, CommandSensor, DEFAULT_EVENT_CAPACITY,
    Harness, ResultSizeGuard, SessionAttempt, StuckDetectionGuard, TokenBudgetGuard,
    ToolTimeoutGuard, TurnSignals,
};
use jessica_runtime::{
    AgentChildExecutor, AgentLoop, ChildToolFactory, DELEGATE_TASK_TOOL_NAME, DelegateTaskTool,
    DelegationError, ParentCapabilities, SessionCheckpoint, recover_session,
};
use jessica_tools::{BashTool, ReadFileTool, WasmTool, WriteFileTool};
use thiserror::Error;
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;

use crate::acceptance_check::CommandAcceptanceCheck;
use crate::acp_store::AcpStoreError;
use crate::config::{ConfigError, HarnessConfig, NATIVE_CHILD_TOOLS};
use crate::delegation::NativeChildToolFactory;
use crate::events::{EventRecorder, RecorderError};
use crate::governance::{GovernanceState, GovernedTool, TokenCountingProvider};
use crate::memory::MemoryAssembly;
use crate::pump::{EventPump, PumpOutput, SessionSnapshot, UpdateSink};
use crate::replay::ReplayError;
use crate::shutdown::ShutdownController;
use crate::skills::{SkillCommandError, SkillsAssembly};

/// Maximum time to wait for the event pump to observe a terminal event.
const PUMP_TIMEOUT: Duration = Duration::from_secs(30);

/// How a new attempt enters the agent loop.
enum AttemptStart {
    /// A new session with a fresh conversation.
    Fresh {
        /// Identifier assigned to the new session.
        session_id: SessionId,
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
    /// Stateful stuck guard reset per attempt.
    stuck_guard: Arc<StuckDetectionGuard>,
}

impl TurnSetup {
    /// Stops the background tasks owned by this turn.
    ///
    /// The turn's cancellation token is a child of the process-level token, so
    /// dropping this setup does not affect the shutdown controller.
    fn shutdown(self) {
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
    /// When present the pump runs with [`PumpOutput::Silent`] and every event
    /// is published to this sink in pump order. When absent the pump keeps the
    /// existing `HumanConsole` rendering.
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
}

/// Observable result of one reusable turn execution.
pub(crate) struct TurnOutcome {
    /// Final reason, including governance overrides.
    pub(crate) reason: SessionEndReason,
    /// Conversation reconstructed for this turn, including history.
    ///
    /// A turn cancelled before it starts returns an empty vector because no
    /// attempt ran. The CLI only needs the reason; the programmatic turn path
    /// consumes this transcript directly.
    pub(crate) messages: Vec<Message>,
    /// Index in [`Self::messages`] just after this turn's user message.
    ///
    /// The new user message itself is at `turn_start - 1`. Everything before
    /// it is the system prompt, recalled guides, or history supplied by the
    /// caller, and everything from this offset onward was generated by the
    /// turn. A turn cancelled before it starts reports `0`.
    pub(crate) turn_start: usize,
    /// Whether the event pump lost events to broadcast lag.
    ///
    /// A programmatic caller must treat this as an internal failure rather
    /// than presenting a truncated transcript as complete.
    pub(crate) lagged: bool,
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
    wasm_tools: Vec<Arc<dyn Tool>>,
    recorder: Option<Arc<EventRecorder>>,
    memory: Option<MemoryAssembly>,
    skills: Option<SkillsAssembly>,
    /// Process-level shutdown state shared by every turn and the REPL.
    shutdown: Arc<ShutdownController>,
}

impl CliRuntime {
    /// Creates a runtime for the current workspace.
    ///
    /// `config` must already contain the fully validated startup configuration;
    /// the same value is cloned at the start of every turn so `initial_messages`
    /// and `build_turn` cannot observe different snapshots.
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
        wasm_tools: Vec<Arc<dyn Tool>>,
        recorder: Option<Arc<EventRecorder>>,
        memory: Option<MemoryAssembly>,
        skills: Option<SkillsAssembly>,
        shutdown: Arc<ShutdownController>,
    ) -> Self {
        Self {
            provider,
            config,
            root,
            wasm_tools,
            recorder,
            memory,
            skills,
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
            })
            .await?;
        Ok(outcome.reason)
    }

    /// Executes one turn with an explicit workspace scope and history.
    ///
    /// A turn starts with `system prompt → recalled memory (if any) → recalled
    /// skills (if any) → history → user input`. Memory and skills are recalled
    /// exactly once per turn, before the first attempt; acceptance retries
    /// reuse the same initial conversation. A turn that finally completes is
    /// written to memory and may enqueue a skill job according to the
    /// configured policies. Signal counts cover only messages created by this
    /// turn, never tool calls replayed from `history`.
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
        } = request;

        // One snapshot for the whole turn. `build_turn` and
        // `initial_messages` must never mix a newer published config with an
        // older one, even if the watcher fires mid-turn.
        let config = self.config_snapshot();
        let setup = self.build_turn(&scope, true, &config, update_sink)?;
        // A signal may arrive between the REPL's last check and this point.
        // The child token is already cancelled in that case: skip memory and
        // skill recall, session recording, and every model request.
        if setup.cancel.is_cancelled() {
            setup.shutdown();
            return Ok(TurnOutcome {
                reason: SessionEndReason::Cancelled,
                messages: Vec::new(),
                turn_start: 0,
                lagged: false,
            });
        }
        let first_session = session_id.unwrap_or_else(next_session_id);
        let initial = self
            .initial_messages(
                &scope,
                &first_session,
                &history,
                user_input.as_str(),
                &config,
            )
            .await;
        // Everything before this offset is history or guide material and never
        // contributes to this turn's structured counts.
        let turn_start = initial.len();

        let context = AttemptContext::from_setup(&setup, self.recorder.clone());
        let (
            reason,
            emit_outer,
            final_session,
            final_output,
            final_messages,
            signals,
            attempt_start,
        ) = if let Some(acceptance) = &config.acceptance {
            let retries = acceptance_retries.unwrap_or(acceptance.max_retries);
            let check = CommandAcceptanceCheck::new(
                acceptance.name.clone(),
                acceptance.command.clone(),
                acceptance.args.clone(),
                resolve_dir(scope.root(), acceptance.working_dir.as_deref()),
            );
            let runner = AcceptanceRunner::new(
                Arc::new(check) as Arc<dyn AcceptanceCheck>,
                retries,
                setup.events.clone(),
            );
            let mut next_session = Some(first_session.clone());
            let mut latest_session = None;
            let mut latest_attempt_start = turn_start;
            let outcome = runner
                .run(initial, |messages| {
                    latest_attempt_start = messages.len();
                    let session_id = next_session.take().unwrap_or_else(next_session_id);
                    latest_session = Some(session_id.clone());
                    let context = context.clone();
                    async move {
                        run_attempt(
                            context,
                            AttemptStart::Fresh {
                                session_id,
                                messages,
                            },
                        )
                        .await
                    }
                })
                .await;
            let signals = acceptance_signals(&outcome, latest_attempt_start);

            (
                outcome.reason,
                true,
                latest_session.unwrap_or(first_session),
                outcome.last_output,
                outcome.messages,
                signals,
                latest_attempt_start,
            )
        } else {
            let attempt = run_attempt(
                context,
                AttemptStart::Fresh {
                    session_id: first_session.clone(),
                    messages: initial,
                },
            )
            .await;
            let (tool_calls, rounds) = structured_counts(&attempt.messages, turn_start);
            (
                attempt.reason,
                setup.governance.tripped(),
                first_session,
                attempt.last_output,
                attempt.messages,
                TurnSignals::new(attempt.reason, tool_calls, rounds),
                turn_start,
            )
        };

        // A budget trip overrides the runtime's Completed/Cancelled reason.
        // Emit an outer session event so the final observable reason is the
        // governed one rather than the last attempt's reason.
        if emit_outer {
            emit_outer_session(&setup.pump, &setup.events, self.recorder.as_deref(), reason).await;
        }
        let governed_completed =
            reason == SessionEndReason::Completed && !setup.governance.tripped();
        let turn_index = stable_turn_index(&final_messages);
        if governed_completed {
            // Memory describes the whole turn, including every acceptance
            // attempt; skill signals describe the final attempt. Both slices
            // exclude history and guide messages replayed from before the
            // turn.
            self.record_turn_memory(
                &final_session,
                &user_input,
                &final_output,
                final_messages.get(turn_start..).unwrap_or(&[]),
                turn_index,
            )
            .await;
            self.record_skill_job(
                &final_session,
                turn_index,
                &user_input,
                &final_output,
                &signals,
                final_messages.get(attempt_start..).unwrap_or(&[]),
            )
            .await;
        }

        let lagged = setup.pump.has_lagged();
        setup.shutdown();
        Ok(TurnOutcome {
            reason,
            messages: final_messages,
            turn_start,
            lagged,
        })
    }

    /// Builds the initial conversation for a fresh turn.
    ///
    /// The order is fixed: system prompt, optional memory guide, optional
    /// skill guide, history, then user input. A disabled or failed guide
    /// simply omits that message. Memory and skills are each recalled exactly
    /// once here; the system prompt is rendered from the same configuration
    /// snapshot as the rest of the turn and from the turn's canonical root.
    async fn initial_messages(
        &self,
        scope: &TurnScope,
        session: &SessionId,
        history: &[Message],
        input: &str,
        config: &HarnessConfig,
    ) -> Vec<Message> {
        let memory_message = match &self.memory {
            Some(memory) => memory.recall(session, input).await,
            None => None,
        };
        let skill_message = match &self.skills {
            Some(skills) => skills.recall(session, input).await,
            None => None,
        };
        fresh_conversation(
            &system_prompt(scope.root(), config),
            memory_message,
            skill_message,
            history,
            input,
        )
    }

    /// Writes the completed turn to memory when the policy allows it.
    ///
    /// `messages` must contain only messages created by this turn, so tool
    /// calls replayed from history never inflate the recorded count. Memory
    /// failures are warned about and never change the reason or exit code of
    /// the turn.
    async fn record_turn_memory(
        &self,
        session_id: &SessionId,
        user_input: &str,
        assistant_answer: &str,
        messages: &[Message],
        turn_index: u64,
    ) {
        let Some(memory) = &self.memory else {
            return;
        };
        let tool_call_count = messages
            .iter()
            .map(|message| message.tool_calls.len())
            .sum();
        if let Err(error) = memory
            .record_completed_turn(
                session_id,
                user_input,
                assistant_answer,
                tool_call_count,
                turn_index,
            )
            .await
        {
            eprintln!("[warn] failed to write memory for session {session_id}: {error}");
        }
    }

    /// Enqueues a skill job when the completed turn crosses the thresholds.
    ///
    /// Counts are taken from the final attempt: `messages` must already be
    /// sliced to that attempt. The enqueue is idempotent and fail-open; it
    /// never writes the event stream and never awaits distillation.
    async fn record_skill_job(
        &self,
        session_id: &SessionId,
        turn_index: u64,
        user_input: &str,
        assistant_answer: &str,
        signals: &TurnSignals,
        messages: &[Message],
    ) {
        let Some(skills) = &self.skills else {
            return;
        };
        skills
            .after_completed_turn(
                session_id,
                turn_index,
                user_input,
                assistant_answer,
                signals,
                messages,
            )
            .await;
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

        let user_input = checkpoint
            .messages()
            .iter()
            .find(|message| message.role == Role::User)
            .map(|message| message.content.clone())
            .unwrap_or_default();

        let config = self.config_snapshot();
        let scope = TurnScope::new(self.root.clone(), self.shutdown.child_token()).await?;
        let setup = self.build_turn(&scope, false, &config, None)?;
        if setup.cancel.is_cancelled() {
            setup.shutdown();
            return Ok(SessionEndReason::Cancelled);
        }
        let context = AttemptContext::from_setup(&setup, self.recorder.clone());
        let attempt = run_attempt(context, AttemptStart::Continue { checkpoint }).await;
        let reason = attempt.reason;
        if setup.governance.tripped() {
            emit_outer_session(&setup.pump, &setup.events, self.recorder.as_deref(), reason).await;
        }
        let governed_completed =
            reason == SessionEndReason::Completed && !setup.governance.tripped();
        let turn_index = stable_turn_index(&attempt.messages);
        if governed_completed {
            let (tool_calls, rounds) = structured_counts(&attempt.messages, 0);
            let signals = TurnSignals::new(reason, tool_calls, rounds);
            self.record_turn_memory(
                &session_id,
                &user_input,
                &attempt.last_output,
                &attempt.messages,
                turn_index,
            )
            .await;
            // Resume never injects skills, but a completed resumed turn may
            // enqueue idempotently through the skills.db unique index.
            self.record_skill_job(
                &session_id,
                turn_index,
                &user_input,
                &attempt.last_output,
                &signals,
                &attempt.messages,
            )
            .await;
        }

        setup.shutdown();
        Ok(reason)
    }

    /// Builds the harness, governance wrappers, agent, and event pump for one
    /// turn.
    ///
    /// `allow_delegation` is false on the resume path. Resume continues a
    /// recorded loop but never constructs a child factory or executor; a fresh
    /// turn constructs them only when `[delegation]` is enabled. `update_sink`
    /// selects silent protocol output for a programmatic client; without a
    /// sink the pump keeps rendering to the terminal.
    fn build_turn(
        &self,
        scope: &TurnScope,
        allow_delegation: bool,
        config: &HarnessConfig,
        update_sink: Option<Arc<dyn UpdateSink>>,
    ) -> Result<TurnSetup, CliError> {
        let timeout = config.budget.tool_timeout();
        let token_guard = Arc::new(TokenBudgetGuard::new(config.budget.max_tokens));
        let timeout_guard = Arc::new(ToolTimeoutGuard::new(timeout));
        let stuck_guard = Arc::new(StuckDetectionGuard::new(config.budget.max_stuck_rounds));
        let result_guard = Arc::new(ResultSizeGuard::new(config.budget.max_result_bytes));

        let guards: Vec<Arc<dyn Guard>> = vec![
            Arc::clone(&token_guard) as Arc<dyn Guard>,
            Arc::clone(&timeout_guard) as Arc<dyn Guard>,
            Arc::clone(&stuck_guard) as Arc<dyn Guard>,
            Arc::clone(&result_guard) as Arc<dyn Guard>,
        ];
        let policy: Arc<dyn Policy> =
            Arc::new(AllowlistPolicy::new(config.policy.allowed_tools.clone()));
        let sensors = self.build_sensors(scope, config);
        let harness = Arc::new(
            Harness::with_event_capacity(Vec::new(), DEFAULT_EVENT_CAPACITY)
                .with_guards(guards)
                .with_policy(policy)
                .with_sensors(sensors),
        );

        let events = harness.events();
        // Every turn derives its own child token; only the scope's parent and
        // this turn (through the loop) may cancel it.
        let cancel = scope.cancel_parent().child_token();
        let governance = Arc::new(GovernanceState::new(events.clone(), cancel.clone()));
        let provider: Arc<dyn LlmProvider> = Arc::new(TokenCountingProvider::new(
            Arc::clone(&self.provider),
            Arc::clone(&governance),
            Arc::clone(&token_guard),
        ));
        // The child executor receives the same counting provider, so every
        // child call charges the parent token guard and can cancel the turn.
        let delegation =
            self.build_child_executor(scope, allow_delegation, Arc::clone(&provider), config)?;

        let registry = self.build_tool_registry(
            scope,
            Arc::clone(&harness),
            Arc::clone(&governance),
            timeout,
            Arc::clone(&timeout_guard),
            Arc::clone(&result_guard),
            delegation,
        )?;
        let agent = Arc::new(AgentLoop::new(
            provider,
            Arc::new(registry),
            config.budget.to_budget(),
            events.clone(),
        ));
        let output = if update_sink.is_some() {
            PumpOutput::Silent
        } else {
            PumpOutput::HumanConsole
        };
        let pump = Arc::new(EventPump::new(
            Arc::clone(&harness),
            Arc::clone(&governance),
            self.recorder.clone(),
            output,
            update_sink,
        ));
        let pump_task = tokio::spawn(Arc::clone(&pump).run(harness.subscribe()));

        Ok(TurnSetup {
            harness,
            events,
            cancel,
            governance,
            agent,
            pump,
            pump_task,
            stuck_guard,
        })
    }

    fn build_sensors(&self, scope: &TurnScope, config: &HarnessConfig) -> Vec<Arc<dyn Sensor>> {
        let Some(sensor) = &config.sensor else {
            return Vec::new();
        };
        let command = CommandSensor::new(&sensor.name, &sensor.command)
            .args(sensor.args.clone())
            .with_current_dir(resolve_dir(scope.root(), sensor.working_dir.as_deref()));
        vec![Arc::new(command) as Arc<dyn Sensor>]
    }

    /// Builds the child executor when delegation is enabled for this path.
    ///
    /// Returns `None` when delegation is disabled or when `allow_delegation`
    /// is false, and in that case does not construct the factory or executor.
    fn build_child_executor(
        &self,
        scope: &TurnScope,
        allow_delegation: bool,
        provider: Arc<dyn LlmProvider>,
        config: &HarnessConfig,
    ) -> Result<Option<Arc<AgentChildExecutor>>, CliError> {
        if !allow_delegation || !config.delegation.enabled {
            return Ok(None);
        }

        let factory: Arc<dyn ChildToolFactory> =
            Arc::new(NativeChildToolFactory::new(scope.root().to_path_buf()));
        let parent = ParentCapabilities::new(
            config.policy.allowed_tools.clone(),
            NATIVE_CHILD_TOOLS.iter().map(|name| (*name).to_owned()),
        );
        let executor =
            AgentChildExecutor::new(provider, factory, parent, config.delegation.to_limits())?;
        Ok(Some(Arc::new(executor)))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_tool_registry(
        &self,
        scope: &TurnScope,
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        timeout: Duration,
        timeout_guard: Arc<ToolTimeoutGuard>,
        result_guard: Arc<ResultSizeGuard>,
        delegation: Option<Arc<AgentChildExecutor>>,
    ) -> Result<ToolRegistry, CliError> {
        let mut registry = ToolRegistry::new();

        let read: Arc<dyn Tool> = Arc::new(ReadFileTool::new(scope.root().to_path_buf()));
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

        let write: Arc<dyn Tool> = Arc::new(WriteFileTool::new(scope.root().to_path_buf()));
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

        let bash: Arc<dyn Tool> =
            Arc::new(BashTool::new().with_current_dir(scope.root().to_path_buf()));
        let bash = GovernedTool::new(
            bash,
            Arc::clone(&harness),
            Arc::clone(&governance),
            timeout,
            Arc::clone(&timeout_guard),
            Arc::clone(&result_guard),
        );
        registry
            .register(Arc::new(bash))
            .map_err(|source| CliError::RegisterTool {
                name: "bash".to_owned(),
                source,
            })?;

        if let Some(executor) = delegation {
            let delegate: Arc<dyn Tool> = Arc::new(DelegateTaskTool::new(executor));
            let delegate = GovernedTool::new(
                delegate,
                harness,
                governance,
                timeout,
                timeout_guard,
                result_guard,
            );
            registry
                .register(Arc::new(delegate))
                .map_err(|source| CliError::RegisterTool {
                    name: DELEGATE_TASK_TOOL_NAME.to_owned(),
                    source,
                })?;
        }

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
        AttemptStart::Fresh {
            session_id,
            messages,
        } => (session_id.clone(), messages.clone()),
        AttemptStart::Continue { checkpoint } => (
            checkpoint.session_id().clone(),
            checkpoint.messages().to_vec(),
        ),
    };

    // A fresh session needs its immutable bootstrap messages before any event
    // is appended. A resumed session already has them in the store.
    if let AttemptStart::Fresh { messages, .. } = &start
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
        AttemptStart::Fresh { messages, .. } => {
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

fn resolve_dir(root: &Path, configured: Option<&Path>) -> PathBuf {
    match configured {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => root.join(path),
        None => root.to_path_buf(),
    }
}

/// Builds a fresh conversation in its required order.
///
/// The order is fixed: system prompt, optional memory guide, optional skill
/// guide, history, then user input. History is empty for a CLI turn and
/// carries earlier multi-turn context for a programmatic client.
fn fresh_conversation(
    system_prompt: &str,
    memory_message: Option<Message>,
    skill_message: Option<Message>,
    history: &[Message],
    input: &str,
) -> Vec<Message> {
    let mut messages = Vec::with_capacity(4 + history.len());
    messages.push(Message::system(system_prompt));
    if let Some(message) = memory_message {
        messages.push(message);
    }
    if let Some(message) = skill_message {
        messages.push(message);
    }
    messages.extend_from_slice(history);
    messages.push(Message::user(input));
    messages
}

/// Counts tool calls and assistant rounds in `messages[attempt_start..]`.
///
/// The acceptance runner passes the accumulated conversation into each retry,
/// so the CLI records the start of the final attempt and counts only that
/// attempt. Counts come from structured messages, never from model text.
fn structured_counts(messages: &[Message], attempt_start: usize) -> (usize, usize) {
    let attempt = messages.get(attempt_start..).unwrap_or(&[]);
    let tool_calls = attempt.iter().map(|message| message.tool_calls.len()).sum();
    let rounds = attempt
        .iter()
        .filter(|message| message.role == Role::Assistant)
        .count();
    (tool_calls, rounds)
}

/// Builds trigger signals from an acceptance outcome and the final attempt
/// start offset.
fn acceptance_signals(outcome: &AcceptanceOutcome, attempt_start: usize) -> TurnSignals {
    let (tool_calls, rounds) = structured_counts(&outcome.messages, attempt_start);
    TurnSignals::from_acceptance(outcome, tool_calls, rounds)
}

/// Derives a stable turn index from the session transcript.
///
/// The index is the number of user turns already present before this one,
/// counted from structured messages. A CLI turn starts one session and both a
/// fresh turn and every resume of that turn derive `0`; a multi-turn client
/// that carries history derives the number of earlier user turns. The value
/// never depends on a process-local counter and is stable across restarts.
fn stable_turn_index(messages: &[Message]) -> u64 {
    let user_turns = messages
        .iter()
        .filter(|message| message.role == Role::User)
        .count();
    u64::try_from(user_turns.saturating_sub(1)).unwrap_or(u64::MAX)
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
    /// ACP session recovery was configured without an event-store path.
    #[error("ACP session recovery requires an [events] path")]
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::StreamExt;
    use jessica_core::llm::{LlmEvent, LlmStream};
    use jessica_core::tool::ToolSchema;
    use jessica_core::types::{TokenUsage, ToolCall};
    use jessica_runtime::CHILD_AGENT_SYSTEM_PROMPT;
    use serde_json::json;

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
        vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new(
                    "parent-call",
                    DELEGATE_TASK_TOOL_NAME,
                    json!({"task": "child task", "tools": ["read_file"]}),
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
             allowed_tools = [\"bash\", \"read_file\", \"write_file\", \"delegate_task\"]\n",
        )
        .expect("enabled delegation config must parse")
    }

    /// Builds a runtime over the given provider and config.
    fn runtime_with(provider: Arc<ScriptedProvider>, config: HarnessConfig) -> CliRuntime {
        let (_sender, receiver) = watch::channel(Arc::new(config));
        CliRuntime::new(
            provider,
            receiver,
            PathBuf::from("."),
            Vec::new(),
            None,
            None,
            None,
            Arc::new(ShutdownController::new()),
        )
    }

    /// Creates a turn scope rooted at the process current directory.
    async fn test_scope() -> TurnScope {
        TurnScope::new(PathBuf::from("."), CancellationToken::new())
            .await
            .expect("the test workspace must canonicalize")
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
        let enabled_config_snapshot = enabled_runtime.config_snapshot();
        let setup = enabled_runtime
            .build_turn(&test_scope().await, true, &enabled_config_snapshot, None)
            .expect("turn should build");
        let reason = setup
            .agent
            .run(
                SessionId::new("enabled"),
                vec![Message::user("hi")],
                setup.cancel.clone(),
            )
            .await;
        assert_eq!(reason, SessionEndReason::Completed);
        setup.shutdown();

        let enabled_calls = enabled_provider.calls();
        let enabled_names = schema_names(&enabled_calls[0].tools);
        assert_eq!(
            enabled_names,
            vec!["bash", "delegate_task", "read_file", "write_file"]
        );

        let disabled_provider = Arc::new(ScriptedProvider::new(vec![done_text("ok", 1)]));
        let disabled_runtime =
            runtime_with(Arc::clone(&disabled_provider), HarnessConfig::default());
        let disabled_config_snapshot = disabled_runtime.config_snapshot();
        let setup = disabled_runtime
            .build_turn(&test_scope().await, true, &disabled_config_snapshot, None)
            .expect("turn should build");
        setup
            .agent
            .run(
                SessionId::new("disabled"),
                vec![Message::user("hi")],
                setup.cancel.clone(),
            )
            .await;
        setup.shutdown();

        let disabled_calls = disabled_provider.calls();
        let disabled_names = schema_names(&disabled_calls[0].tools);
        assert_eq!(disabled_names, vec!["bash", "read_file", "write_file"]);

        let resume_provider = Arc::new(ScriptedProvider::new(vec![done_text("ok", 1)]));
        let resume_runtime = runtime_with(Arc::clone(&resume_provider), enabled_config());
        let resume_config_snapshot = resume_runtime.config_snapshot();
        let setup = resume_runtime
            .build_turn(&test_scope().await, false, &resume_config_snapshot, None)
            .expect("turn should build");
        setup
            .agent
            .run(
                SessionId::new("resume"),
                vec![Message::user("hi")],
                setup.cancel.clone(),
            )
            .await;
        setup.shutdown();

        let resume_calls = resume_provider.calls();
        let resume_names = schema_names(&resume_calls[0].tools);
        assert_eq!(
            resume_names,
            vec!["bash", "read_file", "write_file"],
            "the resume path must not register delegate_task"
        );
    }

    #[tokio::test]
    async fn child_calls_charge_the_parent_token_budget_through_the_shared_wrapper() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            delegate_request(),
            done_text("child answer", 10),
            done_text("parent done", 10),
        ]));
        let runtime = runtime_with(Arc::clone(&provider), enabled_config());
        let config = runtime.config_snapshot();
        let setup = runtime
            .build_turn(&test_scope().await, true, &config, None)
            .expect("turn should build");

        let reason = setup
            .agent
            .run(
                SessionId::new("shared-provider"),
                vec![Message::user("parent task")],
                setup.cancel.clone(),
            )
            .await;

        assert_eq!(reason, SessionEndReason::Completed);
        assert_eq!(
            setup.governance.tokens_used(),
            30,
            "parent and child usage must both be charged"
        );
        setup.shutdown();

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
            vec!["bash", "read_file", "write_file"]
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
            vec!["bash", "read_file", "write_file"]
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
        let reloaded = HarnessConfig::from_toml_str("[budget]\ntool_timeout_ms = 1\n")
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

    #[test]
    fn guides_are_injected_in_system_memory_skill_user_order() {
        let messages = fresh_conversation(
            "system prompt",
            Some(Message::system("[记忆参考] remembered fact")),
            Some(Message::system("[技能参考] reusable procedure")),
            &[],
            "new question",
        );

        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0], Message::system("system prompt"));
        assert_eq!(messages[1], Message::system("[记忆参考] remembered fact"));
        assert_eq!(
            messages[2],
            Message::system("[技能参考] reusable procedure")
        );
        assert_eq!(messages[3], Message::user("new question"));
        assert_eq!(
            messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            vec![Role::System, Role::System, Role::System, Role::User]
        );
    }

    #[test]
    fn absent_guides_keep_the_original_system_then_user_order() {
        let messages = fresh_conversation("system prompt", None, None, &[], "question");

        assert_eq!(
            messages,
            vec![Message::system("system prompt"), Message::user("question"),]
        );
    }

    #[test]
    fn a_skill_guide_without_memory_stays_adjacent_to_the_user() {
        let messages = fresh_conversation(
            "system prompt",
            None,
            Some(Message::system("[技能参考] reuse")),
            &[],
            "question",
        );

        assert_eq!(
            messages,
            vec![
                Message::system("system prompt"),
                Message::system("[技能参考] reuse"),
                Message::user("question"),
            ]
        );
    }

    #[test]
    fn acceptance_counts_use_only_the_final_attempt() {
        let first_attempt = vec![
            Message::system("system"),
            Message::user("goal"),
            Message::assistant_with_tool_calls(
                "first",
                vec![ToolCall::new("call-1", "read_file", json!({}))],
            ),
            Message::tool("call-1", "first output"),
            Message::assistant("first answer"),
            Message::user("acceptance feedback"),
        ];
        let final_attempt = vec![
            Message::assistant_with_tool_calls(
                "second",
                vec![ToolCall::new("call-2", "bash", json!({}))],
            ),
            Message::tool("call-2", "second output"),
            Message::assistant("second answer"),
        ];
        let mut messages = first_attempt;
        let start = messages.len();
        messages.extend(final_attempt);
        let outcome = AcceptanceOutcome {
            reason: SessionEndReason::Completed,
            messages,
            last_output: "second answer".to_owned(),
            attempts: 2,
        };

        let signals = acceptance_signals(&outcome, start);

        assert_eq!(signals.tool_call_count(), 1);
        assert_eq!(signals.assistant_rounds(), 2);
        assert_eq!(signals.acceptance_failures(), 1);
        assert!(!jessica_harness::SkillTrigger::new(1, 2).should_trigger(&signals));
    }

    #[test]
    fn structured_counts_include_recovered_checkpoint_work_on_resume() {
        let messages = vec![
            Message::system("system"),
            Message::user("goal"),
            Message::assistant_with_tool_calls(
                "",
                vec![ToolCall::new("call-1", "read_file", json!({}))],
            ),
            Message::tool("call-1", "output"),
            Message::assistant("finished"),
        ];

        assert_eq!(structured_counts(&messages, 0), (1, 2));
        assert_eq!(stable_turn_index(&messages), 0);
    }

    #[test]
    fn the_turn_index_is_stable_and_does_not_use_a_process_counter() {
        let first = vec![Message::system("system"), Message::user("goal")];
        let resumed = vec![
            Message::system("system"),
            Message::user("goal"),
            Message::assistant("partial"),
        ];

        assert_eq!(stable_turn_index(&first), 0);
        assert_eq!(stable_turn_index(&resumed), 0);
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
            "alpha",
            "the first turn must read from its own root"
        );
        assert!(
            system_prompt_of(&calls[3])
                .contains(canonical_b.to_str().expect("temporary paths must be UTF-8")),
            "the second turn must render the second root into its prompt"
        );
        assert_eq!(
            calls[4].messages.last().expect("tool result").content,
            "beta",
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
        let runtime = runtime_with(
            Arc::new(ScriptedProvider::new(Vec::new())),
            HarnessConfig::default(),
        );
        let config = runtime.config_snapshot();
        let parent_a = CancellationToken::new();
        let parent_b = CancellationToken::new();
        let scope_a = TurnScope::new(PathBuf::from("."), parent_a.clone())
            .await
            .expect("scope a must build");
        let scope_b = TurnScope::new(PathBuf::from("."), parent_b.clone())
            .await
            .expect("scope b must build");
        let setup_a = runtime
            .build_turn(&scope_a, false, &config, None)
            .expect("turn a must build");
        let setup_b = runtime
            .build_turn(&scope_b, false, &config, None)
            .expect("turn b must build");

        parent_a.cancel();

        assert!(
            setup_a.cancel.is_cancelled(),
            "cancelling the scope parent must cancel the child turn token"
        );
        assert!(
            !setup_b.cancel.is_cancelled(),
            "another turn's token must stay independent"
        );
        assert!(!parent_b.is_cancelled());
        setup_a.shutdown();
        setup_b.shutdown();
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

        let turn_start = observed.len();
        let counts = structured_counts(&outcome.messages, turn_start);
        assert_eq!(
            counts,
            (0, 1),
            "tool calls replayed from history must not count toward this turn"
        );
        let signals = TurnSignals::new(SessionEndReason::Completed, counts.0, counts.1);
        assert!(
            !jessica_harness::SkillTrigger::new(1, 2).should_trigger(&signals),
            "historical tool calls must not cross a one-call skill threshold"
        );
        assert_eq!(
            stable_turn_index(&outcome.messages),
            1,
            "one user turn precedes the new input in the carried history"
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
            })
            .await
            .expect("turn must run");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(outcome.messages.len(), 5);
        assert_eq!(outcome.messages[0].role, Role::System);
        assert_eq!(outcome.messages[1], Message::user("inspect"));
        assert_eq!(outcome.messages[2].role, Role::Assistant);
        assert_eq!(outcome.messages[2].tool_calls.len(), 1);
        assert_eq!(outcome.messages[2].tool_calls[0].id, "read-1");
        assert_eq!(outcome.messages[2].tool_calls[0].name, "read_file");
        assert_eq!(
            outcome.messages[3],
            Message::tool("read-1", "contents"),
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
}
