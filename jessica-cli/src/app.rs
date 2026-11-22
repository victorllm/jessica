//! Assembly of the governed CLI runtime and one-turn execution.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jessica_core::acceptance::AcceptanceCheck;
use jessica_core::events::EventSink;
use jessica_core::guard::Guard;
use jessica_core::llm::{LlmError, LlmProvider};
use jessica_core::policy::Policy;
use jessica_core::sensor::Sensor;
use jessica_core::tool::{Tool, ToolRegistry, ToolRegistryError};
use jessica_core::trace::{TraceTurnMode, TurnId};
use jessica_core::types::{AgentEvent, Message, Role, SessionEndReason, SessionId};
use jessica_harness::{
    AcceptanceOutcome, AcceptanceRunner, AllowlistPolicy, CommandSensor, DEFAULT_EVENT_CAPACITY,
    Harness, ResultSizeGuard, SessionAttempt, StuckDetectionGuard, TokenBudgetGuard,
    ToolTimeoutGuard, TurnSignals,
};
use jessica_runtime::{
    AgentChildExecutor, AgentLoop, ChildEventSink, ChildToolFactory, DelegateTaskTool,
    DelegationError, ParentCapabilities, SessionCheckpoint, recover_session,
};
use jessica_tools::WasmTool;
use thiserror::Error;
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;

use crate::acceptance_check::CommandAcceptanceCheck;
use crate::acp_store::AcpStoreError;
use crate::artifacts::ResultManager;
use crate::config::{ConfigError, HarnessConfig};
use crate::delegation::NativeChildToolFactory;
use crate::events::{EventJournal, EventRecorder, RecorderError};
use crate::governance::{
    GovernanceState, GovernedTool, TokenCountingProvider, ToolPermissionGate, cli_permission_gate,
};
use crate::journal::TurnJournal;
use crate::memory::MemoryAssembly;
use crate::policy_v2::{PolicyError, PolicyTool, PolicyV2};
use crate::pump::{EventPump, PumpOutput, SessionSnapshot, UpdateSink};
use crate::replay::ReplayError;
use crate::shutdown::ShutdownController;
use crate::skills::{SkillCommandError, SkillsAssembly};
use crate::tools::{CatalogCandidate, CatalogError, ToolCatalog, ToolSource, native_candidates};

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

/// Shared governance objects wrapped around every exposed tool in one turn.
struct ToolWiring {
    /// Harness owning policy, guards, and sensors.
    harness: Arc<Harness>,
    /// Turn budget and cancellation state.
    governance: Arc<GovernanceState>,
    /// Timeout guard shared with the harness.
    timeout_guard: Arc<ToolTimeoutGuard>,
    /// Result-size guard shared with the harness.
    result_guard: Arc<ResultSizeGuard>,
    /// Result manager applied before the result-size guard.
    result_manager: Arc<ResultManager>,
    /// Optional protocol gate preserving the legacy sensitive-tool flow.
    permission_gate: Option<Arc<dyn ToolPermissionGate>>,
    /// Optional gate used by policy `ask` verdicts.
    ///
    /// ACP reuses its permission gate here. A terminal CLI turn supplies a
    /// one-shot prompt when an `ask` rule exists, without changing the legacy
    /// behavior of sensitive tools that no rule mentions.
    ask_gate: Option<Arc<dyn ToolPermissionGate>>,
    /// Policy v2 evaluator shared by every governed tool in the turn.
    policy_v2: Arc<PolicyV2>,
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
    /// Producer-side journal shared by every emitter and the turn lifecycle.
    journal: Arc<TurnJournal>,
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
        self.journal.shutdown();
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
    /// Producer-side journal for this turn.
    journal: Arc<TurnJournal>,
    /// Cancellation token for the turn.
    cancel: CancellationToken,
}

impl AttemptContext {
    /// Clones the per-turn handles into an attempt context.
    fn from_setup(setup: &TurnSetup) -> Self {
        Self {
            agent: Arc::clone(&setup.agent),
            harness: Arc::clone(&setup.harness),
            pump: Arc::clone(&setup.pump),
            governance: Arc::clone(&setup.governance),
            stuck_guard: Arc::clone(&setup.stuck_guard),
            journal: Arc::clone(&setup.journal),
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
    /// Session-scoped tools registered in addition to the built-in and WASM
    /// tools.
    ///
    /// ACP uses this for MCP tools discovered for one session. Every tool is
    /// still wrapped in [`GovernedTool`], so policy, timeout, and result-size
    /// rules apply exactly as they do to native tools. The CLI and resume
    /// paths pass an empty slice.
    pub(crate) extra_tools: Vec<Arc<dyn Tool>>,
    /// Optional approval gate for sensitive native tools.
    ///
    /// ACP supplies a gate that asks the client through
    /// `session/request_permission`. The CLI and resume paths pass `None`,
    /// leaving `bash`, `exec`, `write_file`, and `delegate_task` governed by policy
    /// alone. The fixed sensitive set is evaluated inside [`GovernedTool`];
    /// configured WASM and MCP tools never request permission.
    pub(crate) permission_gate: Option<Arc<dyn ToolPermissionGate>>,
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

    /// Returns the write-only journal shared by the pump and turn orchestration.
    fn journal(&self) -> Option<Arc<dyn EventJournal>> {
        self.recorder
            .clone()
            .map(|recorder| recorder as Arc<dyn EventJournal>)
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
            extra_tools,
            permission_gate,
        } = request;

        // One snapshot for the whole turn. `build_turn` and
        // `initial_messages` must never mix a newer published config with an
        // older one, even if the watcher fires mid-turn.
        let config = self.config_snapshot();
        let setup = self
            .build_turn(
                &scope,
                true,
                &config,
                update_sink,
                &extra_tools,
                permission_gate,
            )
            .await?;
        let first_session = session_id.unwrap_or_else(next_session_id);
        // A signal may arrive between the REPL's last check and this point.
        // The child token is already cancelled in that case: skip memory and
        // skill recall, session recording, and every model request. The turn
        // still gets a complete start/end trace when persistence is enabled so
        // every normal return has a closed trace.
        if setup.cancel.is_cancelled() {
            let turn_id = next_turn_id();
            let result = close_empty_turn(
                &setup.journal,
                turn_id,
                TraceTurnMode::Fresh,
                Vec::new(),
                first_session,
            )
            .await;
            setup.shutdown();
            result?;
            return Ok(TurnOutcome {
                reason: SessionEndReason::Cancelled,
                messages: Vec::new(),
                turn_start: 0,
                lagged: false,
            });
        }
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

        let turn_id = next_turn_id();
        if let Err(error) = setup
            .journal
            .begin_turn(
                turn_id.clone(),
                TraceTurnMode::Fresh,
                initial.clone(),
                first_session.clone(),
            )
            .await
        {
            setup.shutdown();
            return Err(CliError::Events(error));
        }

        let context = AttemptContext::from_setup(&setup);
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
            )
            .with_supervisor(&config.resolved_supervisor());
            let runner = AcceptanceRunner::new(
                Arc::new(check) as Arc<dyn AcceptanceCheck>,
                retries,
                setup.events.clone(),
            );
            let runner = match setup.journal.sink() {
                Some(sink) => runner.with_event_sink(sink),
                None => runner,
            };
            let mut next_session = Some(first_session.clone());
            let mut latest_session = None;
            let mut latest_attempt_start = turn_start;
            let mut attempt_number = 0_u32;
            let outcome = runner
                .run(initial, |messages| {
                    latest_attempt_start = messages.len();
                    attempt_number = attempt_number.saturating_add(1);
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
                            attempt_number,
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
                1,
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

        // A persistence failure or broadcast gap leaves the turn permanently
        // incomplete. Flush what the producer already captured, but never emit
        // further events or write `turn_ended`: the caller must observe an
        // error instead of a normal turn result.
        if let Some(cause) = setup.journal.integrity_failure() {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(CliError::TraceIncomplete {
                turn_id: turn_id.to_string(),
                cause,
            });
        }

        // A budget trip overrides the runtime's Completed/Cancelled reason.
        // Emit an outer session event so the final observable reason is the
        // governed one rather than the last attempt's reason.
        if emit_outer
            && let Err(error) =
                emit_outer_session(&setup.journal, &setup.pump, &setup.events, reason).await
        {
            let cause = format!("failed to persist the outer session: {error}");
            setup.journal.fail_integrity(cause.clone(), true);
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(CliError::TraceIncomplete {
                turn_id: turn_id.to_string(),
                cause,
            });
        }
        if let Some(cause) = setup.journal.integrity_failure() {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(CliError::TraceIncomplete {
                turn_id: turn_id.to_string(),
                cause,
            });
        }
        if let Err(error) = setup.journal.finish_turn(reason).await {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(CliError::TraceIncomplete {
                turn_id: turn_id.to_string(),
                cause: error.to_string(),
            });
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
        let setup = self
            .build_turn(&scope, false, &config, None, &[], None)
            .await?;
        let trace_messages = checkpoint.messages().to_vec();
        if setup.cancel.is_cancelled() {
            let turn_id = next_turn_id();
            let result = close_empty_turn(
                &setup.journal,
                turn_id,
                TraceTurnMode::Resumed,
                trace_messages,
                session_id.clone(),
            )
            .await;
            setup.shutdown();
            result?;
            return Ok(SessionEndReason::Cancelled);
        }

        let turn_id = next_turn_id();
        if let Err(error) = setup
            .journal
            .begin_turn(
                turn_id.clone(),
                TraceTurnMode::Resumed,
                trace_messages,
                session_id.clone(),
            )
            .await
        {
            setup.shutdown();
            return Err(CliError::Events(error));
        }

        let context = AttemptContext::from_setup(&setup);
        let attempt = run_attempt(context, AttemptStart::Continue { checkpoint }, 1).await;
        let reason = attempt.reason;

        if let Some(cause) = setup.journal.integrity_failure() {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(CliError::TraceIncomplete {
                turn_id: turn_id.to_string(),
                cause,
            });
        }
        if setup.governance.tripped()
            && let Err(error) =
                emit_outer_session(&setup.journal, &setup.pump, &setup.events, reason).await
        {
            let cause = format!("failed to persist the outer session: {error}");
            setup.journal.fail_integrity(cause.clone(), true);
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(CliError::TraceIncomplete {
                turn_id: turn_id.to_string(),
                cause,
            });
        }
        if let Some(cause) = setup.journal.integrity_failure() {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(CliError::TraceIncomplete {
                turn_id: turn_id.to_string(),
                cause,
            });
        }
        if let Err(error) = setup.journal.finish_turn(reason).await {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(CliError::TraceIncomplete {
                turn_id: turn_id.to_string(),
                cause: error.to_string(),
            });
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
    /// sink the pump keeps rendering to the terminal. `extra_tools` contains
    /// session-scoped tools such as MCP discoveries; each is still wrapped in
    /// [`GovernedTool`] by [`CliRuntime::build_tool_registry`].
    /// `permission_gate` is the ACP protocol gate. A terminal CLI turn also
    /// installs an ask-only prompt for policy v2 `ask` verdicts; neither gate
    /// changes the legacy sensitive-tool flow when no approval is configured.
    async fn build_turn(
        &self,
        scope: &TurnScope,
        allow_delegation: bool,
        config: &HarnessConfig,
        update_sink: Option<Arc<dyn UpdateSink>>,
        extra_tools: &[Arc<dyn Tool>],
        permission_gate: Option<Arc<dyn ToolPermissionGate>>,
    ) -> Result<TurnSetup, CliError> {
        let timeout = config.budget.tool_timeout();
        let token_guard = Arc::new(TokenBudgetGuard::new(config.budget.max_tokens));
        let timeout_guard = Arc::new(ToolTimeoutGuard::new(timeout));
        let stuck_guard = Arc::new(StuckDetectionGuard::new(config.budget.max_stuck_rounds));
        let result_guard = Arc::new(ResultSizeGuard::new(config.budget.max_result_bytes));
        let result_manager = Arc::new(ResultManager::new(scope.root(), config));

        let guards: Vec<Arc<dyn Guard>> = vec![
            Arc::clone(&token_guard) as Arc<dyn Guard>,
            Arc::clone(&timeout_guard) as Arc<dyn Guard>,
            Arc::clone(&stuck_guard) as Arc<dyn Guard>,
            Arc::clone(&result_guard) as Arc<dyn Guard>,
        ];
        let policy: Arc<dyn Policy> =
            Arc::new(AllowlistPolicy::new(config.policy.allowed_tools.clone()));
        let policy_v2 = Arc::new(
            PolicyV2::new(scope.root(), config)
                .await
                .map_err(CliError::PolicyV2)?,
        );
        // ACP supplies its protocol gate. A CLI turn installs a terminal
        // prompt for `ask` verdicts (an explicit `ask` mode or an `ask` rule);
        // without any gate an ask outcome fails closed. The CLI prompt is not
        // used as the legacy sensitive-tool gate, so adding one `ask` rule
        // does not start prompting for unrelated sensitive tools.
        let cli_ask_gate = cli_permission_gate(scope.root(), config);
        let ask_gate = permission_gate.clone().or(cli_ask_gate);
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
        let journal = TurnJournal::new(self.journal(), cancel.clone());
        let governance = Arc::new(
            GovernanceState::new(events.clone(), cancel.clone()).with_event_sink(journal.sink()),
        );
        let provider: Arc<dyn LlmProvider> = Arc::new(TokenCountingProvider::new(
            Arc::clone(&self.provider),
            Arc::clone(&governance),
            Arc::clone(&token_guard),
        ));
        // Build the base catalog before delegation so a child can be granted
        // only tools the parent profile actually exposes. The final catalog is
        // rebuilt with the executable delegate tool and then checked against
        // the exposure budgets.
        let mut candidates = self.turn_candidates(scope, config, extra_tools);
        let base = ToolCatalog::build(candidates.clone(), config).map_err(CliError::ToolCatalog)?;

        // The child executor receives the same counting provider, so every
        // child call charges the parent token guard and can cancel the turn.
        let delegation = self.build_child_executor(
            scope,
            allow_delegation,
            Arc::clone(&provider),
            config,
            journal.sink(),
            journal.child_sink(),
            events.clone(),
            ParentCapabilities::new(base.exposed_names(), base.exposed_native_names()),
        )?;

        let catalog = if let Some(executor) = delegation {
            candidates.push(CatalogCandidate::from_tool(
                Arc::new(DelegateTaskTool::new(executor)),
                ToolSource::Delegation,
            ));
            ToolCatalog::build(candidates, config).map_err(CliError::ToolCatalog)?
        } else {
            base
        };
        catalog
            .enforce_budget(config)
            .map_err(CliError::ToolCatalog)?;

        let wiring = ToolWiring {
            harness: Arc::clone(&harness),
            governance: Arc::clone(&governance),
            timeout_guard: Arc::clone(&timeout_guard),
            result_guard: Arc::clone(&result_guard),
            result_manager,
            permission_gate,
            ask_gate,
            policy_v2,
        };
        let registry = self.build_tool_registry(&catalog, &wiring)?;
        let agent = AgentLoop::new(
            provider,
            Arc::new(registry),
            config.budget.to_budget(),
            events.clone(),
        );
        let agent = match journal.sink() {
            Some(sink) => agent.with_event_sink(sink),
            None => agent,
        };
        let agent = Arc::new(agent);
        let output = if update_sink.is_some() {
            PumpOutput::Silent
        } else {
            PumpOutput::HumanConsole
        };
        let pump = Arc::new(EventPump::new(
            Arc::clone(&harness),
            Arc::clone(&governance),
            Arc::clone(&journal),
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
            journal,
            pump,
            pump_task,
            stuck_guard,
        })
    }

    fn build_sensors(&self, scope: &TurnScope, config: &HarnessConfig) -> Vec<Arc<dyn Sensor>> {
        let Some(sensor) = &config.sensor else {
            return Vec::new();
        };
        let supervisor = config.resolved_supervisor();
        let command = CommandSensor::new(&sensor.name, &sensor.command)
            .args(sensor.args.clone())
            .with_current_dir(resolve_dir(scope.root(), sensor.working_dir.as_deref()))
            .with_timeout(supervisor.sensor_timeout)
            .with_output_limit(supervisor.max_output_bytes)
            .with_env_allowlist(supervisor.env_allowlist);
        vec![Arc::new(command) as Arc<dyn Sensor>]
    }

    /// Builds the child executor when delegation is enabled for this path.
    ///
    /// Returns `None` when delegation is disabled or when `allow_delegation`
    /// is false, and in that case does not construct the factory or executor.
    /// The executor receives the turn's sink and broadcast sender so the
    /// parent-side child session lifecycle is traced alongside the parent, and
    /// a child-event sink so child-internal events become trace evidence.
    /// Child events never enter the legacy event log or the parent broadcast.
    #[allow(clippy::too_many_arguments)]
    fn build_child_executor(
        &self,
        scope: &TurnScope,
        allow_delegation: bool,
        provider: Arc<dyn LlmProvider>,
        config: &HarnessConfig,
        sink: Option<Arc<dyn EventSink>>,
        child_sink: Option<Arc<dyn ChildEventSink>>,
        events: broadcast::Sender<AgentEvent>,
        parent: ParentCapabilities,
    ) -> Result<Option<Arc<AgentChildExecutor>>, CliError> {
        if !allow_delegation || !config.delegation.enabled {
            return Ok(None);
        }

        let factory: Arc<dyn ChildToolFactory> =
            Arc::new(NativeChildToolFactory::new(scope.root().to_path_buf()));
        let mut executor =
            AgentChildExecutor::new(provider, factory, parent, config.delegation.to_limits())?;
        if let Some(sink) = sink {
            executor = executor.with_event_sink(sink);
        }
        if let Some(child_sink) = child_sink {
            executor = executor.with_child_event_sink(child_sink);
        }
        executor = executor.with_event_sender(events);
        Ok(Some(Arc::new(executor)))
    }

    /// Builds candidate tools available to this turn before catalog filtering.
    ///
    /// Native tools are rebuilt per turn because they are bound to the turn's
    /// canonical root. WASM tools are process-wide compiled plugins; MCP tools
    /// arrive per session as `extra_tools`.
    fn turn_candidates(
        &self,
        scope: &TurnScope,
        config: &HarnessConfig,
        extra_tools: &[Arc<dyn Tool>],
    ) -> Vec<CatalogCandidate> {
        let mut candidates = native_candidates(scope.root(), config);
        candidates.extend(
            self.wasm_tools
                .iter()
                .map(|tool| CatalogCandidate::from_tool(Arc::clone(tool), ToolSource::Wasm)),
        );
        candidates.extend(
            extra_tools
                .iter()
                .map(|tool| CatalogCandidate::from_tool(Arc::clone(tool), ToolSource::Mcp)),
        );
        candidates
    }

    /// Wraps exactly the exposed catalog entries for the agent loop.
    ///
    /// Hidden tools are not registered, so they are absent from the provider
    /// schema and an attempted call is an unknown tool. Each exposed tool uses
    /// its descriptor's effective timeout and optional argument-size limit;
    /// approval gates attach only to entries the host classified as
    /// interactive.
    fn build_tool_registry(
        &self,
        catalog: &ToolCatalog,
        wiring: &ToolWiring,
    ) -> Result<ToolRegistry, CliError> {
        let mut registry = ToolRegistry::new();
        for entry in catalog.exposed() {
            let name = entry.descriptor.name.clone();
            let tool = entry
                .tool
                .as_ref()
                .ok_or_else(|| CatalogError::MissingImplementation { name: name.clone() })?;
            let mut governed = GovernedTool::new(
                Arc::clone(tool),
                Arc::clone(&wiring.harness),
                Arc::clone(&wiring.governance),
                Duration::from_millis(entry.descriptor.limits.timeout_ms),
                Arc::clone(&wiring.timeout_guard),
                Arc::clone(&wiring.result_guard),
            )
            .with_max_args_bytes(entry.descriptor.limits.max_args_bytes)
            .with_result_manager(
                Some(Arc::clone(&wiring.result_manager)),
                entry.descriptor.result_format,
            )
            .with_policy_v2(
                Arc::clone(&wiring.policy_v2),
                PolicyTool::from_descriptor(&entry.descriptor),
            );
            // A policy `ask` rule can target a tool that the legacy approval
            // set never covered, so the ask gate is attached to every exposed
            // tool. The evaluator decides which gate is consulted.
            if wiring.permission_gate.is_some() {
                governed = governed.with_permission_gate(wiring.permission_gate.clone());
            }
            if wiring.ask_gate.is_some() {
                governed = governed.with_ask_gate(wiring.ask_gate.clone());
            }
            registry
                .register(Arc::new(governed))
                .map_err(|source| CliError::RegisterTool { name, source })?;
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

/// Outcome of racing the pump wait against a journal failure.
enum PumpWait {
    /// The pump returned its session snapshot or lost the session.
    Snapshot(Option<SessionSnapshot>),
    /// The journal failed before the pump could observe a terminal event.
    JournalFailed,
}

/// Builds the fallback snapshot used when the pump cannot complete a session.
fn fallback_snapshot(messages: Vec<Message>) -> SessionSnapshot {
    SessionSnapshot {
        messages,
        last_output: String::new(),
    }
}

/// Runs one agent attempt and waits until its transcript is complete.
///
/// The stuck guard is stateful per session, so it is reset before every
/// acceptance retry; token usage remains cumulative across the whole turn.
async fn run_attempt(
    context: AttemptContext,
    start: AttemptStart,
    attempt_number: u32,
) -> SessionAttempt {
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
    let fallback_messages = initial_messages.clone();

    // Once the trace is broken, no further attempt may start: its events could
    // not be attributed and the turn can never close successfully.
    if let Some(cause) = context.journal.integrity_failure() {
        context.journal.fail_integrity(cause, true);
        return SessionAttempt {
            messages: fallback_messages,
            last_output: String::new(),
            reason: SessionEndReason::Error,
        };
    }

    // A fresh session needs its immutable bootstrap messages before any event
    // is appended. A resumed session already has them in the store. A metadata
    // failure fails the turn: recovery would otherwise lack the user messages
    // needed to replay the attempt. With persistence disabled this is a no-op.
    if let AttemptStart::Fresh { messages, .. } = &start
        && let Err(error) = context
            .journal
            .record_session_start(&session_id, messages)
            .await
    {
        let cause = format!("failed to record session metadata for {session_id}: {error}");
        context.journal.fail_integrity(cause, true);
        return SessionAttempt {
            messages: fallback_messages,
            last_output: String::new(),
            reason: SessionEndReason::Error,
        };
    }

    if let Err(error) = context.journal.begin_attempt(attempt_number, &session_id) {
        let cause = format!(
            "failed to attribute attempt {attempt_number} to session {session_id}: {error}"
        );
        context.journal.fail_integrity(cause, true);
        return SessionAttempt {
            messages: fallback_messages,
            last_output: String::new(),
            reason: SessionEndReason::Error,
        };
    }

    context
        .pump
        .register(session_id.clone(), initial_messages.clone());

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

    // A sink failure suppresses the terminal broadcast, so the pump may never
    // see `SessionEnded`. Race its wait against the journal's failure signal
    // instead of waiting for the full timeout.
    let waited = tokio::time::timeout(PUMP_TIMEOUT, async {
        tokio::select! {
            snapshot = context.pump.wait_for(&session_id) => PumpWait::Snapshot(snapshot),
            _ = context.journal.failure_signal() => PumpWait::JournalFailed,
        }
    })
    .await;
    let (snapshot, pump_failed) = match waited {
        Ok(PumpWait::Snapshot(Some(snapshot))) => (snapshot, false),
        Ok(PumpWait::Snapshot(None)) => {
            eprintln!("[warn] event pump lost session {session_id}");
            (fallback_snapshot(fallback_messages), true)
        }
        Ok(PumpWait::JournalFailed) => (fallback_snapshot(fallback_messages), true),
        Err(_) => {
            eprintln!("[warn] event pump timed out for session {session_id}");
            (fallback_snapshot(fallback_messages), true)
        }
    };
    if pump_failed {
        context.journal.fail_integrity(
            format!("the event pump did not complete session {session_id}"),
            true,
        );
    }

    SessionAttempt {
        messages: snapshot.messages,
        last_output: snapshot.last_output,
        reason: if pump_failed || context.journal.integrity_failure().is_some() {
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
    journal: &TurnJournal,
    pump: &EventPump,
    events: &broadcast::Sender<AgentEvent>,
    reason: SessionEndReason,
) -> Result<(), RecorderError> {
    let session_id = next_session_id();
    if journal.enabled() {
        // Fail closed before broadcasting: the outer session's metadata and
        // terminal pair must be durable before any observer sees them.
        journal.record_outer_session(&session_id, reason).await?;
    }
    pump.register(session_id.clone(), Vec::new());
    let _ = events.send(AgentEvent::SessionStarted {
        session_id: session_id.clone(),
        messages: Vec::new(),
    });
    let _ = events.send(AgentEvent::SessionEnded {
        session_id: session_id.clone(),
        reason,
    });

    let waited = tokio::time::timeout(PUMP_TIMEOUT, pump.wait_for(&session_id)).await;
    if journal.enabled() {
        match waited {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(RecorderError::Integrity {
                message: "the event pump did not observe the outer session".to_owned(),
            }),
            Err(_) => Err(RecorderError::Integrity {
                message: "timed out while flushing the outer session event".to_owned(),
            }),
        }
    } else {
        match waited {
            Ok(Some(_)) => {}
            Ok(None) => eprintln!("[warn] outer session was not registered with the event pump"),
            Err(_) => eprintln!("[warn] timed out while flushing the outer session event"),
        }
        Ok(())
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
    let allowed = config
        .policy
        .allowed_tools
        .iter()
        .filter(|name| config.tools.bash.enabled || *name != "bash")
        .map(String::as_str)
        .collect::<Vec<_>>();
    let allowed = if allowed.is_empty() {
        "none".to_owned()
    } else {
        allowed.join(", ")
    };
    format!(
        "You are Jessica, a deterministic coding agent. The workspace root is {}. \
         Use read_file, list_dir, grep, git_status, git_diff, and write_file with paths \
         relative to the workspace root. exec runs one program with an explicit argument \
         array and never goes through a shell. Tool calls are subject to this allowlist: \
         {allowed}. A denied call returns an error result, so adapt instead of repeating it. \
         A command sensor runs after every tool result. If an acceptance check fails, its \
         feedback is injected as a user message: fix the underlying problem and retry. \
         Verify your work with tools instead of claiming success. When the task is complete, \
         reply with a concise summary.",
        root.display()
    )
}

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);
static TURN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a process-local session id.
fn next_session_id() -> SessionId {
    let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    SessionId::new(format!("cli-{}-{counter}", std::process::id()))
}

/// Generates a process-unique turn id.
///
/// The timestamp, process id, and per-process counter make collisions
/// practically impossible. The store rejects a duplicate id instead of
/// silently merging two turns, so a collision still fails closed.
fn next_turn_id() -> TurnId {
    let counter = TURN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    TurnId::new(format!("turn-{}-{nanos}-{counter}", std::process::id()))
}

/// Starts and immediately closes a turn that never ran an attempt.
///
/// A cancelled-before-start turn is still a normal return, so it must not
/// leave an open trace when persistence is enabled.
async fn close_empty_turn(
    journal: &TurnJournal,
    turn_id: TurnId,
    mode: TraceTurnMode,
    messages: Vec<Message>,
    first_session: SessionId,
) -> Result<(), CliError> {
    journal
        .begin_turn(turn_id.clone(), mode, messages, first_session)
        .await
        .map_err(CliError::Events)?;
    journal
        .finish_turn(SessionEndReason::Cancelled)
        .await
        .map_err(|error| CliError::TraceIncomplete {
            turn_id: turn_id.to_string(),
            cause: error.to_string(),
        })
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
    use jessica_core::trace::{TraceEvent, TracePayload};
    use jessica_core::types::{TokenUsage, ToolCall};
    use jessica_memory::{EventStore, StoredTraceTurn, Timestamp};
    use jessica_runtime::{CHILD_AGENT_SYSTEM_PROMPT, DELEGATE_TASK_TOOL_NAME};
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

    /// Creates a turn scope rooted at the process current directory.
    async fn test_scope() -> TurnScope {
        TurnScope::new(PathBuf::from("."), CancellationToken::new())
            .await
            .expect("the test workspace must canonicalize")
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
        let enabled_config_snapshot = enabled_runtime.config_snapshot();
        let setup = enabled_runtime
            .build_turn(
                &test_scope().await,
                true,
                &enabled_config_snapshot,
                None,
                &[],
                None,
            )
            .await
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
            .build_turn(
                &test_scope().await,
                true,
                &disabled_config_snapshot,
                None,
                &[],
                None,
            )
            .await
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
        assert_eq!(
            disabled_names,
            vec!["read_file", "write_file"],
            "bash is disabled by default even though the default allowlist names it"
        );

        let resume_provider = Arc::new(ScriptedProvider::new(vec![done_text("ok", 1)]));
        let resume_runtime = runtime_with(Arc::clone(&resume_provider), enabled_config());
        let resume_config_snapshot = resume_runtime.config_snapshot();
        let setup = resume_runtime
            .build_turn(
                &test_scope().await,
                false,
                &resume_config_snapshot,
                None,
                &[],
                None,
            )
            .await
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
        let provider = Arc::new(ScriptedProvider::new(vec![
            delegate_request(),
            done_text("child answer", 10),
            done_text("parent done", 10),
        ]));
        let runtime = runtime_with(Arc::clone(&provider), enabled_config());
        let config = runtime.config_snapshot();
        let setup = runtime
            .build_turn(&test_scope().await, true, &config, None, &[], None)
            .await
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
            .build_turn(&scope_a, false, &config, None, &[], None)
            .await
            .expect("turn a must build");
        let setup_b = runtime
            .build_turn(&scope_b, false, &config, None, &[], None)
            .await
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
                extra_tools: Vec::new(),
                permission_gate: None,
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
            .err()
            .expect("a metadata conflict must fail the turn");
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
}
