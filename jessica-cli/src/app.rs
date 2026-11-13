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
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::acceptance_check::CommandAcceptanceCheck;
use crate::config::{ConfigError, HarnessConfig, NATIVE_CHILD_TOOLS};
use crate::delegation::NativeChildToolFactory;
use crate::events::{EventRecorder, RecorderError};
use crate::governance::{GovernanceState, GovernedTool, TokenCountingProvider};
use crate::memory::MemoryAssembly;
use crate::pump::{EventPump, SessionSnapshot};
use crate::replay::ReplayError;
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
    memory: Option<MemoryAssembly>,
    skills: Option<SkillsAssembly>,
}

impl CliRuntime {
    /// Creates a runtime for the current workspace.
    ///
    /// `wasm_tools` contains plugins already loaded by [`load_wasm_tools`].
    /// Compilation happens once at startup rather than on every turn.
    /// `recorder` is present when the configuration enables event persistence.
    /// `memory` is present only when enabled and opened successfully.
    /// `skills` is present only when enabled and opened successfully.
    pub(crate) fn new(
        provider: Arc<dyn LlmProvider>,
        config: HarnessConfig,
        root: PathBuf,
        wasm_tools: Vec<Arc<dyn Tool>>,
        recorder: Option<Arc<EventRecorder>>,
        memory: Option<MemoryAssembly>,
        skills: Option<SkillsAssembly>,
    ) -> Self {
        let system_prompt = system_prompt(&root, &config);
        Self {
            provider,
            config: Arc::new(config),
            root,
            system_prompt,
            wasm_tools,
            recorder,
            memory,
            skills,
        }
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
    /// A fresh turn starts with `system prompt → recalled memory (if any) →
    /// user`. Memory is recalled exactly once per turn, before the first
    /// attempt; acceptance retries reuse the same initial conversation. A turn
    /// that finally completes is written to memory according to the configured
    /// write policy.
    pub(crate) async fn run_turn(&self, input: String) -> Result<SessionEndReason, CliError> {
        let setup = self.build_turn(true)?;
        let first_session = next_session_id();
        let initial = self.initial_messages(&first_session, input.as_str()).await;

        let context = AttemptContext::from_setup(&setup, self.recorder.clone());
        let (
            reason,
            emit_outer,
            final_session,
            final_output,
            final_messages,
            signals,
            attempt_start,
        ) = if let Some(acceptance) = &self.config.acceptance {
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
            let mut next_session = Some(first_session.clone());
            let mut latest_session = None;
            let mut latest_attempt_start = 0_usize;
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
            let (tool_calls, rounds) = structured_counts(&attempt.messages, 0);
            (
                attempt.reason,
                setup.governance.tripped(),
                first_session,
                attempt.last_output,
                attempt.messages,
                TurnSignals::new(attempt.reason, tool_calls, rounds),
                0,
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
            self.record_turn_memory(
                &final_session,
                &input,
                &final_output,
                &final_messages,
                turn_index,
            )
            .await;
            self.record_skill_job(
                &final_session,
                turn_index,
                &input,
                &final_output,
                &signals,
                final_messages.get(attempt_start..).unwrap_or(&[]),
            )
            .await;
        }

        setup.shutdown();
        Ok(reason)
    }

    /// Builds the initial conversation for a fresh turn.
    ///
    /// The order is fixed: system prompt, optional guide message, user input.
    /// A disabled or failed guide simply omits the middle message.
    async fn initial_messages(&self, session: &SessionId, input: &str) -> Vec<Message> {
        let memory_message = match &self.memory {
            Some(memory) => memory.recall(session, input).await,
            None => None,
        };
        let skill_message = match &self.skills {
            Some(skills) => skills.recall(session, input).await,
            None => None,
        };
        fresh_conversation(&self.system_prompt, memory_message, skill_message, input)
    }

    /// Writes the completed turn to memory when the policy allows it.
    ///
    /// Memory failures are warned about and never change the reason or exit
    /// code of the turn.
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

        let setup = self.build_turn(false)?;
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
    /// turn constructs them only when `[delegation]` is enabled.
    fn build_turn(&self, allow_delegation: bool) -> Result<TurnSetup, CliError> {
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
        // The child executor receives the same counting provider, so every
        // child call charges the parent token guard and can cancel the turn.
        let delegation = self.build_child_executor(allow_delegation, Arc::clone(&provider))?;

        let registry = self.build_tool_registry(
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

    /// Builds the child executor when delegation is enabled for this path.
    ///
    /// Returns `None` when delegation is disabled or when `allow_delegation`
    /// is false, and in that case does not construct the factory or executor.
    fn build_child_executor(
        &self,
        allow_delegation: bool,
        provider: Arc<dyn LlmProvider>,
    ) -> Result<Option<Arc<AgentChildExecutor>>, CliError> {
        if !allow_delegation || !self.config.delegation.enabled {
            return Ok(None);
        }

        let factory: Arc<dyn ChildToolFactory> =
            Arc::new(NativeChildToolFactory::new(self.root.clone()));
        let parent = ParentCapabilities::new(
            self.config.policy.allowed_tools.clone(),
            NATIVE_CHILD_TOOLS.iter().map(|name| (*name).to_owned()),
        );
        let executor = AgentChildExecutor::new(
            provider,
            factory,
            parent,
            self.config.delegation.to_limits(),
        )?;
        Ok(Some(Arc::new(executor)))
    }

    fn build_tool_registry(
        &self,
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        timeout: Duration,
        timeout_guard: Arc<ToolTimeoutGuard>,
        result_guard: Arc<ResultSizeGuard>,
        delegation: Option<Arc<AgentChildExecutor>>,
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

/// Builds a fresh conversation in its required order.
///
/// The order is fixed: system prompt, optional memory guide, optional skill
/// guide, then user input.
fn fresh_conversation(
    system_prompt: &str,
    memory_message: Option<Message>,
    skill_message: Option<Message>,
    input: &str,
) -> Vec<Message> {
    let mut messages = Vec::with_capacity(4);
    messages.push(Message::system(system_prompt));
    if let Some(message) = memory_message {
        messages.push(message);
    }
    if let Some(message) = skill_message {
        messages.push(message);
    }
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
/// counted from structured messages. This CLI starts one session per turn, so
/// a fresh turn and every resume of that turn both derive `0`. The value never
/// depends on a process-local counter and is stable across restarts.
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
    /// The current directory could not be determined.
    #[error("failed to determine the current directory: {0}")]
    CurrentDir(#[source] io::Error),
    /// The blocking current-directory task failed.
    #[error("current-directory task failed: {0}")]
    CurrentDirTask(#[source] tokio::task::JoinError),
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

    /// One captured provider call.
    #[derive(Clone)]
    struct ProviderCall {
        /// Messages the loop or executor sent.
        messages: Vec<Message>,
        /// Schemas the provider exposed.
        tools: Vec<ToolSchema>,
    }

    /// Provider that replays scripted event batches and records every call.
    struct ScriptedProvider {
        /// Responses in call order.
        responses: Mutex<VecDeque<Vec<LlmEvent>>>,
        /// Calls observed so far.
        calls: Mutex<Vec<ProviderCall>>,
    }

    impl ScriptedProvider {
        /// Creates a provider from response batches.
        fn new(responses: Vec<Vec<LlmEvent>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                calls: Mutex::new(Vec::new()),
            }
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
            let events = self
                .responses
                .lock()
                .expect("provider responses mutex poisoned")
                .pop_front()
                .expect("scripted provider ran out of responses");
            Ok(futures::stream::iter(events).boxed())
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
        CliRuntime::new(
            provider,
            config,
            PathBuf::from("."),
            Vec::new(),
            None,
            None,
            None,
        )
    }

    #[tokio::test]
    async fn delegate_tool_is_present_only_on_enabled_fresh_turns() {
        let enabled_provider = Arc::new(ScriptedProvider::new(vec![done_text("ok", 1)]));
        let enabled_runtime = runtime_with(Arc::clone(&enabled_provider), enabled_config());
        let setup = enabled_runtime.build_turn(true).expect("turn should build");
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
        let setup = disabled_runtime
            .build_turn(true)
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
        let setup = resume_runtime.build_turn(false).expect("turn should build");
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
        let setup = runtime.build_turn(true).expect("turn should build");

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

    #[test]
    fn guides_are_injected_in_system_memory_skill_user_order() {
        let messages = fresh_conversation(
            "system prompt",
            Some(Message::system("[记忆参考] remembered fact")),
            Some(Message::system("[技能参考] reusable procedure")),
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
        let messages = fresh_conversation("system prompt", None, None, "question");

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
}
