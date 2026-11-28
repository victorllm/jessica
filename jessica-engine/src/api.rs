//! Public engine assembly and turn types.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::acceptance::AcceptanceCheck;
use jessica_core::events::{EventSink, EventSinkError};
use jessica_core::llm::LlmProvider;
use jessica_core::policy::Policy;
use jessica_core::sensor::Sensor;
use jessica_core::tool::ToolRegistry;
use jessica_core::trace::{TraceTurnMode, TurnId};
use jessica_core::types::{AgentEvent, Message, Role, SessionEndReason, SessionId};
use jessica_harness::{
    AcceptanceOutcome, AcceptanceRunner, AllowlistPolicy, DEFAULT_EVENT_CAPACITY, Harness,
    ResultSizeGuard, SessionAttempt, StuckDetectionGuard, TokenBudgetGuard, ToolTimeoutGuard,
    TurnSignals,
};
use jessica_runtime::{
    AgentChildExecutor, ChildToolFactory, DELEGATE_TASK_TOOL_NAME, DelegateTaskTool,
    ParentCapabilities, SessionCheckpoint, delegate_task_schema, recover_session,
};
use tokio_util::sync::CancellationToken;

use crate::artifacts::ResultManager;
use crate::config::{EngineConfig, TracePolicy};
use crate::error::EngineError;
use crate::governance::{GovernanceState, GovernedTool, TokenCountingProvider};
use crate::journal::TurnJournal;
use crate::policy::{PolicyTool, PolicyV2};
use crate::ports::{
    IdSource, MemoryPort, PumpConsole, SessionStore, SkillPort, SystemPromptSource, ToolContext,
    ToolPermissionGate, ToolProvider, ToolResultManager, TraceStore, TurnObserver, TurnTrace,
};
use crate::pump::{EventPump, LagHandling, PumpIntegrity, PumpOutput};
use crate::recorder::EventJournal;
use crate::tooling::{
    ApprovalPolicy, ToolCatalog, ToolDescriptor, ToolEntry, ToolLimits, ToolMetadata, ToolSource,
};

/// Maximum time to wait for the pump to observe a session's terminal event.
const PUMP_TIMEOUT: Duration = Duration::from_secs(30);

/// Input for one governed turn.
///
/// Fields are private on purpose: the `with_*` methods are the only way to
/// shape a request, so a caller cannot fabricate state the engine would not
/// accept.
///
/// ```compile_fail
/// use jessica_engine::TurnRequest;
///
/// // `input` and every other field are private.
/// let forged = TurnRequest {
///     input: "bypass".to_owned(),
///     ..TurnRequest::new("bypass")
/// };
/// ```
#[derive(Clone, Default)]
pub struct TurnRequest {
    input: String,
    workspace_root: PathBuf,
    history: Vec<Message>,
    session_id: Option<SessionId>,
    acceptance_retries: Option<u32>,
    cancellation: Option<CancellationToken>,
    observer: Option<Arc<dyn TurnObserver>>,
    output: Option<PumpOutput>,
}

impl std::fmt::Debug for TurnRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnRequest")
            .field("input", &self.input)
            .field("workspace_root", &self.workspace_root)
            .field("history_len", &self.history.len())
            .field("session_id", &self.session_id)
            .field("acceptance_retries", &self.acceptance_retries)
            .field(
                "cancelled",
                &self
                    .cancellation
                    .as_ref()
                    .is_some_and(|token| token.is_cancelled()),
            )
            .field("has_observer", &self.observer.is_some())
            .field("output", &self.output)
            .finish()
    }
}

impl TurnRequest {
    /// Creates a request from the user's input.
    ///
    /// The workspace root defaults to the current directory and can be
    /// replaced with [`Self::with_workspace_root`]. The engine canonicalizes
    /// it before building tools, policy, or result management.
    pub fn new(input: impl Into<String>) -> Self {
        Self {
            input: input.into(),
            workspace_root: PathBuf::from("."),
            history: Vec::new(),
            session_id: None,
            acceptance_retries: None,
            cancellation: None,
            observer: None,
            output: None,
        }
    }

    /// Sets the workspace root for this turn.
    pub fn with_workspace_root(mut self, workspace_root: impl Into<PathBuf>) -> Self {
        self.workspace_root = workspace_root.into();
        self
    }

    /// Replaces the user input.
    ///
    /// Resume uses this to carry the recovered goal into the tool-provider
    /// context and post-turn memory.
    pub fn with_input(mut self, input: impl Into<String>) -> Self {
        self.input = input.into();
        self
    }

    /// Sets the conversation established before this turn's user input.
    ///
    /// History is placed after the system and guide messages and before the
    /// new user message. It is never counted as work created by this turn.
    pub fn with_history(mut self, history: Vec<Message>) -> Self {
        self.history = history;
        self
    }

    /// Uses `session_id` for the first attempt instead of an engine-generated
    /// identifier.
    ///
    /// Later acceptance attempts still receive engine-generated identifiers.
    pub fn with_session_id(mut self, session_id: SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Overrides the configured acceptance retry budget for this turn.
    ///
    /// `Some(0)` runs the check once without retrying even when the engine
    /// configuration allows retries. `None` uses
    /// [`EngineConfig::acceptance`](crate::EngineConfig::acceptance).
    pub fn with_acceptance_retries(mut self, retries: u32) -> Self {
        self.acceptance_retries = Some(retries);
        self
    }

    /// Binds the turn to `parent`.
    ///
    /// The engine derives a child token so cancelling the parent cancels the
    /// turn without affecting other turns.
    pub fn with_cancellation(mut self, parent: CancellationToken) -> Self {
        self.cancellation = Some(parent);
        self
    }

    /// Sets a programmatic observer for this turn.
    ///
    /// When present, the pump runs with [`PumpOutput::Silent`] unless
    /// [`Self::with_output`] overrides it. The observer receives events in pump
    /// order after durable recording.
    pub fn with_observer(mut self, observer: Arc<dyn TurnObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Overrides the pump's human-console output policy.
    ///
    /// The default is [`PumpOutput::Silent`] when an observer is installed and
    /// [`PumpOutput::HumanConsole`] otherwise.
    pub fn with_output(mut self, output: PumpOutput) -> Self {
        self.output = Some(output);
        self
    }

    /// Returns the user's input.
    pub fn input(&self) -> &str {
        &self.input
    }

    /// Returns the configured workspace root.
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Returns the conversation supplied by the caller.
    pub fn history(&self) -> &[Message] {
        &self.history
    }

    /// Returns the caller-supplied first session id, when any.
    pub fn session_id(&self) -> Option<&SessionId> {
        self.session_id.as_ref()
    }

    /// Returns the per-turn acceptance retry override, when any.
    pub fn acceptance_retries(&self) -> Option<u32> {
        self.acceptance_retries
    }

    /// Returns the cancellation parent, when any.
    pub fn cancellation(&self) -> Option<&CancellationToken> {
        self.cancellation.as_ref()
    }

    /// Returns the per-turn observer, when any.
    pub fn observer(&self) -> Option<&Arc<dyn TurnObserver>> {
        self.observer.as_ref()
    }

    /// Returns the pump output override, when any.
    pub fn output(&self) -> Option<PumpOutput> {
        self.output
    }
}

/// Observable result of one governed turn.
///
/// This is a successful turn record. A trace that could not be closed is
/// returned as [`EngineError`] instead, never as an outcome with a misleading
/// `Completed` reason.
///
/// ```compile_fail
/// use jessica_engine::{EngineError, TurnOutcome};
///
/// let incomplete = EngineError::TraceIncomplete {
///     turn_id: "turn-1".to_owned(),
///     cause: "disk full".to_owned(),
/// };
/// // An incomplete trace is an error, never a successful outcome.
/// let outcome: TurnOutcome = incomplete;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct TurnOutcome {
    /// Why the turn stopped.
    pub reason: SessionEndReason,
    /// Conversation after the turn.
    pub messages: Vec<Message>,
    /// Final text produced by the model.
    pub last_output: String,
    /// Index in `messages` where this turn's initial conversation begins.
    pub turn_start: usize,
    /// Stable identifier for this turn.
    pub turn_id: String,
    /// Whether the event pump observed a broadcast lag.
    pub lagged: bool,
}

/// Dependencies retained by a built [`Engine`].
struct EngineDependencies {
    /// LLM provider wrapped by the engine's token accounting.
    provider: Arc<dyn LlmProvider>,
    /// Host-supplied tool catalog builder.
    tool_provider: Arc<dyn ToolProvider>,
    /// Optional approval gate for legacy-sensitive tools.
    approval_gate: Option<Arc<dyn ToolPermissionGate>>,
    /// Optional gate used by policy `ask` verdicts.
    ask_gate: Option<Arc<dyn ToolPermissionGate>>,
    /// Optional factory for delegated child tools.
    child_tool_factory: Option<Arc<dyn ChildToolFactory>>,
    /// Post-tool sensors run by the harness.
    sensors: Vec<Arc<dyn Sensor>>,
    /// Optional host console for human-readable pump output.
    console: Option<Arc<dyn PumpConsole>>,
    /// Optional host result manager overriding the built-in artifact manager.
    result_manager: Option<Arc<dyn ToolResultManager>>,
    /// Durable trace store used by the E2 [`Engine::run_turn`] path.
    trace_store: Option<Arc<dyn TraceStore>>,
    /// Event journal used by the full turn lifecycle and resume paths.
    event_journal: Option<Arc<dyn EventJournal>>,
    /// Session loader used by [`Engine::resume_turn`].
    session_store: Option<Arc<dyn SessionStore>>,
    /// Optional observer of every event the pump processes.
    observer: Option<Arc<dyn TurnObserver>>,
    /// Optional system-prompt source.
    system_prompt: Option<Arc<dyn SystemPromptSource>>,
    /// Optional turn/session identity source.
    id_source: Option<Arc<dyn IdSource>>,
    /// Optional memory recall and post-turn recording.
    memory: Option<Arc<dyn MemoryPort>>,
    /// Optional skill recall and post-turn enqueue.
    skills: Option<Arc<dyn SkillPort>>,
    /// Optional external acceptance check.
    acceptance_check: Option<Arc<dyn AcceptanceCheck>>,
}

/// Shared governance objects wrapped around every exposed tool in one turn.
struct TurnTooling {
    /// Allowlist policy.
    allowlist: Arc<dyn Policy>,
    /// Budget and cancellation state.
    governance: Arc<GovernanceState>,
    /// Parameter-level policy evaluator.
    policy_v2: Arc<PolicyV2>,
    /// Result manager used before the transcript observes a result.
    result_manager: Arc<dyn ToolResultManager>,
    /// Shared timeout guard.
    timeout_guard: Arc<ToolTimeoutGuard>,
    /// Shared result-size guard.
    result_guard: Arc<ResultSizeGuard>,
}

/// A configured governed engine.
///
/// The engine owns one immutable [`EngineConfig`] snapshot and runs the
/// provider-to-tool vertical slice for each [`TurnRequest`]. It owns the
/// complete turn lifecycle: the producer-side trace sink and event pump,
/// acceptance retries, memory/skill post-processing, delegation parent
/// assembly, and resume from recorded evidence. The CLI and ACP are adapters
/// that supply the host ports.
pub struct Engine {
    config: EngineConfig,
    dependencies: EngineDependencies,
    next_turn: AtomicU64,
    next_session: AtomicU64,
}

impl Engine {
    /// Returns the configuration snapshot owned by this engine.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Runs one governed turn.
    ///
    /// The turn builds a fresh tool catalog, filters it through the allowlist,
    /// wraps every exposed tool with [`GovernedTool`], and drives
    /// [`jessica_runtime::AgentLoop`] through the token-counting provider. A
    /// required trace is opened before the agent starts, every producer event
    /// is recorded before it is broadcast, and the turn only succeeds after
    /// `turn_ended` is acknowledged.
    ///
    /// This path uses a [`TraceStore`] directly and does not run acceptance
    /// retries, memory/skill post-processing, delegation, or resume. Use
    /// [`Self::run_turn_request`] or [`Self::resume_turn`] for the complete
    /// lifecycle.
    ///
    /// A budget trip is reported as [`SessionEndReason::BudgetExceeded`] even
    /// when the runtime loop itself returned another reason, because the
    /// governance state is the authoritative budget signal.
    ///
    /// A pump gap does not discard a producer-complete trace: it sets
    /// [`TurnOutcome::lagged`] so the caller knows the observer-side transcript
    /// and sensor path are incomplete. Any trace write or close failure is
    /// returned as [`EngineError::TraceIncomplete`] instead of a success.
    pub async fn run_turn(&self, request: TurnRequest) -> Result<TurnOutcome, EngineError> {
        let workspace_root = tokio::fs::canonicalize(request.workspace_root())
            .await
            .map_err(|source| EngineError::Workspace {
                root: request.workspace_root().display().to_string(),
                source,
            })?;
        let sequence = self.next_turn.fetch_add(1, Ordering::SeqCst) + 1;
        let turn_id = format!("turn-{sequence}");
        let session_id = SessionId::new(turn_id.clone());
        let cancel = CancellationToken::new();
        let initial = vec![Message::user(request.input())];
        let turn_start = initial.len();
        let trace = TurnTrace {
            turn_id: turn_id.clone(),
            session_id: session_id.clone(),
            attempt: Some(1),
        };
        let trace_store = match self.config.trace_policy() {
            TracePolicy::Disabled => None,
            TracePolicy::Required => Some(Arc::clone(
                self.dependencies
                    .trace_store
                    .as_ref()
                    .ok_or(EngineError::MissingTraceStore)?,
            )),
        };

        let harness = Arc::new(Harness::new(Vec::new()));
        let events = harness.events();
        let trace_failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let sink: Option<Arc<dyn EventSink>> = trace_store.as_ref().map(|store| {
            Arc::new(TraceEventSink {
                store: Arc::clone(store),
                trace: trace.clone(),
                cancel: cancel.clone(),
                failure: Arc::clone(&trace_failure),
            }) as Arc<dyn EventSink>
        });
        let governance = Arc::new(
            GovernanceState::new(events.clone(), cancel.clone()).with_event_sink(sink.clone()),
        );

        let provider: Arc<dyn LlmProvider> = Arc::new(TokenCountingProvider::new(
            Arc::clone(&self.dependencies.provider),
            Arc::clone(&governance),
            Arc::new(TokenBudgetGuard::new(self.config.budget().max_tokens)),
        ));
        let context = ToolContext {
            workspace_root: &workspace_root,
            cancellation: &cancel,
            request: &request,
        };
        let catalog = self.dependencies.tool_provider.provide(&context).await?;
        self.enforce_catalog_limits(&catalog)?;
        let policy_v2 = Arc::new(
            PolicyV2::new(
                &workspace_root,
                self.config.policy(),
                self.config.approval(),
            )
            .await
            .map_err(EngineError::Policy)?,
        );
        let allowlist: Arc<dyn Policy> = Arc::new(AllowlistPolicy::new(
            self.config.policy().allowed_tools.clone(),
        ));
        let allowed: BTreeSet<&str> = self
            .config
            .policy()
            .allowed_tools
            .iter()
            .map(String::as_str)
            .collect();
        let result_manager = self.result_manager(&workspace_root);
        let timeout_guard = Arc::new(ToolTimeoutGuard::new(self.config.tools().timeout));
        let result_guard = Arc::new(ResultSizeGuard::new(self.config.tools().max_result_bytes));
        let tooling = TurnTooling {
            allowlist,
            governance: Arc::clone(&governance),
            policy_v2,
            result_manager,
            timeout_guard,
            result_guard,
        };
        let mut registry = ToolRegistry::new();
        for entry in catalog.iter() {
            if !allowed.contains(entry.descriptor.name.as_str()) {
                continue;
            }
            let governed = self.govern_tool(entry, &tooling);
            let name = entry.descriptor.name.clone();
            registry
                .register(Arc::new(governed))
                .map_err(|_| EngineError::DuplicateTool { name })?;
        }

        let agent = jessica_runtime::AgentLoop::new(
            provider,
            Arc::new(registry),
            self.config.budget().clone(),
            events,
        );
        let agent = match &sink {
            Some(sink) => agent.with_event_sink(Arc::clone(sink)),
            None => agent,
        };
        let pump = Arc::new(EventPump::new(
            Arc::clone(&harness),
            Arc::clone(&governance),
            Arc::new(EnginePumpIntegrity),
            PumpOutput::Silent,
            self.dependencies.observer.clone(),
        ));

        if let Some(store) = &trace_store {
            store
                .begin_turn(&trace, TraceTurnMode::Fresh, &initial)
                .await
                .map_err(|error| trace_incomplete(&turn_id, error))?;
        }

        let pump_task = tokio::spawn(Arc::clone(&pump).run(harness.subscribe()));
        pump.register(session_id.clone(), initial.clone());

        let transcript = agent
            .run_with_transcript(
                session_id.clone(),
                initial,
                self.config.max_tool_calls(),
                cancel.clone(),
            )
            .await;
        if let Some(cause) = failure_slot(&trace_failure).clone() {
            pump_task.abort();
            return Err(EngineError::TraceIncomplete { turn_id, cause });
        }

        let snapshot = match tokio::time::timeout(PUMP_TIMEOUT, pump.wait_for(&session_id)).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                pump_task.abort();
                return Err(EngineError::TraceIncomplete {
                    turn_id,
                    cause: "the event pump did not observe the session terminal event".to_owned(),
                });
            }
            Err(_) => {
                pump_task.abort();
                return Err(EngineError::TraceIncomplete {
                    turn_id,
                    cause: "timed out waiting for the event pump".to_owned(),
                });
            }
        };
        let reason =
            if governance.tripped() && transcript.reason != SessionEndReason::BudgetExceeded {
                SessionEndReason::BudgetExceeded
            } else {
                transcript.reason
            };
        let lagged = pump.has_lagged();
        let (messages, last_output) = if lagged {
            // The observer lost events, so its transcript is not authoritative.
            // The runtime transcript and the producer-side trace are complete;
            // expose the gap instead of returning a silently shortened
            // conversation as if it were whole.
            let last_output = transcript
                .messages
                .iter()
                .rev()
                .find(|message| message.role == Role::Assistant)
                .map(|message| message.content.clone())
                .unwrap_or_default();
            (transcript.messages.clone(), last_output)
        } else {
            (snapshot.messages, snapshot.last_output)
        };

        let outcome = TurnOutcome {
            reason,
            messages,
            last_output,
            turn_start,
            turn_id: turn_id.clone(),
            lagged,
        };
        if let Some(store) = &trace_store
            && let Err(error) = store.finish_turn(&trace, &outcome).await
        {
            pump_task.abort();
            return Err(trace_incomplete(&turn_id, error));
        }
        pump_task.abort();
        Ok(outcome)
    }

    /// Runs one complete governed turn: recall, acceptance, trace lifecycle,
    /// and post-turn memory/skill processing.
    ///
    /// The initial conversation is built in the fixed order `system prompt →
    /// memory recall → skill recall → history → user input`. Both guides are
    /// recalled exactly once, before the first attempt, so acceptance retries
    /// reuse the same prefix. Every attempt gets its own session; an acceptance
    /// retry reuses the accumulated conversation and adds the check's feedback
    /// message. A turn that exhausts retries reports `AcceptanceFailed` on an
    /// outer session whose terminal pair is recorded before it is broadcast.
    ///
    /// The turn trace is opened before any provider call and closed with
    /// `turn_ended` only after the governed reason is known. A trace write or
    /// close failure returns [`EngineError::TraceIncomplete`], never a normal
    /// outcome. Memory and skill post-processing run only when the governed
    /// reason is `Completed` and no guard tripped; their failures are logged and
    /// never change the outcome.
    ///
    /// A budget trip overrides the runtime reason with
    /// `SessionEndReason::BudgetExceeded`. A broadcast lag is exposed through
    /// [`TurnOutcome::lagged`] and, with a journal installed, permanently
    /// invalidates the turn just as it does on the CLI path.
    pub async fn run_turn_request(&self, request: TurnRequest) -> Result<TurnOutcome, EngineError> {
        if self.config.acceptance().is_some() && self.dependencies.acceptance_check.is_none() {
            return Err(EngineError::MissingAcceptanceCheck);
        }
        let workspace_root = tokio::fs::canonicalize(request.workspace_root())
            .await
            .map_err(|source| EngineError::Workspace {
                root: request.workspace_root().display().to_string(),
                source,
            })?;
        let cancel = request
            .cancellation()
            .map(CancellationToken::child_token)
            .unwrap_or_default();
        let setup = self
            .build_turn_setup(&request, &workspace_root, cancel.clone(), true)
            .await?;
        let first_session = request
            .session_id()
            .cloned()
            .unwrap_or_else(|| self.next_session_id());
        let turn_id = self.next_turn_id();

        if cancel.is_cancelled() {
            let result = close_empty_turn(
                &setup.journal,
                TurnId::new(turn_id.clone()),
                TraceTurnMode::Fresh,
                Vec::new(),
                first_session,
            )
            .await;
            setup.shutdown();
            result.map_err(|error| trace_incomplete(&turn_id, error))?;
            return Ok(TurnOutcome {
                reason: SessionEndReason::Cancelled,
                messages: Vec::new(),
                last_output: String::new(),
                turn_start: 0,
                turn_id,
                lagged: false,
            });
        }

        let initial = self.initial_messages(&first_session, &request).await;
        let turn_start = initial.len();
        setup
            .journal
            .begin_turn(
                TurnId::new(turn_id.clone()),
                TraceTurnMode::Fresh,
                initial.clone(),
                first_session.clone(),
            )
            .await
            .map_err(|error| trace_incomplete(&turn_id, error.into()))?;

        let context = AttemptContext::from_setup(&setup);
        let (
            reason,
            emit_outer,
            final_session,
            final_output,
            final_messages,
            signals,
            attempt_start,
        ) = if let Some(acceptance) = self.config.acceptance() {
            let check = Arc::clone(
                self.dependencies
                    .acceptance_check
                    .as_ref()
                    .ok_or(EngineError::MissingAcceptanceCheck)?,
            );
            let retries = request
                .acceptance_retries()
                .unwrap_or_else(|| acceptance.max_retries());
            let runner = AcceptanceRunner::new(check, retries, setup.events.clone());
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
                    let session_id = next_session
                        .take()
                        .unwrap_or_else(|| self.next_session_id());
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

        if let Some(cause) = setup.journal.integrity_failure() {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(EngineError::TraceIncomplete { turn_id, cause });
        }

        if emit_outer {
            let session_id = self.next_session_id();
            if let Err(error) = emit_outer_session(
                &setup.journal,
                &setup.pump,
                &setup.events,
                &session_id,
                reason,
            )
            .await
            {
                let cause = format!("failed to persist the outer session: {error}");
                setup.journal.fail_integrity(cause.clone(), true);
                let _ = setup.journal.flush().await;
                setup.shutdown();
                return Err(EngineError::TraceIncomplete { turn_id, cause });
            }
        }
        if let Some(cause) = setup.journal.integrity_failure() {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(EngineError::TraceIncomplete { turn_id, cause });
        }
        if let Err(error) = setup.journal.finish_turn(reason).await {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(EngineError::TraceIncomplete {
                turn_id,
                cause: error.to_string(),
            });
        }

        let governed_completed =
            reason == SessionEndReason::Completed && !setup.governance.tripped();
        if governed_completed {
            self.post_process_completed_turn(
                &final_session,
                &request,
                &final_output,
                &final_messages,
                turn_start,
                attempt_start,
                &signals,
            )
            .await;
        }

        let lagged = setup.pump.has_lagged();
        setup.shutdown();
        Ok(TurnOutcome {
            reason,
            messages: final_messages,
            last_output: final_output,
            turn_start,
            turn_id,
            lagged,
        })
    }

    /// Resumes an interrupted recorded session.
    ///
    /// The checkpoint is rebuilt from the durable event stream. Work already
    /// present in the log is never replayed: the agent resumes at the first
    /// missing model round, with the remaining round budget. A session that
    /// already has a terminal event returns its recorded reason without opening
    /// a provider call or a new trace.
    ///
    /// `request` supplies the workspace root, cancellation parent, observer,
    /// and tool-provider context for the resumed turn. Its `input` is used for
    /// metadata and memory only; the conversation comes from the checkpoint.
    /// Memory and skills are not recalled on resume, matching the CLI. A
    /// completed resumed turn may still enqueue idempotent post-turn work.
    pub async fn resume_turn(
        &self,
        session_id: SessionId,
        request: TurnRequest,
    ) -> Result<TurnOutcome, EngineError> {
        let store = self
            .dependencies
            .session_store
            .as_ref()
            .ok_or(EngineError::MissingSessionStore)?;
        let stored = store
            .load_session(session_id.clone())
            .await?
            .ok_or_else(|| EngineError::UnknownSession {
                session_id: session_id.to_string(),
            })?;
        let checkpoint =
            recover_session(&session_id, stored.initial_messages, &stored.events, None)?;
        let turn_id = self.next_turn_id();
        if let Some(reason) = checkpoint.end_reason() {
            let messages = checkpoint.messages().to_vec();
            let last_output = last_assistant_output(&messages);
            return Ok(TurnOutcome {
                reason,
                messages,
                last_output,
                turn_start: 0,
                turn_id,
                lagged: false,
            });
        }

        let user_input = first_user_message(checkpoint.messages());
        let request = request.with_input(user_input).with_history(Vec::new());
        let workspace_root = tokio::fs::canonicalize(request.workspace_root())
            .await
            .map_err(|source| EngineError::Workspace {
                root: request.workspace_root().display().to_string(),
                source,
            })?;
        let cancel = request
            .cancellation()
            .map(CancellationToken::child_token)
            .unwrap_or_default();
        let setup = self
            .build_turn_setup(&request, &workspace_root, cancel.clone(), false)
            .await?;
        let trace_messages = checkpoint.messages().to_vec();
        if cancel.is_cancelled() {
            let result = close_empty_turn(
                &setup.journal,
                TurnId::new(turn_id.clone()),
                TraceTurnMode::Resumed,
                trace_messages,
                session_id,
            )
            .await;
            setup.shutdown();
            result.map_err(|error| trace_incomplete(&turn_id, error))?;
            return Ok(TurnOutcome {
                reason: SessionEndReason::Cancelled,
                messages: Vec::new(),
                last_output: String::new(),
                turn_start: 0,
                turn_id,
                lagged: false,
            });
        }

        setup
            .journal
            .begin_turn(
                TurnId::new(turn_id.clone()),
                TraceTurnMode::Resumed,
                trace_messages,
                session_id.clone(),
            )
            .await
            .map_err(|error| trace_incomplete(&turn_id, error.into()))?;

        let context = AttemptContext::from_setup(&setup);
        let attempt = run_attempt(context, AttemptStart::Continue { checkpoint }, 1).await;
        let reason = attempt.reason;

        if let Some(cause) = setup.journal.integrity_failure() {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(EngineError::TraceIncomplete { turn_id, cause });
        }
        if setup.governance.tripped() {
            let outer_session = self.next_session_id();
            if let Err(error) = emit_outer_session(
                &setup.journal,
                &setup.pump,
                &setup.events,
                &outer_session,
                reason,
            )
            .await
            {
                let cause = format!("failed to persist the outer session: {error}");
                setup.journal.fail_integrity(cause.clone(), true);
                let _ = setup.journal.flush().await;
                setup.shutdown();
                return Err(EngineError::TraceIncomplete { turn_id, cause });
            }
        }
        if let Some(cause) = setup.journal.integrity_failure() {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(EngineError::TraceIncomplete { turn_id, cause });
        }
        if let Err(error) = setup.journal.finish_turn(reason).await {
            let _ = setup.journal.flush().await;
            setup.shutdown();
            return Err(EngineError::TraceIncomplete {
                turn_id,
                cause: error.to_string(),
            });
        }

        let governed_completed =
            reason == SessionEndReason::Completed && !setup.governance.tripped();
        if governed_completed {
            let (tool_calls, rounds) = structured_counts(&attempt.messages, 0);
            let signals = TurnSignals::new(reason, tool_calls, rounds);
            self.post_process_completed_turn(
                &session_id,
                &request,
                &attempt.last_output,
                &attempt.messages,
                0,
                0,
                &signals,
            )
            .await;
        }

        let lagged = setup.pump.has_lagged();
        setup.shutdown();
        Ok(TurnOutcome {
            reason,
            messages: attempt.messages,
            last_output: attempt.last_output,
            turn_start: 0,
            turn_id,
            lagged,
        })
    }

    /// Builds the harness, governance wrappers, agent, journal, and pump for
    /// one turn.
    async fn build_turn_setup(
        &self,
        request: &TurnRequest,
        workspace_root: &Path,
        cancel: CancellationToken,
        allow_delegation: bool,
    ) -> Result<TurnSetup, EngineError> {
        let event_journal = match self.config.trace_policy() {
            TracePolicy::Disabled => None,
            TracePolicy::Required => Some(Arc::clone(
                self.dependencies
                    .event_journal
                    .as_ref()
                    .ok_or(EngineError::MissingEventJournal)?,
            )),
        };
        let timeout = self.config.tools().timeout;
        let token_guard = Arc::new(TokenBudgetGuard::new(self.config.budget().max_tokens));
        let timeout_guard = Arc::new(ToolTimeoutGuard::new(timeout));
        let stuck_guard = Arc::new(StuckDetectionGuard::new(self.config.max_stuck_rounds()));
        let result_guard = Arc::new(ResultSizeGuard::new(self.config.tools().max_result_bytes));
        let result_manager = self.result_manager(workspace_root);

        let guards: Vec<Arc<dyn jessica_core::guard::Guard>> = vec![
            Arc::clone(&token_guard) as Arc<dyn jessica_core::guard::Guard>,
            Arc::clone(&timeout_guard) as Arc<dyn jessica_core::guard::Guard>,
            Arc::clone(&stuck_guard) as Arc<dyn jessica_core::guard::Guard>,
            Arc::clone(&result_guard) as Arc<dyn jessica_core::guard::Guard>,
        ];
        let allowlist: Arc<dyn Policy> = Arc::new(AllowlistPolicy::new(
            self.config.policy().allowed_tools.clone(),
        ));
        let policy_v2 = Arc::new(
            PolicyV2::new(workspace_root, self.config.policy(), self.config.approval())
                .await
                .map_err(EngineError::Policy)?,
        );
        let harness = Arc::new(
            Harness::with_event_capacity(Vec::new(), DEFAULT_EVENT_CAPACITY)
                .with_guards(guards)
                .with_policy(Arc::clone(&allowlist))
                .with_sensors(self.dependencies.sensors.clone()),
        );
        let events = harness.events();
        let journal = TurnJournal::new(event_journal, cancel.clone());
        let governance = Arc::new(
            GovernanceState::new(events.clone(), cancel.clone()).with_event_sink(journal.sink()),
        );
        let provider: Arc<dyn LlmProvider> = Arc::new(TokenCountingProvider::new(
            Arc::clone(&self.dependencies.provider),
            Arc::clone(&governance),
            Arc::clone(&token_guard),
        ));

        let context = ToolContext {
            workspace_root,
            cancellation: &cancel,
            request,
        };
        let mut catalog = self.dependencies.tool_provider.provide(&context).await?;
        let allowed: BTreeSet<&str> = self
            .config
            .policy()
            .allowed_tools
            .iter()
            .map(String::as_str)
            .collect();
        if allow_delegation && self.config.delegation().is_some() {
            self.add_delegation_tool(
                &mut catalog,
                &allowed,
                Arc::clone(&provider),
                &journal,
                &events,
            )?;
        }
        self.enforce_catalog_limits(&catalog)?;
        let tooling = TurnTooling {
            allowlist,
            governance: Arc::clone(&governance),
            policy_v2,
            result_manager,
            timeout_guard,
            result_guard,
        };
        let mut registry = ToolRegistry::new();
        for entry in catalog.iter() {
            if !allowed.contains(entry.descriptor.name.as_str()) {
                continue;
            }
            let governed = self.govern_tool(entry, &tooling);
            let name = entry.descriptor.name.clone();
            registry
                .register(Arc::new(governed))
                .map_err(|_| EngineError::DuplicateTool { name })?;
        }

        let agent = jessica_runtime::AgentLoop::new(
            provider,
            Arc::new(registry),
            self.config.budget().clone(),
            events.clone(),
        );
        let agent = match journal.sink() {
            Some(sink) => agent.with_event_sink(sink),
            None => agent,
        };
        let agent = Arc::new(agent);
        let observer = request
            .observer()
            .cloned()
            .or_else(|| self.dependencies.observer.clone());
        let output = request.output().unwrap_or(if observer.is_some() {
            PumpOutput::Silent
        } else {
            PumpOutput::HumanConsole
        });
        let integrity: Arc<dyn PumpIntegrity> = journal.clone();
        let pump = Arc::new(EventPump::new_with_console(
            Arc::clone(&harness),
            Arc::clone(&governance),
            integrity,
            output,
            self.dependencies.console.clone(),
            observer,
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

    /// Builds the initial conversation for a fresh turn.
    async fn initial_messages(&self, session: &SessionId, request: &TurnRequest) -> Vec<Message> {
        let system_prompt = self.dependencies.system_prompt.as_ref().map_or_else(
            || DEFAULT_SYSTEM_PROMPT.to_owned(),
            |source| source.system_prompt(),
        );
        let memory = match &self.dependencies.memory {
            Some(memory) => match memory.recall(session, request.input()).await {
                Ok(message) => message,
                Err(error) => {
                    tracing::warn!(
                        session = %session,
                        error = %error,
                        "memory recall failed; continuing without it"
                    );
                    None
                }
            },
            None => None,
        };
        let skills = match &self.dependencies.skills {
            Some(skills) => match skills.recall(session, request.input()).await {
                Ok(message) => message,
                Err(error) => {
                    tracing::warn!(
                        session = %session,
                        error = %error,
                        "skill recall failed; continuing without it"
                    );
                    None
                }
            },
            None => None,
        };
        fresh_conversation(
            &system_prompt,
            memory,
            skills,
            request.history(),
            request.input(),
        )
    }

    /// Runs memory and skill post-processing after a governed completion.
    #[allow(clippy::too_many_arguments)]
    async fn post_process_completed_turn(
        &self,
        session_id: &SessionId,
        request: &TurnRequest,
        assistant_answer: &str,
        messages: &[Message],
        turn_start: usize,
        attempt_start: usize,
        signals: &TurnSignals,
    ) {
        let turn_index = stable_turn_index(messages);
        if let Some(memory) = &self.dependencies.memory {
            let this_turn = messages.get(turn_start..).unwrap_or(&[]);
            let tool_call_count = this_turn
                .iter()
                .map(|message| message.tool_calls.len())
                .sum();
            if let Err(error) = memory
                .record_completed_turn(
                    session_id,
                    turn_index,
                    request.input(),
                    assistant_answer,
                    tool_call_count,
                )
                .await
            {
                tracing::warn!(
                    session = %session_id,
                    error = %error,
                    "memory write failed; the turn remains successful"
                );
            }
        }
        if let Some(skills) = &self.dependencies.skills {
            let final_attempt = messages.get(attempt_start..).unwrap_or(&[]);
            if let Err(error) = skills
                .after_completed_turn(
                    session_id,
                    turn_index,
                    request.input(),
                    assistant_answer,
                    signals,
                    final_attempt,
                )
                .await
            {
                tracing::warn!(
                    session = %session_id,
                    error = %error,
                    "skill enqueue failed; the turn remains successful"
                );
            }
        }
    }

    /// Returns the next turn identifier.
    fn next_turn_id(&self) -> String {
        match &self.dependencies.id_source {
            Some(source) => source.next_turn_id(),
            None => {
                let sequence = self.next_turn.fetch_add(1, Ordering::SeqCst) + 1;
                format!("turn-{sequence}")
            }
        }
    }

    /// Returns the next session identifier.
    fn next_session_id(&self) -> SessionId {
        match &self.dependencies.id_source {
            Some(source) => source.next_session_id(),
            None => {
                let sequence = self.next_session.fetch_add(1, Ordering::SeqCst) + 1;
                SessionId::new(format!("session-{sequence}"))
            }
        }
    }

    /// Wraps one host tool in the complete E1 governance chain.
    fn govern_tool(&self, entry: &ToolEntry, tooling: &TurnTooling) -> GovernedTool {
        let timeout = entry
            .descriptor
            .limits
            .timeout
            .unwrap_or(self.config.tools().timeout);
        let max_args = entry
            .descriptor
            .limits
            .max_args_bytes
            .or(self.config.tools().max_args_bytes);
        let permission = self.dependencies.approval_gate.clone();
        let ask_gate = self
            .dependencies
            .ask_gate
            .clone()
            .or_else(|| permission.clone());
        GovernedTool::new(
            Arc::clone(&entry.tool),
            Arc::clone(&tooling.allowlist),
            Arc::clone(&tooling.governance),
            timeout,
            Arc::clone(&tooling.timeout_guard),
            Arc::clone(&tooling.result_guard),
        )
        .with_policy_v2(
            Arc::clone(&tooling.policy_v2),
            PolicyTool::from_descriptor(&entry.descriptor),
        )
        .with_max_args_bytes(max_args)
        .with_result_manager(
            Some(Arc::clone(&tooling.result_manager)),
            entry.descriptor.result_format,
        )
        .with_permission_gate(permission)
        .with_ask_gate(ask_gate)
    }

    /// Returns the host result manager, or the engine's artifact manager when
    /// the host did not install one.
    fn result_manager(&self, workspace_root: &Path) -> Arc<dyn ToolResultManager> {
        match &self.dependencies.result_manager {
            Some(manager) => Arc::clone(manager),
            None => Arc::new(ResultManager::new(
                workspace_root,
                self.config.result(),
                self.config.tools().max_result_bytes,
            )),
        }
    }

    /// Detects an over-budget model-visible catalog before any provider call.
    fn enforce_catalog_limits(&self, catalog: &ToolCatalog) -> Result<(), EngineError> {
        let allowed: BTreeSet<&str> = self
            .config
            .policy()
            .allowed_tools
            .iter()
            .map(String::as_str)
            .collect();
        let limits = self.config.catalog_limits();
        let exposed = catalog
            .iter()
            .filter(|entry| allowed.contains(entry.descriptor.name.as_str()));
        let mut count = 0;
        let mut bytes: usize = 0;
        for entry in exposed {
            count += 1;
            bytes = bytes.saturating_add(entry.descriptor.schema_bytes);
        }
        if count > limits.max_exposed_tools {
            return Err(EngineError::CatalogTooLarge {
                exposed: count,
                limit: limits.max_exposed_tools,
            });
        }
        if bytes > limits.max_tool_schema_bytes {
            return Err(EngineError::SchemaBudgetExceeded {
                bytes,
                limit: limits.max_tool_schema_bytes,
            });
        }
        Ok(())
    }

    /// Adds the executable `delegate_task` tool for a fresh turn.
    ///
    /// Parent capabilities are computed from the host catalog after the engine
    /// allowlist, before the delegation tool itself is present, so a child can
    /// never receive `delegate_task` or a tool its parent does not expose.
    fn add_delegation_tool(
        &self,
        catalog: &mut ToolCatalog,
        allowed: &BTreeSet<&str>,
        provider: Arc<dyn LlmProvider>,
        journal: &Arc<TurnJournal>,
        events: &tokio::sync::broadcast::Sender<AgentEvent>,
    ) -> Result<(), EngineError> {
        if !allowed.contains(DELEGATE_TASK_TOOL_NAME) {
            return Ok(());
        }
        let limits = self
            .config
            .delegation()
            .ok_or(EngineError::MissingChildToolFactory)?;
        let factory = Arc::clone(
            self.dependencies
                .child_tool_factory
                .as_ref()
                .ok_or(EngineError::MissingChildToolFactory)?,
        );
        let parent_allowed: Vec<String> = catalog
            .iter()
            .filter(|entry| allowed.contains(entry.descriptor.name.as_str()))
            .map(|entry| entry.descriptor.name.clone())
            .collect();
        let parent_native: Vec<String> = catalog
            .iter()
            .filter(|entry| {
                allowed.contains(entry.descriptor.name.as_str())
                    && entry.descriptor.source == ToolSource::Native
            })
            .map(|entry| entry.descriptor.name.clone())
            .collect();
        let parent = ParentCapabilities::new(parent_allowed, parent_native);
        let mut executor = AgentChildExecutor::new(provider, factory, parent, limits)
            .map_err(EngineError::Delegation)?;
        if let Some(sink) = journal.sink() {
            executor = executor.with_event_sink(sink);
        }
        if let Some(child_sink) = journal.child_sink() {
            executor = executor.with_child_event_sink(child_sink);
        }
        executor = executor.with_event_sender(events.clone());

        let limits = ToolLimits {
            timeout: Some(self.config.tools().timeout),
            max_args_bytes: self.config.tools().max_args_bytes,
            max_inline_bytes: self
                .config
                .result()
                .max_inline_bytes
                .min(limits.max_result_bytes),
            max_artifact_bytes: Some(self.config.result().max_artifact_bytes),
            max_output_lines: None,
        };
        let descriptor = ToolDescriptor::new(
            DELEGATE_TASK_TOOL_NAME.to_owned(),
            ToolSource::Delegation,
            delegate_task_schema(),
            env!("CARGO_PKG_VERSION"),
            limits,
            ApprovalPolicy::Interactive,
            &ToolMetadata::default(),
        )
        .map_err(|error| EngineError::HostPort {
            component: "delegation tool descriptor".to_owned(),
            message: error.to_string(),
        })?;
        catalog.insert(ToolEntry {
            tool: Arc::new(DelegateTaskTool::new(Arc::new(executor))),
            descriptor,
        })
    }
}

/// System prompt used when an embedder does not supply a source.
const DEFAULT_SYSTEM_PROMPT: &str = "You are Jessica, a deterministic coding agent.";

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

/// Handles shared by every attempt in one turn.
#[derive(Clone)]
struct AttemptContext {
    /// Agent loop for this turn.
    agent: Arc<jessica_runtime::AgentLoop>,
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

/// Per-turn middleware, agent, journal, and pump wiring.
struct TurnSetup {
    /// Harness owning the events, guards, and policy.
    harness: Arc<Harness>,
    /// Event channel shared by the harness, agent, and governance.
    events: tokio::sync::broadcast::Sender<AgentEvent>,
    /// Cancellation token for the turn.
    cancel: CancellationToken,
    /// Budget and policy state observed by the wrappers and pump.
    governance: Arc<GovernanceState>,
    /// Agent loop for this turn.
    agent: Arc<jessica_runtime::AgentLoop>,
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
    fn shutdown(self) {
        self.pump_task.abort();
        self.journal.shutdown();
    }
}

/// Outcome of racing the pump wait against a journal failure.
enum PumpWait {
    /// The pump returned its session snapshot or lost the session.
    Snapshot(Option<crate::pump::SessionSnapshot>),
    /// The journal failed before the pump could observe a terminal event.
    JournalFailed,
}

/// Starts and immediately closes a turn that never ran an attempt.
async fn close_empty_turn(
    journal: &TurnJournal,
    turn_id: TurnId,
    mode: TraceTurnMode,
    messages: Vec<Message>,
    first_session: SessionId,
) -> Result<(), EngineError> {
    journal
        .begin_turn(turn_id.clone(), mode, messages, first_session)
        .await
        .map_err(|error| EngineError::TraceIncomplete {
            turn_id: turn_id.to_string(),
            cause: error.to_string(),
        })?;
    journal
        .finish_turn(SessionEndReason::Cancelled)
        .await
        .map_err(|error| EngineError::TraceIncomplete {
            turn_id: turn_id.to_string(),
            cause: error.to_string(),
        })
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
    // is appended. A resumed session already has them in the store.
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
            tracing::warn!(session = %session_id, "event pump lost session");
            (fallback_snapshot(fallback_messages), true)
        }
        Ok(PumpWait::JournalFailed) => (fallback_snapshot(fallback_messages), true),
        Err(_) => {
            tracing::warn!(session = %session_id, "event pump timed out");
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
async fn emit_outer_session(
    journal: &TurnJournal,
    pump: &EventPump,
    events: &tokio::sync::broadcast::Sender<AgentEvent>,
    session_id: &SessionId,
    reason: SessionEndReason,
) -> Result<(), String> {
    if journal.enabled() {
        // Fail closed before broadcasting: the outer session's metadata and
        // terminal pair must be durable before any observer sees them.
        journal
            .record_outer_session(session_id, reason)
            .await
            .map_err(|error| error.to_string())?;
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

    let waited = tokio::time::timeout(PUMP_TIMEOUT, pump.wait_for(session_id)).await;
    if journal.enabled() {
        match waited {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err("the event pump did not observe the outer session".to_owned()),
            Err(_) => Err("timed out while flushing the outer session event".to_owned()),
        }
    } else {
        match waited {
            Ok(Some(_)) => {}
            Ok(None) => tracing::warn!("outer session was not registered with the event pump"),
            Err(_) => tracing::warn!("timed out while flushing the outer session event"),
        }
        Ok(())
    }
}

/// Builds the fallback snapshot used when the pump cannot complete a session.
fn fallback_snapshot(messages: Vec<Message>) -> crate::pump::SessionSnapshot {
    crate::pump::SessionSnapshot {
        messages,
        last_output: String::new(),
    }
}

/// Builds a fresh conversation in its required order.
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
fn stable_turn_index(messages: &[Message]) -> u64 {
    let user_turns = messages
        .iter()
        .filter(|message| message.role == Role::User)
        .count();
    u64::try_from(user_turns.saturating_sub(1)).unwrap_or(u64::MAX)
}

/// Returns the final assistant text in a conversation.
fn last_assistant_output(messages: &[Message]) -> String {
    messages
        .iter()
        .rev()
        .find(|message| message.role == Role::Assistant)
        .map(|message| message.content.clone())
        .unwrap_or_default()
}

/// Returns the first user message's content.
fn first_user_message(messages: &[Message]) -> String {
    messages
        .iter()
        .find(|message| message.role == Role::User)
        .map(|message| message.content.clone())
        .unwrap_or_default()
}

/// Producer-side sink that records every event before it is broadcast.
///
/// The first failure is retained so `run_turn` can return the authoritative
/// cause even though the runtime only reports a generic sink failure.
struct TraceEventSink {
    /// Durable trace store for the turn.
    store: Arc<dyn TraceStore>,
    /// Turn and session attribution for every recorded event.
    trace: TurnTrace,
    /// Cancels the agent so no later event is produced.
    cancel: CancellationToken,
    /// First trace failure, shared with `run_turn`.
    failure: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl EventSink for TraceEventSink {
    async fn emit(&self, event: AgentEvent) -> Result<(), EventSinkError> {
        if let Some(cause) = failure_slot(&self.failure).clone() {
            return Err(EventSinkError::new(cause));
        }
        match self.store.record(&self.trace, event).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let cause = trace_cause(error);
                *failure_slot(&self.failure) = Some(cause.clone());
                self.cancel.cancel();
                Err(EventSinkError::new(cause))
            }
        }
    }
}

/// Pump policy for engine-owned turns: a gap is reported in the outcome.
struct EnginePumpIntegrity;

impl PumpIntegrity for EnginePumpIntegrity {
    fn broadcast_lagged(&self, _skipped: u64, _output: PumpOutput) -> LagHandling {
        LagHandling::Continue
    }
}

/// Locks the first trace-failure slot, recovering from poisoning.
fn failure_slot(failure: &Mutex<Option<String>>) -> MutexGuard<'_, Option<String>> {
    failure
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Normalizes any trace-port failure into [`EngineError::TraceIncomplete`].
fn trace_incomplete(turn_id: &str, error: EngineError) -> EngineError {
    match error {
        error @ EngineError::TraceIncomplete { .. } => error,
        other => EngineError::TraceIncomplete {
            turn_id: turn_id.to_owned(),
            cause: other.to_string(),
        },
    }
}

/// Extracts the underlying cause from a trace-port failure.
fn trace_cause(error: EngineError) -> String {
    match error {
        EngineError::TraceIncomplete { cause, .. } => cause,
        other => other.to_string(),
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Builds an [`Engine`] from explicit host ports and engine decisions.
///
/// The builder deliberately accepts no pre-assembled `Harness` or
/// `ToolRegistry`: every governed dependency crosses a named port, and the
/// trace policy must be explicit. A required trace needs a [`TraceStore`]; a
/// policy that can ask for approval also requires a permission gate at build
/// time.
///
/// ```compile_fail
/// use jessica_engine::EngineBuilder;
///
/// // There is no setter for a pre-assembled harness or tool registry.
/// let builder = EngineBuilder::new().harness(());
/// ```
#[derive(Default)]
pub struct EngineBuilder {
    provider: Option<Arc<dyn LlmProvider>>,
    tool_provider: Option<Arc<dyn ToolProvider>>,
    approval_gate: Option<Arc<dyn ToolPermissionGate>>,
    ask_gate: Option<Arc<dyn ToolPermissionGate>>,
    child_tool_factory: Option<Arc<dyn ChildToolFactory>>,
    sensors: Vec<Arc<dyn Sensor>>,
    console: Option<Arc<dyn PumpConsole>>,
    result_manager: Option<Arc<dyn ToolResultManager>>,
    trace_store: Option<Arc<dyn TraceStore>>,
    event_journal: Option<Arc<dyn EventJournal>>,
    session_store: Option<Arc<dyn SessionStore>>,
    observer: Option<Arc<dyn TurnObserver>>,
    system_prompt: Option<Arc<dyn SystemPromptSource>>,
    id_source: Option<Arc<dyn IdSource>>,
    memory: Option<Arc<dyn MemoryPort>>,
    skills: Option<Arc<dyn SkillPort>>,
    acceptance_check: Option<Arc<dyn AcceptanceCheck>>,
    allow_missing_approval_gate: bool,
    config: Option<EngineConfig>,
}

impl EngineBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the LLM provider.
    pub fn provider(mut self, provider: Arc<dyn LlmProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Sets the turn-scoped tool provider.
    pub fn tool_provider(mut self, tool_provider: Arc<dyn ToolProvider>) -> Self {
        self.tool_provider = Some(tool_provider);
        self
    }

    /// Sets the permission gate used by legacy-sensitive tools.
    ///
    /// This gate is also used for policy `ask` verdicts when
    /// [`Self::ask_gate`] is not installed.
    pub fn approval_gate(mut self, approval_gate: Arc<dyn ToolPermissionGate>) -> Self {
        self.approval_gate = Some(approval_gate);
        self
    }

    /// Sets the gate used by policy `ask` verdicts.
    ///
    /// Keeping this separate from [`Self::approval_gate`] lets a host install
    /// an interactive gate for explicit `ask` rules without changing the
    /// legacy sensitive-tool flow.
    pub fn ask_gate(mut self, ask_gate: Arc<dyn ToolPermissionGate>) -> Self {
        self.ask_gate = Some(ask_gate);
        self
    }

    /// Sets the factory used to build tools for delegated children.
    ///
    /// Required when [`EngineConfig::delegation`] is configured. The engine
    /// computes the parent allowlist and native set from the turn catalog, then
    /// passes only the narrowed names to this factory.
    pub fn child_tool_factory(mut self, factory: Arc<dyn ChildToolFactory>) -> Self {
        self.child_tool_factory = Some(factory);
        self
    }

    /// Sets the post-tool sensors run after every completed tool call.
    ///
    /// Sensor reports are rendered by the pump and never change the turn
    /// reason by themselves.
    pub fn sensors(mut self, sensors: Vec<Arc<dyn Sensor>>) -> Self {
        self.sensors = sensors;
        self
    }

    /// Sets the human-console renderer used by the pump.
    ///
    /// It is invoked only for [`PumpOutput::HumanConsole`]. Without one the
    /// engine keeps its built-in terminal renderer.
    pub fn console(mut self, console: Arc<dyn PumpConsole>) -> Self {
        self.console = Some(console);
        self
    }

    /// Overrides the engine's built-in artifact result manager.
    ///
    /// The host manager runs after every tool result and before the
    /// result-size guard, transcript, trace, or model observes it.
    pub fn result_manager(mut self, manager: Arc<dyn ToolResultManager>) -> Self {
        self.result_manager = Some(manager);
        self
    }

    /// Allows assembly without a gate even when the policy can ask.
    ///
    /// A host that wants a missing or non-interactive gate to fail closed at
    /// execution time, rather than at assembly time, sets this. The governed
    /// tool then returns a policy denial for the ask call.
    pub fn allow_missing_approval_gate(mut self) -> Self {
        self.allow_missing_approval_gate = true;
        self
    }

    /// Sets the durable trace store.
    ///
    /// [`TracePolicy::Required`] is the default and fails assembly without a
    /// store. [`TracePolicy::Disabled`] writes no trace and leaves a configured
    /// store unused.
    pub fn trace_store(mut self, trace_store: Arc<dyn TraceStore>) -> Self {
        self.trace_store = Some(trace_store);
        self
    }

    /// Sets the event journal used by the full turn lifecycle and resume.
    ///
    /// This is a different seam from [`TraceStore`]: it preserves the
    /// per-attempt session attribution, outer-session terminal pair, child
    /// brackets, and integrity signal required by
    /// [`Engine::run_turn_request`] and [`Engine::resume_turn`]. A
    /// [`TracePolicy::Required`] engine must receive either this journal or a
    /// trace store; the method used at runtime determines which one is needed.
    pub fn event_journal(mut self, journal: Arc<dyn EventJournal>) -> Self {
        self.event_journal = Some(journal);
        self
    }

    /// Sets the session loader used by [`Engine::resume_turn`].
    pub fn session_store(mut self, store: Arc<dyn SessionStore>) -> Self {
        self.session_store = Some(store);
        self
    }

    /// Sets the system-prompt source.
    pub fn system_prompt(mut self, source: Arc<dyn SystemPromptSource>) -> Self {
        self.system_prompt = Some(source);
        self
    }

    /// Sets the turn/session identity source.
    pub fn id_source(mut self, source: Arc<dyn IdSource>) -> Self {
        self.id_source = Some(source);
        self
    }

    /// Sets the memory port.
    pub fn memory(mut self, memory: Arc<dyn MemoryPort>) -> Self {
        self.memory = Some(memory);
        self
    }

    /// Sets the skill port.
    pub fn skills(mut self, skills: Arc<dyn SkillPort>) -> Self {
        self.skills = Some(skills);
        self
    }

    /// Sets the external acceptance check.
    ///
    /// Required when [`EngineConfig::acceptance`] is configured.
    pub fn acceptance_check(mut self, check: Arc<dyn AcceptanceCheck>) -> Self {
        self.acceptance_check = Some(check);
        self
    }

    /// Sets the observer that receives every event the pump processes.
    ///
    /// The observer runs after durable recording and before human-console
    /// rendering. It is optional even when a trace is required.
    pub fn observer(mut self, observer: Arc<dyn TurnObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Sets the immutable engine configuration.
    pub fn config(mut self, config: EngineConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Validates the supplied ports and configuration and builds an engine.
    ///
    /// The missing-dependency checks run in a fixed order so callers get a
    /// deterministic first error. A required trace needs a store, and an
    /// approval gate is required when the policy contains an ask rule or when
    /// an explicit ask mode is configured.
    pub fn build(self) -> Result<Engine, EngineError> {
        let provider = self.provider.ok_or(EngineError::MissingProvider)?;
        let tool_provider = self.tool_provider.ok_or(EngineError::MissingToolProvider)?;
        let config = self.config.ok_or(EngineError::MissingConfig)?;
        if config.trace_policy() == TracePolicy::Required
            && self.trace_store.is_none()
            && self.event_journal.is_none()
        {
            return Err(EngineError::MissingTraceStore);
        }
        if config.requires_approval_gate()
            && self.approval_gate.is_none()
            && self.ask_gate.is_none()
            && !self.allow_missing_approval_gate
        {
            return Err(EngineError::MissingApprovalGate);
        }
        if config.delegation().is_some() && self.child_tool_factory.is_none() {
            return Err(EngineError::MissingChildToolFactory);
        }
        if config.acceptance().is_some() && self.acceptance_check.is_none() {
            return Err(EngineError::MissingAcceptanceCheck);
        }

        Ok(Engine {
            config,
            dependencies: EngineDependencies {
                provider,
                tool_provider,
                approval_gate: self.approval_gate,
                ask_gate: self.ask_gate,
                child_tool_factory: self.child_tool_factory,
                sensors: self.sensors,
                console: self.console,
                result_manager: self.result_manager,
                trace_store: self.trace_store,
                event_journal: self.event_journal,
                session_store: self.session_store,
                observer: self.observer,
                system_prompt: self.system_prompt,
                id_source: self.id_source,
                memory: self.memory,
                skills: self.skills,
                acceptance_check: self.acceptance_check,
            },
            next_turn: AtomicU64::new(0),
            next_session: AtomicU64::new(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
    use std::time::Duration;

    use async_trait::async_trait;
    use jessica_core::acceptance::{AcceptanceCheck, AcceptanceContext, AcceptanceResult};
    use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
    use jessica_core::tool::{Tool, ToolSchema};
    use jessica_core::trace::{TraceEvent, TracePayload, TraceTurnMode, TurnId};
    use jessica_core::types::{Budget, Message, TokenUsage, ToolCall, ToolResult};
    use jessica_memory::EventStore;
    use jessica_runtime::{DelegationLimits, RecordedEvent};
    use serde_json::{Value, json};

    use super::*;
    use crate::config::{
        AcceptanceConfig, ApprovalConfig, ApprovalMode, PolicyConfig, PolicyRule, ResultConfig,
        RuleAction, ToolExecutionConfig, ToolSelector, TracePolicy,
    };
    use crate::ports::{
        IdSource, MemoryPort, SessionStore, SkillPort, SystemPromptSource, ToolContext,
        ToolPermissionDecision, TraceStore, TurnObserver, TurnTrace,
    };
    use crate::recorder::{EventJournal, RecorderError, SqliteTraceStore, StoredSession};
    use crate::tooling::{
        ApprovalPolicy, ToolCatalog, ToolDescriptor, ToolLimits, ToolMetadata, ToolSource,
    };

    struct UnusedProvider;

    #[async_trait]
    impl LlmProvider for UnusedProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            Err(LlmError::Request {
                message: "test provider must not be called during assembly".to_owned(),
            })
        }
    }

    struct EmptyTools;

    #[async_trait]
    impl ToolProvider for EmptyTools {
        async fn provide(&self, _context: &ToolContext<'_>) -> Result<ToolCatalog, EngineError> {
            Ok(ToolCatalog::new())
        }
    }

    struct AllowGate;

    #[async_trait]
    impl ToolPermissionGate for AllowGate {
        async fn request_permission(&self, _call: &ToolCall) -> ToolPermissionDecision {
            ToolPermissionDecision::Allow
        }
    }

    fn provider() -> Arc<dyn LlmProvider> {
        Arc::new(UnusedProvider)
    }

    fn tools() -> Arc<dyn ToolProvider> {
        Arc::new(EmptyTools)
    }

    fn config() -> EngineConfig {
        EngineConfig::new(TracePolicy::Disabled)
    }

    fn base_builder() -> EngineBuilder {
        EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .config(config())
    }

    #[test]
    fn every_required_host_dependency_is_reported_by_its_own_variant() {
        let missing_provider = EngineBuilder::new()
            .tool_provider(tools())
            .config(config())
            .build()
            .expect_err("a provider is mandatory");
        assert!(matches!(missing_provider, EngineError::MissingProvider));

        let missing_tools = EngineBuilder::new()
            .provider(provider())
            .config(config())
            .build()
            .expect_err("a tool provider is mandatory");
        assert!(matches!(missing_tools, EngineError::MissingToolProvider));

        let missing_config = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .build()
            .expect_err("an engine config is mandatory");
        assert!(matches!(missing_config, EngineError::MissingConfig));

        let missing_child_factory = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .config(config().with_delegation(Some(DelegationLimits::default())))
            .build()
            .expect_err("delegation needs a child tool factory");
        assert!(matches!(
            missing_child_factory,
            EngineError::MissingChildToolFactory
        ));
    }

    #[test]
    fn an_ask_rule_without_a_gate_fails_assembly() {
        let policy = PolicyConfig {
            allowed_tools: vec!["read_file".to_owned()],
            rules: vec![PolicyRule {
                id: "ask-read".to_owned(),
                tool: Some(ToolSelector::One("read_file".to_owned())),
                path_glob: None,
                program: None,
                args_prefix: None,
                action: RuleAction::Ask,
                reason: None,
            }],
        };
        let configured = EngineConfig::new(TracePolicy::Disabled).with_policy(policy);

        let error = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .config(configured)
            .build()
            .expect_err("an ask rule cannot run without a gate");

        assert!(matches!(error, EngineError::MissingApprovalGate));
    }

    #[test]
    fn a_gate_lets_an_explicit_ask_configuration_assemble() {
        let configured = EngineConfig::new(TracePolicy::Disabled).with_approval(ApprovalConfig {
            mode: ApprovalMode::Ask,
            explicit: true,
        });

        let engine = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .approval_gate(Arc::new(AllowGate))
            .config(configured)
            .build()
            .expect("explicit ask with a gate must assemble");

        assert_eq!(engine.config().approval().mode, ApprovalMode::Ask);
        assert!(engine.config().approval().explicit);
    }

    #[test]
    fn engine_exposes_the_result_and_turn_configuration_it_was_built_with() {
        let result_config = ResultConfig {
            max_inline_bytes: 4096,
            ..ResultConfig::default()
        };
        let configured = config().with_result(result_config.clone());
        let engine = base_builder()
            .config(configured)
            .build()
            .expect("the complete builder must assemble");

        assert_eq!(engine.config().result(), &result_config);
        assert_eq!(engine.config().trace_policy(), TracePolicy::Disabled);
    }

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Creates a unique directory and removes it on drop.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "jessica-engine-api-{label}-{}-{id}",
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

    struct MockTool {
        name: &'static str,
        content: String,
        calls: Arc<AtomicU32>,
    }

    impl MockTool {
        fn new(name: &'static str, content: impl Into<String>) -> Self {
            Self {
                name,
                content: content.into(),
                calls: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    #[async_trait]
    impl Tool for MockTool {
        fn name(&self) -> &str {
            self.name
        }

        fn schema(&self) -> ToolSchema {
            json!({"type": "function", "function": {"name": self.name}})
        }

        async fn execute(&self, _args: Value) -> ToolResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ToolResult::success(String::new(), self.content.clone())
        }
    }

    struct StaticTools {
        catalog: ToolCatalog,
    }

    #[async_trait]
    impl ToolProvider for StaticTools {
        async fn provide(&self, _context: &ToolContext<'_>) -> Result<ToolCatalog, EngineError> {
            Ok(self.catalog.clone())
        }
    }

    struct ScriptedProvider {
        calls: AtomicU32,
        tool_name: &'static str,
        final_text: &'static str,
        usage: TokenUsage,
    }

    impl ScriptedProvider {
        fn new(tool_name: &'static str, final_text: &'static str, usage: TokenUsage) -> Self {
            Self {
                calls: AtomicU32::new(0),
                tool_name,
                final_text,
                usage,
            }
        }
    }

    #[async_trait]
    impl LlmProvider for ScriptedProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let events = if call == 0 {
                vec![
                    LlmEvent::ToolCallRequested {
                        call: ToolCall::new("call-1", self.tool_name, json!({})),
                    },
                    LlmEvent::Done {
                        usage: Some(self.usage),
                    },
                ]
            } else {
                vec![
                    LlmEvent::TextDelta {
                        text: self.final_text.to_owned(),
                    },
                    LlmEvent::Done {
                        usage: Some(self.usage),
                    },
                ]
            };
            Ok(Box::pin(futures::stream::iter(events)))
        }
    }

    struct TextProvider {
        text: &'static str,
        usage: TokenUsage,
    }

    #[async_trait]
    impl LlmProvider for TextProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            Ok(Box::pin(futures::stream::iter(vec![
                LlmEvent::TextDelta {
                    text: self.text.to_owned(),
                },
                LlmEvent::Done {
                    usage: Some(self.usage),
                },
            ])))
        }
    }

    /// Emits one text delta per entry, then a single `Done`.
    struct DeltaProvider {
        texts: Vec<String>,
        usage: TokenUsage,
    }

    #[async_trait]
    impl LlmProvider for DeltaProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            let mut events: Vec<LlmEvent> = self
                .texts
                .iter()
                .map(|text| LlmEvent::TextDelta { text: text.clone() })
                .collect();
            events.push(LlmEvent::Done {
                usage: Some(self.usage),
            });
            Ok(Box::pin(futures::stream::iter(events)))
        }
    }

    /// Counts provider calls so a test can prove the model was never reached.
    struct CountingProvider {
        calls: Arc<AtomicU32>,
        usage: TokenUsage,
    }

    #[async_trait]
    impl LlmProvider for CountingProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter(vec![LlmEvent::Done {
                usage: Some(self.usage),
            }])))
        }
    }

    /// Returns scripted outputs in order and records every prompt it sees.
    struct RecordingProvider {
        outputs: Mutex<Vec<Vec<LlmEvent>>>,
        prompts: Mutex<Vec<Vec<Message>>>,
        calls: AtomicU32,
    }

    impl RecordingProvider {
        fn new(outputs: Vec<Vec<LlmEvent>>) -> Self {
            Self {
                outputs: Mutex::new(outputs),
                prompts: Mutex::new(Vec::new()),
                calls: AtomicU32::new(0),
            }
        }

        fn prompts(&self) -> Vec<Vec<Message>> {
            self.prompts
                .lock()
                .expect("prompts must not be poisoned")
                .clone()
        }

        fn call_count(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    fn text_events(text: &str) -> Vec<LlmEvent> {
        vec![
            LlmEvent::TextDelta {
                text: text.to_owned(),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(1, 1)),
            },
        ]
    }

    #[async_trait]
    impl LlmProvider for RecordingProvider {
        async fn stream_chat(
            &self,
            messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            self.prompts
                .lock()
                .expect("prompts must not be poisoned")
                .push(messages.to_vec());
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut outputs = self.outputs.lock().expect("outputs must not be poisoned");
            let events = if outputs.is_empty() {
                text_events("done")
            } else {
                outputs.remove(0)
            };
            drop(outputs);
            Ok(Box::pin(futures::stream::iter(events)))
        }
    }

    /// Journal mock that retains the trace envelopes and session metadata.
    ///
    /// A trace write can be made to fail at a fixed one-based position so a
    /// test can prove the engine's incomplete-trace handling on the journal
    /// seam itself.
    #[derive(Default)]
    struct RecordingJournal {
        trace: Mutex<Vec<TraceEvent>>,
        sessions: Mutex<Vec<(SessionId, Vec<Message>)>>,
        writes: AtomicU32,
        fail_at: AtomicU32,
    }

    impl RecordingJournal {
        fn trace(&self) -> Vec<TraceEvent> {
            self.trace
                .lock()
                .expect("trace must not be poisoned")
                .clone()
        }

        fn sessions(&self) -> Vec<(SessionId, Vec<Message>)> {
            self.sessions
                .lock()
                .expect("sessions must not be poisoned")
                .clone()
        }

        fn fail_at(&self, write: u32) {
            self.fail_at.store(write, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl EventJournal for RecordingJournal {
        async fn record_session_start(
            &self,
            session_id: &SessionId,
            messages: &[Message],
        ) -> Result<(), RecorderError> {
            self.sessions
                .lock()
                .expect("sessions must not be poisoned")
                .push((session_id.clone(), messages.to_vec()));
            Ok(())
        }

        async fn record_traced(
            &self,
            trace: TraceEvent,
            _legacy: Option<(SessionId, AgentEvent)>,
        ) -> Result<(), RecorderError> {
            let write = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_at.load(Ordering::SeqCst) == write {
                return Err(RecorderError::Integrity {
                    message: "injected journal failure".to_owned(),
                });
            }
            self.trace
                .lock()
                .expect("trace must not be poisoned")
                .push(trace);
            Ok(())
        }
    }

    /// Acceptance check that fails a fixed number of initial calls.
    struct FailingCheck {
        fail_calls: u32,
        calls: AtomicU32,
    }

    impl FailingCheck {
        fn new(fail_calls: u32) -> Self {
            Self {
                fail_calls,
                calls: AtomicU32::new(0),
            }
        }

        fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl AcceptanceCheck for FailingCheck {
        fn name(&self) -> &str {
            "failing-test-check"
        }

        async fn check(&self, _context: &AcceptanceContext<'_>) -> AcceptanceResult {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call <= self.fail_calls {
                AcceptanceResult::fail("not accepted yet")
            } else {
                AcceptanceResult::pass("accepted")
            }
        }
    }

    /// Memory mock with configurable recall and recording.
    #[derive(Default)]
    struct MemoryMock {
        recall_calls: AtomicU32,
        record_calls: AtomicU32,
        recall_message: Mutex<Option<Message>>,
        records: Mutex<Vec<(SessionId, u64, String, usize)>>,
        fail: AtomicBool,
    }

    impl MemoryMock {
        fn with_recall(message: Message) -> Self {
            Self {
                recall_message: Mutex::new(Some(message)),
                ..Self::default()
            }
        }

        fn fail(&self) {
            self.fail.store(true, Ordering::SeqCst);
        }

        fn recall_calls(&self) -> u32 {
            self.recall_calls.load(Ordering::SeqCst)
        }

        fn record_calls(&self) -> u32 {
            self.record_calls.load(Ordering::SeqCst)
        }

        fn records(&self) -> Vec<(SessionId, u64, String, usize)> {
            self.records
                .lock()
                .expect("memory records must not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl MemoryPort for MemoryMock {
        async fn recall(
            &self,
            _session_id: &SessionId,
            _user_input: &str,
        ) -> Result<Option<Message>, EngineError> {
            self.recall_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(EngineError::UnknownSession {
                    session_id: "memory-recall-failure".to_owned(),
                });
            }
            Ok(self
                .recall_message
                .lock()
                .expect("recall message must not be poisoned")
                .clone())
        }

        async fn record_completed_turn(
            &self,
            session_id: &SessionId,
            turn_index: u64,
            user_input: &str,
            _assistant_answer: &str,
            tool_call_count: usize,
        ) -> Result<(), EngineError> {
            self.record_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(EngineError::UnknownSession {
                    session_id: "memory-write-failure".to_owned(),
                });
            }
            self.records
                .lock()
                .expect("records must not be poisoned")
                .push((
                    session_id.clone(),
                    turn_index,
                    user_input.to_owned(),
                    tool_call_count,
                ));
            Ok(())
        }
    }

    /// Skill mock with configurable recall and post-turn recording.
    #[derive(Default)]
    struct SkillMock {
        recall_calls: AtomicU32,
        after_calls: AtomicU32,
        recall_message: Mutex<Option<Message>>,
        signals: Mutex<Vec<TurnSignals>>,
        fail: AtomicBool,
    }

    impl SkillMock {
        fn with_recall(message: Message) -> Self {
            Self {
                recall_message: Mutex::new(Some(message)),
                ..Self::default()
            }
        }

        fn fail(&self) {
            self.fail.store(true, Ordering::SeqCst);
        }

        fn recall_calls(&self) -> u32 {
            self.recall_calls.load(Ordering::SeqCst)
        }

        fn after_calls(&self) -> u32 {
            self.after_calls.load(Ordering::SeqCst)
        }

        fn signals(&self) -> Vec<TurnSignals> {
            self.signals
                .lock()
                .expect("signals must not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl SkillPort for SkillMock {
        async fn recall(
            &self,
            _session_id: &SessionId,
            _user_input: &str,
        ) -> Result<Option<Message>, EngineError> {
            self.recall_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(EngineError::UnknownSession {
                    session_id: "skill-recall-failure".to_owned(),
                });
            }
            Ok(self
                .recall_message
                .lock()
                .expect("recall message must not be poisoned")
                .clone())
        }

        async fn after_completed_turn(
            &self,
            _session_id: &SessionId,
            _turn_index: u64,
            _user_input: &str,
            _assistant_answer: &str,
            signals: &TurnSignals,
            _messages: &[Message],
        ) -> Result<(), EngineError> {
            self.after_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(EngineError::UnknownSession {
                    session_id: "skill-enqueue-failure".to_owned(),
                });
            }
            self.signals
                .lock()
                .expect("signals must not be poisoned")
                .push(*signals);
            Ok(())
        }
    }

    /// Static system prompt source.
    struct StaticPrompt(&'static str);

    impl SystemPromptSource for StaticPrompt {
        fn system_prompt(&self) -> String {
            self.0.to_owned()
        }
    }

    /// Deterministic turn/session identities.
    #[derive(Default)]
    struct SequentialIds {
        turn: AtomicU64,
        session: AtomicU64,
    }

    impl IdSource for SequentialIds {
        fn next_turn_id(&self) -> String {
            let sequence = self.turn.fetch_add(1, Ordering::SeqCst) + 1;
            format!("turn-test-{sequence}")
        }

        fn next_session_id(&self) -> SessionId {
            let sequence = self.session.fetch_add(1, Ordering::SeqCst) + 1;
            SessionId::new(format!("session-test-{sequence}"))
        }
    }

    /// Loads exactly one recorded session, once.
    struct SingleSessionStore {
        session_id: SessionId,
        session: Mutex<Option<StoredSession>>,
    }

    impl SingleSessionStore {
        fn new(session_id: SessionId, session: StoredSession) -> Self {
            Self {
                session_id,
                session: Mutex::new(Some(session)),
            }
        }
    }

    #[async_trait]
    impl SessionStore for SingleSessionStore {
        async fn load_session(
            &self,
            session_id: SessionId,
        ) -> Result<Option<StoredSession>, EngineError> {
            if session_id != self.session_id {
                return Ok(None);
            }
            Ok(self
                .session
                .lock()
                .expect("stored session must not be poisoned")
                .take())
        }
    }

    fn descriptor(name: &str) -> ToolDescriptor {
        ToolDescriptor::new(
            name.to_owned(),
            ToolSource::Native,
            json!({
                "type": "function",
                "function": {"name": name, "parameters": {"type": "object"}}
            }),
            "test-version",
            ToolLimits::default(),
            ApprovalPolicy::NotRequired,
            &ToolMetadata::default(),
        )
        .expect("test schema must serialize")
    }

    fn catalog_with(name: &'static str, tool: Arc<MockTool>) -> ToolCatalog {
        let mut catalog = ToolCatalog::new();
        catalog
            .insert(ToolEntry {
                tool,
                descriptor: descriptor(name),
            })
            .expect("catalog insert must succeed");
        catalog
    }

    fn run_config(allowed: &[&str], rules: Vec<PolicyRule>, max_tokens: u64) -> EngineConfig {
        EngineConfig::new(TracePolicy::Disabled)
            .with_budget(Budget::new(max_tokens, Duration::from_secs(1), 4))
            .with_policy(PolicyConfig {
                allowed_tools: allowed.iter().map(|name| (*name).to_owned()).collect(),
                rules,
            })
            .with_tools(ToolExecutionConfig {
                timeout: Duration::from_secs(1),
                max_args_bytes: None,
                max_result_bytes: 1024,
            })
    }

    fn build_engine(
        provider: Arc<dyn LlmProvider>,
        tools: Arc<dyn ToolProvider>,
        config: EngineConfig,
    ) -> Engine {
        EngineBuilder::new()
            .provider(provider)
            .tool_provider(tools)
            .config(config)
            .build()
            .expect("engine must assemble")
    }

    /// A trace store that logs accepted events and can inject failures.
    #[derive(Default)]
    struct MockTraceStore {
        begin_calls: AtomicU32,
        record_calls: AtomicU32,
        finish_calls: AtomicU32,
        fail_begin: AtomicBool,
        fail_record_at: AtomicU32,
        fail_finish: AtomicBool,
        recorded: Mutex<Vec<AgentEvent>>,
        observed_prefix: Mutex<Option<(Arc<RecordingObserver>, usize)>>,
    }

    impl MockTraceStore {
        fn fail_begin(&self) {
            self.fail_begin.store(true, Ordering::SeqCst);
        }

        fn fail_record_at(&self, call: u32) {
            self.fail_record_at.store(call, Ordering::SeqCst);
        }

        fn fail_finish(&self) {
            self.fail_finish.store(true, Ordering::SeqCst);
        }

        fn begin_count(&self) -> u32 {
            self.begin_calls.load(Ordering::SeqCst)
        }

        fn record_count(&self) -> u32 {
            self.record_calls.load(Ordering::SeqCst)
        }

        fn finish_count(&self) -> u32 {
            self.finish_calls.load(Ordering::SeqCst)
        }

        fn recorded(&self) -> Vec<AgentEvent> {
            self.recorded
                .lock()
                .expect("recorded events must not be poisoned")
                .clone()
        }

        /// Makes the injected record failure wait until `observer` has seen
        /// `count` broadcast events, so the test can prove the sink ran first.
        fn observe_prefix(&self, observer: Arc<RecordingObserver>, count: usize) {
            *self
                .observed_prefix
                .lock()
                .expect("observed-prefix slot must not be poisoned") = Some((observer, count));
        }
    }

    #[async_trait]
    impl TraceStore for MockTraceStore {
        async fn begin_turn(
            &self,
            trace: &TurnTrace,
            _mode: TraceTurnMode,
            _messages: &[Message],
        ) -> Result<(), EngineError> {
            self.begin_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_begin.load(Ordering::SeqCst) {
                return Err(mock_trace_failure(trace, "injected begin failure"));
            }
            Ok(())
        }

        async fn record(&self, trace: &TurnTrace, event: AgentEvent) -> Result<(), EngineError> {
            let call = self.record_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_record_at.load(Ordering::SeqCst) == call {
                let watched = self
                    .observed_prefix
                    .lock()
                    .expect("observed-prefix slot must not be poisoned")
                    .clone();
                if let Some((observer, count)) = watched {
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while observer.event_count() < count {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect(
                        "the pump must observe every durable event before the injected failure",
                    );
                }
                return Err(mock_trace_failure(trace, "injected record failure"));
            }
            self.recorded
                .lock()
                .expect("recorded events must not be poisoned")
                .push(event);
            Ok(())
        }

        async fn finish_turn(
            &self,
            trace: &TurnTrace,
            _outcome: &TurnOutcome,
        ) -> Result<(), EngineError> {
            self.finish_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_finish.load(Ordering::SeqCst) {
                return Err(mock_trace_failure(trace, "injected finish failure"));
            }
            Ok(())
        }
    }

    fn mock_trace_failure(trace: &TurnTrace, cause: &str) -> EngineError {
        EngineError::TraceIncomplete {
            turn_id: trace.turn_id.clone(),
            cause: cause.to_owned(),
        }
    }

    /// Records every event the pump publishes after durable recording.
    #[derive(Default)]
    struct RecordingObserver {
        events: Mutex<Vec<AgentEvent>>,
    }

    impl RecordingObserver {
        fn events(&self) -> Vec<AgentEvent> {
            self.events
                .lock()
                .expect("observed events must not be poisoned")
                .clone()
        }

        fn event_count(&self) -> usize {
            self.events
                .lock()
                .expect("observed events must not be poisoned")
                .len()
        }
    }

    impl TurnObserver for RecordingObserver {
        fn publish(&self, event: &AgentEvent) {
            self.events
                .lock()
                .expect("observed events must not be poisoned")
                .push(event.clone());
        }
    }

    fn required_config() -> EngineConfig {
        EngineConfig::default()
            .with_budget(Budget::new(100, Duration::from_secs(1), 4))
            .with_policy(PolicyConfig {
                allowed_tools: Vec::new(),
                rules: Vec::new(),
            })
            .with_tools(ToolExecutionConfig {
                timeout: Duration::from_secs(1),
                max_args_bytes: None,
                max_result_bytes: 1024,
            })
    }

    fn required_engine(provider: Arc<dyn LlmProvider>, store: Arc<dyn TraceStore>) -> Engine {
        EngineBuilder::new()
            .provider(provider)
            .tool_provider(tools())
            .trace_store(store)
            .config(required_config())
            .build()
            .expect("required-trace engine must assemble")
    }

    fn e3_config() -> EngineConfig {
        EngineConfig::new(TracePolicy::Required)
            .with_budget(Budget::new(10_000, Duration::from_secs(2), 8))
            .with_tools(ToolExecutionConfig {
                timeout: Duration::from_secs(1),
                max_args_bytes: None,
                max_result_bytes: 1024,
            })
    }

    fn e3_engine(
        provider: Arc<dyn LlmProvider>,
        journal: Arc<dyn EventJournal>,
        config: EngineConfig,
    ) -> EngineBuilder {
        EngineBuilder::new()
            .provider(provider)
            .tool_provider(tools())
            .event_journal(journal)
            .config(config)
    }

    fn agent_event(entry: &TraceEvent) -> Option<&AgentEvent> {
        match &entry.payload {
            TracePayload::Agent { event } => Some(event),
            TracePayload::ChildAgent { event, .. } => Some(event),
            _ => None,
        }
    }

    #[test]
    fn a_required_trace_without_a_store_fails_assembly() {
        assert_eq!(
            EngineConfig::default().trace_policy(),
            TracePolicy::Required
        );

        let error = EngineBuilder::new()
            .provider(provider())
            .tool_provider(tools())
            .config(EngineConfig::default())
            .build()
            .expect_err("a required trace must reject a missing store");

        assert!(matches!(error, EngineError::MissingTraceStore));
    }

    #[tokio::test]
    async fn a_trace_that_cannot_open_never_reaches_the_provider() {
        let temp = TempDir::new("trace-begin-failure");
        let store = Arc::new(MockTraceStore::default());
        store.fail_begin();
        let calls = Arc::new(AtomicU32::new(0));
        let provider: Arc<dyn LlmProvider> = Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let engine = required_engine(provider, store_port);

        let error = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect_err("an unopened trace must fail the turn");

        match &error {
            EngineError::TraceIncomplete { turn_id, cause } => {
                assert_eq!(turn_id, "turn-1");
                assert!(
                    cause.contains("injected begin failure"),
                    "unexpected cause: {cause}"
                );
            }
            other => panic!("expected TraceIncomplete, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0, "the model must not run");
        assert_eq!(store.begin_count(), 1);
        assert_eq!(store.record_count(), 0);
        assert_eq!(store.finish_count(), 0);
    }

    #[tokio::test]
    async fn a_trace_write_failure_returns_trace_incomplete_without_turn_ended() {
        let temp = TempDir::new("trace-write-failure");
        let store = Arc::new(MockTraceStore::default());
        store.fail_record_at(3);
        let provider: Arc<dyn LlmProvider> = Arc::new(DeltaProvider {
            texts: vec!["first".to_owned(), "second".to_owned()],
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let engine = required_engine(provider, store_port);

        let error = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect_err("an unrecorded event must fail the turn");

        match &error {
            EngineError::TraceIncomplete { turn_id, cause } => {
                assert_eq!(turn_id, "turn-1");
                assert!(
                    cause.contains("injected record failure"),
                    "unexpected cause: {cause}"
                );
            }
            other => panic!("expected TraceIncomplete, got {other:?}"),
        }
        let recorded = store.recorded();
        assert_eq!(recorded.len(), 2);
        assert!(matches!(
            recorded.first(),
            Some(AgentEvent::SessionStarted { .. })
        ));
        assert!(matches!(
            recorded.get(1),
            Some(AgentEvent::RoundStarted { .. })
        ));
        assert!(
            !recorded
                .iter()
                .any(|event| matches!(event, AgentEvent::SessionEnded { .. })),
            "a rejected trace must not observe the terminal event"
        );
        assert_eq!(
            store.finish_count(),
            0,
            "a rejected trace must never receive turn_ended"
        );
    }

    #[tokio::test]
    async fn a_rejected_event_is_never_broadcast_and_no_later_event_follows() {
        let temp = TempDir::new("sink-before-broadcast");
        let observer = Arc::new(RecordingObserver::default());
        let store = Arc::new(MockTraceStore::default());
        store.fail_record_at(4);
        store.observe_prefix(Arc::clone(&observer), 3);
        let provider: Arc<dyn LlmProvider> = Arc::new(DeltaProvider {
            texts: vec!["first".to_owned(), "second".to_owned()],
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let observer_port: Arc<dyn TurnObserver> = observer.clone();
        let engine = EngineBuilder::new()
            .provider(provider)
            .tool_provider(tools())
            .trace_store(store_port)
            .observer(observer_port)
            .config(required_config())
            .build()
            .expect("the observer engine must assemble");

        let error = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect_err("the rejected event must fail the turn");

        assert!(matches!(error, EngineError::TraceIncomplete { .. }));
        let observed = observer.events();
        assert_eq!(
            observed.len(),
            3,
            "the rejected event and everything after it must stay off the broadcast"
        );
        assert!(matches!(
            observed.first(),
            Some(AgentEvent::SessionStarted { .. })
        ));
        assert!(matches!(
            observed.get(1),
            Some(AgentEvent::RoundStarted { .. })
        ));
        assert!(matches!(
            observed.get(2),
            Some(AgentEvent::ModelDelta { text }) if text == "first"
        ));
        assert_eq!(store.recorded().len(), 3);
        assert_eq!(store.finish_count(), 0);
    }

    #[tokio::test]
    async fn an_unacknowledged_turn_end_is_not_a_successful_turn() {
        let temp = TempDir::new("trace-finish-failure");
        let store = Arc::new(MockTraceStore::default());
        store.fail_finish();
        let provider: Arc<dyn LlmProvider> = Arc::new(TextProvider {
            text: "answer",
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let engine = required_engine(provider, store_port);

        let error = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect_err("a missing close acknowledgement must not be success");

        match &error {
            EngineError::TraceIncomplete { turn_id, cause } => {
                assert_eq!(turn_id, "turn-1");
                assert!(
                    cause.contains("injected finish failure"),
                    "unexpected cause: {cause}"
                );
            }
            other => panic!("expected TraceIncomplete, got {other:?}"),
        }
        let recorded = store.recorded();
        assert!(
            matches!(recorded.last(), Some(AgentEvent::SessionEnded { .. })),
            "the producer stream must be complete before the close is attempted"
        );
        assert_eq!(store.finish_count(), 1);
    }

    #[tokio::test]
    async fn a_pump_gap_is_exposed_in_an_otherwise_complete_turn() {
        let temp = TempDir::new("pump-lag");
        let store = Arc::new(MockTraceStore::default());
        let provider: Arc<dyn LlmProvider> = Arc::new(DeltaProvider {
            texts: vec!["x".to_owned(); 2000],
            usage: TokenUsage::new(1, 1),
        });
        let store_port: Arc<dyn TraceStore> = store.clone();
        let engine = required_engine(provider, store_port);

        let outcome = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect("the producer trace has no gap and must succeed");

        assert!(
            outcome.lagged,
            "the 1024-event broadcast buffer must have overflowed"
        );
        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(
            outcome.messages,
            vec![Message::user("go"), Message::assistant("x".repeat(2000))]
        );
        assert_eq!(outcome.last_output, "x".repeat(2000));
        let recorded = store.recorded();
        assert_eq!(
            recorded
                .iter()
                .filter(|event| matches!(event, AgentEvent::ModelDelta { .. }))
                .count(),
            2000,
            "the producer trace must retain every delta despite the observer gap"
        );
        assert!(matches!(
            recorded.first(),
            Some(AgentEvent::SessionStarted { .. })
        ));
        assert!(matches!(
            recorded.last(),
            Some(AgentEvent::SessionEnded { .. })
        ));
        assert_eq!(store.finish_count(), 1);
    }

    #[tokio::test]
    async fn the_sqlite_store_closes_a_trace_and_keeps_the_legacy_mirror() {
        let temp = TempDir::new("sqlite-trace");
        let database = temp.path.join("trace.sqlite3");
        let provider: Arc<dyn LlmProvider> = Arc::new(TextProvider {
            text: "answer",
            usage: TokenUsage::new(1, 1),
        });
        {
            let store: Arc<dyn TraceStore> = Arc::new(
                SqliteTraceStore::open(database.clone())
                    .await
                    .expect("the trace store must open"),
            );
            let engine = required_engine(provider, Arc::clone(&store));

            let outcome = engine
                .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
                .await
                .expect("a durable required trace must close");
            assert_eq!(outcome.reason, SessionEndReason::Completed);
            assert!(!outcome.lagged);
        }

        let store = EventStore::open(&database).expect("the database must reopen");
        let turn_id = TurnId::new("turn-1");
        let trace = store
            .trace_turn(&turn_id)
            .expect("the trace must load")
            .expect("the turn must exist");
        assert!(trace.closed(), "the trace must end with turn_ended");
        assert_eq!(trace.events().len(), 7);
        match &trace.events()[0].event().payload {
            TracePayload::TurnStarted { mode, messages } => {
                assert_eq!(*mode, TraceTurnMode::Fresh);
                assert_eq!(messages, &vec![Message::user("go")]);
            }
            other => panic!("expected turn_started, got {other:?}"),
        }
        match &trace
            .events()
            .last()
            .expect("the trace must not be empty")
            .event()
            .payload
        {
            TracePayload::TurnEnded { reason } => {
                assert_eq!(*reason, SessionEndReason::Completed);
            }
            other => panic!("expected turn_ended, got {other:?}"),
        }

        let session_id = SessionId::new("turn-1");
        let legacy = store
            .events(&session_id)
            .expect("the legacy mirror must load");
        assert_eq!(legacy.len(), 5);
        assert!(matches!(
            legacy.first().map(|event| event.event()),
            Some(AgentEvent::SessionStarted { .. })
        ));
        assert!(matches!(
            legacy.last().map(|event| event.event()),
            Some(AgentEvent::SessionEnded { .. })
        ));
        assert_eq!(
            store
                .session_messages(&session_id)
                .expect("session metadata must load"),
            Some(vec![Message::user("go")])
        );
    }

    #[tokio::test]
    async fn run_turn_executes_an_allowlisted_tool_and_returns_the_transcript() {
        let temp = TempDir::new("happy-path");
        let inner = Arc::new(MockTool::new("echo", "tool-output"));
        let tools: Arc<dyn ToolProvider> = Arc::new(StaticTools {
            catalog: catalog_with("echo", Arc::clone(&inner)),
        });
        let provider: Arc<dyn LlmProvider> = Arc::new(ScriptedProvider::new(
            "echo",
            "final answer",
            TokenUsage::new(2, 3),
        ));
        let engine = build_engine(provider, tools, run_config(&["echo"], Vec::new(), 100));

        let outcome = engine
            .run_turn(TurnRequest::new("say hi").with_workspace_root(&temp.path))
            .await
            .expect("turn must complete");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(outcome.turn_id, "turn-1");
        assert_eq!(outcome.turn_start, 1);
        assert!(!outcome.lagged);
        assert_eq!(outcome.last_output, "final answer");
        assert_eq!(outcome.messages.len(), 4);
        assert_eq!(outcome.messages[2].role, Role::Tool);
        assert_eq!(outcome.messages[2].content, "tool-output");
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn policy_denial_reaches_the_model_without_running_the_tool() {
        let temp = TempDir::new("policy-denial");
        let inner = Arc::new(MockTool::new("blocked", "must not run"));
        let tools: Arc<dyn ToolProvider> = Arc::new(StaticTools {
            catalog: catalog_with("blocked", Arc::clone(&inner)),
        });
        let provider: Arc<dyn LlmProvider> = Arc::new(ScriptedProvider::new(
            "blocked",
            "adapted",
            TokenUsage::new(1, 1),
        ));
        let rules = vec![PolicyRule {
            id: "deny-blocked".to_owned(),
            tool: Some(ToolSelector::One("blocked".to_owned())),
            path_glob: None,
            program: None,
            args_prefix: None,
            action: RuleAction::Deny,
            reason: Some("blocked by test policy".to_owned()),
        }];
        let engine = build_engine(provider, tools, run_config(&["blocked"], rules, 100));

        let outcome = engine
            .run_turn(TurnRequest::new("try it").with_workspace_root(&temp.path))
            .await
            .expect("a policy denial must not fail the turn");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
        assert_eq!(outcome.messages.len(), 4);
        assert!(outcome.messages[2].content.contains("denied by policy"));
        assert_eq!(outcome.last_output, "adapted");
    }

    #[tokio::test]
    async fn a_token_trip_overrides_a_completed_runtime_reason() {
        let temp = TempDir::new("token-budget");
        let provider: Arc<dyn LlmProvider> = Arc::new(TextProvider {
            text: "too expensive",
            usage: TokenUsage::new(60, 60),
        });
        let tools: Arc<dyn ToolProvider> = Arc::new(EmptyTools);
        let engine = build_engine(provider, tools, run_config(&[], Vec::new(), 10));

        let outcome = engine
            .run_turn(TurnRequest::new("go").with_workspace_root(&temp.path))
            .await
            .expect("budget exhaustion is a turn outcome, not an engine error");

        assert_eq!(outcome.reason, SessionEndReason::BudgetExceeded);
        assert_eq!(outcome.last_output, "too expensive");
    }

    #[tokio::test]
    async fn acceptance_retries_reuse_the_turn_prefix_and_isolate_attempt_sessions() {
        let temp = TempDir::new("e3-acceptance-retry");
        let provider = Arc::new(RecordingProvider::new(vec![
            text_events("first answer"),
            text_events("second answer"),
        ]));
        let journal = Arc::new(RecordingJournal::default());
        let memory = Arc::new(MemoryMock::with_recall(Message::system("memory guidance")));
        let skills = Arc::new(SkillMock::with_recall(Message::system("skill guidance")));
        let check = Arc::new(FailingCheck::new(1));
        let engine = e3_engine(
            Arc::clone(&provider) as Arc<dyn LlmProvider>,
            Arc::clone(&journal) as Arc<dyn EventJournal>,
            e3_config().with_acceptance(Some(AcceptanceConfig::new(1))),
        )
        .system_prompt(Arc::new(StaticPrompt("test system prompt")))
        .id_source(Arc::new(SequentialIds::default()))
        .memory(Arc::clone(&memory) as Arc<dyn MemoryPort>)
        .skills(Arc::clone(&skills) as Arc<dyn SkillPort>)
        .acceptance_check(Arc::clone(&check) as Arc<dyn AcceptanceCheck>)
        .build()
        .expect("the acceptance engine must assemble");
        let expected_prefix = vec![
            Message::system("test system prompt"),
            Message::system("memory guidance"),
            Message::system("skill guidance"),
            Message::user("earlier question"),
            Message::assistant("earlier answer"),
            Message::user("fix it"),
        ];

        let outcome = engine
            .run_turn_request(
                TurnRequest::new("fix it")
                    .with_workspace_root(&temp.path)
                    .with_history(vec![
                        Message::user("earlier question"),
                        Message::assistant("earlier answer"),
                    ])
                    .with_output(PumpOutput::Silent),
            )
            .await
            .expect("the second attempt must be accepted");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(outcome.turn_id, "turn-test-1");
        assert_eq!(outcome.turn_start, expected_prefix.len());
        assert_eq!(outcome.last_output, "second answer");
        let mut expected_second = expected_prefix.clone();
        expected_second.push(Message::assistant("first answer"));
        expected_second.push(Message::user("not accepted yet"));
        let mut expected_final = expected_second.clone();
        expected_final.push(Message::assistant("second answer"));
        assert_eq!(outcome.messages, expected_final);

        let prompts = provider.prompts();
        assert_eq!(prompts.len(), 2, "each attempt calls the model once");
        assert_eq!(
            prompts[0], expected_prefix,
            "recall and history must precede the first attempt unchanged"
        );
        assert_eq!(
            prompts[1], expected_second,
            "a retry must reuse the prefix and append only its feedback"
        );
        assert_eq!(
            memory.recall_calls(),
            1,
            "memory must be recalled once per turn, not once per attempt"
        );
        assert_eq!(skills.recall_calls(), 1);
        assert_eq!(check.calls(), 2);

        let records = memory.records();
        assert_eq!(records.len(), 1, "only the governed completion is recorded");
        assert_eq!(records[0].0, SessionId::new("session-test-2"));
        assert_eq!(records[0].1, 2);
        assert_eq!(records[0].2, "fix it");
        assert_eq!(records[0].3, 0);
        let signals = skills.signals();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].acceptance_failures(), 1);
        assert_eq!(signals[0].assistant_rounds(), 1);

        let sessions = journal.sessions();
        assert_eq!(sessions.len(), 3, "two attempts plus the outer session");
        assert_eq!(
            sessions[0],
            (SessionId::new("session-test-1"), expected_prefix.clone())
        );
        assert_eq!(
            sessions[1],
            (SessionId::new("session-test-2"), expected_second.clone())
        );
        assert_eq!(sessions[2], (SessionId::new("session-test-3"), Vec::new()));

        let trace = journal.trace();
        let first = trace.first().expect("the trace must start the turn");
        assert_eq!(
            first.session_id.as_ref(),
            Some(&SessionId::new("session-test-1"))
        );
        match &first.payload {
            TracePayload::TurnStarted { mode, messages } => {
                assert_eq!(*mode, TraceTurnMode::Fresh);
                assert_eq!(messages, &expected_prefix);
            }
            other => panic!("expected turn_started, got {other:?}"),
        }
        let failed = trace
            .iter()
            .find(|entry| {
                matches!(
                    agent_event(entry),
                    Some(AgentEvent::AcceptanceFailed { attempt, .. }) if *attempt == 1
                )
            })
            .expect("the rejected first attempt must be traced");
        assert_eq!(failed.attempt, Some(1));
        assert_eq!(
            failed.session_id.as_ref(),
            Some(&SessionId::new("session-test-1")),
            "an acceptance verdict belongs to the attempt it judged"
        );
        assert!(matches!(
            trace.last().map(|entry| &entry.payload),
            Some(TracePayload::TurnEnded {
                reason: SessionEndReason::Completed
            })
        ));
    }

    #[tokio::test]
    async fn exhausted_acceptance_labels_the_outer_session_and_skips_post_processing() {
        let temp = TempDir::new("e3-acceptance-failed");
        let provider = Arc::new(RecordingProvider::new(vec![
            text_events("first answer"),
            text_events("second answer"),
        ]));
        let journal = Arc::new(RecordingJournal::default());
        let memory = Arc::new(MemoryMock::default());
        let skills = Arc::new(SkillMock::default());
        let check = Arc::new(FailingCheck::new(2));
        let engine = e3_engine(
            Arc::clone(&provider) as Arc<dyn LlmProvider>,
            Arc::clone(&journal) as Arc<dyn EventJournal>,
            e3_config().with_acceptance(Some(AcceptanceConfig::new(1))),
        )
        .id_source(Arc::new(SequentialIds::default()))
        .memory(Arc::clone(&memory) as Arc<dyn MemoryPort>)
        .skills(Arc::clone(&skills) as Arc<dyn SkillPort>)
        .acceptance_check(Arc::clone(&check) as Arc<dyn AcceptanceCheck>)
        .build()
        .expect("the acceptance engine must assemble");

        let outcome = engine
            .run_turn_request(
                TurnRequest::new("fix it")
                    .with_workspace_root(&temp.path)
                    .with_output(PumpOutput::Silent),
            )
            .await
            .expect("exhausted acceptance is an outcome, not an engine error");

        assert_eq!(outcome.reason, SessionEndReason::AcceptanceFailed);
        assert_eq!(outcome.last_output, "second answer");
        assert_eq!(check.calls(), 2);
        assert_eq!(
            memory.record_calls(),
            0,
            "a failed turn must not update memory"
        );
        assert_eq!(
            skills.after_calls(),
            0,
            "a failed turn must not enqueue skill work"
        );

        let outer = SessionId::new("session-test-3");
        let sessions = journal.sessions();
        assert_eq!(
            sessions.last(),
            Some(&(outer.clone(), Vec::new())),
            "the outer session must be recorded before its terminal pair"
        );
        let trace = journal.trace();
        let outer_started = trace
            .iter()
            .find(|entry| {
                entry.session_id.as_ref() == Some(&outer)
                    && matches!(agent_event(entry), Some(AgentEvent::SessionStarted { .. }))
            })
            .expect("the outer session must start");
        assert_eq!(outer_started.attempt, None);
        let outer_ended = trace
            .iter()
            .find(|entry| {
                entry.session_id.as_ref() == Some(&outer)
                    && matches!(
                        agent_event(entry),
                        Some(AgentEvent::SessionEnded {
                            reason: SessionEndReason::AcceptanceFailed,
                            ..
                        })
                    )
            })
            .expect("the outer session must carry the governed reason");
        assert_eq!(outer_ended.attempt, None);
        assert!(matches!(
            trace.last().map(|entry| &entry.payload),
            Some(TracePayload::TurnEnded {
                reason: SessionEndReason::AcceptanceFailed
            })
        ));
    }

    #[tokio::test]
    async fn a_budget_trip_skips_acceptance_and_post_processing() {
        let temp = TempDir::new("e3-budget-trip");
        let provider: Arc<dyn LlmProvider> = Arc::new(TextProvider {
            text: "spent",
            usage: TokenUsage::new(50, 50),
        });
        let journal = Arc::new(RecordingJournal::default());
        let memory = Arc::new(MemoryMock::default());
        let skills = Arc::new(SkillMock::default());
        let check = Arc::new(FailingCheck::new(0));
        let engine = e3_engine(
            provider,
            Arc::clone(&journal) as Arc<dyn EventJournal>,
            e3_config()
                .with_budget(Budget::new(10, Duration::from_secs(2), 8))
                .with_acceptance(Some(AcceptanceConfig::new(2))),
        )
        .memory(Arc::clone(&memory) as Arc<dyn MemoryPort>)
        .skills(Arc::clone(&skills) as Arc<dyn SkillPort>)
        .acceptance_check(Arc::clone(&check) as Arc<dyn AcceptanceCheck>)
        .build()
        .expect("the acceptance engine must assemble");

        let outcome = engine
            .run_turn_request(
                TurnRequest::new("go")
                    .with_workspace_root(&temp.path)
                    .with_output(PumpOutput::Silent),
            )
            .await
            .expect("budget exhaustion is an outcome, not an engine error");

        assert_eq!(outcome.reason, SessionEndReason::BudgetExceeded);
        assert_eq!(
            check.calls(),
            0,
            "a non-completed attempt must not be judged by the acceptance check"
        );
        assert_eq!(memory.record_calls(), 0);
        assert_eq!(skills.after_calls(), 0);

        let trace = journal.trace();
        assert!(
            !trace.iter().any(|entry| matches!(
                agent_event(entry),
                Some(AgentEvent::AcceptanceFailed { .. })
            )),
            "the check never ran, so no verdict may be traced"
        );
        assert!(
            trace.iter().any(|entry| {
                entry.attempt.is_none()
                    && matches!(
                        agent_event(entry),
                        Some(AgentEvent::SessionEnded {
                            reason: SessionEndReason::BudgetExceeded,
                            ..
                        })
                    )
            }),
            "the outer session must report the governed budget reason"
        );
        assert!(matches!(
            trace.last().map(|entry| &entry.payload),
            Some(TracePayload::TurnEnded {
                reason: SessionEndReason::BudgetExceeded
            })
        ));
    }

    #[tokio::test]
    async fn memory_and_skill_failures_are_fail_open() {
        let temp = TempDir::new("e3-fail-open");
        let provider: Arc<dyn LlmProvider> = Arc::new(TextProvider {
            text: "answer",
            usage: TokenUsage::new(1, 1),
        });
        let journal = Arc::new(RecordingJournal::default());
        let memory = Arc::new(MemoryMock::with_recall(Message::system("memory")));
        memory.fail();
        let skills = Arc::new(SkillMock::with_recall(Message::system("skill")));
        skills.fail();
        let engine = e3_engine(
            provider,
            Arc::clone(&journal) as Arc<dyn EventJournal>,
            e3_config(),
        )
        .system_prompt(Arc::new(StaticPrompt("system")))
        .memory(Arc::clone(&memory) as Arc<dyn MemoryPort>)
        .skills(Arc::clone(&skills) as Arc<dyn SkillPort>)
        .build()
        .expect("the fail-open engine must assemble");

        let outcome = engine
            .run_turn_request(
                TurnRequest::new("go")
                    .with_workspace_root(&temp.path)
                    .with_output(PumpOutput::Silent),
            )
            .await
            .expect("port failures must not fail the turn");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(
            outcome.messages,
            vec![
                Message::system("system"),
                Message::user("go"),
                Message::assistant("answer"),
            ]
        );
        assert_eq!(outcome.last_output, "answer");
        assert_eq!(memory.recall_calls(), 1);
        assert_eq!(memory.record_calls(), 1);
        assert_eq!(skills.recall_calls(), 1);
        assert_eq!(skills.after_calls(), 1);
        assert!(
            memory.records().is_empty(),
            "a failed write must not be reported as a recorded turn"
        );
        assert!(matches!(
            journal.trace().last().map(|entry| &entry.payload),
            Some(TracePayload::TurnEnded {
                reason: SessionEndReason::Completed
            })
        ));
    }

    #[tokio::test]
    async fn the_initial_conversation_follows_the_fixed_order() {
        let temp = TempDir::new("e3-initial-order");
        let provider = Arc::new(RecordingProvider::new(vec![text_events("answer")]));
        let journal = Arc::new(RecordingJournal::default());
        let memory = Arc::new(MemoryMock::with_recall(Message::system("memory")));
        let skills = Arc::new(SkillMock::with_recall(Message::system("skill")));
        let engine = e3_engine(
            Arc::clone(&provider) as Arc<dyn LlmProvider>,
            Arc::clone(&journal) as Arc<dyn EventJournal>,
            e3_config(),
        )
        .system_prompt(Arc::new(StaticPrompt("system")))
        .memory(Arc::clone(&memory) as Arc<dyn MemoryPort>)
        .skills(Arc::clone(&skills) as Arc<dyn SkillPort>)
        .build()
        .expect("the order engine must assemble");

        let outcome = engine
            .run_turn_request(
                TurnRequest::new("go")
                    .with_workspace_root(&temp.path)
                    .with_history(vec![
                        Message::user("earlier"),
                        Message::assistant("earlier answer"),
                    ])
                    .with_output(PumpOutput::Silent),
            )
            .await
            .expect("the turn must complete");

        assert_eq!(
            provider.prompts(),
            vec![vec![
                Message::system("system"),
                Message::system("memory"),
                Message::system("skill"),
                Message::user("earlier"),
                Message::assistant("earlier answer"),
                Message::user("go"),
            ]],
            "the initial conversation order is fixed"
        );
        assert_eq!(outcome.turn_start, 6);
    }

    #[tokio::test]
    async fn resume_continues_the_unfinished_round_without_replaying_recorded_work() {
        let temp = TempDir::new("e3-resume");
        let session_id = SessionId::new("resume-session");
        let initial = vec![
            Message::system("resume system"),
            Message::user("finish the job"),
        ];
        let call = ToolCall::new("call-1", "read_file", json!({}));
        let checkpoint_messages = vec![
            initial[0].clone(),
            initial[1].clone(),
            Message::assistant_with_tool_calls("", vec![call.clone()]),
            Message::tool("call-1", "tool output"),
        ];
        let stored = StoredSession {
            initial_messages: initial.clone(),
            events: vec![
                RecordedEvent::new(
                    1,
                    AgentEvent::SessionStarted {
                        session_id: session_id.clone(),
                        messages: initial.clone(),
                    },
                ),
                RecordedEvent::new(2, AgentEvent::RoundStarted { round: 1 }),
                RecordedEvent::new(
                    3,
                    AgentEvent::RoundFinished {
                        round: 1,
                        usage: None,
                    },
                ),
                RecordedEvent::new(4, AgentEvent::ToolCallStarted { call: call.clone() }),
                RecordedEvent::new(
                    5,
                    AgentEvent::ToolCallFinished {
                        result: ToolResult::success("call-1", "tool output"),
                    },
                ),
            ],
        };
        let provider = Arc::new(RecordingProvider::new(vec![text_events("resumed answer")]));
        let journal = Arc::new(RecordingJournal::default());
        let memory = Arc::new(MemoryMock::default());
        let engine = e3_engine(
            Arc::clone(&provider) as Arc<dyn LlmProvider>,
            Arc::clone(&journal) as Arc<dyn EventJournal>,
            e3_config(),
        )
        .session_store(
            Arc::new(SingleSessionStore::new(session_id.clone(), stored)) as Arc<dyn SessionStore>,
        )
        .id_source(Arc::new(SequentialIds::default()))
        .memory(Arc::clone(&memory) as Arc<dyn MemoryPort>)
        .build()
        .expect("the resume engine must assemble");

        let outcome = engine
            .resume_turn(
                session_id.clone(),
                TurnRequest::new("ignored")
                    .with_workspace_root(&temp.path)
                    .with_output(PumpOutput::Silent),
            )
            .await
            .expect("the checkpoint must resume");

        assert_eq!(outcome.reason, SessionEndReason::Completed);
        assert_eq!(outcome.turn_id, "turn-test-1");
        assert_eq!(outcome.turn_start, 0);
        assert_eq!(outcome.last_output, "resumed answer");
        let mut expected_messages = checkpoint_messages.clone();
        expected_messages.push(Message::assistant("resumed answer"));
        assert_eq!(outcome.messages, expected_messages);
        assert_eq!(
            provider.call_count(),
            1,
            "resume must call the model only for the remaining round"
        );
        assert_eq!(
            provider.prompts(),
            vec![checkpoint_messages.clone()],
            "recorded work must not be requested again"
        );
        assert_eq!(memory.recall_calls(), 0, "resume must not recall memory");
        assert_eq!(memory.record_calls(), 1);

        let trace = journal.trace();
        let first = trace.first().expect("the trace must start the turn");
        assert_eq!(first.session_id.as_ref(), Some(&session_id));
        match &first.payload {
            TracePayload::TurnStarted { mode, messages } => {
                assert_eq!(*mode, TraceTurnMode::Resumed);
                assert_eq!(messages, &checkpoint_messages);
            }
            other => panic!("expected turn_started, got {other:?}"),
        }
        assert!(
            !trace
                .iter()
                .any(|entry| matches!(agent_event(entry), Some(AgentEvent::SessionStarted { .. }))),
            "a resumed loop must not replay its session_started event"
        );
        assert!(
            trace.iter().any(|entry| {
                matches!(agent_event(entry), Some(AgentEvent::RoundStarted { round }) if *round == 2)
            }),
            "the resumed loop must start at the first missing round"
        );
        assert!(matches!(
            trace.last().map(|entry| &entry.payload),
            Some(TracePayload::TurnEnded {
                reason: SessionEndReason::Completed
            })
        ));
        assert!(
            journal.sessions().is_empty(),
            "a resumed session already has its recorded metadata"
        );
    }

    #[tokio::test]
    async fn resuming_an_ended_session_returns_the_recorded_reason_without_calling_the_model() {
        let temp = TempDir::new("e3-resume-ended");
        let session_id = SessionId::new("ended-session");
        let initial = vec![Message::system("system"), Message::user("go")];
        let stored = StoredSession {
            initial_messages: initial.clone(),
            events: vec![
                RecordedEvent::new(
                    1,
                    AgentEvent::SessionStarted {
                        session_id: session_id.clone(),
                        messages: initial.clone(),
                    },
                ),
                RecordedEvent::new(2, AgentEvent::RoundStarted { round: 1 }),
                RecordedEvent::new(
                    3,
                    AgentEvent::SessionEnded {
                        session_id: session_id.clone(),
                        reason: SessionEndReason::Cancelled,
                    },
                ),
            ],
        };
        let calls = Arc::new(AtomicU32::new(0));
        let provider: Arc<dyn LlmProvider> = Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
            usage: TokenUsage::new(1, 1),
        });
        let journal = Arc::new(RecordingJournal::default());
        let engine = e3_engine(
            provider,
            Arc::clone(&journal) as Arc<dyn EventJournal>,
            e3_config(),
        )
        .session_store(
            Arc::new(SingleSessionStore::new(session_id.clone(), stored)) as Arc<dyn SessionStore>,
        )
        .id_source(Arc::new(SequentialIds::default()))
        .build()
        .expect("the resume engine must assemble");

        let outcome = engine
            .resume_turn(
                session_id,
                TurnRequest::new("ignored")
                    .with_workspace_root(&temp.path)
                    .with_output(PumpOutput::Silent),
            )
            .await
            .expect("an ended session still has a recorded outcome");

        assert_eq!(outcome.reason, SessionEndReason::Cancelled);
        assert_eq!(outcome.messages, initial);
        assert_eq!(outcome.last_output, "");
        assert_eq!(outcome.turn_start, 0);
        assert_eq!(outcome.turn_id, "turn-test-1");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "an ended session has no work left"
        );
        assert!(
            journal.trace().is_empty(),
            "an ended session must not open a new trace"
        );
        assert!(journal.sessions().is_empty());
    }

    #[tokio::test]
    async fn a_journal_write_failure_makes_the_turn_trace_incomplete() {
        let temp = TempDir::new("e3-journal-failure");
        let journal = Arc::new(RecordingJournal::default());
        // The first write is `turn_started`; failing the second one breaks the
        // trace after the turn is open but before any session terminal event.
        journal.fail_at(2);
        let provider: Arc<dyn LlmProvider> = Arc::new(TextProvider {
            text: "answer",
            usage: TokenUsage::new(1, 1),
        });
        let engine = e3_engine(
            provider,
            Arc::clone(&journal) as Arc<dyn EventJournal>,
            e3_config(),
        )
        .build()
        .expect("the failure engine must assemble");

        let error = engine
            .run_turn_request(
                TurnRequest::new("go")
                    .with_workspace_root(&temp.path)
                    .with_output(PumpOutput::Silent),
            )
            .await
            .expect_err("a rejected journal write must invalidate the turn");

        match &error {
            EngineError::TraceIncomplete { turn_id, cause } => {
                assert_eq!(turn_id, "turn-1");
                assert!(
                    cause.contains("injected journal failure"),
                    "unexpected cause: {cause}"
                );
            }
            other => panic!("expected TraceIncomplete, got {other:?}"),
        }
        let trace = journal.trace();
        assert!(
            matches!(
                trace.first().map(|entry| &entry.payload),
                Some(TracePayload::TurnStarted { .. })
            ),
            "the turn was open when the failure was injected"
        );
        assert!(
            !trace
                .iter()
                .any(|entry| matches!(&entry.payload, TracePayload::TurnEnded { .. })),
            "an incomplete turn must never receive turn_ended"
        );
    }
}
