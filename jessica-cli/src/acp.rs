//! Stable ACP v1 agent transport over stdio.
//!
//! The server implements `initialize`, `session/new`, `session/prompt`, and
//! `session/cancel`. A prompt is prepared synchronously (session lookup,
//! content validation, busy check), then model and tool work runs in a task
//! spawned on the connection so the SDK dispatch loop keeps receiving
//! messages. Cancellation is scoped to one session prompt; sessions run
//! concurrently and process shutdown or stdin EOF cancels every session before
//! a bounded drain.
//!
//! Limits are copied from the `[acp]` section at startup. Session IDs remain
//! bounded by `max_sessions`, prompt text by `max_prompt_bytes`, and history by
//! its message and byte limits; trimming removes only the oldest complete user
//! turn. Optional session notices are emitted only when the client advertised
//! `clientCapabilities.session.notices`. Reloadable configuration is observed
//! by a watcher started beside the lazily assembled runtime, so the next prompt
//! uses the latest reloadable snapshot while startup-only sections keep their
//! original values.
//!
//! Stdout is reserved for newline-delimited JSON-RPC. This module never writes
//! to stdout itself, and the CLI installs no tracing subscriber, so ACP
//! diagnostics cannot corrupt the protocol stream. Errors and diagnostics are
//! content-free: they never include prompt text, tool arguments, environment
//! values, or credentials.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    AgentCapabilities, CancelNotification, Implementation, InitializeRequest, InitializeResponse,
    McpCapabilities, MessageId, NewSessionRequest, NewSessionResponse, Notice, NoticeSeverity,
    PromptCapabilities, PromptRequest, PromptResponse, SessionCapabilities,
    SessionId as AcpSessionId, SessionNotification, SessionUpdate, StopReason,
};
use agent_client_protocol::{Agent, Client, Error, ErrorCode, Stdio};
use jessica_core::types::{AgentEvent, BudgetKind, Message, SessionEndReason, SessionId};
use serde_json::json;
use tokio::sync::{OnceCell, mpsc, oneshot, watch};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::acp_events::{
    AcpEventMapper, BudgetStopReason, InFlightTools, budget_stop_reason, interrupted_tool_update,
};
use crate::acp_session::{
    AcpSession, AcpSessionRegistry, HistoryLimits, PromptGuard, SessionError, prompt_text,
};
use crate::app::{CliError, CliRuntime, TurnOutcome, TurnRequest, TurnScope};
use crate::config::HarnessConfig;
use crate::hot_reload::ConfigWatcher;
use crate::pump::UpdateSink;
use crate::shutdown::ShutdownController;

/// Maximum time spent waiting for active prompts to finish during shutdown.
const PROMPT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Time allowed for the transport to flush after prompts have drained.
const TRANSPORT_FLUSH_GRACE: Duration = Duration::from_millis(250);

/// Serves stable ACP v1 over stdin/stdout until EOF or shutdown.
///
/// A clean stdin EOF is a successful termination. On SIGINT or SIGTERM the
/// caller's [`ShutdownController`] is cancelled, every session lifetime token
/// is cancelled, active prompts are drained with a bound, skills are drained,
/// and queued responses get a short flush window. A drain timeout only logs a
/// warning; the process never waits forever for a stuck prompt.
pub(crate) async fn run(
    config: HarnessConfig,
    root: PathBuf,
    watch_path: Option<PathBuf>,
    shutdown: Arc<ShutdownController>,
) -> Result<(), CliError> {
    let root = canonical_startup_root(root).await?;
    let service = Arc::new(AcpService::new(
        Arc::new(config),
        root,
        watch_path,
        Arc::clone(&shutdown),
    ));

    let initialize_service = Arc::clone(&service);
    let new_session_service = Arc::clone(&service);
    let prompt_service = Arc::clone(&service);
    let cancel_service = Arc::clone(&service);
    let connection = Agent
        .v1()
        .name("jessica")
        .on_receive_request(
            async move |request: InitializeRequest, responder, _connection| {
                initialize_service.initialize(&request, responder)
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: NewSessionRequest, responder, _connection| {
                match new_session_service.new_session(request).await {
                    Ok(response) => responder.respond(response),
                    Err(error) => responder.respond_with_error(error),
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: PromptRequest, responder, connection| {
                handle_prompt(Arc::clone(&prompt_service), request, responder, connection).await
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            async move |notification: CancelNotification, _connection| {
                cancel_service.cancel(&notification.session_id);
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_to(Stdio::new());
    tokio::pin!(connection);
    let transport_failed = service.transport_failure.clone();

    let mut connection_result = None;
    tokio::select! {
        // A signal that is already recorded takes precedence when the client
        // closes stdin at the same time: the signal determines the exit code.
        biased;
        _ = shutdown.cancelled() => {}
        _ = transport_failed.cancelled() => {}
        result = &mut connection => connection_result = Some(result),
    }

    service.begin_shutdown();
    match connection_result {
        Some(_) => {
            // The transport is gone and the SDK has cancelled its spawned
            // tasks. Wait briefly for their guards, then drain skills.
            if !service.wait_for_prompts(PROMPT_DRAIN_TIMEOUT).await {
                tracing::warn!("acp prompt drain timed out after transport close");
            }
        }
        None => {
            // Keep driving the connection while prompt workers observe their
            // cancellation and enqueue final updates and responses.
            let drained = service.wait_for_prompts(PROMPT_DRAIN_TIMEOUT);
            tokio::pin!(drained);
            tokio::select! {
                _ = &mut drained => {}
                result = &mut connection => connection_result = Some(result),
            }
            if connection_result.is_none() {
                if let Ok(result) = timeout(TRANSPORT_FLUSH_GRACE, &mut connection).await {
                    connection_result = Some(result);
                }
            } else if !service.wait_for_prompts(PROMPT_DRAIN_TIMEOUT).await {
                tracing::warn!("acp prompt drain timed out after transport close");
            }
        }
    }
    service.drain_skills().await;
    match connection_result {
        Some(result) => result.map_err(CliError::Acp),
        None => Ok(()),
    }
}

/// Canonicalizes the startup root once so session cwd containment is reliable.
async fn canonical_startup_root(root: PathBuf) -> Result<PathBuf, CliError> {
    let requested = root.display().to_string();
    tokio::task::spawn_blocking(move || std::fs::canonicalize(root))
        .await
        .map_err(CliError::CanonicalizeRootTask)?
        .map_err(|source| CliError::CanonicalizeRoot {
            path: requested,
            source,
        })
}

/// Runtime state shared by the ACP request handlers.
struct AcpService {
    /// Startup configuration, including immutable ACP limits.
    config: Arc<HarnessConfig>,
    /// Canonical startup workspace root.
    root: PathBuf,
    /// Configuration file to watch once the runtime exists, if `--config` was
    /// supplied.
    watch_path: Option<PathBuf>,
    /// Process-level shutdown token source.
    shutdown: Arc<ShutdownController>,
    /// Live sessions and their bounded histories.
    registry: AcpSessionRegistry,
    /// Tracks prompt workers and rejects new prompts once shutdown begins.
    prompts: Arc<PromptTracker>,
    /// Whether the initialized client advertised `session.notices`.
    ///
    /// Defaults to false. A notice must never be sent unless this is set by a
    /// completed `initialize` request.
    client_notices: AtomicBool,
    /// Cancelled when a protocol write fails because the client disconnected.
    transport_failure: CancellationToken,
    /// Provider, recorder, wasm, memory, and skills, assembled on first prompt.
    ///
    /// Assembly is lazy so `initialize` and `session/new` work even when a
    /// provider credential is absent. The first failure is cached and maps to
    /// a generic internal error; no provider detail is exposed. The config
    /// watcher lives beside the runtime and starts only after assembly, so
    /// every later prompt observes the newest reloadable snapshot.
    runtime: OnceCell<Result<RuntimeAssembly, ()>>,
}

/// The lazily assembled runtime and its optional configuration watcher.
struct RuntimeAssembly {
    /// Shared runtime used by every prompt.
    runtime: Arc<CliRuntime>,
    /// Kept alive for the process lifetime; dropping it stops reloads.
    _config_watcher: Option<ConfigWatcher>,
}

impl AcpService {
    /// Creates the service around a canonical startup root.
    fn new(
        config: Arc<HarnessConfig>,
        root: PathBuf,
        watch_path: Option<PathBuf>,
        shutdown: Arc<ShutdownController>,
    ) -> Self {
        let registry = AcpSessionRegistry::new(
            root.clone(),
            config.acp.max_sessions,
            HistoryLimits {
                max_messages: config.acp.max_history_messages,
                max_bytes: config.acp.max_history_bytes,
            },
            shutdown.child_token(),
        );
        Self {
            config,
            root,
            watch_path,
            shutdown,
            registry,
            prompts: Arc::new(PromptTracker::new()),
            client_notices: AtomicBool::new(false),
            transport_failure: CancellationToken::new(),
            runtime: OnceCell::new(),
        }
    }

    /// Returns whether this service no longer accepts new sessions or prompts.
    fn is_closing(&self) -> bool {
        self.prompts.is_closing()
            || self.shutdown.is_shutting_down()
            || self.transport_failure.is_cancelled()
    }

    /// Stops accepting work and cancels every session lifetime token.
    ///
    /// This is idempotent and synchronous so it can be called from request
    /// handlers and the transport writer without awaiting.
    fn begin_shutdown(&self) {
        self.prompts.close();
        self.registry.cancel_all();
    }

    /// Records a broken transport, cancels work, and wakes the transport loop.
    fn fail_transport(&self) {
        self.begin_shutdown();
        self.transport_failure.cancel();
    }

    /// Cancels the active prompt of one session, ignoring unknown sessions.
    ///
    /// Returns whether an active prompt was cancelled. A notification for an
    /// unknown or idle session is a silent no-op, as ACP requires.
    fn cancel(&self, session_id: &AcpSessionId) -> bool {
        let Some(session) = self.registry.get(&session_id.to_string()) else {
            return false;
        };
        let cancelled = session.cancel_prompt();
        tracing::debug!(
            session_id = session.id(),
            cancelled,
            "acp session cancel requested"
        );
        cancelled
    }

    /// Waits until every prompt worker has released its activity guard.
    ///
    /// Returns `false` if the bound expires. A drain timeout only logs at the
    /// call site; it never extends the process lifetime without bound.
    async fn wait_for_prompts(&self, limit: Duration) -> bool {
        let mut active = self.prompts.changed.subscribe();
        if *active.borrow_and_update() == 0 {
            return true;
        }
        timeout(limit, async {
            loop {
                if *active.borrow_and_update() == 0 {
                    break;
                }
                if active.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .is_ok()
    }

    /// Drains already assembled skills, if a runtime exists.
    ///
    /// A runtime that was never assembled has no skill worker to drain. This
    /// avoids starting provider or memory initialization during shutdown.
    async fn drain_skills(&self) {
        if let Some(Ok(assembly)) = self.runtime.get() {
            assembly.runtime.shutdown().await;
        }
    }

    /// Implements `initialize`.
    fn initialize(
        &self,
        request: &InitializeRequest,
        responder: agent_client_protocol::v1::Responder<InitializeResponse>,
    ) -> Result<(), Error> {
        if self.is_closing() {
            return responder.respond_with_error(shutting_down_error());
        }
        self.client_notices
            .store(client_supports_notices(request), Ordering::SeqCst);
        responder.respond(initialize_response(request))
    }

    /// Returns whether the client advertised `session.notices`.
    fn client_supports_notices(&self) -> bool {
        self.client_notices.load(Ordering::SeqCst)
    }

    /// Returns the shared runtime, assembling it exactly once.
    async fn runtime(&self) -> Result<Arc<CliRuntime>, ()> {
        let config = Arc::clone(&self.config);
        let root = self.root.clone();
        let watch_path = self.watch_path.clone();
        let shutdown = Arc::clone(&self.shutdown);
        match self
            .runtime
            .get_or_init(|| async move {
                match crate::assemble_runtime(config, root, shutdown).await {
                    Ok((runtime, config_sender)) => {
                        let watcher = match watch_path {
                            Some(path) => Some(ConfigWatcher::spawn(path, config_sender).await),
                            None => {
                                // No file to watch: dropping the sender leaves
                                // the startup snapshot in the receiver.
                                drop(config_sender);
                                None
                            }
                        };
                        Ok(RuntimeAssembly {
                            runtime: Arc::new(runtime),
                            _config_watcher: watcher,
                        })
                    }
                    Err(_) => {
                        tracing::warn!("acp runtime assembly failed");
                        Err(())
                    }
                }
            })
            .await
        {
            Ok(assembly) => Ok(Arc::clone(&assembly.runtime)),
            Err(()) => Err(()),
        }
    }

    /// Implements `session/new`.
    async fn new_session(&self, request: NewSessionRequest) -> Result<NewSessionResponse, Error> {
        if self.is_closing() {
            return Err(shutting_down_error());
        }
        if !request.mcp_servers.is_empty() {
            return Err(invalid_params(
                "mcpServers are not supported in this ACP version",
            ));
        }
        if !request.additional_directories.is_empty() {
            return Err(invalid_params(
                "additionalDirectories are not supported in this ACP version",
            ));
        }
        if request.system_prompt.is_some() {
            return Err(invalid_params(
                "systemPrompt is not supported in this ACP version",
            ));
        }

        let session = self
            .registry
            .create(request.cwd)
            .await
            .map_err(session_error)?;
        tracing::debug!(session_id = session.id(), "acp session created");
        Ok(NewSessionResponse::new(session.id().to_owned()))
    }

    /// Validates a prompt and acquires the session's single prompt lease.
    fn prepare_prompt(&self, request: &PromptRequest) -> Result<PreparedPrompt, Error> {
        if self.is_closing() {
            return Err(shutting_down_error());
        }
        let session = self
            .registry
            .get(&request.session_id.to_string())
            .ok_or_else(|| session_not_found(&request.session_id))?;
        let user_input = prompt_text(&request.prompt, self.config.acp.max_prompt_bytes)
            .map_err(|error| invalid_params(error.to_string()))?;
        let activity = self.prompts.begin().ok_or_else(shutting_down_error)?;
        let (history, guard) = match session.begin_prompt() {
            Ok(started) => started,
            Err(_) => {
                return Err(invalid_params("session busy").data(json!({"reason": "session_busy"})));
            }
        };
        let turn_id = session.next_turn_id();
        Ok(PreparedPrompt {
            session,
            history,
            user_input,
            turn_id,
            guard,
            activity,
        })
    }

    /// Injects an already assembled runtime for unit tests.
    #[cfg(test)]
    fn with_runtime(
        config: Arc<HarnessConfig>,
        root: PathBuf,
        shutdown: Arc<ShutdownController>,
        runtime: Arc<CliRuntime>,
    ) -> Self {
        let service = Self::new(config, root, None, shutdown);
        let _ = service.runtime.set(Ok(RuntimeAssembly {
            runtime,
            _config_watcher: None,
        }));
        service
    }
}

/// Inputs captured while the session is marked busy.
struct PreparedPrompt {
    /// Session that owns the prompt lease.
    session: Arc<AcpSession>,
    /// Conversation snapshot taken before the new user input.
    history: Vec<Message>,
    /// Assembled user text.
    user_input: String,
    /// Internal session id for this turn.
    turn_id: SessionId,
    /// Lease released by commit or drop.
    guard: PromptGuard,
    /// Counted until the prompt worker sends its final response.
    activity: PromptActivity,
}

/// Counts active prompt workers and stops accepting new ones during shutdown.
struct PromptTracker {
    /// Closing flag and active prompt count, changed together.
    state: Mutex<PromptTrackerState>,
    /// Observable active prompt count.
    changed: watch::Sender<usize>,
}

/// Mutable state protected by [`PromptTracker::state`].
struct PromptTrackerState {
    /// Whether shutdown has stopped new prompts.
    closing: bool,
    /// Number of prompt workers that have not finished.
    active: usize,
}

impl PromptTracker {
    /// Creates an open tracker with no active prompts.
    fn new() -> Self {
        let (changed, _receiver) = watch::channel(0);
        Self {
            state: Mutex::new(PromptTrackerState {
                closing: false,
                active: 0,
            }),
            changed,
        }
    }

    /// Starts one prompt unless shutdown has already begun.
    fn begin(self: &Arc<Self>) -> Option<PromptActivity> {
        let active = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.closing {
                return None;
            }
            state.active += 1;
            state.active
        };
        self.changed.send_replace(active);
        Some(PromptActivity {
            tracker: Arc::clone(self),
        })
    }

    /// Stops accepting prompts. Existing workers still finish and decrement.
    fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closing = true;
    }

    /// Returns whether prompt acceptance has stopped.
    fn is_closing(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closing
    }
}

/// One active prompt worker's activity count.
struct PromptActivity {
    /// Tracker decremented when this guard drops.
    tracker: Arc<PromptTracker>,
}

impl Drop for PromptActivity {
    fn drop(&mut self) {
        let active = {
            let mut state = self
                .tracker
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.active = state.active.saturating_sub(1);
            state.active
        };
        self.tracker.changed.send_replace(active);
    }
}

/// Handles one `session/prompt` without blocking the dispatch loop.
async fn handle_prompt(
    service: Arc<AcpService>,
    request: PromptRequest,
    responder: agent_client_protocol::v1::Responder<PromptResponse>,
    connection: ConnectionTo<Client>,
) -> Result<(), Error> {
    let prepared = match service.prepare_prompt(&request) {
        Ok(prepared) => prepared,
        Err(error) => return responder.respond_with_error(error),
    };

    let (updates, receiver) = mpsc::unbounded_channel();
    let writer_connection = connection.clone();
    let writer_service = Arc::clone(&service);
    if connection
        .spawn(async move { run_update_writer(writer_connection, receiver, writer_service).await })
        .is_err()
    {
        // Dropping `prepared` releases the busy state and activity count.
        return responder.respond_with_error(internal_prompt_error());
    }

    let worker = async move { run_prompt_worker(service, prepared, responder, updates).await };
    match connection.spawn(worker) {
        Ok(()) => Ok(()),
        // The responder's drop guard sends the automatic internal error.
        Err(_) => Ok(()),
    }
}

/// Runs one prompt turn, its update writer barrier, and its response.
async fn run_prompt_worker(
    service: Arc<AcpService>,
    prepared: PreparedPrompt,
    responder: agent_client_protocol::v1::Responder<PromptResponse>,
    updates: mpsc::UnboundedSender<WriterMessage>,
) -> Result<(), Error> {
    let PreparedPrompt {
        session,
        history,
        user_input,
        turn_id,
        mut guard,
        activity: _activity,
    } = prepared;
    let session_id = session.id().to_owned();
    let cwd = session.cwd().to_path_buf();
    drop(session);

    let observations = Arc::new(PromptObservations::default());
    let in_flight = Arc::new(InFlightTools::default());
    let mapper = AcpEventMapper::new(cwd.clone(), MessageId::new(turn_id.to_string()));
    let sink: Arc<dyn UpdateSink> = Arc::new(AcpUpdateSink {
        session_id: AcpSessionId::new(session_id.clone()),
        mapper,
        observations: Arc::clone(&observations),
        in_flight: Arc::clone(&in_flight),
        updates: updates.clone(),
    });
    let execution = execute_prompt(
        &service,
        PromptExecutionRequest {
            cwd,
            history,
            user_input,
            turn_id,
            token: guard.token().clone(),
            sink,
            observations: Arc::clone(&observations),
        },
    )
    .await;

    // A cancelled or failed turn can stop between a tool start and its result.
    // Close every remainder before the barrier so no client sees a tool stuck
    // in progress and the failed updates precede the prompt response.
    for call in in_flight.take_pending() {
        let notification = SessionNotification::new(
            AcpSessionId::new(session_id.clone()),
            interrupted_tool_update(&call),
        );
        let _ = updates.send(WriterMessage::Update(Box::new(notification)));
    }

    let PromptExecution { terminal, commit } = execution;
    // A notice is optional and only legal when the client advertised
    // `session.notices`. The stop reason already carries the budget outcome,
    // so dropping the notice for other clients loses no protocol information.
    if !guard.cancel_requested()
        && service.client_supports_notices()
        && let Some(update) = budget_notice(terminal)
    {
        let notification = SessionNotification::new(AcpSessionId::new(session_id.clone()), update);
        let _ = updates.send(WriterMessage::Update(Box::new(notification)));
    }

    // Every update was enqueued before `execute_prompt` returned. The barrier
    // is ordered behind them, so a successful acknowledgement proves that the
    // writer attempted every update before the response is sent.
    let updates_delivered = update_barrier(&updates).await;

    // The cancel handler takes this gate before setting the flag. Holding it
    // until the response is enqueued makes the cancellation and response a
    // single ordered decision: a cancel that wins the gate forces `cancelled`
    // even if the underlying turn already completed or failed.
    let response_gate = guard.response_gate();
    let gate = response_gate.lock().unwrap_or_else(PoisonError::into_inner);
    let terminal = if guard.cancel_requested() {
        PromptTerminal::Stop(StopReason::Cancelled)
    } else {
        terminal
    };

    if updates_delivered
        && terminal != PromptTerminal::Error
        && let Some(messages) = commit
    {
        guard.commit(messages);
    }

    let response = if !updates_delivered {
        responder.respond_with_error(internal_prompt_error())
    } else {
        match terminal {
            PromptTerminal::Stop(reason) => responder.respond(PromptResponse::new(reason)),
            PromptTerminal::Error => responder.respond_with_error(internal_prompt_error()),
        }
    };
    if response.is_err() {
        // A response that cannot be written means the client is gone even if
        // no session update exposed the broken pipe first.
        service.fail_transport();
    }
    // Release only after the response is enqueued so a following prompt from
    // the client is not rejected as busy. The activity guard drops after this.
    guard.release();
    drop(gate);
    response
}

/// Inputs captured for one prompt execution.
struct PromptExecutionRequest {
    /// Canonical session working directory.
    cwd: PathBuf,
    /// History snapshot taken before the user message.
    history: Vec<Message>,
    /// Assembled user text.
    user_input: String,
    /// Internal session id for this turn.
    turn_id: SessionId,
    /// Prompt cancellation token (owned by the session guard).
    token: CancellationToken,
    /// Event sink for protocol updates.
    sink: Arc<dyn UpdateSink>,
    /// Budget observations shared with the sink.
    observations: Arc<PromptObservations>,
}

/// Runs the agent loop and maps its terminal state.
async fn execute_prompt(service: &AcpService, request: PromptExecutionRequest) -> PromptExecution {
    let PromptExecutionRequest {
        cwd,
        history,
        user_input,
        turn_id,
        token,
        sink,
        observations,
    } = request;
    let runtime = match service.runtime().await {
        Ok(runtime) => runtime,
        Err(()) => return PromptExecution::failure(),
    };
    let scope = match TurnScope::new(cwd, token.clone()).await {
        Ok(scope) => scope,
        Err(_) => return PromptExecution::failure(),
    };
    let outcome = match runtime
        .run_turn_request(TurnRequest {
            scope,
            history,
            user_input,
            update_sink: Some(sink),
            // ACP-2 deliberately runs at most one acceptance attempt.
            acceptance_retries: Some(0),
            session_id: Some(turn_id),
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(_) => return PromptExecution::failure(),
    };

    if outcome.lagged {
        // A protocol client must never receive a silently truncated
        // transcript. Cancel the prompt token and fail without committing.
        token.cancel();
        return PromptExecution::failure();
    }
    let terminal = match stop_reason(outcome.reason, observations.budget_kind()) {
        Some(stop_reason) => PromptTerminal::Stop(stop_reason),
        None => PromptTerminal::Error,
    };
    let commit = matches!(terminal, PromptTerminal::Stop(_)).then(|| turn_new_messages(&outcome));
    PromptExecution { terminal, commit }
}

/// Returns whether the initialize request advertises live session notices.
///
/// The ACP contract requires `{}` under `clientCapabilities.session.notices`;
/// omitted or `null` means unsupported.
fn client_supports_notices(request: &InitializeRequest) -> bool {
    request
        .client_capabilities
        .session
        .as_ref()
        .and_then(|session| session.notices.as_ref())
        .is_some()
}

/// Builds the optional, content-free notice for a budget stop.
///
/// Policy denials and ordinary failures are already carried by
/// `tool_call_update` and JSON-RPC errors, so only budget exhaustion uses a
/// notice. The title is non-empty as the schema requires and neither field
/// contains prompt text, tool arguments, environment values, or credentials.
fn budget_notice(terminal: PromptTerminal) -> Option<SessionUpdate> {
    match terminal {
        PromptTerminal::Stop(StopReason::MaxTokens) => Some(SessionUpdate::Notice(
            Notice::new(NoticeSeverity::Warning, "Token budget exhausted").description(
                "The prompt stopped at the configured token budget. Start a new session or raise the budget to continue.",
            ),
        )),
        PromptTerminal::Stop(StopReason::MaxTurnRequests) => Some(SessionUpdate::Notice(
            Notice::new(
                NoticeSeverity::Warning,
                "Turn request budget exhausted",
            )
            .description(
                "The prompt stopped at the configured model request limit. Start a new session or raise the budget to continue.",
            ),
        )),
        PromptTerminal::Stop(_) | PromptTerminal::Error => None,
    }
}

/// Maps the outcome reason with the observed budget kind.
fn stop_reason(reason: SessionEndReason, budget: Option<BudgetKind>) -> Option<StopReason> {
    match reason {
        SessionEndReason::Completed => Some(StopReason::EndTurn),
        SessionEndReason::Cancelled => Some(StopReason::Cancelled),
        SessionEndReason::BudgetExceeded => Some(match budget_stop_reason(budget) {
            BudgetStopReason::MaxTokens => StopReason::MaxTokens,
            BudgetStopReason::MaxTurnRequests => StopReason::MaxTurnRequests,
        }),
        SessionEndReason::AcceptanceFailed | SessionEndReason::Error => None,
    }
}

/// Extracts the user message and everything generated after it.
fn turn_new_messages(outcome: &TurnOutcome) -> Vec<Message> {
    let start = outcome
        .turn_start
        .saturating_sub(1)
        .min(outcome.messages.len());
    outcome.messages[start..].to_vec()
}

/// Terminal classification of one prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptTerminal {
    /// Respond with this stop reason.
    Stop(StopReason),
    /// Respond with an internal JSON-RPC error.
    Error,
}

/// Execution result before the update barrier and history commit.
struct PromptExecution {
    /// Protocol outcome of the turn.
    terminal: PromptTerminal,
    /// Messages to append when the update barrier succeeds.
    commit: Option<Vec<Message>>,
}

impl PromptExecution {
    /// A failed execution that changes neither history nor protocol state.
    fn failure() -> Self {
        Self {
            terminal: PromptTerminal::Error,
            commit: None,
        }
    }
}

/// Budget observations made while mapping the turn's events.
#[derive(Default)]
struct PromptObservations {
    /// Last budget kind observed, if any.
    budget: Mutex<Option<BudgetKind>>,
}

impl PromptObservations {
    /// Records an event relevant to the prompt's final stop reason.
    fn observe(&self, event: &AgentEvent) {
        if let AgentEvent::BudgetExceeded { kind, .. } = event {
            *self.lock() = Some(*kind);
        }
    }

    /// Returns the observed budget kind.
    fn budget_kind(&self) -> Option<BudgetKind> {
        *self.lock()
    }

    /// Locks the observations, recovering from poisoning.
    fn lock(&self) -> MutexGuard<'_, Option<BudgetKind>> {
        self.budget.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Update sink that maps events and enqueues protocol notifications.
struct AcpUpdateSink {
    /// ACP session id carried by every notification.
    session_id: AcpSessionId,
    /// Per-prompt event mapper.
    mapper: AcpEventMapper,
    /// Shared budget observations.
    observations: Arc<PromptObservations>,
    /// Tool calls announced but not yet finished.
    in_flight: Arc<InFlightTools>,
    /// FIFO writer queue.
    updates: mpsc::UnboundedSender<WriterMessage>,
}

impl UpdateSink for AcpUpdateSink {
    fn publish(&self, event: &AgentEvent) {
        self.observations.observe(event);
        self.in_flight.observe(event);
        let Some(update) = self.mapper.map(event) else {
            return;
        };
        let notification = SessionNotification::new(self.session_id.clone(), update);
        let _ = self
            .updates
            .send(WriterMessage::Update(Box::new(notification)));
    }
}

/// Messages consumed by the dedicated protocol write task.
enum WriterMessage {
    /// One session update notification.
    Update(Box<SessionNotification>),
    /// Flush boundary; the bool reports whether every prior update was sent.
    Barrier(oneshot::Sender<bool>),
}

/// Writes queued session updates in order and acknowledges the barrier.
async fn run_update_writer(
    connection: ConnectionTo<Client>,
    mut receiver: mpsc::UnboundedReceiver<WriterMessage>,
    service: Arc<AcpService>,
) -> Result<(), Error> {
    let mut failed = false;
    while let Some(message) = receiver.recv().await {
        match message {
            WriterMessage::Update(notification) => {
                if !failed && connection.send_notification(*notification).is_err() {
                    failed = true;
                    // The client is gone. Stop every session instead of
                    // continuing tools whose output can never be delivered.
                    service.fail_transport();
                }
            }
            WriterMessage::Barrier(acknowledge) => {
                let _ = acknowledge.send(!failed);
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Waits until the writer has attempted every previously queued update.
async fn update_barrier(updates: &mpsc::UnboundedSender<WriterMessage>) -> bool {
    let (acknowledge, acknowledged) = oneshot::channel();
    if updates.send(WriterMessage::Barrier(acknowledge)).is_err() {
        return false;
    }
    matches!(acknowledged.await, Ok(true))
}

/// Builds the `initialize` response for one client request.
///
/// Jessica supports only stable ACP v1. A client that requests a different
/// version receives v1, as the v1 schema requires when the requested version
/// is unsupported; the client decides whether to disconnect. All optional
/// capabilities are explicitly disabled so clients never probe unsupported
/// content types, MCP transports, session loading, or session lifecycle
/// methods. Baseline text and resource-link prompts are always supported.
fn initialize_response(request: &InitializeRequest) -> InitializeResponse {
    tracing::debug!(
        requested_protocol_version = request.protocol_version.as_u16(),
        supported_protocol_version = ProtocolVersion::V1.as_u16(),
        "acp initialize"
    );

    InitializeResponse::new(ProtocolVersion::V1)
        .agent_capabilities(
            AgentCapabilities::new()
                .load_session(false)
                .prompt_capabilities(
                    PromptCapabilities::new()
                        .image(false)
                        .audio(false)
                        .embedded_context(false),
                )
                .mcp_capabilities(McpCapabilities::new().http(false).sse(false))
                .session_capabilities(SessionCapabilities::new()),
        )
        .auth_methods(Vec::new())
        .agent_info(Implementation::new("jessica", env!("CARGO_PKG_VERSION")))
}

/// Builds an invalid-params error with a stable, content-free message.
fn invalid_params(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::INVALID_PARAMS, message)
}

/// Maps session creation failures to JSON-RPC errors.
fn session_error(error: SessionError) -> Error {
    match &error {
        SessionError::LimitReached { max_sessions } => invalid_params("session limit reached")
            .data(json!({
                "reason": "session_limit_reached",
                "maxSessions": max_sessions,
            })),
        _ => invalid_params(error.to_string()),
    }
}

/// Builds the protocol-specific resource-not-found error.
fn session_not_found(session_id: &AcpSessionId) -> Error {
    agent_client_protocol::v1::schema::Error::session_not_found(session_id.clone()).into()
}

/// Generic internal error that cannot leak provider or prompt details.
fn internal_prompt_error() -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, "prompt failed")
}

/// Error returned once shutdown has stopped accepting new work.
fn shutting_down_error() -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, "agent is shutting down")
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::path::Path;

    use agent_client_protocol::v1::schema::{ContentBlock, TextContent};
    use async_trait::async_trait;
    use futures::StreamExt;
    use jessica_core::llm::{LlmError, LlmEvent, LlmProvider, LlmStream};
    use jessica_core::tool::ToolSchema;
    use jessica_core::types::{TokenUsage, ToolCall};
    use tokio::sync::Notify;
    use tokio::sync::watch;

    /// One provider call observed by the scripted provider.
    #[derive(Clone)]
    struct ProviderCall {
        /// Messages sent by the agent loop.
        messages: Vec<Message>,
    }

    /// Provider that replays scripted event batches and records calls.
    struct ScriptedProvider {
        /// Responses in call order.
        responses: Mutex<VecDeque<Vec<LlmEvent>>>,
        /// Calls observed so far.
        calls: Mutex<Vec<ProviderCall>>,
    }

    impl ScriptedProvider {
        /// Creates a provider from sequential responses.
        fn new(responses: Vec<Vec<LlmEvent>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        /// Returns a snapshot of observed calls.
        fn calls(&self) -> Vec<ProviderCall> {
            self.calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    #[async_trait]
    impl LlmProvider for ScriptedProvider {
        async fn stream_chat(
            &self,
            messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            self.calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(ProviderCall {
                    messages: messages.to_vec(),
                });
            let events = self
                .responses
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pop_front()
                .ok_or_else(|| LlmError::Request {
                    message: "script exhausted".to_owned(),
                })?;
            Ok(futures::stream::iter(events).boxed())
        }
    }

    /// Provider whose stream never yields.
    struct PendingProvider {
        /// Notified when the provider is entered.
        started: Arc<Notify>,
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

    /// No-op update sink for execution-focused tests.
    struct NullSink;

    impl UpdateSink for NullSink {
        fn publish(&self, _event: &AgentEvent) {}
    }

    /// Builds one successful text response.
    fn done_text(text: &str) -> Vec<LlmEvent> {
        vec![
            LlmEvent::TextDelta {
                text: text.to_owned(),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(1, 1)),
            },
        ]
    }

    /// Builds one model turn that requests a tool call.
    fn tool_call_response(id: &str, name: &str, arguments: serde_json::Value) -> Vec<LlmEvent> {
        vec![
            LlmEvent::ToolCallRequested {
                call: ToolCall::new(id, name, arguments),
            },
            LlmEvent::Done {
                usage: Some(TokenUsage::new(1, 1)),
            },
        ]
    }

    /// Creates a unique temporary workspace.
    fn temp_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "jessica-acp-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock must follow the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("temporary root must be creatable");
        std::fs::canonicalize(root).expect("temporary root must resolve")
    }

    /// Builds a runtime around a scripted provider and one root.
    fn runtime_with(
        provider: Arc<ScriptedProvider>,
        config: Arc<HarnessConfig>,
        root: &Path,
        shutdown: Arc<ShutdownController>,
    ) -> Arc<CliRuntime> {
        let (_sender, receiver) = watch::channel(config);
        Arc::new(CliRuntime::new(
            provider,
            receiver,
            root.to_path_buf(),
            Vec::new(),
            None,
            None,
            None,
            shutdown,
        ))
    }

    /// Creates a service whose lazy runtime is already assembled.
    fn service_with(
        provider: Arc<ScriptedProvider>,
        root: &Path,
        config: HarnessConfig,
    ) -> (Arc<AcpService>, Arc<HarnessConfig>) {
        let config = Arc::new(config);
        let shutdown = Arc::new(ShutdownController::new());
        let runtime = runtime_with(provider, Arc::clone(&config), root, Arc::clone(&shutdown));
        (
            Arc::new(AcpService::with_runtime(
                Arc::clone(&config),
                root.to_path_buf(),
                shutdown,
                runtime,
            )),
            config,
        )
    }

    /// Creates a session through the same validation path as `session/new`.
    async fn create_session(service: &AcpService, cwd: PathBuf) -> Arc<AcpSession> {
        service
            .registry
            .create(cwd)
            .await
            .expect("session must be created")
    }

    /// Executes one prompt directly and commits it on success.
    async fn run_turn_for_test(
        service: &AcpService,
        session: &Arc<AcpSession>,
        input: &str,
    ) -> PromptTerminal {
        let (history, mut guard) = session.begin_prompt().expect("prompt must start");
        let execution = execute_prompt(
            service,
            PromptExecutionRequest {
                cwd: session.cwd().to_path_buf(),
                history,
                user_input: input.to_owned(),
                turn_id: session.next_turn_id(),
                token: guard.token().clone(),
                sink: Arc::new(NullSink),
                observations: Arc::new(PromptObservations::default()),
            },
        )
        .await;
        if let Some(messages) = execution.commit {
            guard.commit(messages);
        } else {
            drop(guard);
        }
        execution.terminal
    }

    #[tokio::test]
    async fn a_second_prompt_receives_the_first_turn_in_bootstrap_history_order() {
        let root = temp_root("history");
        let provider = Arc::new(ScriptedProvider::new(vec![
            done_text("first answer"),
            done_text("second answer"),
        ]));
        let (service, _config) =
            service_with(Arc::clone(&provider), &root, HarnessConfig::default());
        let session = create_session(&service, root).await;

        assert_eq!(
            run_turn_for_test(&service, &session, "first question").await,
            PromptTerminal::Stop(StopReason::EndTurn)
        );
        assert_eq!(
            run_turn_for_test(&service, &session, "second question").await,
            PromptTerminal::Stop(StopReason::EndTurn)
        );

        let calls = provider.calls();
        assert_eq!(calls.len(), 2, "each prompt must make one provider call");
        let second = &calls[1].messages;
        let contents: Vec<&str> = second
            .iter()
            .map(|message| message.content.as_str())
            .collect();
        assert!(
            second[0].content.contains("You are Jessica"),
            "the first message must be the Jessica system prompt"
        );
        assert_eq!(second[1].content, "first question");
        assert_eq!(second[2].content, "first answer");
        assert_eq!(
            second.last().map(|message| message.content.as_str()),
            Some("second question")
        );
        assert_eq!(
            &contents[contents.len() - 2..],
            ["first answer", "second question"]
        );
    }

    #[tokio::test]
    async fn trimmed_history_keeps_tool_pairs_while_dropping_the_oldest_turn() {
        let root = temp_root("history-trim-provider");
        std::fs::write(root.join("NOTE.txt"), "note contents")
            .expect("fixture note must be written");
        let config = HarnessConfig::from_toml_str("[acp]\nmax_history_messages = 4\n")
            .expect("history limit config must parse");
        let provider = Arc::new(ScriptedProvider::new(vec![
            done_text("first answer"),
            tool_call_response("call_read", "read_file", json!({"path": "NOTE.txt"})),
            done_text("tool answer"),
            done_text("third answer"),
        ]));
        let (service, _config) = service_with(Arc::clone(&provider), &root, config);
        let session = create_session(&service, root).await;

        assert_eq!(
            run_turn_for_test(&service, &session, "first question").await,
            PromptTerminal::Stop(StopReason::EndTurn)
        );
        assert_eq!(
            run_turn_for_test(&service, &session, "use the tool").await,
            PromptTerminal::Stop(StopReason::EndTurn)
        );
        assert_eq!(
            run_turn_for_test(&service, &session, "third question").await,
            PromptTerminal::Stop(StopReason::EndTurn)
        );

        let calls = provider.calls();
        assert_eq!(calls.len(), 4, "the tool turn makes two provider calls");
        let third_prompt = &calls[3].messages;
        let contents: Vec<&str> = third_prompt
            .iter()
            .map(|message| message.content.as_str())
            .collect();
        assert!(
            !contents.contains(&"first question") && !contents.contains(&"first answer"),
            "the oldest complete turn must be gone: {contents:?}"
        );

        let start = third_prompt
            .iter()
            .position(|message| message.content == "use the tool")
            .expect("the retained turn must start at its user message");
        assert_eq!(start + 4, third_prompt.len() - 1);
        assert_eq!(
            third_prompt[start + 1].role,
            jessica_core::types::Role::Assistant
        );
        assert_eq!(third_prompt[start + 1].tool_calls.len(), 1);
        assert_eq!(
            third_prompt[start + 2].role,
            jessica_core::types::Role::Tool
        );
        assert_eq!(
            third_prompt[start + 2].tool_call_id.as_deref(),
            Some(third_prompt[start + 1].tool_calls[0].id.as_str()),
            "the tool result must immediately follow its call and use the same id"
        );
        assert_eq!(third_prompt[start + 3].content, "tool answer");
        assert_eq!(third_prompt[start + 4].content, "third question");
    }

    #[tokio::test]
    async fn prompt_content_errors_do_not_call_the_provider() {
        let root = temp_root("invalid-content");
        let provider = Arc::new(ScriptedProvider::new(vec![done_text("unused")]));
        let (service, _config) =
            service_with(Arc::clone(&provider), &root, HarnessConfig::default());
        let session = create_session(&service, root).await;
        let request = PromptRequest::new(
            AcpSessionId::new(session.id().to_owned()),
            vec![ContentBlock::Image(
                agent_client_protocol::v1::schema::ImageContent::new("data", "image/png"),
            )],
        );

        let error = match service.prepare_prompt(&request) {
            Ok(_) => panic!("image prompt must be rejected"),
            Err(error) => error,
        };

        assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
        assert!(provider.calls().is_empty());
        assert_eq!(session.history_len(), 0);
    }

    #[tokio::test]
    async fn busy_is_reported_without_waiting_for_the_active_prompt() {
        let root = temp_root("busy-service");
        let provider = Arc::new(ScriptedProvider::new(vec![]));
        let (service, _config) = service_with(provider, &root, HarnessConfig::default());
        let session = create_session(&service, root).await;
        let prompt = vec![ContentBlock::Text(TextContent::new("hello"))];
        let request =
            PromptRequest::new(AcpSessionId::new(session.id().to_owned()), prompt.clone());
        let _prepared = service.prepare_prompt(&request).expect("first prompt");

        let error = match service.prepare_prompt(&request) {
            Ok(_) => panic!("the second prompt must be busy"),
            Err(error) => error,
        };

        assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
        assert_eq!(error.message, "session busy");
        assert_eq!(error.data, Some(json!({"reason": "session_busy"})));
    }

    #[tokio::test]
    async fn cancel_marks_only_the_target_session_prompt() {
        let root = temp_root("cancel-scope");
        let provider = Arc::new(ScriptedProvider::new(vec![]));
        let (service, _config) = service_with(provider, &root, HarnessConfig::default());
        let first = create_session(&service, root.clone()).await;
        let second = create_session(&service, root).await;
        let request_for = |session: &Arc<AcpSession>| {
            PromptRequest::new(
                AcpSessionId::new(session.id().to_owned()),
                vec![ContentBlock::Text(TextContent::new("hello"))],
            )
        };
        let first_prompt = service
            .prepare_prompt(&request_for(&first))
            .expect("first session prompt");
        let second_prompt = service
            .prepare_prompt(&request_for(&second))
            .expect("second session prompt");

        service.cancel(&AcpSessionId::new(first.id().to_owned()));

        assert!(first_prompt.guard.cancel_requested());
        assert!(first_prompt.guard.token().is_cancelled());
        assert!(
            !second_prompt.guard.token().is_cancelled(),
            "cancelling one prompt must not cancel another session"
        );
        assert!(
            !second_prompt.guard.cancel_requested(),
            "only an explicit client cancel marks the flag"
        );
        assert!(
            !service.cancel(&AcpSessionId::new("acp-missing")),
            "an unknown session cancel is ignored"
        );
    }

    #[tokio::test]
    async fn shutdown_rejects_new_work_and_waits_for_active_prompts() {
        let root = temp_root("drain");
        let provider = Arc::new(ScriptedProvider::new(vec![]));
        let (service, _config) = service_with(provider, &root, HarnessConfig::default());
        let session = create_session(&service, root.clone()).await;
        let request = PromptRequest::new(
            AcpSessionId::new(session.id().to_owned()),
            vec![ContentBlock::Text(TextContent::new("active"))],
        );
        let active = service.prepare_prompt(&request).expect("active prompt");

        service.begin_shutdown();

        assert!(service.is_closing());
        assert!(
            active.guard.token().is_cancelled(),
            "shutdown must cancel the session lifetime token"
        );
        assert!(
            !service.wait_for_prompts(Duration::from_millis(10)).await,
            "the active prompt must still be counted"
        );
        let error = service
            .prepare_prompt(&request)
            .err()
            .expect("shutdown must reject a new prompt");
        assert_eq!(error.code, ErrorCode::INTERNAL_ERROR);
        assert_eq!(error.message, "agent is shutting down");
        let new_session_error = service
            .new_session(NewSessionRequest::new(root))
            .await
            .expect_err("shutdown must reject session creation");
        assert_eq!(new_session_error.code, ErrorCode::INTERNAL_ERROR);

        drop(active);
        assert!(
            service.wait_for_prompts(Duration::from_secs(1)).await,
            "dropping the final prompt must release the drain wait"
        );
    }

    #[tokio::test]
    async fn a_transport_failure_cancels_all_sessions_and_wakes_the_run_loop() {
        let root = temp_root("transport-failure");
        let provider = Arc::new(ScriptedProvider::new(vec![]));
        let (service, _config) = service_with(provider, &root, HarnessConfig::default());
        let first = create_session(&service, root.clone()).await;
        let second = create_session(&service, root).await;
        let request = |session: &Arc<AcpSession>| {
            PromptRequest::new(
                AcpSessionId::new(session.id().to_owned()),
                vec![ContentBlock::Text(TextContent::new("active"))],
            )
        };
        let first_prompt = service
            .prepare_prompt(&request(&first))
            .expect("first prompt");
        let second_prompt = service
            .prepare_prompt(&request(&second))
            .expect("second prompt");

        service.fail_transport();

        assert!(service.transport_failure.is_cancelled());
        assert!(service.is_closing());
        assert!(first_prompt.guard.token().is_cancelled());
        assert!(second_prompt.guard.token().is_cancelled());
    }

    #[tokio::test]
    async fn session_new_rejects_mcp_additional_roots_and_system_prompt() {
        let root = temp_root("new-session");
        let provider = Arc::new(ScriptedProvider::new(vec![]));
        let (service, _config) = service_with(provider, &root, HarnessConfig::default());

        let mut with_mcp = NewSessionRequest::new(root.clone());
        with_mcp.mcp_servers = vec![agent_client_protocol::v1::schema::McpServer::Stdio(
            agent_client_protocol::v1::schema::McpServerStdio::new(
                "server",
                PathBuf::from("/bin/server"),
            ),
        )];
        let mut with_directory = NewSessionRequest::new(root.clone());
        with_directory.additional_directories = vec![root.clone()];
        let mut with_prompt = NewSessionRequest::new(root);
        with_prompt.system_prompt =
            Some(agent_client_protocol::v1::schema::SystemPrompt::new(vec![
                ContentBlock::Text(TextContent::new("custom")),
            ]));

        for request in [with_mcp, with_directory, with_prompt] {
            let error = service
                .new_session(request)
                .await
                .expect_err("unsupported session field must be rejected");
            assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
        }
        assert_eq!(service.registry.len(), 0);
    }

    #[tokio::test]
    async fn provider_errors_return_internal_error_and_leave_history_unchanged() {
        let root = temp_root("provider-error");
        let provider = Arc::new(ScriptedProvider::new(vec![]));
        let (service, _config) = service_with(provider, &root, HarnessConfig::default());
        let session = create_session(&service, root).await;

        let terminal = run_turn_for_test(&service, &session, "will fail").await;

        assert_eq!(terminal, PromptTerminal::Error);
        assert_eq!(session.history_len(), 0);
    }

    #[tokio::test]
    async fn a_cancelled_prompt_retains_partial_output_and_closes_tool_calls() {
        let root = temp_root("cancel");
        let started = Arc::new(Notify::new());
        let provider: Arc<dyn LlmProvider> = Arc::new(PendingProvider {
            started: Arc::clone(&started),
        });
        let config = Arc::new(HarnessConfig::default());
        let shutdown = Arc::new(ShutdownController::new());
        let (_sender, receiver) = watch::channel(Arc::clone(&config));
        let runtime = Arc::new(CliRuntime::new(
            provider,
            receiver,
            root.clone(),
            Vec::new(),
            None,
            None,
            None,
            Arc::clone(&shutdown),
        ));
        let service = Arc::new(AcpService::with_runtime(
            Arc::clone(&config),
            root.clone(),
            Arc::clone(&shutdown),
            runtime,
        ));
        let session = create_session(&service, root).await;

        let turn = run_turn_for_test(&service, &session, "cancel me");
        let canceller = async {
            started.notified().await;
            shutdown.request_shutdown(crate::shutdown::ShutdownSignal::SigTerm);
        };
        let (terminal, ()) = tokio::join!(turn, canceller);

        assert_eq!(terminal, PromptTerminal::Stop(StopReason::Cancelled));
        let history = session.history_snapshot();
        assert_eq!(history[0], Message::user("cancel me"));
        for (index, message) in history.iter().enumerate() {
            if message.role == jessica_core::types::Role::Assistant
                && !message.tool_calls.is_empty()
            {
                for call in &message.tool_calls {
                    assert!(
                        history[index + 1..]
                            .iter()
                            .any(|later| later.role == jessica_core::types::Role::Tool
                                && later.tool_call_id.as_deref() == Some(call.id.as_str())),
                        "cancelled history must close every tool call"
                    );
                }
            }
        }
    }

    #[test]
    fn stop_reasons_cover_completed_cancelled_and_budget_boundaries() {
        assert_eq!(
            stop_reason(SessionEndReason::Completed, None),
            Some(StopReason::EndTurn)
        );
        assert_eq!(
            stop_reason(SessionEndReason::Cancelled, None),
            Some(StopReason::Cancelled)
        );
        assert_eq!(
            stop_reason(
                SessionEndReason::BudgetExceeded,
                Some(BudgetKind::TokenLimit)
            ),
            Some(StopReason::MaxTokens)
        );
        for kind in [
            BudgetKind::MaxRounds,
            BudgetKind::ToolTimeout,
            BudgetKind::Stuck,
            BudgetKind::ResultSize,
        ] {
            assert_eq!(
                stop_reason(SessionEndReason::BudgetExceeded, Some(kind)),
                Some(StopReason::MaxTurnRequests)
            );
        }
        assert_eq!(stop_reason(SessionEndReason::Error, None), None);
        assert_eq!(stop_reason(SessionEndReason::AcceptanceFailed, None), None);
    }

    #[test]
    fn turn_messages_start_with_the_new_user_message() {
        let outcome = TurnOutcome {
            reason: SessionEndReason::Completed,
            messages: vec![
                Message::system("system"),
                Message::user("old"),
                Message::assistant("old answer"),
                Message::user("new"),
                Message::assistant("new answer"),
            ],
            // The generated messages begin after the new user message.
            turn_start: 4,
            lagged: false,
        };

        let messages = turn_new_messages(&outcome);

        assert_eq!(messages[0], Message::user("new"));
        assert_eq!(messages[1], Message::assistant("new answer"));
    }

    #[test]
    fn budget_stop_reason_helper_is_used_without_acceptance_errors() {
        // Keeps the shared helper's contract visible beside the stop mapping.
        assert_eq!(
            budget_stop_reason(Some(BudgetKind::TokenLimit)),
            BudgetStopReason::MaxTokens
        );
    }

    #[test]
    fn notice_capability_requires_an_explicit_session_notices_entry() {
        let mut request = InitializeRequest::new(ProtocolVersion::V1);
        assert!(
            !client_supports_notices(&request),
            "an empty clientCapabilities object means no notices"
        );

        request.client_capabilities = agent_client_protocol::v1::schema::ClientCapabilities::new()
            .session(agent_client_protocol::v1::schema::ClientSessionCapabilities::new());
        assert!(
            !client_supports_notices(&request),
            "session capabilities without notices are not support"
        );

        request.client_capabilities = agent_client_protocol::v1::schema::ClientCapabilities::new()
            .session(
                agent_client_protocol::v1::schema::ClientSessionCapabilities::new()
                    .notices(agent_client_protocol::v1::schema::NoticeCapabilities::new()),
            );
        assert!(
            client_supports_notices(&request),
            "an explicit empty notices object advertises support"
        );
    }

    #[test]
    fn budget_stops_build_schema_valid_content_free_notices() {
        for (stop, expected_title) in [
            (StopReason::MaxTokens, "Token budget exhausted"),
            (StopReason::MaxTurnRequests, "Turn request budget exhausted"),
        ] {
            let update = budget_notice(PromptTerminal::Stop(stop))
                .expect("a budget stop may be accompanied by a notice");
            let value = serde_json::to_value(update).expect("notice must serialize");
            assert_eq!(value["sessionUpdate"], "notice");
            assert_eq!(value["severity"], "warning");
            assert_eq!(value["title"], expected_title);
            assert!(
                value["description"]
                    .as_str()
                    .is_some_and(|description| !description.is_empty()),
                "the notice should explain how to continue"
            );
        }
        for stop in [
            StopReason::EndTurn,
            StopReason::Cancelled,
            StopReason::Refusal,
        ] {
            assert!(
                budget_notice(PromptTerminal::Stop(stop)).is_none(),
                "non-budget stops must not produce notices"
            );
        }
        assert!(budget_notice(PromptTerminal::Error).is_none());
    }

    #[test]
    fn initialize_advertises_v1_and_no_optional_capabilities() {
        let response = initialize_response(&InitializeRequest::new(ProtocolVersion::V1));

        assert_eq!(response.protocol_version, ProtocolVersion::V1);
        assert!(!response.agent_capabilities.load_session);
        assert!(!response.agent_capabilities.prompt_capabilities.image);
        assert!(!response.agent_capabilities.prompt_capabilities.audio);
        assert!(
            !response
                .agent_capabilities
                .prompt_capabilities
                .embedded_context
        );
        assert!(!response.agent_capabilities.mcp_capabilities.http);
        assert!(!response.agent_capabilities.mcp_capabilities.sse);
        assert!(response.auth_methods.is_empty());
        let info = response
            .agent_info
            .expect("initialize must identify the agent");
        assert_eq!(info.name, "jessica");
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn initialize_with_a_different_version_still_answers_with_v1() {
        for requested in [
            ProtocolVersion::V0,
            ProtocolVersion::from(2_u16),
            ProtocolVersion::from(999_u16),
        ] {
            let response = initialize_response(&InitializeRequest::new(requested));

            assert_eq!(
                response.protocol_version,
                ProtocolVersion::V1,
                "the agent must answer with its latest supported version, not the requested one"
            );
        }
    }
}
