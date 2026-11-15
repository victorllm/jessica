//! Stable ACP v1 agent transport over stdio.
//!
//! The server implements `initialize`, `session/new`, and `session/prompt`.
//! A prompt is prepared synchronously (session lookup, content validation,
//! busy check), then model and tool work runs in a task spawned on the
//! connection so the SDK dispatch loop keeps receiving messages. Cancellation,
//! session close, permissions, session load, and MCP arrive in later tasks.
//!
//! Stdout is reserved for newline-delimited JSON-RPC. This module never writes
//! to stdout itself, and the CLI installs no tracing subscriber, so ACP
//! diagnostics cannot corrupt the protocol stream.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    AgentCapabilities, Implementation, InitializeRequest, InitializeResponse, McpCapabilities,
    MessageId, NewSessionRequest, NewSessionResponse, PromptCapabilities, PromptRequest,
    PromptResponse, SessionCapabilities, SessionId as AcpSessionId, SessionNotification,
    StopReason,
};
use agent_client_protocol::{Agent, Client, Error, ErrorCode, Stdio};
use jessica_core::types::{AgentEvent, BudgetKind, Message, SessionEndReason, SessionId};
use serde_json::json;
use tokio::sync::{OnceCell, mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::acp_events::{AcpEventMapper, BudgetStopReason, budget_stop_reason};
use crate::acp_session::{
    AcpSession, AcpSessionRegistry, HistoryLimits, PromptGuard, SessionError, prompt_text,
};
use crate::app::{CliError, CliRuntime, TurnOutcome, TurnRequest, TurnScope};
use crate::config::HarnessConfig;
use crate::pump::UpdateSink;
use crate::shutdown::ShutdownController;

/// Maximum time allowed for the transport to flush after a shutdown signal.
///
/// ACP-2 has no cancellation or session-close semantics; ACP-3 owns draining
/// live prompts and revisits this bound. Until then a signal returns after at
/// most this grace period, leaving already queued responses time to flush.
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_millis(250);

/// Serves stable ACP v1 over stdin/stdout until EOF or shutdown.
///
/// A clean stdin EOF is a successful termination. On SIGINT or SIGTERM the
/// caller's [`ShutdownController`] is cancelled and the transport gets a short
/// grace period to flush. Session cancellation and draining are ACP-3 work.
pub(crate) async fn run(
    config: HarnessConfig,
    root: PathBuf,
    shutdown: Arc<ShutdownController>,
) -> Result<(), CliError> {
    let root = canonical_startup_root(root).await?;
    let service = Arc::new(AcpService::new(
        Arc::new(config),
        root,
        Arc::clone(&shutdown),
    ));

    let new_session_service = Arc::clone(&service);
    let prompt_service = Arc::clone(&service);
    let connection = Agent
        .v1()
        .name("jessica")
        .on_receive_request(
            async move |request: InitializeRequest, responder, _connection| {
                responder.respond(initialize_response(&request))
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
        .connect_to(Stdio::new());
    tokio::pin!(connection);

    tokio::select! {
        // A signal that is already recorded takes precedence when the client
        // closes stdin at the same time: the signal determines the exit code.
        biased;
        _ = shutdown.cancelled() => {
            match timeout(SHUTDOWN_GRACE_PERIOD, &mut connection).await {
                Ok(result) => result.map_err(CliError::Acp),
                Err(_) => Ok(()),
            }
        }
        result = &mut connection => result.map_err(CliError::Acp),
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
    /// Startup configuration, including ACP limits.
    config: Arc<HarnessConfig>,
    /// Canonical startup workspace root.
    root: PathBuf,
    /// Process-level shutdown token source.
    shutdown: Arc<ShutdownController>,
    /// Live sessions and their bounded histories.
    registry: AcpSessionRegistry,
    /// Provider, recorder, wasm, memory, and skills, assembled on first prompt.
    ///
    /// Assembly is lazy so `initialize` and `session/new` work even when a
    /// provider credential is absent. The first failure is cached and maps to
    /// a generic internal error; no provider detail is exposed.
    runtime: OnceCell<Result<Arc<CliRuntime>, ()>>,
}

impl AcpService {
    /// Creates the service around a canonical startup root.
    fn new(config: Arc<HarnessConfig>, root: PathBuf, shutdown: Arc<ShutdownController>) -> Self {
        let registry = AcpSessionRegistry::new(
            root.clone(),
            config.acp.max_sessions,
            HistoryLimits {
                max_messages: config.acp.max_history_messages,
                max_bytes: config.acp.max_history_bytes,
            },
        );
        Self {
            config,
            root,
            shutdown,
            registry,
            runtime: OnceCell::new(),
        }
    }

    /// Returns the shared runtime, assembling it exactly once.
    async fn runtime(&self) -> Result<Arc<CliRuntime>, ()> {
        let config = Arc::clone(&self.config);
        let root = self.root.clone();
        let shutdown = Arc::clone(&self.shutdown);
        match self
            .runtime
            .get_or_init(|| async move {
                match crate::assemble_runtime(config, root, shutdown).await {
                    Ok((runtime, _config_sender)) => Ok(Arc::new(runtime)),
                    Err(_) => {
                        tracing::warn!("acp runtime assembly failed");
                        Err(())
                    }
                }
            })
            .await
        {
            Ok(runtime) => Ok(Arc::clone(runtime)),
            Err(()) => Err(()),
        }
    }

    /// Implements `session/new`.
    async fn new_session(&self, request: NewSessionRequest) -> Result<NewSessionResponse, Error> {
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
        let session = self
            .registry
            .get(&request.session_id.to_string())
            .ok_or_else(|| session_not_found(&request.session_id))?;
        let user_input = prompt_text(&request.prompt, self.config.acp.max_prompt_bytes)
            .map_err(|error| invalid_params(error.to_string()))?;
        let token = self.shutdown.child_token();
        let (history, guard) = session
            .begin_prompt(token)
            .map_err(|_| invalid_params("session busy").data(json!({"reason": "session_busy"})))?;
        let turn_id = session.next_turn_id();
        Ok(PreparedPrompt {
            session,
            history,
            user_input,
            turn_id,
            guard,
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
        let service = Self::new(config, root, shutdown);
        let _ = service.runtime.set(Ok(runtime));
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
    if connection
        .spawn(async move { run_update_writer(writer_connection, receiver).await })
        .is_err()
    {
        // Dropping `prepared` releases the busy state.
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
        guard,
    } = prepared;
    let session_id = session.id().to_owned();
    let cwd = session.cwd().to_path_buf();
    drop(session);

    let observations = Arc::new(PromptObservations::default());
    let mapper = AcpEventMapper::new(cwd.clone(), MessageId::new(turn_id.to_string()));
    let sink: Arc<dyn UpdateSink> = Arc::new(AcpUpdateSink {
        session_id: AcpSessionId::new(session_id),
        mapper,
        observations: Arc::clone(&observations),
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

    // Every update was enqueued before `execute_prompt` returned. The barrier
    // is ordered behind them, so a successful acknowledgement proves that the
    // writer attempted every update before the response is sent.
    let updates_delivered = update_barrier(&updates).await;
    let PromptExecution { terminal, commit } = execution;
    if updates_delivered && terminal != PromptTerminal::Error {
        match commit {
            Some(messages) => guard.commit(messages),
            None => drop(guard),
        }
    } else {
        drop(guard);
    }

    if !updates_delivered {
        return responder.respond_with_error(internal_prompt_error());
    }
    match terminal {
        PromptTerminal::Stop(reason) => responder.respond(PromptResponse::new(reason)),
        PromptTerminal::Error => responder.respond_with_error(internal_prompt_error()),
    }
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
    /// FIFO writer queue.
    updates: mpsc::UnboundedSender<WriterMessage>,
}

impl UpdateSink for AcpUpdateSink {
    fn publish(&self, event: &AgentEvent) {
        self.observations.observe(event);
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
) -> Result<(), Error> {
    let mut failed = false;
    while let Some(message) = receiver.recv().await {
        match message {
            WriterMessage::Update(notification) => {
                if !failed && connection.send_notification(*notification).is_err() {
                    failed = true;
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
    use jessica_core::types::TokenUsage;
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
        let (history, guard) = session
            .begin_prompt(service.shutdown.child_token())
            .expect("prompt must start");
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
