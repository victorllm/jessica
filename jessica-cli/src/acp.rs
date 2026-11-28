//! Stable ACP v1 agent transport over stdio.
//!
//! The server implements `initialize`, `session/new`, `session/prompt`,
//! `session/cancel`, and `session/close`. When persistence is configured it
//! also implements `session/load`, `session/list`, `session/resume`, and
//! `session/delete`. A prompt is prepared synchronously (session lookup,
//! content validation, busy check), then model and tool work runs in a task
//! spawned on the connection so the SDK dispatch loop keeps receiving
//! messages. Cancellation is scoped to one session prompt; sessions run
//! concurrently and process shutdown or stdin EOF cancels every session
//! before a bounded drain.
//!
//! When `[acp].state_dir` and `[events].path` are both configured, each ACP
//! session is indexed on disk and every committed prompt turn is linked to its
//! event-store stream. `session/load` reconstructs the newest stable history
//! from those turns and replays it as `session/update` notifications before the
//! load response. `session/resume` restores the same context without replay.
//! `session/list` exposes only session id, cwd, and last-update time.
//! `session/delete` removes only the index; the event store stays append-only.
//! Without persistence, `loadSession` and all indexed lifecycle methods are
//! not advertised; `session/close` remains available because it only releases
//! live in-memory sessions.
//!
//! `session/new` starts stdio MCP servers supplied by the client. Their tools
//! are exposed as `mcp__<server>__<tool>` and pass through the same policy,
//! timeout, and result-size guards as native tools; they are not automatically
//! allowed by the default policy. MCP children are reclaimed on close, delete,
//! transport failure, and process shutdown. HTTP/SSE MCP servers and MCP
//! servers on `session/load` or `session/resume` remain unsupported and are
//! rejected with `-32602`.
//!
//! Sensitive native tools (`bash`, `exec`, `write_file`, `delegate_task`) ask through
//! `session/request_permission` before they run. The pending `tool_call` update
//! is queued first on the same FIFO writer, so the client always sees the call
//! it is being asked about. Only one-shot `allow_once`/`reject_once` options
//! are offered. An approval publishes `in_progress` with the same provider
//! `toolCallId` and then executes; rejection, cancellation, the fixed
//! five-minute answer deadline, and transport failures never execute the tool.
//! Only the top-level `delegate_task` call is asked about; child tools run
//! under that approval.
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

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    AgentCapabilities, CancelNotification, CloseSessionRequest, CloseSessionResponse,
    DeleteSessionRequest, DeleteSessionResponse, Implementation, InitializeRequest,
    InitializeResponse, ListSessionsRequest, ListSessionsResponse, LoadSessionRequest,
    LoadSessionResponse, McpCapabilities, McpServer, MessageId, NewSessionRequest,
    NewSessionResponse, Notice, NoticeSeverity, PermissionOption, PermissionOptionKind,
    PromptCapabilities, PromptRequest, PromptResponse, RequestPermissionOutcome,
    RequestPermissionRequest, ResumeSessionRequest, ResumeSessionResponse, SessionCapabilities,
    SessionCloseCapabilities, SessionDeleteCapabilities, SessionId as AcpSessionId, SessionInfo,
    SessionListCapabilities, SessionNotification, SessionResumeCapabilities, SessionUpdate,
    StopReason,
};
use agent_client_protocol::{
    Agent, Client, Error, ErrorCode, Stdio, is_connection_terminated, is_incoming_transport_closed,
};
use async_trait::async_trait;
use jessica_core::types::{AgentEvent, BudgetKind, Message, SessionEndReason, SessionId, ToolCall};
use serde_json::json;
use tokio::sync::{Notify, OnceCell, mpsc, oneshot, watch};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::acp_events::{
    AcpEventMapper, BudgetStopReason, InFlightTools, budget_stop_reason, history_updates,
    interrupted_tool_update,
};
use crate::acp_session::{
    AcpSession, AcpSessionRegistry, HistoryLimits, PromptGuard, SessionError, prompt_text,
};
use crate::acp_store::{AcpLoadError, AcpPersistence, AcpSessionIndex};
use crate::app::{CliError, CliRuntime, TurnOutcome, TurnRequest, TurnScope};
use crate::approval::approval_request_input;
use crate::config::HarnessConfig;
use crate::governance::{ToolPermissionDecision, ToolPermissionGate};
use crate::hot_reload::ConfigWatcher;
use crate::mcp::{MAX_MCP_SERVERS_PER_SESSION, McpError, McpSession, StdioServerConfig};
use crate::pump::UpdateSink;
use crate::shutdown::ShutdownController;
use crate::tools::CatalogCandidate;

/// Maximum time spent waiting for active prompts to finish during shutdown.
const PROMPT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Time allowed for the transport to flush after prompts have drained.
const TRANSPORT_FLUSH_GRACE: Duration = Duration::from_millis(250);

/// Number of sessions returned by one `session/list` page.
const SESSION_LIST_PAGE_SIZE: usize = 100;

/// Prefix marking the versioned opaque `session/list` cursor.
const SESSION_LIST_CURSOR_PREFIX: &str = "acp-list-v1:";

/// Maximum accepted cursor length, well above any real cursor.
const SESSION_LIST_CURSOR_MAX_BYTES: usize = 512;

/// Bounded wait for a closed or deleted session's active prompt to stop.
const SESSION_CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Fixed time a client has to answer one `session/request_permission`.
///
/// The deadline is not configurable: an unanswered request must never leave a
/// prompt blocked indefinitely, and a timeout never authorizes the tool.
const TOOL_PERMISSION_TIMEOUT: Duration = Duration::from_secs(300);

/// Option id the client selects to approve one execution.
const PERMISSION_ALLOW_ONCE: &str = "allow_once";

/// Option id the client selects to reject one execution.
const PERMISSION_REJECT_ONCE: &str = "reject_once";

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
    let config = Arc::new(config);
    let persistence = AcpPersistence::open(&config, &root).await?.map(Arc::new);
    let service = Arc::new(AcpService::new(
        config,
        root,
        watch_path,
        Arc::clone(&shutdown),
        persistence,
    ));

    let initialize_service = Arc::clone(&service);
    let new_session_service = Arc::clone(&service);
    let load_session_service = Arc::clone(&service);
    let list_sessions_service = Arc::clone(&service);
    let delete_session_service = Arc::clone(&service);
    let resume_session_service = Arc::clone(&service);
    let close_session_service = Arc::clone(&service);
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
            async move |request: LoadSessionRequest, responder, connection| {
                load_session_service
                    .load_session(request, responder, connection)
                    .await
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: ListSessionsRequest, responder, _connection| {
                match list_sessions_service.list_sessions(request).await {
                    Ok(response) => responder.respond(response),
                    Err(error) => responder.respond_with_error(error),
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: DeleteSessionRequest, responder, _connection| {
                match delete_session_service.delete_session(request).await {
                    Ok(response) => responder.respond(response),
                    Err(error) => responder.respond_with_error(error),
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: ResumeSessionRequest, responder, _connection| {
                match resume_session_service.resume_session(request).await {
                    Ok(response) => responder.respond(response),
                    Err(error) => responder.respond_with_error(error),
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: CloseSessionRequest, responder, _connection| {
                match close_session_service.close_session(request).await {
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
    /// Optional durable ACP session index and event-store reader.
    persistence: Option<Arc<AcpPersistence>>,
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
        persistence: Option<Arc<AcpPersistence>>,
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
            persistence,
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
        responder.respond(initialize_response(request, self.persistence.is_some()))
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
        let mcp_configs = stdio_mcp_configs(&request.mcp_servers).map_err(mcp_config_error)?;
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
        if !mcp_configs.is_empty() {
            // Start MCP before returning session/new so the client never sees
            // a session whose declared servers failed silently. Any already
            // started child is reclaimed by McpSession::start; the session
            // itself is removed here before the error response.
            let mcp = match McpSession::start(mcp_configs, session.cwd()).await {
                Ok(mcp) => mcp,
                Err(error) => {
                    self.registry.remove(session.id());
                    tracing::warn!(
                        error = %error,
                        "acp MCP server startup failed"
                    );
                    return Err(mcp_start_error(error));
                }
            };
            session.attach_mcp(mcp);
        }
        if let Some(persistence) = &self.persistence
            && let Err(error) = persistence
                .create_session(session.id(), session.cwd())
                .await
        {
            self.registry.remove(session.id());
            tracing::warn!(
                error = %error,
                "acp session index creation failed"
            );
            return Err(internal_session_error());
        }
        tracing::debug!(session_id = session.id(), "acp session created");
        Ok(NewSessionResponse::new(session.id().to_owned()))
    }

    /// Implements `session/load`.
    ///
    /// The request cwd must resolve to the canonical cwd recorded when the
    /// session was created. Replay notifications are written before the load
    /// response, and the restored session is immediately usable for prompts.
    async fn load_session(
        &self,
        request: LoadSessionRequest,
        responder: agent_client_protocol::v1::Responder<LoadSessionResponse>,
        connection: ConnectionTo<Client>,
    ) -> Result<(), Error> {
        if self.is_closing() {
            return responder.respond_with_error(shutting_down_error());
        }
        let Some(persistence) = &self.persistence else {
            return responder.respond_with_error(persistence_required());
        };
        if let Err(error) = reject_unsupported_lifecycle_fields(
            !request.mcp_servers.is_empty(),
            !request.additional_directories.is_empty(),
            request.system_prompt.is_some(),
        ) {
            return responder.respond_with_error(error);
        }

        let session_id = request.session_id.to_string();
        if self.registry.get(&session_id).is_some() {
            return responder.respond_with_error(invalid_params("session is already loaded"));
        }

        let index = match self
            .resolve_persisted_session(persistence, &request.session_id, request.cwd)
            .await
        {
            Ok(index) => index,
            Err(error) => return responder.respond_with_error(error),
        };

        let history = match persistence.history_for(&index).await {
            Ok(history) => history,
            Err(AcpLoadError::NoRecoverableTurn { .. }) => {
                tracing::warn!("acp session has no recoverable turn");
                return responder.respond_with_error(internal_load_error());
            }
        };
        let replay_history = history.clone();
        let session = match self
            .registry
            .restore(session_id.clone(), index.cwd().to_path_buf(), history)
            .await
        {
            Ok(session) => session,
            Err(error) => return responder.respond_with_error(session_error(error)),
        };

        for update in history_updates(session.cwd(), &replay_history) {
            let notification =
                SessionNotification::new(AcpSessionId::new(session_id.clone()), update);
            if connection.send_notification(notification).is_err() {
                self.fail_transport();
                self.registry.remove(&session_id);
                return responder.respond_with_error(internal_load_error());
            }
        }
        if responder.respond(LoadSessionResponse::new()).is_err() {
            self.fail_transport();
            self.registry.remove(&session_id);
            return Ok(());
        }
        tracing::debug!(session_id = session.id(), "acp session loaded");
        Ok(())
    }

    /// Looks up a persisted session and verifies its canonical cwd.
    ///
    /// The request cwd must resolve to exactly the cwd recorded when the
    /// session was created. The caller receives the index whose cwd is already
    /// canonical, so it can restore the session without resolving it again.
    async fn resolve_persisted_session(
        &self,
        persistence: &AcpPersistence,
        session_id: &AcpSessionId,
        requested_cwd: PathBuf,
    ) -> Result<AcpSessionIndex, Error> {
        let session_id_string = session_id.to_string();
        let index = match persistence.lookup(&session_id_string).await {
            Ok(Some(index)) => index,
            Ok(None) => return Err(session_not_found(session_id)),
            Err(error) => {
                tracing::warn!(error = %error, "acp session index lookup failed");
                return Err(internal_load_error());
            }
        };
        let cwd = self
            .registry
            .validate_cwd(requested_cwd)
            .await
            .map_err(session_error)?;
        if cwd != index.cwd() {
            return Err(invalid_params("cwd does not match the persisted session"));
        }
        Ok(index)
    }

    /// Implements `session/list`.
    ///
    /// Results are filtered to the startup workspace root and optionally to
    /// one exact canonical cwd. Ordering is `updatedAt` descending with the
    /// session id ascending as a deterministic tie-break. Only session id,
    /// cwd, and `updatedAt` are exposed; titles and conversation content are
    /// never returned.
    async fn list_sessions(
        &self,
        request: ListSessionsRequest,
    ) -> Result<ListSessionsResponse, Error> {
        if self.is_closing() {
            return Err(shutting_down_error());
        }
        let Some(persistence) = &self.persistence else {
            return Err(persistence_required());
        };
        let cwd_filter = match request.cwd {
            Some(cwd) => Some(
                self.registry
                    .validate_cwd(cwd)
                    .await
                    .map_err(session_error)?,
            ),
            None => None,
        };
        let cursor = match request.cursor.as_deref() {
            Some(value) => Some(decode_list_cursor(value)?),
            None => None,
        };

        let mut indexes = persistence.list().await.map_err(|error| {
            tracing::warn!(error = %error, "acp session index listing failed");
            internal_list_error()
        })?;
        indexes.retain(|index| {
            index.cwd().starts_with(&self.root)
                && cwd_filter.as_ref().is_none_or(|cwd| index.cwd() == cwd)
        });
        indexes.sort_by(|left, right| {
            right
                .updated_at()
                .cmp(&left.updated_at())
                .then_with(|| left.session_id().cmp(right.session_id()))
        });
        if let Some(cursor) = &cursor {
            indexes.retain(|index| cursor.is_before(index));
        }

        let page_len = indexes.len().min(SESSION_LIST_PAGE_SIZE);
        let sessions = indexes[..page_len]
            .iter()
            .map(|index| {
                let mut info = SessionInfo::new(
                    AcpSessionId::new(index.session_id().to_owned()),
                    index.cwd().to_path_buf(),
                );
                if let Some(updated_at) = format_rfc3339_millis(index.updated_at()) {
                    info = info.updated_at(updated_at);
                }
                info
            })
            .collect();
        let next_cursor = if indexes.len() > page_len {
            Some(encode_list_cursor(&indexes[page_len - 1]))
        } else {
            None
        };
        Ok(ListSessionsResponse::new(sessions).next_cursor(next_cursor))
    }

    /// Implements `session/delete`.
    ///
    /// Deleting removes the ACP index and cancels a live session, but never
    /// removes event-store rows. A repeated delete of an unknown session maps
    /// to the protocol's session-not-found error.
    async fn delete_session(
        &self,
        request: DeleteSessionRequest,
    ) -> Result<DeleteSessionResponse, Error> {
        if self.is_closing() {
            return Err(shutting_down_error());
        }
        let Some(persistence) = &self.persistence else {
            return Err(persistence_required());
        };
        let session_id = request.session_id.to_string();
        let live = self.registry.get(&session_id);
        let was_live = live.is_some();
        let deleted = persistence.delete(&session_id).await.map_err(|error| {
            tracing::warn!(error = %error, "acp session index deletion failed");
            internal_delete_error()
        })?;
        if let Some(session) = live {
            self.registry.remove(&session_id);
            if !session.wait_idle(SESSION_CLOSE_DRAIN_TIMEOUT).await {
                tracing::warn!(
                    session_id = %session_id,
                    "acp deleted session drain timed out"
                );
            }
        }
        if !deleted && !was_live {
            return Err(session_not_found(&request.session_id));
        }
        tracing::debug!(session_id = %session_id, "acp session deleted");
        Ok(DeleteSessionResponse::new())
    }

    /// Implements `session/resume`.
    ///
    /// Unlike `session/load`, resume restores conversation context without
    /// replaying it. Resuming an already live session with the same cwd is
    /// idempotent. The index is retained on disk.
    async fn resume_session(
        &self,
        request: ResumeSessionRequest,
    ) -> Result<ResumeSessionResponse, Error> {
        if self.is_closing() {
            return Err(shutting_down_error());
        }
        let Some(persistence) = &self.persistence else {
            return Err(persistence_required());
        };
        reject_unsupported_lifecycle_fields(
            !request.mcp_servers.is_empty(),
            !request.additional_directories.is_empty(),
            request.system_prompt.is_some(),
        )?;

        let session_id = request.session_id.to_string();
        if let Some(session) = self.registry.get(&session_id) {
            let cwd = self
                .registry
                .validate_cwd(request.cwd)
                .await
                .map_err(session_error)?;
            if session.cwd() != cwd {
                return Err(invalid_params("cwd does not match the persisted session"));
            }
            tracing::debug!(session_id = %session_id, "acp live session resumed");
            return Ok(ResumeSessionResponse::new());
        }

        let index = self
            .resolve_persisted_session(persistence, &request.session_id, request.cwd)
            .await?;
        let history = match persistence.history_for(&index).await {
            Ok(history) => history,
            Err(AcpLoadError::NoRecoverableTurn { .. }) => {
                tracing::warn!("acp session has no recoverable turn");
                return Err(internal_resume_error());
            }
        };
        let session = self
            .registry
            .restore(session_id.clone(), index.cwd().to_path_buf(), history)
            .await
            .map_err(session_error)?;
        tracing::debug!(session_id = session.id(), "acp session resumed");
        Ok(ResumeSessionResponse::new())
    }

    /// Implements `session/close`.
    ///
    /// Close always releases in-memory state and cancels active work. When
    /// persistence is configured, the index is deliberately retained so the
    /// client can load or resume the session later. Closing an indexed session
    /// that is not live is an idempotent success.
    async fn close_session(
        &self,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, Error> {
        if self.is_closing() {
            return Err(shutting_down_error());
        }
        let session_id = request.session_id.to_string();
        if let Some(session) = self.registry.get(&session_id) {
            self.registry.remove(&session_id);
            if !session.wait_idle(SESSION_CLOSE_DRAIN_TIMEOUT).await {
                tracing::warn!(
                    session_id = %session_id,
                    "acp closed session drain timed out"
                );
            }
            tracing::debug!(session_id = %session_id, "acp session closed");
            return Ok(CloseSessionResponse::new());
        }
        if let Some(persistence) = &self.persistence {
            match persistence.exists(&session_id).await {
                Ok(true) => return Ok(CloseSessionResponse::new()),
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(error = %error, "acp session existence check failed");
                    return Err(internal_close_error());
                }
            }
        }
        Err(session_not_found(&request.session_id))
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
        let mcp_tools = session.mcp_tools();
        Ok(PreparedPrompt {
            session,
            history,
            user_input,
            turn_id,
            guard,
            activity,
            mcp_tools,
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
        let service = Self::new(config, root, None, shutdown, None);
        let _ = service.runtime.set(Ok(RuntimeAssembly {
            runtime,
            _config_watcher: None,
        }));
        service
    }
}

/// Position of the last session returned by one `session/list` page.
#[derive(Debug)]
struct SessionListCursor {
    /// Update time of the last returned session.
    updated_at: u64,
    /// Session id of the last returned session.
    session_id: String,
}

impl SessionListCursor {
    /// Returns whether `index` sorts strictly after this cursor.
    fn is_before(&self, index: &AcpSessionIndex) -> bool {
        index.updated_at() < self.updated_at
            || (index.updated_at() == self.updated_at
                && index.session_id() > self.session_id.as_str())
    }
}

/// Encodes the opaque cursor for the last session on a page.
fn encode_list_cursor(index: &AcpSessionIndex) -> String {
    format!(
        "{SESSION_LIST_CURSOR_PREFIX}{}:{}",
        index.updated_at(),
        index.session_id()
    )
}

/// Decodes and validates an opaque `session/list` cursor.
fn decode_list_cursor(value: &str) -> Result<SessionListCursor, Error> {
    if value.len() > SESSION_LIST_CURSOR_MAX_BYTES {
        return Err(invalid_params("cursor is invalid"));
    }
    let rest = value
        .strip_prefix(SESSION_LIST_CURSOR_PREFIX)
        .ok_or_else(|| invalid_params("cursor is invalid"))?;
    let (updated_at, session_id) = rest
        .split_once(':')
        .ok_or_else(|| invalid_params("cursor is invalid"))?;
    if session_id.is_empty() {
        return Err(invalid_params("cursor is invalid"));
    }
    let updated_at = updated_at
        .parse::<u64>()
        .map_err(|_| invalid_params("cursor is invalid"))?;
    Ok(SessionListCursor {
        updated_at,
        session_id: session_id.to_owned(),
    })
}

/// Formats Unix milliseconds as a UTC RFC 3339 timestamp.
///
/// The ACP schema describes `updatedAt` as ISO 8601. Jessica does not depend
/// on a date-formatting crate, so this implements the civil-date conversion
/// directly. Out-of-range years are omitted instead of returning an invalid
/// timestamp; current wall-clock times always fit.
fn format_rfc3339_millis(millis: u64) -> Option<String> {
    let seconds = i64::try_from(millis / 1_000).ok()?;
    let milliseconds = millis % 1_000;
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    let (year, month, day) = civil_from_days(days);
    if !(0..=9_999).contains(&year) {
        return None;
    }
    Some(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{milliseconds:03}Z"
    ))
}

/// Converts days since the Unix epoch to a civil Gregorian date.
///
/// This is Howard Hinnant's `civil_from_days` algorithm, which is valid for
/// the full range needed by RFC 3339 formatting.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

/// Converts ACP MCP server declarations to validated stdio configurations.
///
/// Only `session/new` calls this. HTTP and SSE transports are rejected; the
/// remaining name, command, duplicate, and tool-shape validation happens in
/// [`McpSession::start`] before any child process is spawned.
fn stdio_mcp_configs(servers: &[McpServer]) -> Result<Vec<StdioServerConfig>, McpError> {
    if servers.len() > MAX_MCP_SERVERS_PER_SESSION {
        return Err(McpError::TooManyServers {
            max: MAX_MCP_SERVERS_PER_SESSION,
        });
    }
    let mut configs = Vec::with_capacity(servers.len());
    for server in servers {
        match server {
            McpServer::Stdio(server) => configs.push(StdioServerConfig {
                name: server.name.clone(),
                command: server.command.clone(),
                args: server.args.clone(),
                env: server
                    .env
                    .iter()
                    .map(|variable| (variable.name.clone(), variable.value.clone()))
                    .collect(),
            }),
            McpServer::Http(_) => {
                return Err(McpError::UnsupportedTransport { transport: "http" });
            }
            McpServer::Sse(_) => {
                return Err(McpError::UnsupportedTransport { transport: "sse" });
            }
            _ => {
                return Err(McpError::UnsupportedTransport {
                    transport: "unknown",
                });
            }
        }
    }
    Ok(configs)
}

/// Maps MCP configuration errors to invalid params.
///
/// These errors are all caused by the client's `mcpServers` declaration and
/// occur before any process is started.
fn mcp_config_error(error: McpError) -> Error {
    invalid_params(error.to_string())
}

/// Maps MCP startup failures to a stable JSON-RPC error.
///
/// Configuration and discovery-shape failures use `-32602`; process startup,
/// protocol, timeout, and JSON-RPC failures use a content-free `-32603`.
fn mcp_start_error(error: McpError) -> Error {
    match error {
        McpError::TooManyServers { .. }
        | McpError::EmptyServerName
        | McpError::DuplicateServerName { .. }
        | McpError::InvalidCommand { .. }
        | McpError::TooManyTools { .. }
        | McpError::ToolNameCollision { .. }
        | McpError::InvalidTool { .. } => invalid_params(error.to_string()),
        McpError::UnsupportedTransport { .. }
        | McpError::Spawn { .. }
        | McpError::Protocol { .. }
        | McpError::Timeout { .. }
        | McpError::Rpc { .. }
        | McpError::Closed { .. } => Error::new(
            ErrorCode::INTERNAL_ERROR,
            "MCP server initialization failed",
        )
        .data(json!({"reason": "mcp_server_startup_failed"})),
    }
}

/// Rejects session lifecycle fields Jessica does not support.
///
/// `session/load` and `session/resume` share these fields. MCP servers are
/// only started by `session/new`; load and resume still reject them, as do
/// additional roots and custom prompts.
fn reject_unsupported_lifecycle_fields(
    has_mcp_servers: bool,
    has_additional_directories: bool,
    has_system_prompt: bool,
) -> Result<(), Error> {
    if has_mcp_servers {
        return Err(invalid_params(
            "mcpServers are only supported on session/new",
        ));
    }
    if has_additional_directories {
        return Err(invalid_params(
            "additionalDirectories are not supported in this ACP version",
        ));
    }
    if has_system_prompt {
        return Err(invalid_params(
            "systemPrompt is not supported in this ACP version",
        ));
    }
    Ok(())
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
    /// Session-scoped MCP tools snapshotted before the worker starts.
    ///
    /// The worker drops the session handle early, so the executable tool
    /// handles and their annotations must travel with the prepared prompt.
    mcp_tools: Vec<CatalogCandidate>,
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
        mcp_tools,
    } = prepared;
    let session_id = session.id().to_owned();
    let cwd = session.cwd().to_path_buf();
    drop(session);

    let observations = Arc::new(PromptObservations::default());
    let in_flight = Arc::new(InFlightTools::default());
    let announced = Arc::new(AnnouncedToolCalls::default());
    let mapper = AcpEventMapper::new(cwd.clone(), MessageId::new(turn_id.to_string()));
    let acp_session_id = AcpSessionId::new(session_id.clone());
    let sink: Arc<dyn UpdateSink> = Arc::new(AcpUpdateSink {
        session_id: acp_session_id.clone(),
        mapper: mapper.clone(),
        observations: Arc::clone(&observations),
        in_flight: Arc::clone(&in_flight),
        announced: Arc::clone(&announced),
        updates: updates.clone(),
    });
    let permission_gate: Arc<dyn ToolPermissionGate> = Arc::new(AcpPermissionGate {
        session_id: acp_session_id,
        mapper,
        root: cwd.clone(),
        updates: updates.clone(),
        cancel: guard.token().clone(),
        timeout: TOOL_PERMISSION_TIMEOUT,
        announced,
    });
    let execution = execute_prompt(
        &service,
        PromptExecutionRequest {
            cwd,
            history,
            user_input,
            turn_id: turn_id.clone(),
            token: guard.token().clone(),
            sink,
            observations: Arc::clone(&observations),
            extra_tools: mcp_tools,
            permission_gate: Some(permission_gate),
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

    // A turn is committed exactly when the barrier succeeded and execution
    // produced commit messages. Persist its turn id before the response so a
    // successfully answered prompt is recoverable. Index failure only warns:
    // the in-memory session remains usable, while load falls back to an older
    // recoverable turn.
    if updates_delivered
        && commit.is_some()
        && let Some(persistence) = &service.persistence
        && let Err(error) = persistence.record_turn(&session_id, &turn_id).await
    {
        tracing::warn!(
            error = %error,
            "acp committed turn index update failed"
        );
    }

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
    /// Session-scoped MCP tool candidates to register for this turn.
    extra_tools: Vec<CatalogCandidate>,
    /// Approval gate for sensitive native tools.
    ///
    /// `None` keeps policy-only behavior for direct in-process test turns.
    permission_gate: Option<Arc<dyn ToolPermissionGate>>,
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
        extra_tools,
        permission_gate,
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
            extra_tools,
            permission_gate,
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
    /// Tool calls whose announcement update has been queued.
    announced: Arc<AnnouncedToolCalls>,
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
        // Mark only after the queue received the update: a permission gate
        // waiting on this id may then enqueue its request behind it.
        if let AgentEvent::ToolCallStarted { call } = event {
            self.announced.mark(&call.id);
        }
    }
}

/// Tool calls whose `tool_call` update is already in the writer queue.
///
/// The event pump publishes asynchronously while the agent loop continues into
/// tool execution, so a permission request could overtake the update that
/// announced its own call id. The gate waits here before enqueueing the
/// request. The writer queue is FIFO, so once the update is queued the request
/// cannot be written first.
#[derive(Default)]
struct AnnouncedToolCalls {
    /// Provider ids whose announcement is queued.
    ids: Mutex<HashSet<String>>,
    /// Wakes gates waiting for an announcement.
    notify: Notify,
}

impl AnnouncedToolCalls {
    /// Records that `id`'s announcement update is queued.
    fn mark(&self, id: &str) {
        self.ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id.to_owned());
        self.notify.notify_waiters();
    }

    /// Waits until `id` is announced or `cancel` fires.
    ///
    /// Returns whether the announcement was observed. A `false` result means
    /// the prompt was cancelled first and no permission request may be sent.
    async fn wait(&self, id: &str, cancel: &CancellationToken) -> bool {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register before checking the set so a `mark` between the check
            // and the await cannot lose its wakeup.
            notified.as_mut().enable();
            if self
                .ids
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(id)
            {
                return true;
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return false,
                () = &mut notified => {}
            }
        }
    }
}

/// Permission gate backed by ACP `session/request_permission`.
struct AcpPermissionGate {
    /// Session id carried by every request.
    session_id: AcpSessionId,
    /// Mapper shared with the update sink so request and update fields agree.
    mapper: AcpEventMapper,
    /// Session workspace root used for bounded write-file diff summaries.
    root: PathBuf,
    /// FIFO writer queue shared with session updates.
    updates: mpsc::UnboundedSender<WriterMessage>,
    /// Prompt cancellation token.
    cancel: CancellationToken,
    /// Fixed deadline for one client answer.
    timeout: Duration,
    /// Announcements already queued for this prompt.
    announced: Arc<AnnouncedToolCalls>,
}

#[async_trait]
impl ToolPermissionGate for AcpPermissionGate {
    async fn request_permission(&self, call: &ToolCall) -> ToolPermissionDecision {
        // The client must know the id before it can be asked about it. This
        // also guarantees the `tool_call` update precedes the request on the
        // same writer queue.
        if !self.announced.wait(&call.id, &self.cancel).await {
            return ToolPermissionDecision::Cancelled;
        }

        let raw_input = approval_request_input(&self.root, call).await;
        let request = RequestPermissionRequest::new(
            self.session_id.clone(),
            self.mapper.permission_request_update(call, raw_input),
            permission_options(),
        );
        // The child token lets the writer drop this one request (and send
        // `$/cancel_request`) without ending the writer task.
        let cancel = self.cancel.child_token();
        let (reply, response) = oneshot::channel();
        if self
            .updates
            .send(WriterMessage::Permission {
                request: Box::new(request),
                cancel: cancel.clone(),
                reply,
            })
            .is_err()
        {
            return ToolPermissionDecision::TransportFailed;
        }

        let outcome = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {
                cancel.cancel();
                return ToolPermissionDecision::Cancelled;
            }
            _ = tokio::time::sleep(self.timeout) => {
                cancel.cancel();
                return ToolPermissionDecision::TimedOut;
            }
            outcome = response => outcome,
        };

        let outcome = match outcome {
            // The writer task disappeared before answering.
            Err(_) => return ToolPermissionDecision::TransportFailed,
            Ok(PermissionReply::Cancelled) => {
                // The prompt was cancelled while the request was pending; the
                // ACP contract requires the turn to end cancelled.
                return ToolPermissionDecision::Cancelled;
            }
            Ok(PermissionReply::Failed) => return ToolPermissionDecision::TransportFailed,
            Ok(PermissionReply::Outcome(outcome)) => outcome,
        };

        match outcome {
            RequestPermissionOutcome::Cancelled => {
                // The client aborted the turn while answering. Ending the
                // prompt is the only way to honour that cancellation.
                self.cancel.cancel();
                ToolPermissionDecision::Cancelled
            }
            RequestPermissionOutcome::Selected(selected) => {
                match selected.option_id.0.as_ref() {
                    PERMISSION_ALLOW_ONCE => {
                        let update = self.mapper.permission_granted_update(call);
                        let notification = SessionNotification::new(
                            self.session_id.clone(),
                            SessionUpdate::ToolCallUpdate(update),
                        );
                        if self
                            .updates
                            .send(WriterMessage::Update(Box::new(notification)))
                            .is_err()
                        {
                            return ToolPermissionDecision::TransportFailed;
                        }
                        ToolPermissionDecision::Allow
                    }
                    PERMISSION_REJECT_ONCE => ToolPermissionDecision::Reject,
                    _ => {
                        // Only the two offered ids are valid. Anything else is
                        // treated as a rejection so a malformed or unexpected
                        // answer can never authorize execution.
                        tracing::warn!("acp permission response selected an unknown option");
                        ToolPermissionDecision::Reject
                    }
                }
            }
            // `RequestPermissionOutcome` is non-exhaustive; an unknown future
            // outcome must fail closed.
            _ => {
                tracing::warn!("acp permission response used an unknown outcome");
                ToolPermissionDecision::Reject
            }
        }
    }
}

/// Options offered to the client for every permission request.
///
/// Only one-shot choices are offered: Jessica keeps no persistent approval
/// state across calls or turns.
fn permission_options() -> Vec<PermissionOption> {
    vec![
        PermissionOption::new(
            PERMISSION_ALLOW_ONCE,
            "Allow this execution",
            PermissionOptionKind::AllowOnce,
        ),
        PermissionOption::new(
            PERMISSION_REJECT_ONCE,
            "Reject this execution",
            PermissionOptionKind::RejectOnce,
        ),
    ]
}

/// Messages consumed by the dedicated protocol write task.
enum WriterMessage {
    /// One session update notification.
    Update(Box<SessionNotification>),
    /// One client permission request, answered through `reply`.
    Permission {
        /// Request sent as an agent-to-client JSON-RPC request.
        request: Box<RequestPermissionRequest>,
        /// Per-request cancellation from the permission gate.
        cancel: CancellationToken,
        /// Reply carrying the client's outcome or a failure.
        reply: oneshot::Sender<PermissionReply>,
    },
    /// Flush boundary; the bool reports whether every prior update was sent.
    Barrier(oneshot::Sender<bool>),
}

/// Result of delivering one permission request.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PermissionReply {
    /// The client answered with this outcome.
    Outcome(RequestPermissionOutcome),
    /// The request was cancelled before an answer arrived.
    Cancelled,
    /// The request could not be delivered or answered.
    Failed,
}

/// Writes queued session updates in order and acknowledges the barrier.
///
/// Permission requests share the same FIFO queue, so every update enqueued
/// before a request is written first. While a request is pending this task
/// waits for the client; cancellation drops the request, which sends
/// `$/cancel_request`, and the writer continues with later messages.
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
            WriterMessage::Permission {
                request,
                cancel,
                reply,
            } => {
                if failed {
                    let _ = reply.send(PermissionReply::Failed);
                    continue;
                }
                if cancel.is_cancelled() {
                    let _ = reply.send(PermissionReply::Cancelled);
                    continue;
                }

                let sent = connection.send_request(*request);
                let result = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        // Dropping `sent` requests cancellation from the
                        // client with the same JSON-RPC id.
                        let _ = reply.send(PermissionReply::Cancelled);
                        continue;
                    }
                    result = sent.block_task() => result,
                };

                match result {
                    Ok(response) => {
                        let _ = reply.send(PermissionReply::Outcome(response.outcome));
                    }
                    Err(error) => {
                        // Local termination means every later write is
                        // meaningless too; a peer error response fails only
                        // this permission and leaves the transport usable.
                        if is_connection_terminated(&error) || is_incoming_transport_closed(&error)
                        {
                            failed = true;
                            service.fail_transport();
                        } else {
                            tracing::debug!(
                                code = %error.code,
                                "acp permission request received an error response"
                            );
                        }
                        let _ = reply.send(PermissionReply::Failed);
                    }
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
/// is unsupported; the client decides whether to disconnect. Indexed session
/// lifecycle methods are advertised only when a persistence index and event
/// store were opened at startup. `session/close` is always advertised because
/// it only releases live in-memory state and needs no index. Every other
/// optional capability stays explicitly disabled so clients never probe
/// unsupported content types, HTTP/SSE MCP transports, or lifecycle methods.
/// Stdio MCP has no capability flag in ACP v1 and is accepted by `session/new`.
/// Baseline text and resource-link prompts are always supported.
fn initialize_response(request: &InitializeRequest, persistence: bool) -> InitializeResponse {
    tracing::debug!(
        requested_protocol_version = request.protocol_version.as_u16(),
        supported_protocol_version = ProtocolVersion::V1.as_u16(),
        persistence,
        "acp initialize"
    );

    let mut session_capabilities =
        SessionCapabilities::new().close(SessionCloseCapabilities::new());
    if persistence {
        session_capabilities = session_capabilities
            .list(SessionListCapabilities::new())
            .delete(SessionDeleteCapabilities::new())
            .resume(SessionResumeCapabilities::new());
    }

    InitializeResponse::new(ProtocolVersion::V1)
        .agent_capabilities(
            AgentCapabilities::new()
                .load_session(persistence)
                .prompt_capabilities(
                    PromptCapabilities::new()
                        .image(false)
                        .audio(false)
                        .embedded_context(false),
                )
                .mcp_capabilities(McpCapabilities::new().http(false).sse(false))
                .session_capabilities(session_capabilities),
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

/// Generic internal error for a session that could not be indexed.
fn internal_session_error() -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, "session creation failed")
}

/// Generic internal error for an unreadable or unrecoverable session.
fn internal_load_error() -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, "session load failed")
}

/// Generic internal error for an unlistable session index.
fn internal_list_error() -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, "session list failed")
}

/// Generic internal error for an undeletable session index.
fn internal_delete_error() -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, "session delete failed")
}

/// Generic internal error for an unrestorable session.
fn internal_resume_error() -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, "session resume failed")
}

/// Generic internal error for an unreadable close target.
fn internal_close_error() -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, "session close failed")
}

/// Method-not-found error when an indexed method has no persistence.
fn persistence_required() -> Error {
    Error::method_not_found()
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

    use agent_client_protocol::v1::schema::{
        ContentBlock, SelectedPermissionOutcome, TextContent, ToolCallStatus, ToolCallUpdate,
    };
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
                extra_tools: Vec::new(),
                permission_gate: None,
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
    async fn session_new_rejects_unsupported_mcp_transports_and_fields() {
        let root = temp_root("new-session");
        let provider = Arc::new(ScriptedProvider::new(vec![]));
        let (service, _config) = service_with(provider, &root, HarnessConfig::default());

        let mut with_http = NewSessionRequest::new(root.clone());
        with_http.mcp_servers = vec![agent_client_protocol::v1::schema::McpServer::Http(
            agent_client_protocol::v1::schema::McpServerHttp::new(
                "remote",
                "https://example.invalid/mcp",
            ),
        )];
        let mut with_sse = NewSessionRequest::new(root.clone());
        with_sse.mcp_servers = vec![agent_client_protocol::v1::schema::McpServer::Sse(
            agent_client_protocol::v1::schema::McpServerSse::new(
                "remote",
                "https://example.invalid/mcp",
            ),
        )];
        let mut with_relative_command = NewSessionRequest::new(root.clone());
        with_relative_command.mcp_servers =
            vec![agent_client_protocol::v1::schema::McpServer::Stdio(
                agent_client_protocol::v1::schema::McpServerStdio::new(
                    "server",
                    PathBuf::from("relative-server"),
                ),
            )];
        let mut with_directory = NewSessionRequest::new(root.clone());
        with_directory.additional_directories = vec![root.clone()];
        let mut with_prompt = NewSessionRequest::new(root);
        with_prompt.system_prompt =
            Some(agent_client_protocol::v1::schema::SystemPrompt::new(vec![
                ContentBlock::Text(TextContent::new("custom")),
            ]));

        for request in [
            with_http,
            with_sse,
            with_relative_command,
            with_directory,
            with_prompt,
        ] {
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
            last_output: "new answer".to_owned(),
            // The generated messages begin after the new user message.
            turn_start: 4,
            turn_id: "turn-test".to_owned(),
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
    fn list_cursors_are_versioned_and_reject_malformed_values() {
        let cursor = decode_list_cursor("acp-list-v1:1700000000123:acp-1-2-0")
            .expect("a versioned cursor must decode");
        assert_eq!(cursor.updated_at, 1_700_000_000_123);
        assert_eq!(cursor.session_id, "acp-1-2-0");

        for invalid in [
            "",
            "1700000000123:acp-1",
            "acp-list-v1:not-a-number:acp-1",
            "acp-list-v1:1700000000123:",
            "acp-list-v1:1700000000123",
        ] {
            let error =
                decode_list_cursor(invalid).expect_err("malformed cursors must be rejected");
            assert_eq!(
                error.code,
                ErrorCode::INVALID_PARAMS,
                "cursor `{invalid}` must fail as invalid params"
            );
        }
    }

    #[test]
    fn updated_at_formatting_is_utc_rfc3339_with_milliseconds() {
        for (millis, expected) in [
            (0, "1970-01-01T00:00:00.000Z"),
            (951_782_400_000, "2000-02-29T00:00:00.000Z"),
            (1_700_000_000_123, "2023-11-14T22:13:20.123Z"),
        ] {
            assert_eq!(
                format_rfc3339_millis(millis).as_deref(),
                Some(expected),
                "Unix millisecond {millis} must format deterministically"
            );
        }
    }

    #[test]
    fn initialize_advertises_v1_and_only_the_configured_optional_capabilities() {
        let response = initialize_response(&InitializeRequest::new(ProtocolVersion::V1), false);
        assert_eq!(response.protocol_version, ProtocolVersion::V1);
        assert!(!response.agent_capabilities.load_session);
        let session = &response.agent_capabilities.session_capabilities;
        assert!(
            session.close.is_some(),
            "close only releases live state and needs no persistence"
        );
        assert!(session.list.is_none());
        assert!(session.delete.is_none());
        assert!(session.resume.is_none());

        let persisted = initialize_response(&InitializeRequest::new(ProtocolVersion::V1), true);
        assert!(
            persisted.agent_capabilities.load_session,
            "persistence must advertise session/load"
        );
        let session = &persisted.agent_capabilities.session_capabilities;
        assert!(session.list.is_some(), "persistence must advertise list");
        assert!(
            session.delete.is_some(),
            "persistence must advertise delete"
        );
        assert!(
            session.resume.is_some(),
            "persistence must advertise resume"
        );
        assert!(session.close.is_some(), "close is always advertised");
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
            let response = initialize_response(&InitializeRequest::new(requested), false);

            assert_eq!(
                response.protocol_version,
                ProtocolVersion::V1,
                "the agent must answer with its latest supported version, not the requested one"
            );
        }
    }

    /// Creates a gate and the receiving end of its writer queue.
    fn permission_gate(
        timeout: Duration,
    ) -> (
        Arc<AcpPermissionGate>,
        mpsc::UnboundedReceiver<WriterMessage>,
        Arc<AnnouncedToolCalls>,
        CancellationToken,
    ) {
        let (updates, receiver) = mpsc::unbounded_channel();
        let announced = Arc::new(AnnouncedToolCalls::default());
        let cancel = CancellationToken::new();
        let gate = Arc::new(AcpPermissionGate {
            session_id: AcpSessionId::new("sess-1"),
            mapper: AcpEventMapper::new(PathBuf::from("/workspace"), MessageId::new("turn-1")),
            root: PathBuf::from("/workspace"),
            updates,
            cancel: cancel.clone(),
            timeout,
            announced: Arc::clone(&announced),
        });
        (gate, receiver, announced, cancel)
    }

    /// Replies to the next queued permission request.
    async fn answer_permission(
        receiver: &mut mpsc::UnboundedReceiver<WriterMessage>,
        outcome: RequestPermissionOutcome,
    ) -> RequestPermissionRequest {
        let WriterMessage::Permission { request, reply, .. } = receiver
            .recv()
            .await
            .expect("a permission request must be queued")
        else {
            panic!("expected a permission request");
        };
        let _ = reply.send(PermissionReply::Outcome(outcome));
        *request
    }

    /// Waits for the next queued update and returns its tool update.
    async fn next_tool_update(
        receiver: &mut mpsc::UnboundedReceiver<WriterMessage>,
    ) -> ToolCallUpdate {
        let WriterMessage::Update(notification) =
            receiver.recv().await.expect("an update must be queued")
        else {
            panic!("expected an update");
        };
        let SessionUpdate::ToolCallUpdate(update) = notification.update else {
            panic!("expected a tool_call_update");
        };
        update
    }

    fn selected(option: &'static str) -> RequestPermissionOutcome {
        RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option))
    }

    #[tokio::test]
    async fn permission_gate_waits_for_the_call_announcement_before_asking() {
        let (gate, mut receiver, announced, _cancel) = permission_gate(Duration::from_secs(1));
        let call = ToolCall::new("call-wait", "bash", json!({"command": "true"}));
        let task = tokio::spawn(async move { gate.request_permission(&call).await });

        assert!(
            timeout(Duration::from_millis(30), receiver.recv())
                .await
                .is_err(),
            "no permission request may be sent before the tool_call update is queued"
        );

        announced.mark("call-wait");
        let request = answer_permission(&mut receiver, selected(PERMISSION_ALLOW_ONCE)).await;
        assert_eq!(request.tool_call.tool_call_id.to_string(), "call-wait");
        let update = next_tool_update(&mut receiver).await;
        assert_eq!(update.fields.status, Some(ToolCallStatus::InProgress));
        assert_eq!(
            task.await.expect("gate task must finish"),
            ToolPermissionDecision::Allow
        );
    }

    #[tokio::test]
    async fn a_cancelled_prompt_wakes_a_gate_waiting_for_the_announcement() {
        let (gate, mut receiver, _announced, cancel) = permission_gate(Duration::from_secs(60));
        let call = ToolCall::new("call-cancel", "bash", json!({"command": "true"}));
        cancel.cancel();

        let decision = gate.request_permission(&call).await;

        assert_eq!(decision, ToolPermissionDecision::Cancelled);
        assert!(
            receiver.try_recv().is_err(),
            "no request may be queued after cancellation"
        );
    }

    #[tokio::test]
    async fn allow_once_returns_allow_and_sends_the_in_progress_update() {
        let (gate, mut receiver, announced, _cancel) = permission_gate(Duration::from_secs(1));
        announced.mark("call-allow");
        let call = ToolCall::new("call-allow", "bash", json!({"command": "true"}));
        let task = tokio::spawn(async move { gate.request_permission(&call).await });

        let request = answer_permission(&mut receiver, selected(PERMISSION_ALLOW_ONCE)).await;
        assert_eq!(request.session_id.0.as_ref(), "sess-1");
        assert_eq!(request.tool_call.tool_call_id.to_string(), "call-allow");
        assert_eq!(
            request.tool_call.fields.status,
            Some(ToolCallStatus::Pending)
        );
        assert_eq!(
            request.tool_call.fields.title.as_deref(),
            Some("bash: true")
        );
        assert_eq!(
            request
                .options
                .iter()
                .map(|option| (option.option_id.0.as_ref(), option.kind))
                .collect::<Vec<_>>(),
            vec![
                (PERMISSION_ALLOW_ONCE, PermissionOptionKind::AllowOnce),
                (PERMISSION_REJECT_ONCE, PermissionOptionKind::RejectOnce),
            ]
        );
        let update = next_tool_update(&mut receiver).await;
        assert_eq!(update.tool_call_id.to_string(), "call-allow");
        assert_eq!(update.fields.status, Some(ToolCallStatus::InProgress));

        assert_eq!(
            task.await.expect("gate task must finish"),
            ToolPermissionDecision::Allow
        );
    }

    #[tokio::test]
    async fn reject_once_returns_reject_without_an_execution_update() {
        let (gate, mut receiver, announced, cancel) = permission_gate(Duration::from_secs(1));
        announced.mark("call-reject");
        let call = ToolCall::new("call-reject", "write_file", json!({"path": "out.txt"}));
        let task = tokio::spawn(async move { gate.request_permission(&call).await });

        let request = answer_permission(&mut receiver, selected(PERMISSION_REJECT_ONCE)).await;
        assert_eq!(request.tool_call.tool_call_id.to_string(), "call-reject");
        assert_eq!(
            task.await.expect("gate task must finish"),
            ToolPermissionDecision::Reject
        );
        assert!(
            receiver.try_recv().is_err(),
            "a rejected call must not get an in_progress update"
        );
        assert!(
            !cancel.is_cancelled(),
            "rejection must not cancel the prompt"
        );
    }

    #[tokio::test]
    async fn a_cancelled_outcome_ends_the_prompt_and_never_approves() {
        let (gate, mut receiver, announced, cancel) = permission_gate(Duration::from_secs(1));
        announced.mark("call-cancel-outcome");
        let call = ToolCall::new("call-cancel-outcome", "bash", json!({"command": "true"}));
        let task = tokio::spawn(async move { gate.request_permission(&call).await });

        answer_permission(&mut receiver, RequestPermissionOutcome::Cancelled).await;

        assert_eq!(
            task.await.expect("gate task must finish"),
            ToolPermissionDecision::Cancelled
        );
        assert!(cancel.is_cancelled(), "the prompt must end cancelled");
        assert!(
            receiver.try_recv().is_err(),
            "a cancelled permission must not open an execution phase"
        );
    }

    #[tokio::test]
    async fn unknown_or_future_outcomes_fail_closed() {
        for outcome in [
            selected("allow_always"),
            selected("something_else"),
            selected(""),
        ] {
            let (gate, mut receiver, announced, _cancel) = permission_gate(Duration::from_secs(1));
            announced.mark("call-unknown");
            let call = ToolCall::new("call-unknown", "bash", json!({"command": "true"}));
            let task = tokio::spawn(async move { gate.request_permission(&call).await });

            answer_permission(&mut receiver, outcome).await;

            assert_eq!(
                task.await.expect("gate task must finish"),
                ToolPermissionDecision::Reject,
                "an unoffered option must never authorize the tool"
            );
            assert!(receiver.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn an_unanswered_request_times_out_and_cancels_only_that_request() {
        let (gate, mut receiver, announced, cancel) = permission_gate(Duration::from_millis(10));
        announced.mark("call-timeout");
        let call = ToolCall::new("call-timeout", "bash", json!({"command": "true"}));

        let decision = gate.request_permission(&call).await;

        assert_eq!(decision, ToolPermissionDecision::TimedOut);
        assert!(
            !cancel.is_cancelled(),
            "a timeout fails the tool but does not cancel the prompt"
        );
        let WriterMessage::Permission {
            cancel: request_cancel,
            ..
        } = receiver.recv().await.expect("the request must be queued")
        else {
            panic!("expected a permission request");
        };
        assert!(
            request_cancel.is_cancelled(),
            "the abandoned request must be cancelled on the wire"
        );
    }

    #[tokio::test]
    async fn a_prompt_cancel_while_the_request_is_pending_returns_cancelled() {
        let (gate, mut receiver, announced, cancel) = permission_gate(Duration::from_secs(60));
        announced.mark("call-pending-cancel");
        let call = ToolCall::new("call-pending-cancel", "bash", json!({"command": "true"}));
        let task = tokio::spawn(async move { gate.request_permission(&call).await });

        let WriterMessage::Permission {
            cancel: request_cancel,
            ..
        } = receiver.recv().await.expect("the request must be queued")
        else {
            panic!("expected a permission request");
        };
        cancel.cancel();

        assert_eq!(
            task.await.expect("gate task must finish"),
            ToolPermissionDecision::Cancelled
        );
        assert!(
            request_cancel.is_cancelled(),
            "prompt cancellation must withdraw the pending request"
        );
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_closed_writer_queue_fails_the_permission_request() {
        let (gate, receiver, announced, _cancel) = permission_gate(Duration::from_secs(1));
        announced.mark("call-closed");
        drop(receiver);
        let call = ToolCall::new("call-closed", "bash", json!({"command": "true"}));

        assert_eq!(
            gate.request_permission(&call).await,
            ToolPermissionDecision::TransportFailed
        );
    }
}
