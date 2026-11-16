//! ACP session registry, prompt state, and bounded multi-turn history.
//!
//! A session owns one canonical working directory, the conversation retained
//! from completed prompts, and a lease that makes one prompt at a time active.
//! History is trimmed only at user-turn boundaries, so an assistant message
//! carrying tool calls can never be separated from its tool results.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_client_protocol::v1::schema::ContentBlock;
use jessica_core::types::{Message, Role, SessionId};
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Process-wide session sequence.
///
/// A single counter keeps ids unique even if a test creates several registries
/// in one process. In normal operation the ACP service owns one registry.
static SESSION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Nonce that distinguishes this process's ACP sessions from earlier runs.
static PROCESS_NONCE: OnceLock<String> = OnceLock::new();

/// Returns a process-unique nonce for session ids and therefore turn ids.
///
/// The process id alone is reused by the operating system, so the startup
/// nanosecond timestamp is included. A loaded session keeps its old id; only
/// newly created sessions use this nonce.
fn process_nonce() -> &'static str {
    PROCESS_NONCE.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        format!("{}-{nanos}", std::process::id())
    })
}

/// Text used to close a tool call whose result never reached the transcript.
const INTERRUPTED_TOOL_RESULT: &str = "tool error: tool execution was interrupted";

/// History limits copied from the `[acp]` configuration at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryLimits {
    /// Maximum number of messages retained after a commit.
    pub(crate) max_messages: usize,
    /// Maximum serialized byte length retained after a commit.
    pub(crate) max_bytes: usize,
}

/// Errors raised while creating an ACP session.
#[derive(Debug, Error)]
pub(crate) enum SessionError {
    /// The requested working directory is not absolute.
    #[error("cwd must be an absolute path")]
    CwdNotAbsolute,
    /// The requested working directory could not be resolved.
    #[error("cwd must be an existing directory")]
    CwdUnavailable(#[source] std::io::Error),
    /// The requested working directory resolves to a non-directory.
    #[error("cwd must be a directory")]
    CwdNotDirectory,
    /// The requested working directory resolves outside the startup root.
    #[error("cwd must be inside the startup workspace root")]
    CwdOutsideRoot,
    /// The process already owns the configured maximum number of sessions.
    #[error("session limit reached (maximum {max_sessions})")]
    LimitReached {
        /// Configured upper bound.
        max_sessions: usize,
    },
    /// A restored session id is already live.
    #[error("session is already loaded")]
    AlreadyLoaded,
    /// A restored session id was empty.
    #[error("session id must not be empty")]
    EmptyId,
}

/// Errors raised while preparing prompt content.
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum PromptContentError {
    /// The prompt assembled to an empty or whitespace-only string.
    #[error("prompt must not be empty")]
    Empty,
    /// The assembled prompt exceeded the configured byte limit.
    #[error("prompt exceeds the maximum of {max_bytes} bytes")]
    TooLarge {
        /// Configured upper bound.
        max_bytes: usize,
    },
    /// The prompt contained a content block this agent does not support.
    #[error("unsupported prompt content block: {kind}")]
    Unsupported {
        /// Stable name of the unsupported variant.
        kind: &'static str,
    },
}

/// One ACP session.
pub(crate) struct AcpSession {
    /// Protocol-visible session id (`acp-{process-nonce}-{seq}`).
    id: String,
    /// Canonical working directory validated at creation time.
    cwd: PathBuf,
    /// Conversation retained from previously completed prompts.
    state: Mutex<SessionState>,
    /// Monotonic per-session turn sequence.
    turn_sequence: AtomicU64,
    /// Limits applied whenever a prompt commits history.
    limits: HistoryLimits,
    /// Broadcasts after each prompt releases the session.
    ///
    /// `session/close` and `session/delete` use this to wait a bounded time
    /// for cancelled work to observe the cancellation before acknowledging.
    activity: watch::Sender<()>,
    /// Lifetime token cancelled when the process or transport shuts down.
    ///
    /// Prompt tokens are children of this token, so cancelling it stops the
    /// current prompt without making the session permanently unusable when a
    /// client sends `session/cancel`.
    lifetime: CancellationToken,
}

impl AcpSession {
    /// Returns the protocol-visible session id.
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    /// Returns the canonical working directory.
    pub(crate) fn cwd(&self) -> &Path {
        &self.cwd
    }

    /// Allocates the internal turn id for the next prompt.
    ///
    /// The id includes the current process nonce as well as the per-session
    /// sequence. A restored session starts its sequence at zero again, so
    /// without the nonce its first new turn would reuse an event-store key from
    /// the process that originally ran the session.
    pub(crate) fn next_turn_id(&self) -> SessionId {
        let sequence = self.turn_sequence.fetch_add(1, Ordering::Relaxed);
        SessionId::new(format!("{}-{}-t{sequence}", self.id, process_nonce()))
    }

    /// Marks a prompt active and snapshots the history it must receive.
    ///
    /// The busy check and the active flag are one mutex operation. Callers
    /// hold the returned guard for the whole prompt; dropping it without
    /// [`PromptGuard::commit`] releases the session without changing history.
    /// The prompt token is a child of this session's lifetime token.
    pub(crate) fn begin_prompt(
        self: &Arc<Self>,
    ) -> Result<(Vec<Message>, PromptGuard), PromptBusy> {
        let mut state = self.lock_state();
        if state.active.is_some() {
            return Err(PromptBusy);
        }
        let control = Arc::new(PromptControl {
            token: self.lifetime.child_token(),
            cancel_requested: AtomicBool::new(false),
            response_gate: Arc::new(Mutex::new(())),
        });
        state.active = Some(Arc::clone(&control));
        let history = state.history.clone();
        drop(state);
        Ok((
            history,
            PromptGuard {
                session: Arc::clone(self),
                control,
                released: false,
            },
        ))
    }

    /// Cancels the active prompt, returning whether one was active.
    ///
    /// Cancelling is idempotent and leaves the session available for a later
    /// prompt. The response gate is taken before the flag is set so a worker
    /// that has not yet chosen its final response observes the cancellation.
    pub(crate) fn cancel_prompt(&self) -> bool {
        let control = self.lock_state().active.clone();
        let Some(control) = control else {
            return false;
        };
        control.cancel();
        true
    }

    /// Cancels the session lifetime token and therefore any active prompt.
    pub(crate) fn cancel_lifetime(&self) {
        self.lifetime.cancel();
    }

    /// Waits until no prompt owns the session, up to `limit`.
    ///
    /// Returns `false` when the bound expires. Callers use this after
    /// cancelling a session so `session/close` and `session/delete` can wait
    /// for active work to observe cancellation before acknowledging.
    pub(crate) async fn wait_idle(&self, limit: Duration) -> bool {
        let mut activity = self.activity.subscribe();
        tokio::time::timeout(limit, async {
            loop {
                if self.lock_state().active.is_none() {
                    return;
                }
                if activity.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        .is_ok()
    }

    /// Appends a committed turn and trims the history.
    fn append_history(&self, messages: Vec<Message>) {
        let mut state = self.lock_state();
        state.history.extend(messages);
        trim_history(&mut state.history, self.limits);
    }

    /// Clears the active flag and wakes close/delete waiters.
    fn finish_prompt(&self) {
        self.lock_state().active = None;
        self.activity.send_replace(());
    }

    /// Locks the session state, recovering from a poisoned lock.
    fn lock_state(&self) -> MutexGuard<'_, SessionState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Returns the number of retained history messages (test support).
    #[cfg(test)]
    pub(crate) fn history_len(&self) -> usize {
        self.lock_state().history.len()
    }

    /// Returns a clone of the retained history (test support).
    #[cfg(test)]
    pub(crate) fn history_snapshot(&self) -> Vec<Message> {
        self.lock_state().history.clone()
    }
}

/// Mutable per-session state behind one mutex.
#[derive(Default)]
struct SessionState {
    /// Conversation retained from earlier prompts.
    history: Vec<Message>,
    /// Control block for the prompt currently owning the session.
    active: Option<Arc<PromptControl>>,
}

/// Cancellation and response ordering for one active prompt.
struct PromptControl {
    /// Prompt token, a child of the session lifetime token.
    token: CancellationToken,
    /// Whether the client explicitly sent `session/cancel`.
    cancel_requested: AtomicBool,
    /// Serializes the cancel handler with the worker's final response choice.
    response_gate: Arc<Mutex<()>>,
}

impl PromptControl {
    /// Marks the prompt cancelled and stops its underlying work.
    fn cancel(&self) {
        let _gate = self
            .response_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.cancel_requested.store(true, Ordering::SeqCst);
        self.token.cancel();
    }
}

/// The session already has an active prompt.
#[derive(Debug, Error)]
#[error("session busy")]
pub(crate) struct PromptBusy;

/// Exclusive ownership of one session prompt.
///
/// The guard releases the busy state on every exit path. A successful or
/// partial terminal turn calls [`PromptGuard::commit`]; an internal failure
/// simply drops the guard and leaves history unchanged. [`PromptGuard::release`]
/// can be called once the response is enqueued so the next prompt is not
/// rejected as busy after the client observes the result.
pub(crate) struct PromptGuard {
    /// Session whose active flag this guard owns.
    session: Arc<AcpSession>,
    /// Cancellation and ordering state shared with `session/cancel`.
    control: Arc<PromptControl>,
    /// Whether the guard already released the session.
    released: bool,
}

impl PromptGuard {
    /// Returns this prompt's cancellation token.
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.control.token
    }

    /// Returns whether the client explicitly cancelled this prompt.
    pub(crate) fn cancel_requested(&self) -> bool {
        self.control.cancel_requested.load(Ordering::SeqCst)
    }

    /// Returns the gate that orders cancellation against the final response.
    pub(crate) fn response_gate(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.control.response_gate)
    }

    /// Appends this turn's messages without releasing the session.
    ///
    /// Missing tool results are synthesized immediately after the assistant
    /// message that requested them. History is then trimmed at complete-turn
    /// boundaries.
    pub(crate) fn commit(&mut self, messages: Vec<Message>) {
        let messages = complete_tool_pairs(messages);
        self.session.append_history(messages);
    }

    /// Releases the active prompt so the session can accept another prompt.
    pub(crate) fn release(&mut self) {
        if !self.released {
            self.session.finish_prompt();
            self.released = true;
        }
    }
}

impl Drop for PromptGuard {
    fn drop(&mut self) {
        self.release();
    }
}

/// Registry of live ACP sessions.
pub(crate) struct AcpSessionRegistry {
    /// Canonical startup root every session cwd must remain inside.
    root: PathBuf,
    /// Maximum live sessions.
    max_sessions: usize,
    /// History limits copied into every new session.
    limits: HistoryLimits,
    /// Sessions keyed by protocol id.
    sessions: Mutex<HashMap<String, Arc<AcpSession>>>,
    /// Parent token for every session lifetime token.
    lifetime_parent: CancellationToken,
}

impl AcpSessionRegistry {
    /// Creates an empty registry rooted at an already canonical startup path.
    ///
    /// `lifetime_parent` is normally a child of the process shutdown token.
    /// Cancelling it or [`AcpSessionRegistry::cancel_all`] ends every session
    /// and its active prompt while leaving the process free to drain.
    pub(crate) fn new(
        root: PathBuf,
        max_sessions: usize,
        limits: HistoryLimits,
        lifetime_parent: CancellationToken,
    ) -> Self {
        Self {
            root,
            max_sessions,
            limits,
            sessions: Mutex::new(HashMap::new()),
            lifetime_parent,
        }
    }

    /// Creates and registers a session for `requested_cwd`.
    ///
    /// The path must be absolute, resolve to a directory, and stay inside the
    /// startup root after symlinks are resolved. The session limit is checked
    /// under the registry lock so concurrent `session/new` requests cannot
    /// overshoot it.
    pub(crate) async fn create(
        &self,
        requested_cwd: PathBuf,
    ) -> Result<Arc<AcpSession>, SessionError> {
        let canonical = self.validate_cwd(requested_cwd).await?;
        let sequence = SESSION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let id = format!("acp-{}-{sequence}", process_nonce());
        self.insert(id, canonical, Vec::new())
    }

    /// Restores a persisted session with its reconstructed history.
    ///
    /// The restored session uses the same validation and limit checks as a new
    /// session. History is completed and trimmed with the live commit rules so
    /// a recovered session cannot start with an orphan tool result.
    pub(crate) async fn restore(
        &self,
        id: String,
        requested_cwd: PathBuf,
        history: Vec<Message>,
    ) -> Result<Arc<AcpSession>, SessionError> {
        if id.is_empty() {
            return Err(SessionError::EmptyId);
        }
        let canonical = self.validate_cwd(requested_cwd).await?;
        let mut history = complete_tool_pairs(history);
        trim_history(&mut history, self.limits);
        self.insert(id, canonical, history)
    }

    /// Validates a requested session working directory.
    pub(crate) async fn validate_cwd(
        &self,
        requested_cwd: PathBuf,
    ) -> Result<PathBuf, SessionError> {
        if !requested_cwd.is_absolute() {
            return Err(SessionError::CwdNotAbsolute);
        }
        let canonical = tokio::fs::canonicalize(&requested_cwd)
            .await
            .map_err(SessionError::CwdUnavailable)?;
        let metadata = tokio::fs::metadata(&canonical)
            .await
            .map_err(SessionError::CwdUnavailable)?;
        if !metadata.is_dir() {
            return Err(SessionError::CwdNotDirectory);
        }
        if !canonical.starts_with(&self.root) {
            return Err(SessionError::CwdOutsideRoot);
        }
        Ok(canonical)
    }

    /// Inserts a session after validating the live-session bound.
    fn insert(
        &self,
        id: String,
        canonical: PathBuf,
        history: Vec<Message>,
    ) -> Result<Arc<AcpSession>, SessionError> {
        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        if sessions.contains_key(&id) {
            return Err(SessionError::AlreadyLoaded);
        }
        if sessions.len() >= self.max_sessions {
            return Err(SessionError::LimitReached {
                max_sessions: self.max_sessions,
            });
        }
        let session = Arc::new(AcpSession {
            id: id.clone(),
            cwd: canonical,
            state: Mutex::new(SessionState {
                history,
                active: None,
            }),
            turn_sequence: AtomicU64::new(0),
            limits: self.limits,
            activity: watch::channel(()).0,
            lifetime: self.lifetime_parent.child_token(),
        });
        sessions.insert(id, Arc::clone(&session));
        Ok(session)
    }

    /// Removes a session whose index could not be persisted.
    pub(crate) fn remove(&self, id: &str) {
        let removed = self
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id);
        if let Some(session) = removed {
            session.cancel_lifetime();
        }
    }

    /// Looks up a session by its protocol id.
    pub(crate) fn get(&self, id: &str) -> Option<Arc<AcpSession>> {
        self.sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    /// Cancels every session lifetime token and their active prompts.
    pub(crate) fn cancel_all(&self) {
        self.lifetime_parent.cancel();
        let sessions: Vec<Arc<AcpSession>> = self
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for session in sessions {
            session.cancel_lifetime();
        }
    }

    /// Returns the number of live sessions (test support).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

/// Assembles a prompt from supported content blocks in client order.
///
/// Text blocks are joined with a newline; empty text blocks contribute
/// nothing. A resource link becomes the single line
/// `[resource] name=... uri=...` with line breaks in either field collapsed to
/// spaces. Other content blocks are rejected before any model call.
pub(crate) fn prompt_text(
    prompt: &[ContentBlock],
    max_bytes: usize,
) -> Result<String, PromptContentError> {
    let mut parts = Vec::new();
    for block in prompt {
        match block {
            ContentBlock::Text(text) => {
                if !text.text.is_empty() {
                    parts.push(text.text.clone());
                }
            }
            ContentBlock::ResourceLink(link) => parts.push(format!(
                "[resource] name={} uri={}",
                single_line(&link.name),
                single_line(&link.uri)
            )),
            ContentBlock::Image(_) => {
                return Err(PromptContentError::Unsupported { kind: "image" });
            }
            ContentBlock::Audio(_) => {
                return Err(PromptContentError::Unsupported { kind: "audio" });
            }
            ContentBlock::Resource(_) => {
                return Err(PromptContentError::Unsupported { kind: "resource" });
            }
            _ => return Err(PromptContentError::Unsupported { kind: "unknown" }),
        }
    }

    let text = parts.join("\n");
    if text.trim().is_empty() {
        return Err(PromptContentError::Empty);
    }
    if text.len() > max_bytes {
        return Err(PromptContentError::TooLarge { max_bytes });
    }
    Ok(text)
}

/// Collapses carriage returns and newlines so a resource link stays one line.
fn single_line(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\n' | '\r' => ' ',
            other => other,
        })
        .collect()
}

/// Synthesizes a tool result for every unanswered assistant tool call.
///
/// The pump normally closes interrupted calls itself, but cancellation can
/// still leave a call unanswered if the event stream is torn down between the
/// assistant message and its result. This pass is idempotent for well-formed
/// transcripts and never reorders existing tool results.
pub(crate) fn complete_tool_pairs(messages: Vec<Message>) -> Vec<Message> {
    let mut completed = Vec::with_capacity(messages.len());
    let mut index = 0;
    while index < messages.len() {
        let message = &messages[index];
        if message.role == Role::Assistant && !message.tool_calls.is_empty() {
            completed.push(message.clone());
            let tool_calls = message.tool_calls.clone();
            index += 1;

            let mut answered = std::collections::HashSet::new();
            while index < messages.len() && messages[index].role == Role::Tool {
                if let Some(id) = &messages[index].tool_call_id {
                    answered.insert(id.clone());
                }
                completed.push(messages[index].clone());
                index += 1;
            }
            for call in tool_calls {
                if !answered.contains(&call.id) {
                    completed.push(Message::tool(call.id, INTERRUPTED_TOOL_RESULT));
                }
            }
        } else {
            completed.push(message.clone());
            index += 1;
        }
    }
    completed
}

/// Drops the oldest complete turns until both history limits hold.
///
/// A turn starts at a user message. If a single newest turn exceeds a limit it
/// is dropped too, leaving empty history rather than splitting a tool call
/// from its result.
pub(crate) fn trim_history(history: &mut Vec<Message>, limits: HistoryLimits) {
    let mut starts: Vec<usize> = history
        .iter()
        .enumerate()
        .filter(|(_, message)| message.role == Role::User)
        .map(|(index, _)| index)
        .collect();
    if starts.first().copied() != Some(0) {
        // Defensive: retain the prefix with the first turn so trimming only
        // ever removes whole user turns.
        starts.insert(0, 0);
    }

    let mut keep_from = history.len();
    for start in starts.into_iter().rev() {
        let candidate = &history[start..];
        let bytes = serde_json::to_vec(candidate).map_or(usize::MAX, |encoded| encoded.len());
        if candidate.len() > limits.max_messages || bytes > limits.max_bytes {
            break;
        }
        keep_from = start;
    }
    history.drain(..keep_from);
}

#[cfg(test)]
mod tests {
    use super::*;

    use agent_client_protocol::v1::schema::{
        AudioContent, EmbeddedResource, EmbeddedResourceResource, ImageContent, ResourceLink,
        TextContent,
    };
    use jessica_core::types::ToolCall;
    use serde_json::json;

    /// Builds a session registry rooted at a fresh temporary directory.
    async fn registry(label: &str, max_sessions: usize) -> (AcpSessionRegistry, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "jessica-acp-session-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock must follow the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("temporary root must be creatable");
        let root = std::fs::canonicalize(&root).expect("temporary root must resolve");
        (
            AcpSessionRegistry::new(
                root.clone(),
                max_sessions,
                HistoryLimits {
                    max_messages: 200,
                    max_bytes: 2 * 1024 * 1024,
                },
                CancellationToken::new(),
            ),
            root,
        )
    }

    #[tokio::test]
    async fn cwd_validation_rejects_relative_missing_file_and_outside_paths() {
        let (registry, root) = registry("cwd", 4).await;
        let file = root.join("file.txt");
        std::fs::write(&file, "x").expect("file must be writable");
        let outside = std::env::temp_dir().join(format!(
            "jessica-acp-outside-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock must follow the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&outside).expect("outside directory must exist");

        let cases = [
            (PathBuf::from("relative"), "absolute"),
            (root.join("missing"), "existing directory"),
            (file, "directory"),
            (outside, "startup workspace root"),
        ];
        for (path, expected) in cases {
            let error = registry
                .create(path)
                .await
                .err()
                .expect("invalid cwd must be rejected");
            assert!(
                error.to_string().contains(expected),
                "unexpected error for {expected}: {error}"
            );
        }
        assert_eq!(
            registry.len(),
            0,
            "rejected requests must not create sessions"
        );
    }

    #[tokio::test]
    async fn session_limit_is_enforced_atomically() {
        let (registry, root) = registry("limit", 2).await;
        registry
            .create(root.clone())
            .await
            .expect("first session must be created");
        registry
            .create(root.clone())
            .await
            .expect("second session must be created");
        let error = registry
            .create(root)
            .await
            .err()
            .expect("limit must reject the third session");

        assert!(
            error.to_string().contains("maximum 2"),
            "limit error must include the bound: {error}"
        );
        assert_eq!(registry.len(), 2);
    }

    #[tokio::test]
    async fn one_active_prompt_per_session_is_enforced_atomically() {
        let (registry, root) = registry("busy", 1).await;
        let session = registry
            .create(root)
            .await
            .expect("session must be created");
        let (_history, guard) = session.begin_prompt().expect("first prompt must start");
        let busy = session
            .begin_prompt()
            .err()
            .expect("second prompt must be rejected");
        assert_eq!(busy.to_string(), "session busy");

        drop(guard);
        session
            .begin_prompt()
            .expect("a new prompt must start after the first ends");
    }

    #[tokio::test]
    async fn different_sessions_can_run_prompts_concurrently() {
        let (registry, root) = registry("parallel", 2).await;
        let first = registry.create(root.clone()).await.expect("first session");
        let second = registry.create(root).await.expect("second session");
        let (_history, first_guard) = first.begin_prompt().expect("first session prompt");
        let (_history, second_guard) = second
            .begin_prompt()
            .expect("another session must not be busy");

        assert_ne!(first.id(), second.id());
        assert!(first.id().starts_with("acp-"));
        assert!(!first_guard.token().is_cancelled());
        assert!(!second_guard.token().is_cancelled());
    }

    #[tokio::test]
    async fn session_ids_remain_unique_across_registries_in_one_process() {
        let (first_registry, first_root) = registry("unique-a", 2).await;
        let (second_registry, second_root) = registry("unique-b", 2).await;
        let first = first_registry
            .create(first_root)
            .await
            .expect("first registry session");
        let second = second_registry
            .create(second_root)
            .await
            .expect("second registry session");

        assert_ne!(
            first.id(),
            second.id(),
            "a process-wide sequence prevents index collisions across registries"
        );
        assert!(
            first
                .id()
                .starts_with(&format!("acp-{}-", std::process::id())),
            "the id must include the process component: {}",
            first.id()
        );
    }

    #[tokio::test]
    async fn restored_sessions_keep_the_persisted_id_and_trimmed_history() {
        let (_, root) = registry("restore", 2).await;
        let registry = AcpSessionRegistry::new(
            root.clone(),
            2,
            HistoryLimits {
                max_messages: 2,
                max_bytes: 2 * 1024 * 1024,
            },
            CancellationToken::new(),
        );
        let session = registry
            .restore(
                "acp-persisted-0".to_owned(),
                root,
                vec![
                    Message::system("bootstrap"),
                    Message::user("old"),
                    Message::assistant("old answer"),
                    Message::user("new"),
                    Message::assistant("new answer"),
                ],
            )
            .await
            .expect("session must be restored");

        assert_eq!(session.id(), "acp-persisted-0");
        assert_eq!(
            session.history_snapshot(),
            vec![Message::user("new"), Message::assistant("new answer")],
            "restore must enforce the same history limits as a live session"
        );
        let turn_id = session.next_turn_id();
        assert!(
            turn_id.as_str().starts_with("acp-persisted-0-") && turn_id.as_str().ends_with("-t0"),
            "turn ids continue from the restored session id: {turn_id}"
        );
        assert_ne!(
            turn_id.as_str(),
            "acp-persisted-0-t0",
            "a restored session must not reuse the original process's turn key"
        );
        assert_eq!(
            session.next_turn_id().as_str(),
            turn_id.as_str().replace("-t0", "-t1"),
            "the restored session still increments its own turn sequence"
        );
    }

    #[tokio::test]
    async fn restore_rejects_duplicate_ids_invalid_cwd_and_session_limit() {
        let (registry, root) = registry("restore-errors", 1).await;
        registry
            .restore("acp-same".to_owned(), root.clone(), Vec::new())
            .await
            .expect("first restore must succeed");

        let duplicate = registry
            .restore("acp-same".to_owned(), root.clone(), Vec::new())
            .await
            .err()
            .expect("duplicate restore must fail");
        assert_eq!(duplicate.to_string(), "session is already loaded");

        let outside = std::env::temp_dir().join(format!(
            "jessica-acp-restore-outside-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock must follow the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&outside).expect("outside directory must exist");
        let invalid = registry
            .restore("acp-outside".to_owned(), outside, Vec::new())
            .await
            .err()
            .expect("outside cwd must fail");
        assert!(invalid.to_string().contains("startup workspace root"));

        let limited = registry
            .restore("acp-second".to_owned(), root, Vec::new())
            .await
            .err()
            .expect("the configured session limit must still apply");
        assert!(limited.to_string().contains("maximum 1"));
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn cancel_prompt_is_idempotent_and_the_session_stays_usable() {
        let (registry, root) = registry("cancel", 1).await;
        let session = registry
            .create(root)
            .await
            .expect("session must be created");

        assert!(
            !session.cancel_prompt(),
            "cancel without an active prompt must be a no-op"
        );

        let (_history, mut guard) = session.begin_prompt().expect("prompt must start");
        assert!(
            session.cancel_prompt(),
            "the active prompt must be cancelled"
        );
        assert!(guard.token().is_cancelled());
        assert!(guard.cancel_requested());
        assert!(
            session.cancel_prompt(),
            "repeated cancellation must remain a no-op on the active prompt"
        );
        assert!(guard.token().is_cancelled());
        assert!(guard.cancel_requested());

        guard.release();
        session
            .begin_prompt()
            .expect("a cancelled prompt must not poison the session");
    }

    #[tokio::test]
    async fn session_lifetime_cancellation_reaches_prompt_tokens_only_for_that_session() {
        let lifetime = CancellationToken::new();
        let root = std::env::temp_dir().join(format!(
            "jessica-acp-lifetime-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock must follow the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("root must exist");
        let root = std::fs::canonicalize(root).expect("root must resolve");
        let registry = AcpSessionRegistry::new(
            root.clone(),
            2,
            HistoryLimits {
                max_messages: 200,
                max_bytes: 2 * 1024 * 1024,
            },
            lifetime.clone(),
        );
        let first = registry.create(root.clone()).await.expect("first session");
        let second = registry.create(root).await.expect("second session");
        let (_history, first_guard) = first.begin_prompt().expect("first prompt");
        let (_history, second_guard) = second.begin_prompt().expect("second prompt");

        lifetime.cancel();

        assert!(first_guard.token().is_cancelled());
        assert!(second_guard.token().is_cancelled());
        assert!(
            !first_guard.cancel_requested(),
            "process-level cancellation is not a client cancel"
        );

        drop(first_guard);
        first.cancel_lifetime();
        let (_history, replacement) = first
            .begin_prompt()
            .expect("the session lease is released after shutdown cancellation");
        assert!(
            replacement.token().is_cancelled(),
            "a prompt started after lifetime cancellation must start cancelled"
        );
    }

    #[test]
    fn prompt_content_keeps_order_and_renders_resource_links() {
        let prompt = vec![
            ContentBlock::Text(TextContent::new("first")),
            ContentBlock::ResourceLink(ResourceLink::new("notes\nfile", "file:///notes.txt")),
            ContentBlock::Text(TextContent::new("last")),
        ];

        let text = prompt_text(&prompt, 1024).expect("prompt must assemble");

        assert_eq!(
            text,
            "first\n[resource] name=notes file uri=file:///notes.txt\nlast"
        );
    }

    #[test]
    fn prompt_content_rejects_empty_oversized_and_unsupported_blocks() {
        assert_eq!(
            prompt_text(&[ContentBlock::Text(TextContent::new("  \n"))], 1024),
            Err(PromptContentError::Empty)
        );
        assert_eq!(
            prompt_text(&[ContentBlock::Text(TextContent::new("12345678"))], 8),
            Ok("12345678".to_owned()),
            "the configured byte limit is inclusive"
        );
        assert_eq!(
            prompt_text(&[ContentBlock::Text(TextContent::new("123456789"))], 8),
            Err(PromptContentError::TooLarge { max_bytes: 8 })
        );
        assert_eq!(
            prompt_text(&[ContentBlock::Text(TextContent::new("too long"))], 3),
            Err(PromptContentError::TooLarge { max_bytes: 3 })
        );
        for (block, kind) in [
            (
                ContentBlock::Image(ImageContent::new("image", "image/png")),
                "image",
            ),
            (
                ContentBlock::Audio(AudioContent::new("audio", "audio/wav")),
                "audio",
            ),
            (
                ContentBlock::Resource(EmbeddedResource::new(
                    EmbeddedResourceResource::TextResourceContents(
                        agent_client_protocol::v1::schema::TextResourceContents::new(
                            "contents",
                            "file:///a.txt",
                        ),
                    ),
                )),
                "resource",
            ),
        ] {
            assert_eq!(
                prompt_text(&[block], 1024),
                Err(PromptContentError::Unsupported { kind })
            );
        }
    }

    #[test]
    fn missing_tool_results_are_synthesized_without_reordering() {
        let messages = vec![
            Message::user("read"),
            Message::assistant_with_tool_calls(
                "",
                vec![
                    ToolCall::new("one", "read_file", json!({"path": "a"})),
                    ToolCall::new("two", "read_file", json!({"path": "b"})),
                ],
            ),
            Message::tool("one", "contents"),
        ];

        let completed = complete_tool_pairs(messages);

        assert_eq!(completed.len(), 4);
        assert_eq!(completed[2], Message::tool("one", "contents"));
        assert_eq!(completed[3].role, Role::Tool);
        assert_eq!(completed[3].tool_call_id.as_deref(), Some("two"));
        assert_eq!(
            completed[3].content,
            "tool error: tool execution was interrupted"
        );
    }

    #[tokio::test]
    async fn history_trimming_drops_oldest_complete_turn_and_keeps_pairs() {
        let root = std::env::temp_dir().join(format!(
            "jessica-acp-trim-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock must follow the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("root must exist");
        let root = std::fs::canonicalize(root).expect("root must resolve");
        let registry = AcpSessionRegistry::new(
            root.clone(),
            1,
            HistoryLimits {
                max_messages: 2,
                max_bytes: 2 * 1024 * 1024,
            },
            CancellationToken::new(),
        );
        let session = registry
            .create(root)
            .await
            .expect("session must be created");
        let (_history, mut guard) = session.begin_prompt().expect("prompt must start");
        let old_call = ToolCall::new("old", "read_file", json!({"path": "old"}));
        guard.commit(vec![
            Message::user("old"),
            Message::assistant_with_tool_calls("", vec![old_call]),
            Message::tool("old", "old result"),
            Message::assistant("old answer"),
            Message::user("new"),
            Message::assistant("new answer"),
        ]);

        let history = session.history_snapshot();
        assert_eq!(
            history.len(),
            2,
            "only the newest complete turn must remain"
        );
        assert_eq!(history[0], Message::user("new"));
        assert_eq!(history[1], Message::assistant("new answer"));
        assert!(
            !history.iter().any(|message| message.role == Role::Tool),
            "trimming must not leave orphan tool results"
        );
    }

    #[tokio::test]
    async fn history_byte_trimming_counts_whole_turns() {
        let limits = HistoryLimits {
            max_messages: 200,
            max_bytes: 120,
        };
        let root = std::env::temp_dir().join(format!("jessica-acp-bytes-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("root must exist");
        let root = std::fs::canonicalize(root).expect("root must resolve");
        let registry = AcpSessionRegistry::new(root.clone(), 1, limits, CancellationToken::new());
        let session = registry
            .create(root)
            .await
            .expect("session must be created");
        let (_history, mut guard) = session.begin_prompt().expect("prompt must start");
        guard.commit(vec![
            Message::user("first message"),
            Message::assistant("first answer"),
            Message::user("second message"),
            Message::assistant("second answer"),
        ]);

        let history = session.history_snapshot();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0], Message::user("second message"));
    }

    #[test]
    fn history_limits_are_inclusive_and_only_excess_turns_are_dropped() {
        let messages = vec![
            Message::user("first message"),
            Message::assistant("first answer"),
            Message::user("second message"),
            Message::assistant("second answer"),
        ];
        let exact_bytes = serde_json::to_vec(&messages)
            .expect("history messages must serialize")
            .len();

        let mut at_both_limits = messages.clone();
        trim_history(
            &mut at_both_limits,
            HistoryLimits {
                max_messages: messages.len(),
                max_bytes: exact_bytes,
            },
        );
        assert_eq!(
            at_both_limits, messages,
            "a history exactly at both limits must be retained"
        );

        let mut one_message_over = messages.clone();
        trim_history(
            &mut one_message_over,
            HistoryLimits {
                max_messages: messages.len() - 1,
                max_bytes: exact_bytes,
            },
        );
        assert_eq!(
            one_message_over,
            messages[2..],
            "exceeding only the message limit must drop the oldest complete turn"
        );

        let mut one_byte_over = messages.clone();
        trim_history(
            &mut one_byte_over,
            HistoryLimits {
                max_messages: messages.len(),
                max_bytes: exact_bytes - 1,
            },
        );
        assert_eq!(
            one_byte_over,
            messages[2..],
            "exceeding only the byte limit must drop the oldest complete turn"
        );
    }

    #[test]
    fn a_single_turn_larger_than_the_limit_is_dropped_whole() {
        let mut history = vec![
            Message::user("new"),
            Message::assistant_with_tool_calls(
                "",
                vec![ToolCall::new("call", "read_file", json!({"path": "a"}))],
            ),
            Message::tool("call", "contents"),
        ];

        trim_history(
            &mut history,
            HistoryLimits {
                max_messages: 2,
                max_bytes: 1024,
            },
        );

        assert!(
            history.is_empty(),
            "a partial turn must never remain after trimming"
        );
    }
}
