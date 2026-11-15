//! ACP session registry, prompt state, and bounded multi-turn history.
//!
//! A session owns one canonical working directory, the conversation retained
//! from completed prompts, and a lease that makes one prompt at a time active.
//! History is trimmed only at user-turn boundaries, so an assistant message
//! carrying tool calls can never be separated from its tool results.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use agent_client_protocol::v1::schema::ContentBlock;
use jessica_core::types::{Message, Role, SessionId};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

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
    /// Protocol-visible session id (`acp-{pid}-{seq}`).
    id: String,
    /// Canonical working directory validated at creation time.
    cwd: PathBuf,
    /// Conversation retained from previously completed prompts.
    state: Mutex<SessionState>,
    /// Monotonic per-session turn sequence.
    turn_sequence: AtomicU64,
    /// Limits applied whenever a prompt commits history.
    limits: HistoryLimits,
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
    pub(crate) fn next_turn_id(&self) -> SessionId {
        let sequence = self.turn_sequence.fetch_add(1, Ordering::Relaxed);
        SessionId::new(format!("{}-t{sequence}", self.id))
    }

    /// Marks a prompt active and snapshots the history it must receive.
    ///
    /// The busy check and the active flag are one mutex operation. Callers
    /// hold the returned guard for the whole prompt; dropping it without
    /// [`PromptGuard::commit`] releases the session without changing history.
    pub(crate) fn begin_prompt(
        self: &Arc<Self>,
        token: CancellationToken,
    ) -> Result<(Vec<Message>, PromptGuard), PromptBusy> {
        let mut state = self.lock_state();
        if state.active.is_some() {
            return Err(PromptBusy);
        }
        state.active = Some(token.clone());
        let history = state.history.clone();
        drop(state);
        Ok((
            history,
            PromptGuard {
                session: Arc::clone(self),
                token,
                finished: false,
            },
        ))
    }

    /// Clears the active flag and optionally appends a committed turn.
    fn finish_prompt(&self, commit: Option<Vec<Message>>) {
        let mut state = self.lock_state();
        state.active = None;
        if let Some(messages) = commit {
            state.history.extend(messages);
            trim_history(&mut state.history, self.limits);
        }
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
    /// Cancellation token for the prompt currently owning the session.
    active: Option<CancellationToken>,
}

/// The session already has an active prompt.
#[derive(Debug, Error)]
#[error("session busy")]
pub(crate) struct PromptBusy;

/// Exclusive ownership of one session prompt.
///
/// The guard releases the busy state on every exit path. A successful or
/// partial terminal turn calls [`PromptGuard::commit`]; an internal failure
/// simply drops the guard and leaves history unchanged.
pub(crate) struct PromptGuard {
    /// Session whose active flag this guard owns.
    session: Arc<AcpSession>,
    /// Cancellation token created for this prompt.
    token: CancellationToken,
    /// Whether the guard already released the session.
    finished: bool,
}

impl PromptGuard {
    /// Returns this prompt's cancellation token.
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Appends this turn's messages and releases the session.
    ///
    /// Missing tool results are synthesized immediately after the assistant
    /// message that requested them. History is then trimmed at complete-turn
    /// boundaries.
    pub(crate) fn commit(mut self, messages: Vec<Message>) {
        let messages = complete_tool_pairs(messages);
        self.session.finish_prompt(Some(messages));
        self.finished = true;
    }
}

impl Drop for PromptGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.session.finish_prompt(None);
        }
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
    /// Process-local session sequence.
    sequence: AtomicU64,
    /// Sessions keyed by protocol id.
    sessions: Mutex<HashMap<String, Arc<AcpSession>>>,
}

impl AcpSessionRegistry {
    /// Creates an empty registry rooted at an already canonical startup path.
    pub(crate) fn new(root: PathBuf, max_sessions: usize, limits: HistoryLimits) -> Self {
        Self {
            root,
            max_sessions,
            limits,
            sequence: AtomicU64::new(0),
            sessions: Mutex::new(HashMap::new()),
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

        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        if sessions.len() >= self.max_sessions {
            return Err(SessionError::LimitReached {
                max_sessions: self.max_sessions,
            });
        }
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let id = format!("acp-{}-{sequence}", std::process::id());
        let session = Arc::new(AcpSession {
            id: id.clone(),
            cwd: canonical,
            state: Mutex::new(SessionState::default()),
            turn_sequence: AtomicU64::new(0),
            limits: self.limits,
        });
        sessions.insert(id, Arc::clone(&session));
        Ok(session)
    }

    /// Looks up a session by its protocol id.
    pub(crate) fn get(&self, id: &str) -> Option<Arc<AcpSession>> {
        self.sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
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
        let (_history, guard) = session
            .begin_prompt(CancellationToken::new())
            .expect("first prompt must start");
        let busy = session
            .begin_prompt(CancellationToken::new())
            .err()
            .expect("second prompt must be rejected");
        assert_eq!(busy.to_string(), "session busy");

        drop(guard);
        session
            .begin_prompt(CancellationToken::new())
            .expect("a new prompt must start after the first ends");
    }

    #[tokio::test]
    async fn different_sessions_can_run_prompts_concurrently() {
        let (registry, root) = registry("parallel", 2).await;
        let first = registry.create(root.clone()).await.expect("first session");
        let second = registry.create(root).await.expect("second session");
        let _first_guard = first
            .begin_prompt(CancellationToken::new())
            .expect("first session prompt");
        second
            .begin_prompt(CancellationToken::new())
            .expect("another session must not be busy");

        assert_ne!(first.id(), second.id());
        assert!(first.id().starts_with("acp-"));
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
        );
        let session = registry
            .create(root)
            .await
            .expect("session must be created");
        let (_history, guard) = session
            .begin_prompt(CancellationToken::new())
            .expect("prompt must start");
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
        let registry = AcpSessionRegistry::new(root.clone(), 1, limits);
        let session = registry
            .create(root)
            .await
            .expect("session must be created");
        let (_history, guard) = session
            .begin_prompt(CancellationToken::new())
            .expect("prompt must start");
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
