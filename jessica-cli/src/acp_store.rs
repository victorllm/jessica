//! Durable ACP session index and history reconstruction.
//!
//! Each ACP session is identified by the client-visible session id and owns one
//! small JSON index file under `[acp].state_dir`. The index records the
//! canonical working directory and the event-store turn ids committed by
//! prompts. Conversation history itself stays in the append-only event store:
//! loading a session reads the newest recoverable turn stream and projects the
//! recorded messages back into ACP protocol updates.
//!
//! The index is deliberately not a transcript. It never stores prompt text,
//! tool arguments, model output, environment values, or credentials. Missing
//! indexes are treated as unknown sessions; malformed indexes are reported as
//! internal load failures rather than silently discarded.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use jessica_core::types::{Message, Role, SessionId};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

use crate::acp_session::{HistoryLimits, complete_tool_pairs, trim_history};
use crate::app::CliError;
use crate::config::HarnessConfig;
use crate::events::EventRecorder;

/// Current on-disk index schema version.
const INDEX_VERSION: u64 = 1;

/// Serializes temporary file names used for atomic index replacement.
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// ACP persistence opened from configuration.
pub(crate) struct AcpPersistence {
    /// Per-session JSON index.
    store: AcpSessionStore,
    /// Event store used to reconstruct committed turns.
    recorder: Arc<EventRecorder>,
    /// ACP history limits applied after reconstruction.
    limits: HistoryLimits,
}

impl AcpPersistence {
    /// Opens the configured ACP session index and event store.
    ///
    /// Returns `None` when `[acp].state_dir` is absent. Configuration
    /// validation guarantees that a present state directory also has an
    /// event-store path. Both paths resolve against `root` when relative.
    pub(crate) async fn open(
        config: &HarnessConfig,
        root: &Path,
    ) -> Result<Option<Self>, CliError> {
        let Some(state_dir) = config.acp.state_dir.clone() else {
            return Ok(None);
        };
        let Some(events_path) = config.events.path.clone() else {
            return Err(CliError::AcpPersistenceWithoutEvents);
        };
        let state_dir = resolve_path(root, state_dir);
        let events_path = resolve_path(root, events_path);
        let store = AcpSessionStore::open(state_dir, config.acp.max_history_messages).await?;
        let recorder = Arc::new(EventRecorder::open(events_path).await?);
        Ok(Some(Self {
            store,
            recorder,
            limits: HistoryLimits {
                max_messages: config.acp.max_history_messages,
                max_bytes: config.acp.max_history_bytes,
            },
        }))
    }

    /// Creates the index for a newly allocated ACP session.
    pub(crate) async fn create_session(
        &self,
        session_id: &str,
        cwd: &Path,
    ) -> Result<(), AcpStoreError> {
        self.store
            .create_session(session_id, cwd, now_millis())
            .await
    }

    /// Adds one committed turn id to a session's index.
    pub(crate) async fn record_turn(
        &self,
        session_id: &str,
        turn_id: &SessionId,
    ) -> Result<(), AcpStoreError> {
        self.store
            .record_turn(session_id, turn_id.as_str(), now_millis())
            .await
    }

    /// Loads the index for one ACP session.
    pub(crate) async fn lookup(
        &self,
        session_id: &str,
    ) -> Result<Option<AcpSessionIndex>, AcpStoreError> {
        self.store.load(session_id).await
    }

    /// Reconstructs the most recent recoverable history for `index`.
    ///
    /// Turn ids are tried newest first so one missing or corrupt event stream
    /// does not make every earlier turn unavailable. A turn stream is accepted
    /// only when recovery reaches a stable checkpoint. System messages are
    /// ACP-internal bootstrap and never replayed; the remaining messages are
    /// completed and trimmed with the same rules as live history commits.
    pub(crate) async fn history_for(
        &self,
        index: &AcpSessionIndex,
    ) -> Result<Vec<Message>, AcpLoadError> {
        for turn_id in index.turns.iter().rev() {
            let turn_id = SessionId::new(turn_id.clone());
            let stored = match self.recorder.load(turn_id.clone()).await {
                Ok(Some(stored)) => stored,
                Ok(None) => {
                    tracing::warn!("acp session turn metadata is missing");
                    continue;
                }
                Err(_) => {
                    tracing::warn!("acp session turn metadata could not be read");
                    continue;
                }
            };
            let checkpoint = match jessica_runtime::recover_session(
                &turn_id,
                stored.initial_messages,
                &stored.events,
                None,
            ) {
                Ok(checkpoint) => checkpoint,
                Err(_) => {
                    tracing::warn!("acp session turn could not be recovered");
                    continue;
                }
            };

            let mut history = checkpoint.messages().to_vec();
            history.retain(|message| message.role != Role::System);
            let mut history = complete_tool_pairs(history);
            trim_history(&mut history, self.limits);
            return Ok(history);
        }

        if index.turns.is_empty() {
            Ok(Vec::new())
        } else {
            Err(AcpLoadError::NoRecoverableTurn {
                session_id: index.session_id.clone(),
            })
        }
    }
}

/// Resolves a configured path against the startup workspace root.
fn resolve_path(root: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

/// One ACP session's persisted index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AcpSessionIndex {
    /// On-disk schema version.
    version: u64,
    /// Client-visible ACP session id.
    session_id: String,
    /// Canonical working directory selected at session creation.
    cwd: PathBuf,
    /// Creation time in milliseconds since the Unix epoch.
    created_at: u64,
    /// Last successful update time in milliseconds since the Unix epoch.
    updated_at: u64,
    /// Committed event-store turn ids, oldest first.
    turns: Vec<String>,
}

impl AcpSessionIndex {
    /// Returns the canonical working directory recorded for this session.
    pub(crate) fn cwd(&self) -> &Path {
        &self.cwd
    }
}

/// Per-session JSON index rooted at one directory.
struct AcpSessionStore {
    /// Canonical state directory.
    directory: PathBuf,
    /// Maximum number of turn ids retained per session.
    max_turns: usize,
}

impl AcpSessionStore {
    /// Creates or opens the state directory.
    async fn open(directory: PathBuf, max_turns: usize) -> Result<Self, AcpStoreError> {
        let display_path = directory.clone();
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|source| AcpStoreError::CreateDirectory {
                path: display_path,
                source,
            })?;
        let directory = tokio::fs::canonicalize(&directory)
            .await
            .map_err(|source| AcpStoreError::ReadDirectory {
                path: directory,
                source,
            })?;
        let metadata = tokio::fs::metadata(&directory).await.map_err(|source| {
            AcpStoreError::ReadDirectory {
                path: directory.clone(),
                source,
            }
        })?;
        if !metadata.is_dir() {
            return Err(AcpStoreError::NotDirectory { path: directory });
        }
        Ok(Self {
            directory,
            max_turns: max_turns.max(1),
        })
    }

    /// Creates a new index, refusing to overwrite an existing session.
    async fn create_session(
        &self,
        session_id: &str,
        cwd: &Path,
        now: u64,
    ) -> Result<(), AcpStoreError> {
        if session_id.is_empty() {
            return Err(AcpStoreError::Corrupt {
                path: self.directory.clone(),
                reason: "empty session id".to_owned(),
            });
        }
        if cwd.as_os_str().is_empty() {
            return Err(AcpStoreError::Corrupt {
                path: self.directory.clone(),
                reason: "empty canonical cwd".to_owned(),
            });
        }
        let path = self.path_for(session_id);
        match tokio::fs::metadata(&path).await {
            Ok(_) => return Err(AcpStoreError::AlreadyExists { path }),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(AcpStoreError::Read { path, source }),
        }
        let index = AcpSessionIndex {
            version: INDEX_VERSION,
            session_id: session_id.to_owned(),
            cwd: cwd.to_path_buf(),
            created_at: now,
            updated_at: now,
            turns: Vec::new(),
        };
        self.write(&index).await
    }

    /// Appends one committed turn id and bounds the retained list.
    async fn record_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        now: u64,
    ) -> Result<(), AcpStoreError> {
        if turn_id.is_empty() {
            return Err(AcpStoreError::Corrupt {
                path: self.path_for(session_id),
                reason: "empty turn id".to_owned(),
            });
        }
        let mut index = self
            .load(session_id)
            .await?
            .ok_or_else(|| AcpStoreError::NotFound {
                session_id: session_id.to_owned(),
            })?;
        index.turns.retain(|recorded| recorded != turn_id);
        index.turns.push(turn_id.to_owned());
        if index.turns.len() > self.max_turns {
            let excess = index.turns.len() - self.max_turns;
            index.turns.drain(..excess);
        }
        index.updated_at = now;
        self.write(&index).await
    }

    /// Loads and validates one index.
    async fn load(&self, session_id: &str) -> Result<Option<AcpSessionIndex>, AcpStoreError> {
        let path = self.path_for(session_id);
        let encoded = match tokio::fs::read(&path).await {
            Ok(encoded) => encoded,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(AcpStoreError::Read { path, source }),
        };
        let index: AcpSessionIndex =
            serde_json::from_slice(&encoded).map_err(|source| AcpStoreError::Corrupt {
                path: path.clone(),
                reason: source.to_string(),
            })?;
        if index.version != INDEX_VERSION {
            return Err(AcpStoreError::UnsupportedVersion {
                path,
                version: index.version,
            });
        }
        if index.session_id != session_id {
            return Err(AcpStoreError::SessionMismatch {
                path,
                expected: session_id.to_owned(),
                found: index.session_id,
            });
        }
        if index.cwd.as_os_str().is_empty() || index.turns.iter().any(String::is_empty) {
            return Err(AcpStoreError::Corrupt {
                path,
                reason: "index contains an empty path or turn id".to_owned(),
            });
        }
        Ok(Some(index))
    }

    /// Atomically replaces one index through a temporary file and rename.
    async fn write(&self, index: &AcpSessionIndex) -> Result<(), AcpStoreError> {
        let path = self.path_for(&index.session_id);
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = self.directory.join(format!(
            ".{}.{}.{}.tmp",
            file_stem(&index.session_id),
            std::process::id(),
            sequence
        ));
        let encoded = serde_json::to_vec(index).map_err(|source| AcpStoreError::Corrupt {
            path: path.clone(),
            reason: source.to_string(),
        })?;

        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temporary)
                .await
                .map_err(|source| AcpStoreError::Write {
                    path: temporary.clone(),
                    source,
                })?;
            file.write_all(&encoded)
                .await
                .map_err(|source| AcpStoreError::Write {
                    path: temporary.clone(),
                    source,
                })?;
            file.sync_all()
                .await
                .map_err(|source| AcpStoreError::Write {
                    path: temporary.clone(),
                    source,
                })?;
            drop(file);
            tokio::fs::rename(&temporary, &path)
                .await
                .map_err(|source| AcpStoreError::Write {
                    path: path.clone(),
                    source,
                })
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result
    }

    /// Returns the hex-encoded index path for one session id.
    fn path_for(&self, session_id: &str) -> PathBuf {
        self.directory
            .join(format!("{}.json", hex_encode(session_id.as_bytes())))
    }
}

/// Returns the same filename stem used by [`AcpSessionStore::path_for`].
fn file_stem(session_id: &str) -> String {
    hex_encode(session_id.as_bytes())
}

/// Hex-encodes bytes without introducing a new dependency.
fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// ACP index failures.
///
/// Messages are used only in startup errors and diagnostics. JSON-RPC handlers
/// map every non-`NotFound` variant to a content-free internal error.
#[derive(Debug, Error)]
pub(crate) enum AcpStoreError {
    /// The state directory could not be created.
    #[error("ACP state directory `{path}` could not be created: {source}")]
    CreateDirectory {
        /// Requested directory.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// The state directory could not be canonicalized or inspected.
    #[error("ACP state directory `{path}` is not accessible: {source}")]
    ReadDirectory {
        /// Requested directory.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// The configured state path exists but is not a directory.
    #[error("ACP state path `{path}` is not a directory")]
    NotDirectory {
        /// Offending path.
        path: PathBuf,
    },
    /// An index could not be read.
    #[error("ACP session index `{path}` could not be read: {source}")]
    Read {
        /// Index path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// An index could not be written.
    #[error("ACP session index `{path}` could not be written: {source}")]
    Write {
        /// Index or temporary path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// An index was not valid JSON or violated the schema contract.
    #[error("ACP session index `{path}` is corrupt: {reason}")]
    Corrupt {
        /// Index path.
        path: PathBuf,
        /// Content-free decoding reason.
        reason: String,
    },
    /// The index uses a newer unsupported schema.
    #[error("ACP session index `{path}` uses unsupported version {version}")]
    UnsupportedVersion {
        /// Index path.
        path: PathBuf,
        /// Version found in the file.
        version: u64,
    },
    /// The file's session id does not match the requested id.
    #[error("ACP session index `{path}` contains session `{found}`, not `{expected}`")]
    SessionMismatch {
        /// Index path.
        path: PathBuf,
        /// Session id requested by the caller.
        expected: String,
        /// Session id stored in the file.
        found: String,
    },
    /// A new session id already has an index.
    #[error("ACP session index `{path}` already exists")]
    AlreadyExists {
        /// Colliding index path.
        path: PathBuf,
    },
    /// A turn update referenced a session without an index.
    #[error("ACP session `{session_id}` is not indexed")]
    NotFound {
        /// Missing session id.
        session_id: String,
    },
}

/// Failures while reconstructing history from indexed turns.
#[derive(Debug, Error)]
pub(crate) enum AcpLoadError {
    /// The index exists but every referenced turn was unrecoverable.
    #[error("ACP session `{session_id}` has no recoverable turn")]
    NoRecoverableTurn {
        /// Session id whose turns could not be read.
        session_id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    use jessica_core::types::{AgentEvent, SessionEndReason};
    use serde_json::json;

    /// Creates a unique temporary directory and returns its canonical path.
    async fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "jessica-acp-store-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock must follow the epoch")
                .as_nanos()
        ));
        tokio::fs::create_dir_all(&path)
            .await
            .expect("temporary directory must be created");
        tokio::fs::canonicalize(path)
            .await
            .expect("temporary directory must resolve")
    }

    #[tokio::test]
    async fn index_round_trips_and_turn_ids_are_bounded() {
        let directory = temp_dir("round-trip").await;
        let store = AcpSessionStore::open(directory, 2)
            .await
            .expect("store must open");
        store
            .create_session("acp-session", Path::new("/workspace"), 10)
            .await
            .expect("index must be created");

        for (index, turn) in ["turn-1", "turn-2", "turn-3"].into_iter().enumerate() {
            store
                .record_turn("acp-session", turn, 11 + index as u64)
                .await
                .expect("committed turn must be recorded");
        }

        let index = store
            .load("acp-session")
            .await
            .expect("index must load")
            .expect("index must exist");
        assert_eq!(index.version, INDEX_VERSION);
        assert_eq!(index.cwd, Path::new("/workspace"));
        assert_eq!(index.turns, vec!["turn-2", "turn-3"]);
        assert_eq!(index.created_at, 10);
        assert_eq!(index.updated_at, 13);
    }

    #[tokio::test]
    async fn missing_corrupt_and_mismatched_indexes_are_distinguishable() {
        let directory = temp_dir("errors").await;
        let store = AcpSessionStore::open(directory.clone(), 8)
            .await
            .expect("store must open");

        assert!(
            store
                .load("missing")
                .await
                .expect("missing index is not an I/O error")
                .is_none()
        );

        tokio::fs::write(store.path_for("corrupt"), b"{not-json")
            .await
            .expect("fixture index must be written");
        let corrupt = store
            .load("corrupt")
            .await
            .expect_err("corrupt JSON must fail");
        assert!(matches!(corrupt, AcpStoreError::Corrupt { .. }));

        tokio::fs::write(
            store.path_for("mismatch"),
            serde_json::to_vec(&AcpSessionIndex {
                version: INDEX_VERSION,
                session_id: "other".to_owned(),
                cwd: PathBuf::from("/workspace"),
                created_at: 1,
                updated_at: 1,
                turns: Vec::new(),
            })
            .expect("fixture must encode"),
        )
        .await
        .expect("fixture index must be written");
        let mismatch = store
            .load("mismatch")
            .await
            .expect_err("mismatched id must fail");
        assert!(matches!(mismatch, AcpStoreError::SessionMismatch { .. }));
    }

    #[tokio::test]
    async fn creating_an_existing_session_does_not_overwrite_its_turns() {
        let directory = temp_dir("duplicate").await;
        let store = AcpSessionStore::open(directory, 8)
            .await
            .expect("store must open");
        store
            .create_session("same", Path::new("/workspace"), 1)
            .await
            .expect("first index must be created");
        store
            .record_turn("same", "turn", 2)
            .await
            .expect("turn must be recorded");

        let error = store
            .create_session("same", Path::new("/other"), 3)
            .await
            .expect_err("duplicate creation must fail without overwriting");
        assert!(matches!(error, AcpStoreError::AlreadyExists { .. }));
        let index = store
            .load("same")
            .await
            .expect("index must load")
            .expect("index must exist");
        assert_eq!(index.cwd, Path::new("/workspace"));
        assert_eq!(index.turns, vec!["turn"]);
    }

    #[tokio::test]
    async fn history_uses_the_newest_recoverable_turn_and_drops_system_bootstrap() {
        let root = temp_dir("history").await;
        let events_path = root.join("events.sqlite");
        let state_dir = root.join("state");
        let recorder = Arc::new(
            EventRecorder::open(events_path)
                .await
                .expect("event store must open"),
        );
        let store = AcpSessionStore::open(state_dir, 8)
            .await
            .expect("session store must open");
        let persistence = AcpPersistence {
            store,
            recorder: Arc::clone(&recorder),
            limits: HistoryLimits {
                max_messages: 200,
                max_bytes: 2 * 1024 * 1024,
            },
        };
        persistence
            .create_session("acp-session", &root)
            .await
            .expect("index must be created");

        for (turn_id, initial, answer) in [
            (
                "turn-1",
                vec![Message::system("bootstrap"), Message::user("first")],
                "first answer",
            ),
            (
                "turn-2",
                vec![
                    Message::system("bootstrap"),
                    Message::user("first"),
                    Message::assistant("first answer"),
                    Message::user("second"),
                ],
                "second answer",
            ),
        ] {
            let turn_id = SessionId::new(turn_id);
            recorder
                .record_session_start(&turn_id, &initial)
                .await
                .expect("metadata must be recorded");
            for event in [
                AgentEvent::SessionStarted {
                    session_id: turn_id.clone(),
                },
                AgentEvent::ModelDelta {
                    text: answer.to_owned(),
                },
                AgentEvent::SessionEnded {
                    session_id: turn_id.clone(),
                    reason: SessionEndReason::Completed,
                },
            ] {
                recorder
                    .append(&turn_id, &event)
                    .await
                    .expect("event must be recorded");
            }
            persistence
                .record_turn("acp-session", &turn_id)
                .await
                .expect("turn index must be updated");
        }

        persistence
            .record_turn("acp-session", &SessionId::new("missing-newest"))
            .await
            .expect("a missing turn can still be indexed");
        let index = persistence
            .lookup("acp-session")
            .await
            .expect("index must load")
            .expect("index must exist");
        let history = persistence
            .history_for(&index)
            .await
            .expect("an older recoverable turn must be used");
        assert_eq!(
            history,
            vec![
                Message::user("first"),
                Message::assistant("first answer"),
                Message::user("second"),
                Message::assistant("second answer"),
            ],
            "the newest recoverable turn supplies the complete conversation"
        );
    }

    #[tokio::test]
    async fn an_indexed_session_without_recoverable_turns_fails_closed() {
        let root = temp_dir("unrecoverable").await;
        let events_path = root.join("events.sqlite");
        let state_dir = root.join("state");
        let recorder = Arc::new(
            EventRecorder::open(events_path)
                .await
                .expect("event store must open"),
        );
        let store = AcpSessionStore::open(state_dir, 8)
            .await
            .expect("session store must open");
        let persistence = AcpPersistence {
            store,
            recorder,
            limits: HistoryLimits {
                max_messages: 200,
                max_bytes: 2 * 1024 * 1024,
            },
        };
        persistence
            .create_session("acp-session", &root)
            .await
            .expect("index must be created");
        persistence
            .record_turn("acp-session", &SessionId::new("missing"))
            .await
            .expect("missing turn can be indexed");
        let index = persistence
            .lookup("acp-session")
            .await
            .expect("index must load")
            .expect("index must exist");

        let error = persistence
            .history_for(&index)
            .await
            .expect_err("unrecoverable history must fail instead of returning empty");
        assert!(matches!(error, AcpLoadError::NoRecoverableTurn { .. }));
    }

    #[test]
    fn hex_filenames_are_stable_and_do_not_contain_path_separators() {
        assert_eq!(hex_encode(b"acp-1-2"), "6163702d312d32");
        assert_eq!(
            file_stem("../../etc/passwd"),
            "2e2e2f2e2e2f6574632f706173737764"
        );
        assert!(!file_stem("a/b").contains('/'));
    }

    #[test]
    fn index_json_uses_camel_case_and_rejects_unknown_fields() {
        let encoded = serde_json::to_value(AcpSessionIndex {
            version: INDEX_VERSION,
            session_id: "acp-1".to_owned(),
            cwd: PathBuf::from("/workspace"),
            created_at: 1,
            updated_at: 2,
            turns: vec!["turn-1".to_owned()],
        })
        .expect("index must serialize");
        assert_eq!(encoded["sessionId"], json!("acp-1"));
        assert_eq!(encoded["createdAt"], json!(1));
        assert_eq!(encoded["turns"], json!(["turn-1"]));

        let error = serde_json::from_value::<AcpSessionIndex>(json!({
            "version": 1,
            "sessionId": "acp-1",
            "cwd": "/workspace",
            "createdAt": 1,
            "updatedAt": 2,
            "turns": [],
            "unexpected": true
        }))
        .expect_err("unknown index fields must fail");
        assert!(error.to_string().contains("unexpected"));
    }
}
