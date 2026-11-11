//! Assembly of layered memory for the CLI.
//!
//! The CLI owns the boundary between configuration, the SQLite memory store,
//! and the backend-agnostic [`Guide`] used for context injection. Opening the
//! store is fail-open: a broken or unwritable database disables memory for the
//! run with a warning instead of failing the turn. Invalid configuration values
//! are rejected earlier by [`crate::config::HarnessConfig`] and never reach
//! this module.
//!
//! Writes are explicit and deterministic: only a turn that finishes with
//! [`SessionEndReason::Completed`](jessica_core::types::SessionEndReason) is
//! remembered, the content is built solely from the user input and the final
//! assistant answer, and tool outputs are never persisted.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use jessica_core::memory::{MemoryEntry, MemoryError, MemoryStore};
use jessica_core::types::{Message, SessionId};
use jessica_harness::{Guide, MemoryGuide};
use jessica_memory::{DeterministicEmbedder, Embedder, SqliteMemoryStore, Timestamp};
use serde_json::json;
use thiserror::Error;

use crate::config::{MemoryConfig, MemoryEmbedderKind, MemoryWritePolicy};

/// Maximum bytes kept from the user input of one remembered turn.
const USER_INPUT_MAX_BYTES: usize = 2 * 1024;

/// Maximum bytes kept from the final assistant answer of one remembered turn.
const ASSISTANT_ANSWER_MAX_BYTES: usize = 4 * 1024;

/// Separator placed between the user input and the final answer.
const TURN_SEPARATOR: &str = "\n---\n";

/// Memory components assembled for a CLI run.
pub(crate) struct MemoryAssembly {
    /// Store used for automatic completed-turn writes.
    store: Arc<SqliteMemoryStore>,
    /// Guide used to render context for fresh turns.
    guide: Arc<dyn Guide>,
    /// Automatic write policy from configuration.
    write_policy: MemoryWritePolicy,
}

impl MemoryAssembly {
    /// Opens the configured memory store and constructs its guide.
    ///
    /// Returns `None` when memory is disabled or cannot be opened. Open
    /// failures are warned about and degrade to a run without memory; they
    /// never fail the turn. Relative database paths resolve against `root`,
    /// and missing parent directories are created for an enabled store.
    pub(crate) async fn open(config: &MemoryConfig, root: &Path) -> Option<Self> {
        if !config.enabled {
            return None;
        }

        let database_path = resolve_path(root, &config.path);
        let workspace_root = root.to_path_buf();
        let embedder = embedder_for(config.embedder);
        let opened = tokio::task::spawn_blocking(move || {
            if let Some(parent) = database_path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent).map_err(|source| MemoryOpenError::Directory {
                    path: parent.to_path_buf(),
                    source,
                })?;
            }
            let workspace = std::fs::canonicalize(&workspace_root).unwrap_or(workspace_root);
            SqliteMemoryStore::open(&database_path, &workspace, embedder)
                .map_err(MemoryOpenError::Store)
        })
        .await;

        match opened {
            Ok(Ok(store)) => {
                let store = Arc::new(store);
                let memory_store = Arc::<SqliteMemoryStore>::clone(&store);
                let memory_store: Arc<dyn MemoryStore> = memory_store;
                let guide: Arc<dyn Guide> = Arc::new(MemoryGuide::new(
                    memory_store,
                    config.recall_limit,
                    config.max_inject_bytes,
                ));
                Some(Self {
                    store,
                    guide,
                    write_policy: config.write_policy,
                })
            }
            Ok(Err(error)) => {
                eprintln!("[warn] memory disabled for this run: {error}");
                None
            }
            Err(error) => {
                eprintln!("[warn] memory disabled for this run: memory open task failed: {error}");
                None
            }
        }
    }

    /// Renders recalled context for a fresh turn.
    ///
    /// Returns `None` when nothing is recalled or the guide fails. Guide
    /// failures are warned about and skipped.
    pub(crate) async fn recall(&self, session: &SessionId, query: &str) -> Option<Message> {
        match self.guide.render(session, query).await {
            Ok(message) => message,
            Err(error) => {
                eprintln!("[warn] memory recall failed: {error}");
                None
            }
        }
    }

    /// Writes one `kind=turn` entry when the configured policy allows it.
    ///
    /// Only the user input and final assistant answer are stored. Both are
    /// truncated on UTF-8 character boundaries. The store's unique constraint
    /// deduplicates identical turns without producing an error. With an
    /// `explicit` policy this method does nothing.
    pub(crate) async fn record_completed_turn(
        &self,
        session: &SessionId,
        user_input: &str,
        assistant_answer: &str,
        tool_call_count: usize,
        turn_index: u64,
    ) -> Result<(), MemoryError> {
        if self.write_policy != MemoryWritePolicy::Completed {
            return Ok(());
        }

        let entry = turn_entry(user_input, assistant_answer, tool_call_count, turn_index);
        self.store.write_at(session, entry, now()).await
    }
}

/// Resolves a configured database path against the workspace root.
fn resolve_path(root: &Path, configured: &Path) -> PathBuf {
    if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        root.join(configured)
    }
}

/// Returns the configured embedder, or `None` when the vector path is disabled.
fn embedder_for(kind: MemoryEmbedderKind) -> Option<Arc<dyn Embedder>> {
    match kind {
        MemoryEmbedderKind::Deterministic => Some(Arc::new(DeterministicEmbedder::new())),
        MemoryEmbedderKind::None => None,
    }
}

/// Builds the memory entry for one completed turn.
fn turn_entry(
    user_input: &str,
    assistant_answer: &str,
    tool_call_count: usize,
    turn_index: u64,
) -> MemoryEntry {
    let user = truncate_on_char_boundary(user_input, USER_INPUT_MAX_BYTES);
    let answer = truncate_on_char_boundary(assistant_answer, ASSISTANT_ANSWER_MAX_BYTES);
    let truncated = user.len() != user_input.len() || answer.len() != assistant_answer.len();

    MemoryEntry {
        content: format!("{user}{TURN_SEPARATOR}{answer}"),
        metadata: json!({
            "kind": "turn",
            "truncated": truncated,
            "tool_call_count": tool_call_count,
            "turn_index": turn_index,
        }),
    }
}

/// Returns the longest prefix of `content` of at most `max_bytes` bytes that
/// ends on a UTF-8 character boundary.
fn truncate_on_char_boundary(content: &str, max_bytes: usize) -> &str {
    if content.len() <= max_bytes {
        return content;
    }
    let mut end = max_bytes;
    while end > 0 && !content.is_char_boundary(end) {
        end -= 1;
    }
    &content[..end]
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
fn now() -> Timestamp {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    Timestamp::from_millis(millis)
}

/// Errors raised while opening the memory database.
#[derive(Debug, Error)]
enum MemoryOpenError {
    /// The parent directory could not be created.
    #[error("failed to create memory directory `{path}`: {source}")]
    Directory {
        /// Directory that could not be created.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The memory store could not be opened.
    #[error("failed to open memory store: {0}")]
    Store(#[from] jessica_memory::SqliteMemoryStoreError),
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use jessica_memory::{MemoryLog, MemoryRecord};

    use super::*;
    use crate::config::MemoryWritePolicy;

    /// Temporary directory removed when the test finishes.
    struct TempDir {
        /// Absolute path of the directory.
        path: PathBuf,
    }

    impl TempDir {
        /// Creates a uniquely named temporary directory.
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "jessica-cli-memory-{label}-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temporary directory should be created");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Builds an enabled configuration for `path`.
    fn enabled_config(path: &str) -> MemoryConfig {
        MemoryConfig {
            enabled: true,
            path: PathBuf::from(path),
            ..MemoryConfig::default()
        }
    }

    /// Reads live entries written by `assembly` for `session`.
    fn records_for(
        assembly: &MemoryAssembly,
        database: &Path,
        session: &SessionId,
    ) -> Vec<MemoryRecord> {
        let log = MemoryLog::open(database).expect("memory database should reopen");
        log.recent_for_session(session, assembly.store.workspace_scope(), 10)
            .expect("entries should load")
    }

    #[tokio::test]
    async fn disabled_memory_never_creates_the_database_or_its_directory() {
        let temp = TempDir::new("disabled");
        let config = MemoryConfig {
            enabled: false,
            ..enabled_config(".jessica/memory.db")
        };

        let assembly = MemoryAssembly::open(&config, &temp.path).await;

        assert!(assembly.is_none());
        assert!(!temp.path.join(".jessica").exists());
        assert!(!temp.path.join(".jessica/memory.db").exists());
    }

    #[tokio::test]
    async fn enabled_memory_resolves_relative_paths_and_creates_parent_directories() {
        let temp = TempDir::new("relative");
        let config = enabled_config(".jessica/memory.db");
        let session = SessionId::new("relative-session");

        let assembly = MemoryAssembly::open(&config, &temp.path)
            .await
            .expect("valid memory configuration should open");
        let database = temp.path.join(".jessica/memory.db");
        assert!(database.is_file(), "memory database should be created");

        assembly
            .record_completed_turn(&session, "remember the code", "the code is GREEN-2", 0, 0)
            .await
            .expect("completed turn should be written");

        let records = records_for(&assembly, &database, &session);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].content(),
            "remember the code\n---\nthe code is GREEN-2"
        );
    }

    #[tokio::test]
    async fn open_failure_is_fail_open_and_leaves_the_invalid_path_alone() {
        let temp = TempDir::new("open-failure");
        let blocker = temp.path.join(".jessica");
        std::fs::write(&blocker, b"not a directory").expect("blocking file should be written");
        let config = enabled_config(".jessica/memory.db");

        let assembly = MemoryAssembly::open(&config, &temp.path).await;

        assert!(
            assembly.is_none(),
            "open failures must degrade to no memory"
        );
        assert!(blocker.is_file());
        assert_eq!(
            std::fs::read(&blocker).expect("blocking file should remain readable"),
            b"not a directory"
        );
    }

    #[tokio::test]
    async fn completed_turn_truncates_user_and_answer_on_utf8_boundaries() {
        let temp = TempDir::new("truncate");
        let config = enabled_config("memory.db");
        let session = SessionId::new("truncate-session");
        let assembly = MemoryAssembly::open(&config, &temp.path)
            .await
            .expect("memory should open");

        let user_input = "用".repeat(1000);
        let assistant_answer = "答".repeat(2000);
        assembly
            .record_completed_turn(&session, &user_input, &assistant_answer, 4, 9)
            .await
            .expect("completed turn should be written");

        let expected_user = "用".repeat(682);
        let expected_answer = "答".repeat(1365);
        let records = records_for(&assembly, &temp.path.join("memory.db"), &session);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].content(),
            format!("{expected_user}{TURN_SEPARATOR}{expected_answer}")
        );
        assert!(records[0].content().is_char_boundary(expected_user.len()));
        assert_eq!(
            records[0].metadata(),
            &json!({
                "kind": "turn",
                "truncated": true,
                "tool_call_count": 4,
                "turn_index": 9,
            })
        );
    }

    #[tokio::test]
    async fn inputs_at_the_limits_are_not_marked_truncated() {
        let temp = TempDir::new("at-limit");
        let config = enabled_config("memory.db");
        let session = SessionId::new("at-limit-session");
        let assembly = MemoryAssembly::open(&config, &temp.path)
            .await
            .expect("memory should open");

        let user_input = "u".repeat(USER_INPUT_MAX_BYTES);
        let assistant_answer = "a".repeat(ASSISTANT_ANSWER_MAX_BYTES);
        assembly
            .record_completed_turn(&session, &user_input, &assistant_answer, 0, 0)
            .await
            .expect("completed turn should be written");

        let records = records_for(&assembly, &temp.path.join("memory.db"), &session);
        assert_eq!(
            records[0].content(),
            format!("{user_input}{TURN_SEPARATOR}{assistant_answer}")
        );
        assert_eq!(records[0].metadata()["truncated"], false);
    }

    #[tokio::test]
    async fn duplicate_completed_turns_do_not_add_rows() {
        let temp = TempDir::new("dedup");
        let config = enabled_config("memory.db");
        let session = SessionId::new("dedup-session");
        let assembly = MemoryAssembly::open(&config, &temp.path)
            .await
            .expect("memory should open");

        for _ in 0..2 {
            assembly
                .record_completed_turn(&session, "same question", "same answer", 1, 0)
                .await
                .expect("duplicate writes succeed without adding a row");
        }

        let records = records_for(&assembly, &temp.path.join("memory.db"), &session);
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn explicit_policy_never_writes_automatically() {
        let temp = TempDir::new("explicit");
        let config = MemoryConfig {
            write_policy: MemoryWritePolicy::Explicit,
            ..enabled_config("memory.db")
        };
        let session = SessionId::new("explicit-session");
        let assembly = MemoryAssembly::open(&config, &temp.path)
            .await
            .expect("memory should open");

        assembly
            .record_completed_turn(&session, "do not store", "this turn", 0, 0)
            .await
            .expect("explicit policy is a successful no-op");

        let records = records_for(&assembly, &temp.path.join("memory.db"), &session);
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn memory_without_an_embedder_still_recalls_the_completed_turn() {
        let temp = TempDir::new("no-embedder");
        let config = MemoryConfig {
            embedder: MemoryEmbedderKind::None,
            ..enabled_config("memory.db")
        };
        let session = SessionId::new("no-embedder-session");
        let assembly = MemoryAssembly::open(&config, &temp.path)
            .await
            .expect("memory should open");

        assembly
            .record_completed_turn(
                &session,
                "the build command is cargo test",
                "use cargo test",
                0,
                0,
            )
            .await
            .expect("completed turn should be written");

        let message = assembly
            .recall(&session, "what is the build command?")
            .await
            .expect("the completed turn should be recalled from the short-term path");
        assert_eq!(message.role, jessica_core::types::Role::System);
        assert!(message.content.contains("the build command is cargo test"));
        assert!(message.content.contains("use cargo test"));
    }
}
