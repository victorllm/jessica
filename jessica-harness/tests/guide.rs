//! Behavioral tests for the Guide abstraction and MemoryGuide rendering.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use jessica_core::memory::{MemoryEntry, MemoryError, MemoryStore};
use jessica_core::types::{Role, SessionId};
use jessica_harness::{Guide, MemoryGuide};

/// Title rendered at the start of every memory context message.
const TITLE: &str = "[记忆参考 — 以下内容来自历史会话，仅供参考，不代表当前事实]";

/// Observable state of the mock memory store.
#[derive(Default)]
struct MockState {
    /// Entries returned by `recall`.
    entries: Vec<MemoryEntry>,
    /// Error returned by `recall`, when configured.
    error: Option<MemoryError>,
    /// Number of `recall` calls.
    recall_calls: usize,
    /// Number of `write` calls.
    write_calls: usize,
    /// Session passed to the last `recall`.
    last_session: Option<SessionId>,
    /// Query passed to the last `recall`.
    last_query: Option<String>,
    /// Limit passed to the last `recall`.
    last_limit: Option<usize>,
}

/// A behavioral `MemoryStore` mock that records calls.
struct MockStore {
    /// Recorded calls and configured results.
    state: Mutex<MockState>,
}

impl MockStore {
    /// Creates a store that returns `entries`.
    fn returning(entries: Vec<MemoryEntry>) -> Self {
        Self {
            state: Mutex::new(MockState {
                entries,
                ..MockState::default()
            }),
        }
    }

    /// Creates a store whose recall fails with `message`.
    fn failing(message: &str) -> Self {
        Self {
            state: Mutex::new(MockState {
                error: Some(MemoryError::Backend {
                    message: message.to_owned(),
                }),
                ..MockState::default()
            }),
        }
    }

    /// Returns the number of recall calls.
    fn recall_calls(&self) -> usize {
        self.state
            .lock()
            .expect("mock state mutex should not be poisoned")
            .recall_calls
    }

    /// Returns the number of write calls.
    fn write_calls(&self) -> usize {
        self.state
            .lock()
            .expect("mock state mutex should not be poisoned")
            .write_calls
    }

    /// Returns the session passed to the last recall.
    fn last_session(&self) -> Option<SessionId> {
        self.state
            .lock()
            .expect("mock state mutex should not be poisoned")
            .last_session
            .clone()
    }

    /// Returns the query passed to the last recall.
    fn last_query(&self) -> Option<String> {
        self.state
            .lock()
            .expect("mock state mutex should not be poisoned")
            .last_query
            .clone()
    }

    /// Returns the limit passed to the last recall.
    fn last_limit(&self) -> Option<usize> {
        self.state
            .lock()
            .expect("mock state mutex should not be poisoned")
            .last_limit
    }
}

#[async_trait]
impl MemoryStore for MockStore {
    async fn write(&self, _session: &SessionId, _entry: MemoryEntry) -> Result<(), MemoryError> {
        self.state
            .lock()
            .expect("mock state mutex should not be poisoned")
            .write_calls += 1;
        Ok(())
    }

    async fn recall(
        &self,
        session: &SessionId,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>, MemoryError> {
        let mut state = self
            .state
            .lock()
            .expect("mock state mutex should not be poisoned");
        state.recall_calls += 1;
        state.last_session = Some(session.clone());
        state.last_query = Some(query.to_owned());
        state.last_limit = Some(limit);

        if let Some(error) = state.error.clone() {
            return Err(error);
        }
        Ok(state.entries.clone())
    }
}

/// Builds a guide trait object over `store`.
fn guide(store: &Arc<MockStore>, limit: usize, max_inject_bytes: usize) -> Arc<dyn Guide> {
    let store = Arc::<MockStore>::clone(store);
    let store: Arc<dyn MemoryStore> = store;
    Arc::new(MemoryGuide::new(store, limit, max_inject_bytes))
}

#[tokio::test]
async fn empty_recall_returns_none_without_writing() {
    let store = Arc::new(MockStore::returning(Vec::new()));
    let guide = guide(&store, 7, 4096);
    let session = SessionId::new("session-1");

    let rendered = guide
        .render(&session, "what happened?")
        .await
        .expect("an empty recall must not fail");

    assert!(rendered.is_none());
    assert_eq!(store.recall_calls(), 1);
    assert_eq!(store.write_calls(), 0);
    assert_eq!(store.last_session(), Some(session));
    assert_eq!(store.last_query(), Some("what happened?".to_owned()));
    assert_eq!(store.last_limit(), Some(7));
}

#[tokio::test]
async fn non_empty_recall_renders_one_system_message_in_store_order() {
    let store = Arc::new(MockStore::returning(vec![
        MemoryEntry::new("second in store"),
        MemoryEntry::new("first in store"),
        MemoryEntry::new("third in store"),
    ]));
    let guide = guide(&store, 3, 4096);
    let session = SessionId::new("session-1");

    let message = guide
        .render(&session, "what is next?")
        .await
        .expect("a successful recall must render")
        .expect("non-empty recall must produce a message");

    assert_eq!(message.role, Role::System);
    assert_eq!(
        message.content,
        format!("{TITLE}\nsecond in store\n---\nfirst in store\n---\nthird in store")
    );
    assert!(message.tool_calls.is_empty());
    assert!(message.tool_call_id.is_none());
    assert_eq!(store.write_calls(), 0);
    assert_eq!(store.last_limit(), Some(3));
}

#[tokio::test]
async fn exact_budget_preserves_the_full_rendering() {
    let store = Arc::new(MockStore::returning(vec![
        MemoryEntry::new("记忆条目一"),
        MemoryEntry::new("记忆条目二"),
    ]));
    let session = SessionId::new("session-1");
    let full = format!("{TITLE}\n记忆条目一\n---\n记忆条目二");
    let guide = guide(&store, 2, full.len());

    let message = guide
        .render(&session, "query")
        .await
        .expect("recall should succeed")
        .expect("non-empty recall should render");

    assert_eq!(message.content, full);
}

#[tokio::test]
async fn oversized_budget_truncates_on_a_utf8_boundary() {
    let store = Arc::new(MockStore::returning(vec![
        MemoryEntry::new("记忆条目一"),
        MemoryEntry::new("记忆条目二"),
    ]));
    let session = SessionId::new("session-1");
    let full = format!("{TITLE}\n记忆条目一\n---\n记忆条目二");
    let boundary = full.len() - 3;
    assert!(full.is_char_boundary(boundary));
    let max_bytes = full.len() - 2;
    let guide = guide(&store, 2, max_bytes);

    let message = guide
        .render(&session, "query")
        .await
        .expect("recall should succeed")
        .expect("non-empty recall should render");

    assert_eq!(message.content, &full[..boundary]);
    assert!(message.content.len() <= max_bytes);
    assert!(
        full.starts_with(&message.content),
        "truncation must preserve a prefix"
    );
    assert!(
        message.content.ends_with("记忆条目"),
        "the final multi-byte character must be dropped whole"
    );
}

#[tokio::test]
async fn zero_budget_returns_none_for_non_empty_recall() {
    let store = Arc::new(MockStore::returning(vec![MemoryEntry::new("not rendered")]));
    let guide = guide(&store, 1, 0);
    let session = SessionId::new("session-1");

    let rendered = guide
        .render(&session, "query")
        .await
        .expect("recall should succeed");

    assert!(
        rendered.is_none(),
        "a zero budget must not produce an empty system message"
    );
    assert_eq!(store.recall_calls(), 1);
}

#[tokio::test]
async fn store_error_is_fail_open_and_never_writes() {
    let store = Arc::new(MockStore::failing("database offline"));
    let guide = guide(&store, 5, 4096);
    let session = SessionId::new("session-1");

    let rendered = guide
        .render(&session, "query")
        .await
        .expect("memory failures must not fail the turn");

    assert!(rendered.is_none());
    assert_eq!(store.recall_calls(), 1);
    assert_eq!(store.write_calls(), 0);
}
