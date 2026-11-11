//! Behavioral tests for layered recall, RRF fusion, and the frozen
//! `MemoryStore` adapter.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use jessica_core::memory::{MemoryEntry, MemoryError, MemoryStore};
use jessica_core::types::SessionId;
use jessica_memory::{
    DeterministicEmbedder, EmbedError, Embedder, RecallRequest, SqliteMemoryStore,
    SqliteMemoryStoreError, Timestamp,
};
use rusqlite::Connection;
use serde_json::json;
use sha2::{Digest, Sha256};

/// Process-local counter so parallel tests never share a database file.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A temporary SQLite database removed on drop.
struct TempDatabase {
    /// Database file path.
    path: PathBuf,
}

impl TempDatabase {
    /// Reserves a unique database path without creating the file.
    fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-memory-store-{}-{id}.db",
            std::process::id()
        ));
        Self { path }
    }

    /// Returns the database file path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.path.clone().into_os_string();
            path.push(suffix);
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Returns a unique, non-existent workspace path.
fn workspace_path(label: &str) -> PathBuf {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "jessica-memory-workspace-{label}-{}-{id}",
        std::process::id()
    ))
}

/// Appends one entry at `at`.
async fn write_at(store: &SqliteMemoryStore, session: &SessionId, content: &str, at: i64) {
    store
        .write_at(
            session,
            MemoryEntry::new(content),
            Timestamp::from_millis(at),
        )
        .await
        .expect("memory write should succeed");
}

/// Creates a recall request that only uses the short-term path.
fn short_only(session: &SessionId, limit: usize, max_bytes: usize) -> RecallRequest {
    let mut request = RecallRequest::new(session.clone(), "", limit);
    request.short_weight = 1.0;
    request.keyword_weight = 0.0;
    request.vector_weight = 0.0;
    request.max_bytes = max_bytes;
    request
}

/// Creates a recall request that only uses the keyword path.
fn keyword_only(session: &SessionId, query: &str, limit: usize) -> RecallRequest {
    let mut request = RecallRequest::new(session.clone(), query, limit);
    request.short_weight = 0.0;
    request.keyword_weight = 1.0;
    request.vector_weight = 0.0;
    request
}

/// Creates a recall request that only uses the vector path.
fn vector_only(session: &SessionId, query: &str, limit: usize) -> RecallRequest {
    let mut request = RecallRequest::new(session.clone(), query, limit);
    request.short_weight = 0.0;
    request.keyword_weight = 0.0;
    request.vector_weight = 1.0;
    request
}

/// Returns the content strings of `entries`.
fn contents(entries: &[MemoryEntry]) -> Vec<&str> {
    entries.iter().map(|entry| entry.content.as_str()).collect()
}

/// Computes the expected workspace scope for `path`.
fn expected_scope(path: &Path) -> String {
    let digest = Sha256::digest(path.as_os_str().as_encoded_bytes());
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// An embedder that always fails, used to exercise degradation.
struct FailingEmbedder;

#[async_trait]
impl Embedder for FailingEmbedder {
    async fn embed(&self, _text: &str) -> Result<Vec<f32>, EmbedError> {
        Err(EmbedError::Failed {
            message: "synthetic embedder failure".to_owned(),
        })
    }

    fn model_id(&self) -> &str {
        "failing-v1"
    }

    fn dims(&self) -> usize {
        3
    }
}

#[tokio::test]
async fn trait_object_write_and_recall_preserve_metadata() {
    let store = SqliteMemoryStore::open_in_memory(
        workspace_path("trait"),
        Some(Arc::new(DeterministicEmbedder::new())),
    )
    .expect("in-memory store should open");
    let store: Arc<dyn MemoryStore> = Arc::new(store);
    let session = SessionId::new("session-1");

    store
        .write(
            &session,
            MemoryEntry {
                content: "the launch code is ORANGE-7".to_owned(),
                metadata: json!({"kind": "turn"}),
            },
        )
        .await
        .expect("trait write should succeed");
    let recalled = store
        .recall(&session, "launch code", 5)
        .await
        .expect("trait recall should succeed");

    assert_eq!(recalled.len(), 1);
    assert_eq!(recalled[0].content, "the launch code is ORANGE-7");
    assert_eq!(recalled[0].metadata, json!({"kind": "turn"}));
}

#[tokio::test]
async fn short_term_is_session_scoped_while_keyword_spans_the_workspace() {
    let store =
        SqliteMemoryStore::open_in_memory(workspace_path("scope"), None).expect("store opens");
    let session_a = SessionId::new("session-a");
    let session_b = SessionId::new("session-b");
    write_at(&store, &session_a, "alpha from session a", 1).await;
    write_at(&store, &session_b, "beta from session b", 2).await;

    let short_a = store
        .recall_with(short_only(&session_a, 10, 4096))
        .await
        .expect("short recall should succeed");
    assert_eq!(
        contents(&short_a),
        vec!["alpha from session a"],
        "the short-term path must not return another session's entries"
    );

    let keyword_a = store
        .recall_with(keyword_only(&session_a, "beta", 10))
        .await
        .expect("keyword recall should succeed");
    assert_eq!(
        contents(&keyword_a),
        vec!["beta from session b"],
        "the keyword path must span sessions inside one workspace"
    );
}

#[tokio::test]
async fn workspaces_are_isolated_across_all_recall_paths() {
    let database = TempDatabase::new();
    let workspace_a = workspace_path("isolation-a");
    let workspace_b = workspace_path("isolation-b");
    let embedder = Arc::new(DeterministicEmbedder::new());
    let store_a = SqliteMemoryStore::open(
        database.path(),
        &workspace_a,
        Some(Arc::clone(&embedder) as Arc<dyn Embedder>),
    )
    .expect("workspace-a store should open");
    let store_b = SqliteMemoryStore::open(
        database.path(),
        &workspace_b,
        Some(Arc::clone(&embedder) as Arc<dyn Embedder>),
    )
    .expect("workspace-b store should open");
    let session = SessionId::new("shared-session");

    store_a
        .write_at(
            &session,
            MemoryEntry {
                content: "shared alpha fact".to_owned(),
                metadata: json!({"owner": "a"}),
            },
            Timestamp::from_millis(1),
        )
        .await
        .expect("workspace-a write should succeed");
    store_b
        .write_at(
            &session,
            MemoryEntry {
                content: "shared alpha fact".to_owned(),
                metadata: json!({"owner": "b"}),
            },
            Timestamp::from_millis(2),
        )
        .await
        .expect("workspace-b write should succeed");

    let short_a = store_a
        .recall_with(short_only(&session, 10, 4096))
        .await
        .expect("workspace-a short recall should succeed");
    assert_eq!(short_a.len(), 1);
    assert_eq!(short_a[0].metadata, json!({"owner": "a"}));

    let keyword_a = store_a
        .recall_with(keyword_only(&session, "shared", 10))
        .await
        .expect("workspace-a keyword recall should succeed");
    assert_eq!(keyword_a.len(), 1);
    assert_eq!(keyword_a[0].metadata, json!({"owner": "a"}));

    let vector_a = store_a
        .recall_with(vector_only(&session, "shared alpha fact", 10))
        .await
        .expect("workspace-a vector recall should succeed");
    assert_eq!(vector_a.len(), 1);
    assert_eq!(vector_a[0].metadata, json!({"owner": "a"}));

    let vector_b = store_b
        .recall_with(vector_only(&session, "shared alpha fact", 10))
        .await
        .expect("workspace-b vector recall should succeed");
    assert_eq!(vector_b.len(), 1);
    assert_eq!(vector_b[0].metadata, json!({"owner": "b"}));
}

/// Seeds the two-list RRF fixture used by the ranking tests.
async fn seed_rrf_fixture(store: &SqliteMemoryStore, session_a: &SessionId, session_b: &SessionId) {
    write_at(store, session_a, "alpha shared", 2).await;
    write_at(store, session_a, "alpha unique-a", 1).await;
    write_at(store, session_a, "gamma only-short", 3).await;
    write_at(store, session_b, "alpha other-session", 4).await;
}

#[tokio::test]
async fn rrf_promotes_entries_found_by_multiple_paths() {
    let store =
        SqliteMemoryStore::open_in_memory(workspace_path("rrf"), None).expect("store opens");
    let session_a = SessionId::new("session-a");
    let session_b = SessionId::new("session-b");
    seed_rrf_fixture(&store, &session_a, &session_b).await;

    let mut request = RecallRequest::new(session_a.clone(), "alpha", 10);
    request.vector_weight = 0.0;
    let recalled = store
        .recall_with(request)
        .await
        .expect("fused recall should succeed");

    assert_eq!(
        contents(&recalled),
        vec![
            "alpha shared",
            "alpha unique-a",
            "gamma only-short",
            "alpha other-session",
        ],
        "entries on both lists must outrank single-list entries; equal scores break by entry id"
    );
}

#[tokio::test]
async fn path_weights_change_the_fused_order() {
    let store =
        SqliteMemoryStore::open_in_memory(workspace_path("weights"), None).expect("store opens");
    let session_a = SessionId::new("session-a");
    let session_b = SessionId::new("session-b");
    seed_rrf_fixture(&store, &session_a, &session_b).await;

    let mut request = RecallRequest::new(session_a.clone(), "alpha", 10);
    request.short_weight = 0.1;
    request.keyword_weight = 10.0;
    request.vector_weight = 0.0;
    let recalled = store
        .recall_with(request)
        .await
        .expect("weighted recall should succeed");

    assert_eq!(
        contents(&recalled),
        vec![
            "alpha other-session",
            "alpha shared",
            "alpha unique-a",
            "gamma only-short",
        ],
        "raising the keyword weight must reorder the fused result"
    );
}

#[tokio::test]
async fn long_term_recall_crosses_sessions_within_one_workspace() {
    let store = SqliteMemoryStore::open_in_memory(
        workspace_path("long-term"),
        Some(Arc::new(DeterministicEmbedder::new())),
    )
    .expect("store opens");
    let session_a = SessionId::new("session-a");
    let session_b = SessionId::new("session-b");
    write_at(&store, &session_a, "the deployment token is VECTOR-42", 1).await;
    write_at(&store, &session_b, "bananas are yellow", 2).await;

    let recalled = store
        .recall_with(vector_only(&session_b, "deployment token", 10))
        .await
        .expect("vector recall should succeed");
    assert!(
        recalled
            .iter()
            .any(|entry| entry.content == "the deployment token is VECTOR-42"),
        "the long-term path must find another session's entry inside the workspace"
    );
}

#[tokio::test]
async fn byte_cap_respects_utf8_boundaries_and_hard_limits() {
    let store =
        SqliteMemoryStore::open_in_memory(workspace_path("bytes"), None).expect("store opens");
    let session = SessionId::new("session");
    write_at(&store, &session, "记忆一", 1).await;
    write_at(&store, &session, "记忆二", 2).await;
    write_at(&store, &session, "记忆三", 3).await;

    let capped = store
        .recall_with(short_only(&session, 10, 20))
        .await
        .expect("capped recall should succeed");
    assert_eq!(
        contents(&capped),
        vec!["记忆三", "记忆"],
        "the second entry must be cut back to the previous UTF-8 boundary"
    );
    let used: usize = capped
        .iter()
        .map(|entry| entry.content.len())
        .sum::<usize>()
        + 3 * capped.len().saturating_sub(1);
    assert!(used <= 20, "returned content must fit the byte cap");

    let exact = store
        .recall_with(short_only(&session, 10, 9))
        .await
        .expect("exact-cap recall should succeed");
    assert_eq!(contents(&exact), vec!["记忆三"]);

    let oversized = store
        .recall_with(short_only(&session, 10, 4))
        .await
        .expect("oversized recall should succeed");
    assert_eq!(
        contents(&oversized),
        vec!["记"],
        "a single oversized entry must be truncated rather than dropped"
    );

    let zero_cap = store
        .recall_with(short_only(&session, 10, 0))
        .await
        .expect("zero-cap recall should succeed");
    assert!(zero_cap.is_empty());

    let zero_limit = store
        .recall_with(short_only(&session, 0, 100))
        .await
        .expect("zero-limit recall should succeed");
    assert!(zero_limit.is_empty());
}

#[tokio::test]
async fn failing_embedder_degrades_vector_recall_without_losing_the_entry() {
    let store = SqliteMemoryStore::open_in_memory(
        workspace_path("failing-embedder"),
        Some(Arc::new(FailingEmbedder)),
    )
    .expect("store opens");
    let session = SessionId::new("session");

    write_at(&store, &session, "keyword recovery fact", 1).await;

    let keyword = store
        .recall_with(keyword_only(&session, "recovery", 10))
        .await
        .expect("keyword recall should survive embedder failure");
    assert_eq!(contents(&keyword), vec!["keyword recovery fact"]);

    let short = store
        .recall_with(short_only(&session, 10, 4096))
        .await
        .expect("short recall should survive embedder failure");
    assert_eq!(contents(&short), vec!["keyword recovery fact"]);

    let vector = store
        .recall_with(vector_only(&session, "recovery", 10))
        .await
        .expect("a failed query embedding must degrade to an empty vector path");
    assert!(vector.is_empty());
}

#[tokio::test]
async fn keyword_query_failure_degrades_when_short_term_works() {
    let database = TempDatabase::new();
    let store = SqliteMemoryStore::open(database.path(), workspace_path("degrade-keyword"), None)
        .expect("store opens");
    let session = SessionId::new("session");
    write_at(&store, &session, "alpha recovery fact", 1).await;

    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute_batch("DROP TABLE memory_fts;")
            .expect("the FTS table should be removable in this degraded-path fixture");
    }

    let mut request = RecallRequest::new(session, "alpha", 10);
    request.vector_weight = 0.0;
    let recalled = store
        .recall_with(request)
        .await
        .expect("a failing keyword path must not fail the recall");
    assert_eq!(contents(&recalled), vec!["alpha recovery fact"]);
}

#[tokio::test]
async fn unavailable_database_is_a_backend_error() {
    let database = TempDatabase::new();
    let store = SqliteMemoryStore::open(database.path(), workspace_path("unavailable"), None)
        .expect("store opens");
    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute_batch("DROP TABLE memory_entries;")
            .expect("the entries table should be removable in this fixture");
    }

    let error = store
        .recall_with(RecallRequest::new(SessionId::new("session"), "anything", 5))
        .await
        .expect_err("every path failing must surface a backend error");
    let MemoryError::Backend { message } = error;
    assert!(
        message.contains("unavailable"),
        "backend error should explain the database is unavailable: {message}"
    );
}

#[tokio::test]
async fn duplicate_writes_are_deduplicated_and_soft_deletes_disappear() {
    let database = TempDatabase::new();
    let store = SqliteMemoryStore::open(
        database.path(),
        workspace_path("dedup"),
        Some(Arc::new(DeterministicEmbedder::new())),
    )
    .expect("store opens");
    let session = SessionId::new("session");
    write_at(&store, &session, "deduplicated fact", 1).await;
    write_at(&store, &session, "deduplicated fact", 2).await;

    let before = store
        .recall_with(short_only(&session, 10, 4096))
        .await
        .expect("short recall should succeed");
    assert_eq!(contents(&before), vec!["deduplicated fact"]);

    {
        let connection = Connection::open(database.path()).expect("raw connection should open");
        connection
            .execute(
                "UPDATE memory_entries SET deleted_at = 33 WHERE content = 'deduplicated fact'",
                [],
            )
            .expect("one-way soft delete should be permitted");
    }

    assert!(
        store
            .recall_with(short_only(&session, 10, 4096))
            .await
            .expect("short recall should succeed")
            .is_empty()
    );
    assert!(
        store
            .recall_with(keyword_only(&session, "deduplicated", 10))
            .await
            .expect("keyword recall should succeed")
            .is_empty()
    );
    assert!(
        store
            .recall_with(vector_only(&session, "deduplicated fact", 10))
            .await
            .expect("vector recall should succeed")
            .is_empty()
    );

    write_at(&store, &session, "deduplicated fact", 3).await;
    let rewritten = store
        .recall_with(short_only(&session, 10, 4096))
        .await
        .expect("short recall after re-append should succeed");
    assert_eq!(contents(&rewritten), vec!["deduplicated fact"]);
    let rewritten_vector = store
        .recall_with(vector_only(&session, "deduplicated fact", 10))
        .await
        .expect("vector recall after re-append should succeed");
    assert_eq!(contents(&rewritten_vector), vec!["deduplicated fact"]);

    let connection = Connection::open(database.path()).expect("raw connection should open");
    let count: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_entries", [], |row| row.get(0))
        .expect("row count should be readable");
    assert_eq!(count, 2, "the soft-deleted row must remain for audit");
}

#[tokio::test]
async fn reopening_preserves_entries_and_vectors() {
    let database = TempDatabase::new();
    let workspace = workspace_path("reopen");
    let session = SessionId::new("session");
    {
        let store = SqliteMemoryStore::open(
            database.path(),
            &workspace,
            Some(Arc::new(DeterministicEmbedder::new())),
        )
        .expect("store opens");
        write_at(&store, &session, "persisted vector fact", 1).await;
    }

    let store = SqliteMemoryStore::open(
        database.path(),
        &workspace,
        Some(Arc::new(DeterministicEmbedder::new())),
    )
    .expect("reopened store should open");
    let vector = store
        .recall_with(vector_only(&session, "persisted vector fact", 10))
        .await
        .expect("vector recall after reopen should succeed");
    assert_eq!(contents(&vector), vec!["persisted vector fact"]);

    let keyword = store
        .recall_with(keyword_only(&session, "persisted", 10))
        .await
        .expect("keyword recall after reopen should succeed");
    assert_eq!(contents(&keyword), vec!["persisted vector fact"]);
}

#[tokio::test]
async fn invalid_recall_parameters_are_rejected() {
    let store =
        SqliteMemoryStore::open_in_memory(workspace_path("invalid"), None).expect("store opens");
    let base = RecallRequest::new(SessionId::new("session"), "query", 5);

    let mut negative = base.clone();
    negative.short_weight = -0.1;
    let mut nan = base.clone();
    nan.keyword_weight = f32::NAN;
    let mut infinite = base.clone();
    infinite.vector_weight = f32::INFINITY;
    let mut zero_k = base.clone();
    zero_k.rrf_k = 0;

    for (request, expected) in [
        (negative, "short weight"),
        (nan, "keyword weight"),
        (infinite, "vector weight"),
        (zero_k, "rrf_k"),
    ] {
        match store.recall_with(request).await {
            Err(MemoryError::Backend { message }) => assert!(
                message.contains(expected),
                "error should mention {expected}: {message}"
            ),
            other => panic!("expected a backend error, got {other:?}"),
        }
    }

    let mut zero_weights = base;
    zero_weights.short_weight = 0.0;
    zero_weights.keyword_weight = 0.0;
    zero_weights.vector_weight = 0.0;
    assert!(
        store
            .recall_with(zero_weights)
            .await
            .expect("all-zero weights are valid")
            .is_empty()
    );
}

#[tokio::test]
async fn empty_writes_are_rejected_through_both_entry_points() {
    let store =
        SqliteMemoryStore::open_in_memory(workspace_path("empty"), None).expect("store opens");
    let session = SessionId::new("session");

    for content in ["", "   ", "\t\n"] {
        let error = store
            .write_at(
                &session,
                MemoryEntry::new(content),
                Timestamp::from_millis(1),
            )
            .await
            .expect_err("empty content must be rejected");
        assert!(matches!(
            error,
            MemoryError::Backend { ref message } if message.contains("must not be empty")
        ));
    }

    let trait_store: &dyn MemoryStore = &store;
    let error = trait_store
        .write(&session, MemoryEntry::new(""))
        .await
        .expect_err("the frozen trait must reject empty content too");
    assert!(matches!(
        error,
        MemoryError::Backend { ref message } if message.contains("must not be empty")
    ));
}

#[test]
fn workspace_scope_is_the_sha256_prefix_and_is_fixed_at_construction() {
    let workspace = workspace_path("scope-hash");
    let store = SqliteMemoryStore::open_in_memory(&workspace, None).expect("store opens");
    let expected = expected_scope(&workspace);
    assert_eq!(expected.len(), 16);
    assert_eq!(store.workspace_scope().as_str(), expected);

    let same = SqliteMemoryStore::open_in_memory(&workspace, None).expect("store opens");
    assert_eq!(same.workspace_scope(), store.workspace_scope());

    let other = SqliteMemoryStore::open_in_memory(workspace_path("scope-hash-other"), None)
        .expect("store opens");
    assert_ne!(other.workspace_scope(), store.workspace_scope());

    let error = SqliteMemoryStore::open_in_memory(Path::new(""), None)
        .expect_err("an empty workspace path must be rejected");
    assert!(matches!(error, SqliteMemoryStoreError::EmptyWorkspacePath));
}

#[test]
fn store_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SqliteMemoryStore>();
}
