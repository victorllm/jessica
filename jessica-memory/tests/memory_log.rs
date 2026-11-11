//! Behavioral tests for the append-only layered-memory storage foundation.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_core::types::SessionId;
use jessica_memory::{
    BruteForceIndex, MemoryEntryId, MemoryLog, MemoryLogError, NewMemoryEntry, Timestamp,
    VectorIndex, WorkspaceScope,
};
use rusqlite::Connection;
use serde_json::json;

/// Unique database file removed when the test finishes.
struct TempDatabase {
    /// Path passed to [`MemoryLog::open`].
    path: PathBuf,
}

impl TempDatabase {
    /// Creates an unused path in the system temporary directory.
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-memory-log-{label}-{}-{unique}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }

    /// Returns the database path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        for suffix in ["-journal", "-wal", "-shm"] {
            let mut sidecar = self.path.as_os_str().to_owned();
            sidecar.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(sidecar));
        }
    }
}

/// Builds a non-empty workspace scope.
fn workspace(scope: &str) -> WorkspaceScope {
    WorkspaceScope::new(scope).expect("test workspace scope should be valid")
}

/// Builds a new entry for tests.
fn entry(
    session: &str,
    workspace: &WorkspaceScope,
    content: &str,
    created_at: i64,
) -> NewMemoryEntry {
    NewMemoryEntry::new(
        SessionId::new(session),
        workspace.clone(),
        content,
        Timestamp::from_millis(created_at),
    )
    .expect("test entry should be valid")
}

/// Appends an entry and requires it to be new.
fn append(
    log: &MemoryLog,
    session: &str,
    workspace: &WorkspaceScope,
    content: &str,
    created_at: i64,
) -> MemoryEntryId {
    log.append(entry(session, workspace, content, created_at))
        .expect("append should succeed")
        .expect("entry should not be a duplicate")
}

#[test]
fn schema_version_is_two_and_data_survives_reopen() {
    let database = TempDatabase::new("schema");
    let scope = workspace("workspace-a");

    {
        let log = MemoryLog::open(database.path()).expect("database should open");
        append(&log, "session-a", &scope, "persisted memory", 10);
    }

    let raw = Connection::open(database.path()).expect("raw connection should open");
    let version: i64 = raw
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version should be readable");
    assert_eq!(version, 2);
    drop(raw);

    let log = MemoryLog::open(database.path()).expect("database should reopen");
    let records = log
        .recent_for_session(&SessionId::new("session-a"), &scope, 10)
        .expect("recent entries should load");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].content(), "persisted memory");
    assert_eq!(records[0].deleted_at(), None);
}

#[test]
fn future_schema_version_is_rejected() {
    let database = TempDatabase::new("future-version");
    {
        let raw = Connection::open(database.path()).expect("raw connection should open");
        raw.execute_batch("PRAGMA user_version = 3;")
            .expect("future version should be writable");
    }

    let error = MemoryLog::open(database.path()).expect_err("newer schema must be rejected");
    assert!(matches!(
        error,
        MemoryLogError::UnsupportedVersion {
            found: 3,
            supported: 2
        }
    ));
}

#[test]
fn version_one_database_is_migrated_without_losing_entries() {
    let database = TempDatabase::new("migration");
    let scope = workspace("workspace");
    let entry_id;

    {
        let log = MemoryLog::open(database.path()).expect("database should open");
        entry_id = append(&log, "session", &scope, "survives migration", 1);
    }

    {
        let raw = Connection::open(database.path()).expect("raw connection should open");
        raw.execute_batch(
            "DROP TABLE memory_embeddings;
             PRAGMA user_version = 1;",
        )
        .expect("version-one state should be reproducible");
    }

    let log = MemoryLog::open(database.path()).expect("version one must migrate");
    let recent = log
        .recent_for_session(&SessionId::new("session"), &scope, 10)
        .expect("entries should survive migration");
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].id(), entry_id);

    let index = BruteForceIndex::new(&log);
    index
        .add(
            entry_id,
            "test-model",
            &[1.0, 0.0],
            Timestamp::from_millis(2),
        )
        .expect("migration must create the embeddings table");
    let matches = index
        .search(&[1.0, 0.0], "test-model", &scope, 10)
        .expect("migrated vector storage should be searchable");
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].entry_id(), entry_id);

    let raw = Connection::open(database.path()).expect("raw connection should reopen");
    let version: i64 = raw
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version should be readable");
    assert_eq!(version, 2);
}

#[test]
fn special_character_queries_are_safe() {
    let log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace");
    append(&log, "session", &scope, "alpha beta gamma", 1);
    append(&log, "session", &scope, "100% coverage of foo_bar", 2);

    let queries = [
        "\"",
        "*",
        "AND",
        "NEAR",
        "-",
        "(",
        ")",
        ":",
        "%",
        "_",
        "\"foo\" OR \"bar\"",
        "NEAR(alpha beta)",
        "alpha:beta",
    ];

    for query in queries {
        let result = log.search_keyword(&scope, query, 10);
        assert!(result.is_ok(), "query {query:?} must not fail: {result:?}");
    }

    assert_eq!(
        log.search_keyword(&scope, "*", 10)
            .expect("symbol-only query should succeed")
            .len(),
        0,
        "a symbol-only query must not match everything"
    );

    let underscore = log
        .search_keyword(&scope, "_", 10)
        .expect("underscore query should succeed");
    assert_eq!(underscore.len(), 1);
    assert_eq!(underscore[0].content(), "100% coverage of foo_bar");
}

#[test]
fn keyword_search_handles_chinese_trigrams_and_short_like_queries() {
    let log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace");
    append(&log, "session", &scope, "记忆层使用 SQLite 全文检索", 1);
    append(
        &log,
        "session",
        &scope,
        "the deployment target is staging",
        2,
    );

    let chinese = log
        .search_keyword(&scope, "全文检索", 10)
        .expect("trigram query should succeed");
    assert_eq!(chinese.len(), 1);
    assert_eq!(chinese[0].content(), "记忆层使用 SQLite 全文检索");

    let two_character = log
        .search_keyword(&scope, "记忆", 10)
        .expect("short chinese query should fall back to LIKE");
    assert_eq!(two_character.len(), 1);
    assert_eq!(two_character[0].content(), "记忆层使用 SQLite 全文检索");

    let prefix = log
        .search_keyword(&scope, "deploy", 10)
        .expect("trigram prefix query should succeed");
    assert_eq!(prefix.len(), 1);
    assert_eq!(prefix[0].content(), "the deployment target is staging");

    assert!(
        log.search_keyword(&scope, "not-present", 10)
            .expect("missing query should succeed")
            .is_empty()
    );
}

#[test]
fn duplicates_are_ignored_within_scope_only() {
    let log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope_one = workspace("workspace-one");
    let scope_two = workspace("workspace-two");

    let first = append(&log, "session-one", &scope_one, "same content", 1);
    assert_eq!(
        log.append(entry("session-one", &scope_one, "same content", 2))
            .expect("duplicate append should succeed"),
        None
    );

    let other_session = log
        .append(entry("session-two", &scope_one, "same content", 3))
        .expect("other-session append should succeed")
        .expect("other session is a distinct scope");
    let other_workspace = log
        .append(entry("session-one", &scope_two, "same content", 4))
        .expect("other-workspace append should succeed")
        .expect("other workspace is a distinct scope");

    assert_ne!(first, other_session);
    assert_ne!(first, other_workspace);
    assert_eq!(
        log.recent_for_session(&SessionId::new("session-one"), &scope_one, 10)
            .expect("recent entries should load")
            .len(),
        1
    );
    assert_eq!(
        log.recent_for_session(&SessionId::new("session-two"), &scope_one, 10)
            .expect("recent entries should load")
            .len(),
        1
    );
    assert_eq!(
        log.recent_for_session(&SessionId::new("session-one"), &scope_two, 10)
            .expect("recent entries should load")
            .len(),
        1
    );
}

#[test]
fn soft_delete_hides_entry_from_all_recall_paths() {
    let mut log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace");
    let deleted_id = append(&log, "session", &scope, "delete me alpha", 1);
    let kept_id = append(&log, "session", &scope, "keep beta", 2);

    assert_eq!(
        log.search_keyword(&scope, "alpha", 10)
            .expect("search should succeed")
            .len(),
        1
    );

    log.soft_delete(deleted_id, Timestamp::from_millis(99))
        .expect("first soft delete should succeed");

    let record = log
        .get(deleted_id)
        .expect("deleted row should remain readable")
        .expect("deleted row should still exist");
    assert_eq!(record.deleted_at(), Some(Timestamp::from_millis(99)));

    assert!(
        log.search_keyword(&scope, "alpha", 10)
            .expect("search should succeed")
            .is_empty(),
        "FTS must not return the soft-deleted entry"
    );
    let recent = log
        .recent_for_session(&SessionId::new("session"), &scope, 10)
        .expect("recent entries should load");
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].id(), kept_id);

    let second_delete = log
        .soft_delete(deleted_id, Timestamp::from_millis(100))
        .expect_err("second soft delete must fail");
    assert!(matches!(
        second_delete,
        MemoryLogError::AlreadyDeleted { id } if id == deleted_id.get()
    ));

    let missing = MemoryEntryId::from_raw(deleted_id.get() + 1_000);
    let missing_delete = log
        .soft_delete(missing, Timestamp::from_millis(101))
        .expect_err("missing entry must fail");
    assert!(matches!(missing_delete, MemoryLogError::NotFound { .. }));
}

#[test]
fn soft_deleted_content_can_be_appended_again() {
    let mut log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace");
    let first = append(&log, "session", &scope, "repeatable content", 1);

    log.soft_delete(first, Timestamp::from_millis(2))
        .expect("soft delete should succeed");
    let second = log
        .append(entry("session", &scope, "repeatable content", 3))
        .expect("re-append should succeed")
        .expect("soft-deleted content may be stored again");

    assert_ne!(first, second);
    let recent = log
        .recent_for_session(&SessionId::new("session"), &scope, 10)
        .expect("recent entries should load");
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].id(), second);
}

#[test]
fn physical_updates_and_deletes_are_rejected() {
    let database = TempDatabase::new("tamper");
    let log = MemoryLog::open(database.path()).expect("database should open");
    let scope = workspace("workspace");
    let id = append(&log, "session", &scope, "original content", 1);

    let raw = Connection::open(database.path()).expect("raw connection should open");
    let update = raw.execute(
        "UPDATE memory_entries SET content = 'tampered' WHERE id = ?1",
        [id.get()],
    );
    assert!(update.is_err(), "content updates must be rejected");

    let combined = raw.execute(
        "UPDATE memory_entries SET deleted_at = 5, content = 'tampered' WHERE id = ?1",
        [id.get()],
    );
    assert!(
        combined.is_err(),
        "soft deletion combined with content changes must be rejected"
    );

    let delete = raw.execute("DELETE FROM memory_entries WHERE id = ?1", [id.get()]);
    assert!(delete.is_err(), "physical deletes must be rejected");

    let record = log
        .get(id)
        .expect("row should remain readable")
        .expect("row should still exist");
    assert_eq!(record.content(), "original content");
    assert_eq!(
        log.recent_for_session(&SessionId::new("session"), &scope, 10)
            .expect("recent entries should load")
            .len(),
        1
    );
}

#[test]
fn session_and_workspace_scopes_are_isolated() {
    let log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope_one = workspace("workspace-one");
    let scope_two = workspace("workspace-two");

    append(&log, "session-a", &scope_one, "alpha memory", 1);
    append(&log, "session-b", &scope_one, "beta memory", 2);
    append(&log, "session-a", &scope_two, "gamma memory", 3);

    let recent_a = log
        .recent_for_session(&SessionId::new("session-a"), &scope_one, 10)
        .expect("recent entries should load");
    assert_eq!(recent_a.len(), 1);
    assert_eq!(recent_a[0].content(), "alpha memory");

    let recent_b = log
        .recent_for_session(&SessionId::new("session-b"), &scope_one, 10)
        .expect("recent entries should load");
    assert_eq!(recent_b.len(), 1);
    assert_eq!(recent_b[0].content(), "beta memory");

    let workspace_one = log
        .search_keyword(&scope_one, "memory", 10)
        .expect("workspace search should succeed");
    assert_eq!(workspace_one.len(), 2);
    assert!(
        workspace_one
            .iter()
            .all(|record| record.workspace_scope() == &scope_one)
    );

    let workspace_two = log
        .search_keyword(&scope_two, "memory", 10)
        .expect("workspace search should succeed");
    assert_eq!(workspace_two.len(), 1);
    assert_eq!(workspace_two[0].content(), "gamma memory");
}

#[test]
fn same_timestamp_orders_by_row_id_descending() {
    let log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace");
    let first = append(&log, "session", &scope, "first", 1_000);
    let second = append(&log, "session", &scope, "second", 1_000);
    let third = append(&log, "session", &scope, "third", 1_000);

    let records = log
        .recent_for_session(&SessionId::new("session"), &scope, 10)
        .expect("recent entries should load");
    assert_eq!(
        records.iter().map(|record| record.id()).collect::<Vec<_>>(),
        vec![third, second, first]
    );
}

#[test]
fn limit_zero_returns_empty_and_limit_is_honored() {
    let log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace");
    append(&log, "session", &scope, "oldest", 1);
    append(&log, "session", &scope, "middle", 2);
    let newest = append(&log, "session", &scope, "newest", 3);

    assert!(
        log.recent_for_session(&SessionId::new("session"), &scope, 0)
            .expect("zero limit should succeed")
            .is_empty()
    );

    let records = log
        .recent_for_session(&SessionId::new("session"), &scope, 1)
        .expect("one-row limit should succeed");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id(), newest);
}

#[test]
fn metadata_round_trips_and_corrupt_metadata_is_reported() {
    let database = TempDatabase::new("metadata");
    let scope = workspace("workspace");

    {
        let log = MemoryLog::open(database.path()).expect("database should open");
        let id = log
            .append(
                entry("session", &scope, "with metadata", 1)
                    .with_metadata(json!({"kind": "turn", "tool_call_count": 3})),
            )
            .expect("append should succeed")
            .expect("entry should be new");
        let record = log
            .get(id)
            .expect("entry should load")
            .expect("entry should exist");
        assert_eq!(
            record.metadata(),
            &json!({"kind": "turn", "tool_call_count": 3})
        );
    }

    let raw = Connection::open(database.path()).expect("raw connection should open");
    raw.execute(
        "INSERT INTO memory_entries \
         (session_id, workspace_scope, content, metadata_json, created_at) \
         VALUES ('session', 'workspace', 'corrupt metadata', 'not json', 2)",
        [],
    )
    .expect("raw insert should succeed");
    let corrupt_id = raw.last_insert_rowid();
    drop(raw);

    let log = MemoryLog::open(database.path()).expect("database should reopen");
    let error = log
        .get(MemoryEntryId::from_raw(corrupt_id))
        .expect_err("corrupt metadata must not be silently accepted");
    assert!(matches!(
        error,
        MemoryLogError::Decode { id, .. } if id == corrupt_id
    ));
}

#[test]
fn memory_log_can_move_to_a_blocking_worker() {
    fn assert_send<T: Send>() {}
    assert_send::<MemoryLog>();
}

#[test]
fn empty_content_and_workspace_are_rejected() {
    let workspace_error =
        WorkspaceScope::new("   ").expect_err("whitespace workspace must be rejected");
    assert!(matches!(workspace_error, MemoryLogError::EmptyWorkspace));

    let content_error = NewMemoryEntry::new(
        SessionId::new("session"),
        workspace("workspace"),
        "   ",
        Timestamp::from_millis(1),
    )
    .expect_err("whitespace content must be rejected");
    assert!(matches!(content_error, MemoryLogError::EmptyContent));
}
