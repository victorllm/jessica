//! Behavioral tests for the append-only skill library, FTS search, and the
//! asynchronous SQLite adapter.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jessica_core::types::SessionId;
use jessica_memory::{
    NewSkill, Skill, SkillId, SkillLog, SkillLogError, SkillSource, SkillStatus, SqliteSkillStore,
    Timestamp, WorkspaceScope,
};
use rusqlite::Connection;
use sha2::{Digest, Sha256};

/// Process-local counter so parallel tests never share a database file.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A temporary skills database removed on drop.
struct TempDatabase {
    /// Database file path.
    path: PathBuf,
}

impl TempDatabase {
    /// Reserves a unique database path in the system temporary directory.
    fn new(label: &str) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-skill-log-{label}-{}-{id}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }

    /// Returns the database file path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut path = self.path.clone().into_os_string();
            path.push(suffix);
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Builds a non-empty workspace scope.
fn workspace(scope: &str) -> WorkspaceScope {
    WorkspaceScope::new(scope).expect("test workspace scope should be valid")
}

/// Builds provenance for one test turn.
fn source(session: &str, turn_index: u64) -> SkillSource {
    SkillSource::new(
        SessionId::new(session),
        turn_index,
        turn_index + 3,
        turn_index + 2,
        0,
    )
}

/// Builds a valid skill using fixed test content.
#[allow(clippy::too_many_arguments)]
fn skill(
    workspace_scope: &WorkspaceScope,
    session: &str,
    turn_index: u64,
    name: &str,
    description: &str,
    instructions: &str,
    tags: &[&str],
    created_at: i64,
) -> NewSkill {
    NewSkill::new(
        workspace_scope.clone(),
        source(session, turn_index),
        name,
        description,
        instructions,
        tags.iter().map(|tag| (*tag).to_owned()).collect(),
        Timestamp::from_millis(created_at),
    )
    .expect("test skill should be valid")
}

/// Appends a skill and requires it to be new.
#[allow(clippy::too_many_arguments)]
fn append(
    log: &SkillLog,
    workspace_scope: &WorkspaceScope,
    session: &str,
    turn_index: u64,
    name: &str,
    description: &str,
    instructions: &str,
    tags: &[&str],
    created_at: i64,
) -> SkillId {
    log.append(skill(
        workspace_scope,
        session,
        turn_index,
        name,
        description,
        instructions,
        tags,
        created_at,
    ))
    .expect("append should succeed")
}

/// Returns the ids of a skill list.
fn ids(skills: &[Skill]) -> Vec<i64> {
    skills.iter().map(|skill| skill.id().get()).collect()
}

/// Encodes bytes as lowercase hexadecimal.
fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn schema_version_is_one_and_reopen_preserves_rows() {
    let database = TempDatabase::new("reopen");
    let scope = workspace("workspace-reopen");
    let id = {
        let log = SkillLog::open(database.path()).expect("skill log should open");
        append(
            &log,
            &scope,
            "session-reopen",
            1,
            "reopen me",
            "description",
            "instructions",
            &["one"],
            10,
        )
    };

    let raw = Connection::open(database.path()).expect("raw connection should open");
    let version: i64 = raw
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version should be readable");
    assert_eq!(version, 1);
    drop(raw);

    let reopened = SkillLog::open(database.path()).expect("skill log should reopen");
    let stored = reopened
        .get(id)
        .expect("get should succeed")
        .expect("skill should survive reopen");
    assert_eq!(stored.name(), "reopen me");
    assert_eq!(stored.status(), SkillStatus::Draft);
}

#[test]
fn future_schema_version_is_rejected() {
    let database = TempDatabase::new("future");
    {
        let raw = Connection::open(database.path()).expect("raw connection should open");
        raw.execute_batch("PRAGMA user_version = 2;")
            .expect("user_version should be writable");
    }

    let error = SkillLog::open(database.path()).expect_err("future schema must be rejected");
    match error {
        SkillLogError::UnsupportedVersion { found, supported } => {
            assert_eq!(found, 2);
            assert_eq!(supported, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn duplicate_source_is_rejected_and_soft_delete_allows_reinsert() {
    let mut log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-duplicate");
    let first = append(
        &log,
        &scope,
        "session-duplicate",
        1,
        "first name",
        "first description",
        "first instructions",
        &["one"],
        10,
    );

    let duplicate = skill(
        &scope,
        "session-duplicate",
        1,
        "second name",
        "second description",
        "second instructions",
        &["two"],
        20,
    )
    .with_auto_activate(true);
    match log.append(duplicate) {
        Err(SkillLogError::Duplicate { id }) => assert_eq!(id, first),
        other => panic!("expected duplicate id, got {other:?}"),
    }

    let stored = log
        .get(first)
        .expect("get should succeed")
        .expect("skill exists");
    assert_eq!(stored.name(), "first name");
    assert_eq!(stored.status(), SkillStatus::Draft);
    assert_eq!(stored.activated_at(), None);

    log.soft_delete(first, Timestamp::from_millis(30))
        .expect("first soft delete should succeed");
    let reinserted = append(
        &log,
        &scope,
        "session-duplicate",
        1,
        "second name",
        "second description",
        "second instructions",
        &["two"],
        40,
    );
    assert_ne!(reinserted, first);
    let stored = log
        .get(reinserted)
        .expect("get should succeed")
        .expect("reinserted skill exists");
    assert_eq!(stored.name(), "second name");
    assert_eq!(
        log.find_by_source(&scope, &SessionId::new("session-duplicate"), 1)
            .expect("find_by_source should succeed")
            .expect("live source should match")
            .id(),
        reinserted
    );
}

#[test]
fn content_hash_is_tag_order_insensitive_and_workspace_scoped() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope_one = workspace("workspace-a");
    let scope_two = workspace("workspace-b");
    let first = append(
        &log,
        &scope_one,
        "session-a",
        1,
        "shared skill",
        "shared description",
        "shared instructions",
        &["beta", "alpha"],
        10,
    );

    let stored = log
        .get(first)
        .expect("get should succeed")
        .expect("skill exists");
    assert_eq!(stored.tags(), ["alpha", "beta"]);
    let mut expected_hash = Sha256::new();
    expected_hash.update(b"shared skill");
    expected_hash.update([0]);
    expected_hash.update(b"shared description");
    expected_hash.update([0]);
    expected_hash.update(b"shared instructions");
    expected_hash.update([0]);
    expected_hash.update(br#"["alpha","beta"]"#);
    assert_eq!(
        stored.content_hash(),
        hex_lower(&expected_hash.finalize()),
        "content hash must follow name \\0 description \\0 instructions \\0 tags_json"
    );
    assert_eq!(stored.content_hash().len(), 64);
    assert!(
        stored
            .content_hash()
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    );

    match log.append(
        skill(
            &scope_one,
            "session-b",
            7,
            "shared skill",
            "shared description",
            "shared instructions",
            &["alpha", "beta"],
            20,
        )
        .with_auto_activate(true),
    ) {
        Err(SkillLogError::Duplicate { id }) => assert_eq!(id, first),
        other => panic!("expected content duplicate, got {other:?}"),
    }
    assert_eq!(
        log.get(first)
            .expect("get should succeed")
            .expect("skill exists")
            .status(),
        SkillStatus::Draft,
        "a content duplicate must not auto-activate the existing draft"
    );

    let other_workspace = append(
        &log,
        &scope_two,
        "session-a",
        1,
        "shared skill",
        "shared description",
        "shared instructions",
        &["alpha", "beta"],
        30,
    );
    let other = log
        .get(other_workspace)
        .expect("get should succeed")
        .expect("other workspace skill exists");
    assert_eq!(other.content_hash(), stored.content_hash());

    let different_tags = append(
        &log,
        &scope_one,
        "session-c",
        9,
        "shared skill",
        "shared description",
        "shared instructions",
        &["gamma"],
        40,
    );
    assert_ne!(different_tags, first);
}

#[test]
fn field_limits_are_enforced_without_truncation() {
    let scope = workspace("workspace-limits");
    let max_tags = (0..16)
        .map(|index| format!("{index:02}{}", "x".repeat(62)))
        .collect::<Vec<_>>();
    assert!(max_tags.iter().all(|tag| tag.len() == 64));

    let valid = skill(
        &scope,
        "session-limits",
        1,
        &"n".repeat(128),
        &"d".repeat(512),
        &"i".repeat(8192),
        &max_tags.iter().map(String::as_str).collect::<Vec<_>>(),
        10,
    );
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    log.append(valid).expect("boundary values should append");

    let oversized_name = NewSkill::new(
        scope.clone(),
        source("session-limits", 2),
        "n".repeat(129),
        "description",
        "instructions",
        Vec::new(),
        Timestamp::from_millis(20),
    );
    match oversized_name {
        Err(SkillLogError::FieldTooLong {
            field,
            max_bytes,
            actual,
        }) => {
            assert_eq!(field, "name");
            assert_eq!(max_bytes, 128);
            assert_eq!(actual, 129);
        }
        other => panic!("expected name limit error, got {other:?}"),
    }

    let multibyte_name = NewSkill::new(
        scope.clone(),
        source("session-limits", 9),
        "汉".repeat(43),
        "description",
        "instructions",
        Vec::new(),
        Timestamp::from_millis(25),
    );
    assert!(
        matches!(
            multibyte_name,
            Err(SkillLogError::FieldTooLong {
                field: "name",
                max_bytes: 128,
                actual: 129
            })
        ),
        "limits are measured in UTF-8 bytes, not characters"
    );

    let oversized_description = NewSkill::new(
        scope.clone(),
        source("session-limits", 3),
        "name",
        "d".repeat(513),
        "instructions",
        Vec::new(),
        Timestamp::from_millis(30),
    );
    assert!(matches!(
        oversized_description,
        Err(SkillLogError::FieldTooLong {
            field: "description",
            max_bytes: 512,
            actual: 513
        })
    ));

    let oversized_instructions = NewSkill::new(
        scope.clone(),
        source("session-limits", 4),
        "name",
        "description",
        "i".repeat(8193),
        Vec::new(),
        Timestamp::from_millis(40),
    );
    assert!(matches!(
        oversized_instructions,
        Err(SkillLogError::FieldTooLong {
            field: "instructions",
            max_bytes: 8192,
            actual: 8193
        })
    ));

    let oversized_tag = NewSkill::new(
        scope.clone(),
        source("session-limits", 5),
        "name",
        "description",
        "instructions",
        vec!["t".repeat(65)],
        Timestamp::from_millis(50),
    );
    assert!(matches!(
        oversized_tag,
        Err(SkillLogError::TagTooLong {
            index: 0,
            max_bytes: 64,
            actual: 65
        })
    ));

    let too_many_tags = NewSkill::new(
        scope.clone(),
        source("session-limits", 6),
        "name",
        "description",
        "instructions",
        (0..17)
            .map(|index| format!("{index:02}{}", "x".repeat(62)))
            .collect(),
        Timestamp::from_millis(60),
    );
    assert!(matches!(
        too_many_tags,
        Err(SkillLogError::TooManyTags {
            max: 16,
            actual: 17
        })
    ));

    let empty_name = NewSkill::new(
        scope.clone(),
        source("session-limits", 7),
        "   ",
        "description",
        "instructions",
        Vec::new(),
        Timestamp::from_millis(70),
    );
    assert!(matches!(
        empty_name,
        Err(SkillLogError::EmptyField { field: "name" })
    ));

    let empty_tag = NewSkill::new(
        scope,
        source("session-limits", 8),
        "name",
        "description",
        "instructions",
        vec!["  ".to_owned()],
        Timestamp::from_millis(80),
    );
    assert!(matches!(
        empty_tag,
        Err(SkillLogError::EmptyTag { index: 0 })
    ));
}

#[test]
fn keyword_search_is_safe_and_deterministic() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-search");
    let login = skill(
        &scope,
        "session-search",
        1,
        "登录错误修复",
        "login error handling",
        "deployable fix for authentication",
        &["auth"],
        10,
    )
    .with_auto_activate(true);
    let login = log.append(login).expect("active skill should append");
    let activated = log
        .get(login)
        .expect("get should succeed")
        .expect("active skill exists");
    assert_eq!(activated.status(), SkillStatus::Active);
    assert_eq!(
        activated.activated_at(),
        Some(Timestamp::from_millis(10)),
        "auto-activation uses the creation timestamp"
    );
    let deployment = skill(
        &scope,
        "session-search",
        2,
        "deployment guide",
        "staging rollout",
        "deploy careful changes",
        &["ops"],
        20,
    )
    .with_auto_activate(true);
    let deployment = log.append(deployment).expect("active skill should append");

    let chinese = log
        .search_keyword(&scope, "登录错误", 10, None)
        .expect("trigram query should succeed");
    assert_eq!(chinese.len(), 1);
    assert_eq!(chinese[0].0.id(), login);
    assert!(chinese[0].1 < 0.0, "bm25 should be negative for a match");

    let short_chinese = log
        .search_keyword(&scope, "登录", 10, None)
        .expect("short chinese query should use LIKE");
    assert_eq!(short_chinese.len(), 1);
    assert_eq!(short_chinese[0].0.id(), login);
    assert_eq!(short_chinese[0].1, 0.0);

    let prefix = log
        .search_keyword(&scope, "deploy", 10, None)
        .expect("prefix query should succeed");
    assert!(prefix.iter().any(|(skill, _)| skill.id() == deployment));
    assert!(prefix.iter().any(|(skill, _)| skill.id() == login));

    let special_queries = [
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
        "\"rust\" OR *",
        "NEAR(deploy guide)",
        "deploy:guide",
    ];
    for query in special_queries {
        let result = log.search_keyword(&scope, query, 10, None);
        assert!(result.is_ok(), "query {query:?} must not fail: {result:?}");
    }
    assert!(
        log.search_keyword(&scope, "*", 10, None)
            .expect("symbol-only query should succeed")
            .is_empty(),
        "a symbol-only query must not match everything"
    );

    let first = log
        .search_keyword(&scope, "deploy", 10, None)
        .expect("first query should succeed");
    let second = log
        .search_keyword(&scope, "deploy", 10, None)
        .expect("second query should succeed");
    assert_eq!(
        first
            .iter()
            .map(|(skill, score)| (skill.id().get(), score.to_bits()))
            .collect::<Vec<_>>(),
        second
            .iter()
            .map(|(skill, score)| (skill.id().get(), score.to_bits()))
            .collect::<Vec<_>>()
    );

    let filtered = log
        .search_keyword(&scope, "deploy", 10, Some(f64::MAX))
        .expect("high floor should succeed");
    assert!(filtered.is_empty(), "a positive floor must filter FTS hits");
    let disabled = log
        .search_keyword(&scope, "deploy", 10, Some(0.0))
        .expect("zero floor should be disabled");
    assert_eq!(disabled.len(), first.len());
    let short_filtered = log
        .search_keyword(&scope, "登录", 10, Some(0.1))
        .expect("short-query floor should succeed");
    assert!(
        short_filtered.is_empty(),
        "LIKE sentinel hits must not pass a positive floor"
    );
    assert!(matches!(
        log.search_keyword(&scope, "deploy", 10, Some(-1.0)),
        Err(SkillLogError::InvalidMinScore { .. })
    ));
    assert!(matches!(
        log.search_keyword(&scope, "deploy", 10, Some(f64::NAN)),
        Err(SkillLogError::InvalidMinScore { .. })
    ));
}

#[test]
fn drafts_are_invisible_until_activation_and_delete_hides_fts() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace-lifecycle");
    let id = append(
        &log,
        &scope,
        "session-lifecycle",
        1,
        "alpha lifecycle",
        "draft description",
        "draft instructions",
        &["draft"],
        10,
    );

    assert!(
        log.search_keyword(&scope, "alpha", 10, None)
            .expect("search should succeed")
            .is_empty(),
        "draft must not be searchable"
    );
    assert_eq!(
        ids(&log.list_recent(&scope, 10).expect("list works")),
        [id.get()]
    );
    assert_eq!(
        ids(&log
            .list_by_status(&scope, SkillStatus::Draft, 10)
            .expect("list works")),
        [id.get()]
    );
    assert!(
        log.list_by_status(&scope, SkillStatus::Active, 10)
            .expect("list works")
            .is_empty()
    );

    let mut log = log;
    log.activate(id, Timestamp::from_millis(20))
        .expect("activation should succeed");
    let active = log.get(id).expect("get works").expect("skill exists");
    assert_eq!(active.status(), SkillStatus::Active);
    assert_eq!(active.activated_at(), Some(Timestamp::from_millis(20)));
    assert_eq!(
        log.search_keyword(&scope, "alpha", 10, None)
            .expect("search should succeed")
            .len(),
        1
    );

    log.activate(id, Timestamp::from_millis(30))
        .expect("repeated activation should be idempotent");
    assert_eq!(
        log.get(id)
            .expect("get works")
            .expect("skill exists")
            .activated_at(),
        Some(Timestamp::from_millis(20)),
        "repeated activation must keep the first activation time"
    );

    log.soft_delete(id, Timestamp::from_millis(40))
        .expect("soft delete should succeed");
    assert!(
        log.search_keyword(&scope, "alpha", 10, None)
            .expect("search should succeed")
            .is_empty()
    );
    assert!(log.list_recent(&scope, 10).expect("list works").is_empty());
    assert!(matches!(
        log.soft_delete(id, Timestamp::from_millis(50)),
        Err(SkillLogError::AlreadyDeleted { .. })
    ));
    assert!(matches!(
        log.activate(id, Timestamp::from_millis(50)),
        Err(SkillLogError::AlreadyDeleted { .. })
    ));
}

#[test]
fn soft_delete_removes_rows_from_the_fts_index() {
    let database = TempDatabase::new("fts-delete");
    let scope = workspace("workspace-fts-delete");
    let id = {
        let mut log = SkillLog::open(database.path()).expect("skill log should open");
        let id = append(
            &log,
            &scope,
            "session-fts-delete",
            1,
            "fts deletion marker",
            "description",
            "instructions",
            &["fts"],
            10,
        );
        log.activate(id, Timestamp::from_millis(20))
            .expect("activation should succeed");
        log.soft_delete(id, Timestamp::from_millis(30))
            .expect("soft delete should succeed");
        id
    };

    let raw = Connection::open(database.path()).expect("raw connection should open");
    let count: i64 = raw
        .query_row(
            "SELECT count(*) FROM skills_fts WHERE skills_fts MATCH '\"marker\"'",
            [],
            |row| row.get(0),
        )
        .expect("FTS query should succeed");
    assert_eq!(count, 0, "soft-deleted rows must leave the FTS index");
    assert!(
        raw.query_row(
            "SELECT count(*) FROM skills WHERE id = ?1",
            [id.get()],
            |row| row.get::<_, i64>(0)
        )
        .expect("row count should succeed")
            > 0,
        "the source row must remain"
    );
}

#[test]
fn physical_updates_and_deletes_are_rejected() {
    let database = TempDatabase::new("guards");
    let scope = workspace("workspace-guards");
    let id = {
        let mut log = SkillLog::open(database.path()).expect("skill log should open");
        let id = append(
            &log,
            &scope,
            "session-guards",
            1,
            "guard name",
            "guard description",
            "guard instructions",
            &["guard"],
            10,
        );
        log.activate(id, Timestamp::from_millis(20))
            .expect("activation should succeed");
        id
    };

    let raw = Connection::open(database.path()).expect("raw connection should open");
    for statement in [
        "UPDATE skills SET name = 'changed' WHERE id = ?1",
        "UPDATE skills SET status = 'draft' WHERE id = ?1",
        "UPDATE skills SET activated_at = 999 WHERE id = ?1",
        "UPDATE skills SET deleted_at = 999, name = 'changed' WHERE id = ?1",
        "DELETE FROM skills WHERE id = ?1",
    ] {
        let error = raw
            .execute(statement, [id.get()])
            .expect_err("physical mutation must be rejected");
        assert!(
            error.to_string().contains("append-only"),
            "unexpected trigger error for {statement}: {error}"
        );
    }

    let reopened = SkillLog::open(database.path()).expect("skill log should reopen");
    let stored = reopened
        .get(id)
        .expect("get should succeed")
        .expect("skill should remain");
    assert_eq!(stored.name(), "guard name");
    assert_eq!(stored.status(), SkillStatus::Active);
    assert_eq!(stored.deleted_at(), None);
}

#[test]
fn illegal_activation_transitions_are_rejected_without_changing_the_row() {
    let database = TempDatabase::new("activation-guard");
    let scope = workspace("workspace-activation-guard");
    let id = {
        let log = SkillLog::open(database.path()).expect("skill log should open");
        append(
            &log,
            &scope,
            "session-activation-guard",
            1,
            "draft name",
            "description",
            "instructions",
            &["draft"],
            10,
        )
    };

    let raw = Connection::open(database.path()).expect("raw connection should open");
    let invalid = [
        "UPDATE skills SET status = 'active' WHERE id = ?1",
        "UPDATE skills SET activated_at = 20 WHERE id = ?1",
        "UPDATE skills SET status = 'active', activated_at = 20, name = 'changed' WHERE id = ?1",
    ];
    for statement in invalid {
        raw.execute(statement, [id.get()])
            .expect_err("illegal transition must be rejected");
    }

    let reopened = SkillLog::open(database.path()).expect("skill log should reopen");
    let stored = reopened
        .get(id)
        .expect("get should succeed")
        .expect("skill should remain");
    assert_eq!(stored.name(), "draft name");
    assert_eq!(stored.status(), SkillStatus::Draft);
    assert_eq!(stored.activated_at(), None);
}

#[test]
fn find_by_source_listing_and_limits_are_workspace_isolated() {
    let log = SkillLog::open_in_memory().expect("in-memory log should open");
    let scope_one = workspace("workspace-list-a");
    let scope_two = workspace("workspace-list-b");
    let old = append(
        &log,
        &scope_one,
        "session-list",
        1,
        "old",
        "description",
        "instructions",
        &[],
        100,
    );
    let newest = append(
        &log,
        &scope_one,
        "session-list",
        2,
        "newest",
        "description",
        "instructions",
        &[],
        300,
    );
    let middle = append(
        &log,
        &scope_one,
        "session-list",
        3,
        "middle",
        "description",
        "instructions",
        &[],
        200,
    );
    let tied = append(
        &log,
        &scope_one,
        "session-list",
        4,
        "tied",
        "description",
        "instructions",
        &[],
        200,
    );
    let other = append(
        &log,
        &scope_two,
        "session-list",
        1,
        "other workspace",
        "description",
        "instructions",
        &[],
        400,
    );

    assert_eq!(
        ids(&log.list_recent(&scope_one, 10).expect("list works")),
        [newest.get(), tied.get(), middle.get(), old.get()]
    );
    assert_eq!(
        ids(&log.list_recent(&scope_one, 2).expect("limit works")),
        [newest.get(), tied.get()]
    );
    assert!(
        log.list_recent(&scope_one, 0)
            .expect("zero limit works")
            .is_empty()
    );
    assert_eq!(
        ids(&log.list_recent(&scope_two, 10).expect("list works")),
        [other.get()]
    );

    let found = log
        .find_by_source(&scope_one, &SessionId::new("session-list"), 3)
        .expect("find works")
        .expect("source should match");
    assert_eq!(found.id(), middle);
    assert!(
        log.find_by_source(&scope_one, &SessionId::new("session-list"), 99)
            .expect("find works")
            .is_none()
    );
    assert!(
        log.find_by_source(&scope_two, &SessionId::new("session-list"), 3)
            .expect("find works")
            .is_none(),
        "source lookup must not cross workspaces"
    );

    let mut log = log;
    log.soft_delete(middle, Timestamp::from_millis(500))
        .expect("soft delete works");
    assert!(
        log.find_by_source(&scope_one, &SessionId::new("session-list"), 3)
            .expect("find works")
            .is_none(),
        "soft-deleted sources must not match"
    );
}

#[test]
fn corrupt_tags_and_unknown_status_are_reported() {
    let database = TempDatabase::new("corrupt");
    let scope = workspace("workspace-corrupt");
    let _ = SkillLog::open(database.path()).expect("skill log should open");

    let raw = Connection::open(database.path()).expect("raw connection should open");
    raw.execute_batch("PRAGMA ignore_check_constraints = ON;")
        .expect("check constraints should be ignorable for the test");
    raw.execute(
        "INSERT INTO skills \
         (workspace_scope, source_session, source_turn_index, source_tool_calls, source_rounds, \
          source_acceptance_failures, name, description, instructions, tags_json, content_hash, \
          status, created_at) \
         VALUES (?1, 'corrupt-session', 1, 0, 0, 0, 'bad tags', 'description', 'instructions', \
                 'not-json', 'hash-bad-tags', 'draft', 10)",
        [scope.as_str()],
    )
    .expect("corrupt tags row should insert");
    let bad_tags_id: i64 = raw.last_insert_rowid();

    raw.execute(
        "INSERT INTO skills \
         (workspace_scope, source_session, source_turn_index, source_tool_calls, source_rounds, \
          source_acceptance_failures, name, description, instructions, tags_json, content_hash, \
          status, created_at) \
         VALUES (?1, 'corrupt-session', 2, 0, 0, 0, 'bad status', 'description', 'instructions', \
                 '[]', 'hash-bad-status', 'retired', 20)",
        [scope.as_str()],
    )
    .expect("unknown status row should insert with check constraints ignored");
    let bad_status_id: i64 = raw.last_insert_rowid();
    drop(raw);

    let log = SkillLog::open(database.path()).expect("skill log should reopen");
    assert!(matches!(
        log.get(SkillId::from_raw(bad_tags_id)),
        Err(SkillLogError::Decode { .. })
    ));
    assert!(matches!(
        log.get(SkillId::from_raw(bad_status_id)),
        Err(SkillLogError::Corrupt { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_store_enforces_workspace_isolation_across_all_operations() {
    let database = TempDatabase::new("async-isolation");
    let workspace_a = std::env::temp_dir().join(format!(
        "jessica-skill-workspace-a-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let workspace_b = std::env::temp_dir().join(format!(
        "jessica-skill-workspace-b-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let store_a =
        SqliteSkillStore::open(database.path(), &workspace_a).expect("store a should open");
    let store_b =
        SqliteSkillStore::open(database.path(), &workspace_b).expect("store b should open");

    let skill_a = NewSkill::new(
        store_a.workspace_scope().clone(),
        source("session-async-a", 1),
        "alpha async",
        "description",
        "instructions",
        vec!["async".to_owned()],
        Timestamp::from_millis(10),
    )
    .expect("skill should be valid");
    let id_a = store_a
        .append(skill_a)
        .await
        .expect("append should succeed");
    assert_eq!(
        store_a
            .get(id_a)
            .await
            .expect("get works")
            .expect("skill exists")
            .name(),
        "alpha async"
    );
    assert!(
        store_b.get(id_a).await.expect("get works").is_none(),
        "cross-workspace get must be hidden"
    );
    assert_eq!(
        ids(&store_a.list_recent(10).await.expect("list works")),
        [id_a.get()]
    );
    assert!(
        store_b
            .list_recent(10)
            .await
            .expect("list works")
            .is_empty()
    );
    assert!(
        store_a
            .list_by_status(SkillStatus::Draft, 10)
            .await
            .expect("list works")
            .len()
            == 1
    );
    assert!(
        store_a
            .search_keyword("alpha", 10, None)
            .await
            .expect("search works")
            .is_empty(),
        "drafts must remain invisible"
    );
    assert!(
        store_a
            .find_by_source(&SessionId::new("session-async-a"), 1)
            .await
            .expect("find works")
            .is_some()
    );
    assert!(
        store_b
            .find_by_source(&SessionId::new("session-async-a"), 1)
            .await
            .expect("find works")
            .is_none()
    );

    assert!(matches!(
        store_b.soft_delete(id_a, Timestamp::from_millis(20)).await,
        Err(SkillLogError::NotFound { .. })
    ));
    assert!(matches!(
        store_b.activate(id_a, Timestamp::from_millis(20)).await,
        Err(SkillLogError::NotFound { .. })
    ));

    store_a
        .activate(id_a, Timestamp::from_millis(30))
        .await
        .expect("activation should succeed");
    let hits = store_a
        .search_keyword("alpha", 10, None)
        .await
        .expect("search works");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0.id(), id_a);

    let skill_b = NewSkill::new(
        store_b.workspace_scope().clone(),
        source("session-async-b", 1),
        "alpha async",
        "description",
        "instructions",
        vec!["async".to_owned()],
        Timestamp::from_millis(40),
    )
    .expect("skill should be valid");
    let id_b = store_b
        .append(skill_b.clone())
        .await
        .expect("same content in another workspace should append");
    assert_ne!(id_a, id_b);
    assert!(matches!(
        store_a.append(skill_b).await,
        Err(SkillLogError::WorkspaceMismatch)
    ));

    store_a
        .soft_delete(id_a, Timestamp::from_millis(50))
        .await
        .expect("soft delete should succeed");
    assert!(
        store_a
            .search_keyword("alpha", 10, None)
            .await
            .expect("search works")
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn async_sqlite_waits_do_not_block_the_tokio_worker() {
    let database = TempDatabase::new("async-blocking");
    let workspace_path = std::env::temp_dir().join(format!(
        "jessica-skill-workspace-blocking-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let store =
        SqliteSkillStore::open(database.path(), &workspace_path).expect("store should open");
    let database_path = database.path().to_path_buf();
    let blocker = tokio::task::spawn_blocking(move || {
        let connection = Connection::open(database_path).expect("blocker should open");
        connection
            .execute_batch("BEGIN IMMEDIATE;")
            .expect("blocker should acquire a write lock");
        connection
    })
    .await
    .expect("blocker task should join");

    let skill = NewSkill::new(
        store.workspace_scope().clone(),
        source("session-blocking", 1),
        "blocking marker",
        "description",
        "instructions",
        Vec::new(),
        Timestamp::from_millis(10),
    )
    .expect("skill should be valid");
    let store_for_append = store.clone();
    let append = tokio::spawn(async move { store_for_append.append(skill).await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let timer = tokio::time::timeout(
        Duration::from_millis(500),
        tokio::time::sleep(Duration::from_millis(100)),
    )
    .await;
    assert!(
        timer.is_ok(),
        "a waiting SQLite operation must not block the Tokio worker"
    );

    tokio::task::spawn_blocking(move || {
        blocker
            .execute_batch("ROLLBACK;")
            .expect("blocker should release the write lock");
    })
    .await
    .expect("rollback task should join");

    let appended = tokio::time::timeout(Duration::from_secs(2), append)
        .await
        .expect("append should finish after the lock is released")
        .expect("append task should join")
        .expect("append should succeed");
    assert_eq!(
        store
            .get(appended)
            .await
            .expect("get works")
            .expect("appended skill exists")
            .name(),
        "blocking marker"
    );
}
