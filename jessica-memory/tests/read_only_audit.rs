//! Behavioral tests for the immutable read-only audit path.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use jessica_core::types::{AgentEvent, SessionId};
use jessica_memory::{EventStore, EventStoreError, Timestamp};

/// Unique database file removed when the test finishes.
struct TempDatabase {
    /// Path passed to [`EventStore`].
    path: PathBuf,
}

impl TempDatabase {
    /// Creates an unused path in the system temporary directory.
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-read-only-{label}-{}-{unique}.sqlite",
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
            let _ = std::fs::remove_file(sidecar(&self.path, suffix));
        }
    }
}

/// Unique directory removed when the test finishes.
struct TempDirectory {
    /// Directory created for the test.
    path: PathBuf,
}

impl TempDirectory {
    /// Creates an empty directory in the system temporary directory.
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "jessica-read-only-dir-{label}-{}-{unique}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("the temporary directory should be created");
        Self { path }
    }

    /// Returns the directory path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o755));
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Builds the path of a SQLite sidecar file.
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

/// Builds a simple event for audit tests.
fn model_delta(text: &str) -> AgentEvent {
    AgentEvent::ModelDelta {
        text: text.to_owned(),
    }
}

/// Records one event and closes the store cleanly.
fn seed_database(path: &Path, session: &SessionId, text: &str) {
    let mut store = EventStore::open(path).expect("store should open");
    store
        .append(session, &model_delta(text), Timestamp::from_millis(1))
        .expect("the seed append should succeed");
}

#[test]
fn a_live_writer_with_pending_wal_frames_is_rejected() {
    let database = TempDatabase::new("pending-wal");
    let session = SessionId::new("pending-wal-session");
    let mut store = EventStore::open(database.path()).expect("store should open");
    store
        .append(
            &session,
            &model_delta("committed while live"),
            Timestamp::from_millis(2),
        )
        .expect("the append should commit");

    let wal = sidecar(database.path(), "-wal");
    let pending = std::fs::metadata(&wal)
        .expect("a live writer must keep a WAL sidecar")
        .len();
    assert!(pending > 0, "the live writer must leave pending WAL frames");

    let error = EventStore::open_read_only(database.path())
        .expect_err("pending WAL frames must be refused");
    match error {
        EventStoreError::PendingWal {
            path,
            bytes: reported,
        } => {
            assert_eq!(path.as_path(), database.path());
            assert_eq!(
                reported, pending,
                "the error must report the sidecar's real size"
            );
        }
        other => panic!("expected PendingWal, found {other:?}"),
    }

    drop(store);
    let read_only = EventStore::open_read_only(database.path())
        .expect("a clean close checkpoints the WAL and allows the audit");
    let events = read_only.events(&session).expect("events should read");
    assert_eq!(events.len(), 1);
}

#[test]
fn a_clean_database_is_audited_without_creating_sidecars() {
    let database = TempDatabase::new("clean");
    let session = SessionId::new("clean-session");
    seed_database(database.path(), &session, "clean snapshot");
    assert!(
        !sidecar(database.path(), "-wal").exists(),
        "the fixture must be checkpointed before the audit"
    );

    let read_only =
        EventStore::open_read_only(database.path()).expect("the clean database should open");
    let events = read_only.events(&session).expect("events should read");
    assert_eq!(events.len(), 1);
    drop(read_only);

    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(
            !sidecar(database.path(), suffix).exists(),
            "the audit open must not create {suffix} sidecars"
        );
    }
}

#[test]
fn an_empty_wal_sidecar_is_accepted() {
    let database = TempDatabase::new("empty-wal");
    let session = SessionId::new("empty-wal-session");
    seed_database(database.path(), &session, "no pending frames");

    let wal = sidecar(database.path(), "-wal");
    std::fs::File::create(&wal).expect("an empty WAL sidecar should be created");
    assert_eq!(
        std::fs::metadata(&wal)
            .expect("the empty sidecar should exist")
            .len(),
        0
    );

    let read_only = EventStore::open_read_only(database.path())
        .expect("an empty WAL sidecar contains no frames and must be accepted");
    assert_eq!(
        read_only
            .events(&session)
            .expect("events should read")
            .len(),
        1
    );
}

#[test]
fn a_read_only_directory_is_supported_without_creating_sidecars() {
    let directory = TempDirectory::new("ro");
    let database = directory.path().join("audit.sqlite");
    let session = SessionId::new("read-only-dir-session");
    seed_database(&database, &session, "read-only directory");

    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o444))
        .expect("the database should become read-only");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o555))
        .expect("the directory should become read-only");

    let read_only = EventStore::open_read_only(&database)
        .expect("an immutable audit must open inside a read-only directory");
    assert_eq!(
        read_only
            .events(&session)
            .expect("events should read")
            .len(),
        1
    );
    drop(read_only);

    let entries: Vec<_> = std::fs::read_dir(directory.path())
        .expect("the directory should read")
        .map(|entry| {
            entry
                .expect("the directory entry should read")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert_eq!(
        entries,
        vec!["audit.sqlite".to_owned()],
        "the audit must not create sidecars in the read-only directory"
    );
}

#[test]
fn a_path_with_uri_separators_is_audited() {
    let directory = TempDirectory::new("uri");
    let database = directory.path().join("a?b#c%d e中文.sqlite");
    let session = SessionId::new("uri-session");
    seed_database(&database, &session, "separator path");

    let read_only = EventStore::open_read_only(&database)
        .expect("URI separators in the file name must be percent-encoded");
    let events = read_only.events(&session).expect("events should read");
    assert_eq!(events.len(), 1);
}
