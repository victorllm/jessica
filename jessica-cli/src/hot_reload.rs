//! Background configuration hot reload for long-lived modes.
//!
//! The watcher is started by the interactive REPL and, after lazy assembly, by
//! the ACP server. It polls the configuration file's `(mtime, len)` fingerprint
//! once per second, parses and validates a changed file, and publishes the new
//! immutable snapshot through a [`tokio::sync::watch`] channel. Sections that
//! own startup-only resources (events, memory, skills, wasm, reliability, and
//! ACP limits) reject the whole reload: those connections and workers are never
//! rebuilt while the process runs.
//!
//! A bad or partially invalid file updates the fingerprint before it is
//! processed, so repeated polls of the same file do not spam the console. The
//! previous configuration stays in effect until a later fingerprint produces a
//! valid, fully reloadable document.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::config::{HarnessConfig, changed_immutable_section};

/// How often the REPL checks the configuration file for changes.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// A live REPL configuration watcher.
///
/// Dropping the watcher aborts its background task, which is how the REPL
/// stops monitoring when it exits or the process is shutting down.
pub(crate) struct ConfigWatcher {
    /// Background polling task.
    task: JoinHandle<()>,
}

impl ConfigWatcher {
    /// Starts watching `path` for changes.
    ///
    /// The initial fingerprint is captured before this future resolves, so a
    /// change made after the returned watcher exists is always observed.
    pub(crate) async fn spawn(path: PathBuf, sender: watch::Sender<Arc<HarnessConfig>>) -> Self {
        Self::spawn_with_interval(path, sender, POLL_INTERVAL).await
    }

    /// Starts watching with an explicit poll interval.
    ///
    /// Tests use a short interval; production always uses [`POLL_INTERVAL`].
    async fn spawn_with_interval(
        path: PathBuf,
        sender: watch::Sender<Arc<HarnessConfig>>,
        poll_interval: Duration,
    ) -> Self {
        let fingerprint = read_fingerprint(&path).await;
        Self {
            task: tokio::spawn(watch_loop(path, sender, poll_interval, fingerprint)),
        }
    }
}

impl Drop for ConfigWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Polls the file and publishes valid reloads until the receiver disappears.
async fn watch_loop(
    path: PathBuf,
    sender: watch::Sender<Arc<HarnessConfig>>,
    poll_interval: Duration,
    mut fingerprint: Fingerprint,
) {
    let mut ticker = tokio::time::interval(poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first tick completes immediately; the initial fingerprint was taken
    // at spawn time, so waiting one interval gives the file time to settle.
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let observed = read_fingerprint(&path).await;
        if observed == fingerprint {
            continue;
        }
        if matches!(observed, Fingerprint::Unreadable) {
            fingerprint = observed;
            eprintln!(
                "config reload failed: cannot read metadata for {}",
                path.display()
            );
            continue;
        }

        let next_config = HarnessConfig::load(&path).await;
        // A non-atomic writer truncates and rewrites the file. If the
        // fingerprint moved while the document was being read, the parsed
        // value may describe a transient state (for example an empty file
        // that serde fills with defaults), so discard this sample and retry
        // on the next tick. The last accepted fingerprint is intentionally
        // left unchanged so the settled file is seen as a new change.
        if read_fingerprint(&path).await != observed {
            continue;
        }
        // Update before processing: a failed parse must not be retried on
        // every subsequent tick for the same file contents.
        fingerprint = observed;

        match next_config {
            Ok(next_config) => {
                let current = sender.borrow().clone();
                if let Some(section) = changed_immutable_section(&current, &next_config) {
                    eprintln!("[config] reload rejected: `{section}` is not hot-reloadable");
                    continue;
                }
                if sender.send(Arc::new(next_config)).is_err() {
                    // The REPL dropped its receiver; there is nothing to
                    // publish to and the task can stop.
                    break;
                }
                eprintln!("[config] reloaded");
            }
            Err(error) => eprintln!("config reload failed: {error}"),
        }
    }
}

/// File identity used to detect changes without re-parsing every second.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Fingerprint {
    /// The file exists and its modification time and length are known.
    Present {
        /// Last modification time, when the filesystem reports one.
        modified: Option<SystemTime>,
        /// File length in bytes.
        len: u64,
    },
    /// The file is missing or its metadata cannot be read.
    Unreadable,
}

/// Reads the current `(mtime, len)` fingerprint of `path`.
async fn read_fingerprint(path: &Path) -> Fingerprint {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Fingerprint::Present {
            modified: metadata.modified().ok(),
            len: metadata.len(),
        },
        Err(_) => Fingerprint::Unreadable,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    /// Poll interval fast enough for tests but still observable.
    const TEST_INTERVAL: Duration = Duration::from_millis(20);
    /// Bound for one expected reload; generous enough for a loaded CI host.
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);
    /// Time allowed for a rejected file to be observed and discarded.
    const SETTLE: Duration = Duration::from_millis(120);

    const BASE: &str = "\
[budget]
max_rounds = 4
tool_timeout_ms = 9000

[policy]
allowed_tools = [\"bash\", \"read_file\"]
";

    /// Temporary directory removed when the test finishes.
    struct TempDir {
        /// Directory path.
        path: PathBuf,
    }

    impl TempDir {
        /// Creates a uniquely named directory below the system temp directory.
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "jessica-cli-hot-reload-{label}-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temporary directory should be created");
            Self { path }
        }

        /// Returns the configuration file path inside this directory.
        fn config_path(&self) -> PathBuf {
            self.path.join("harness.toml")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Creates a channel and watcher over `path`, returning the receiver.
    async fn watch(path: &Path) -> (ConfigWatcher, watch::Receiver<Arc<HarnessConfig>>) {
        let config = HarnessConfig::load(path)
            .await
            .expect("base config should load");
        let (sender, receiver) = watch::channel(Arc::new(config));
        let watcher =
            ConfigWatcher::spawn_with_interval(path.to_path_buf(), sender, TEST_INTERVAL).await;
        (watcher, receiver)
    }

    /// Atomically replaces `path` with `body`.
    ///
    /// The watcher is designed to tolerate non-atomic writers, but tests use
    /// rename so the fixture itself never exposes a transient document.
    async fn write_atomic(path: &Path, body: &str) -> std::io::Result<()> {
        let temporary = path.with_extension("tmp");
        tokio::fs::write(&temporary, body).await?;
        tokio::fs::rename(&temporary, path).await
    }

    /// Waits for one published snapshot and returns it.
    async fn next_snapshot(
        receiver: &mut watch::Receiver<Arc<HarnessConfig>>,
    ) -> Arc<HarnessConfig> {
        tokio::time::timeout(TEST_TIMEOUT, receiver.changed())
            .await
            .expect("a reload should be published")
            .expect("watcher channel should stay open");
        receiver.borrow().clone()
    }

    #[tokio::test]
    async fn a_reloadable_change_is_published_to_the_next_turn() {
        let dir = TempDir::new("reloadable");
        let path = dir.config_path();
        write_atomic(&path, BASE)
            .await
            .expect("base config should be written");
        let (_watcher, mut receiver) = watch(&path).await;

        let updated = "\
[budget]
max_rounds = 7
tool_timeout_ms = 4000

[policy]
allowed_tools = [\"read_file\"]

[delegation]
enabled = false
";
        write_atomic(&path, updated)
            .await
            .expect("updated config should be written");

        let snapshot = next_snapshot(&mut receiver).await;
        assert_eq!(snapshot.budget.max_rounds, 7);
        assert_eq!(snapshot.budget.tool_timeout_ms, 4000);
        assert_eq!(snapshot.policy.allowed_tools, vec!["read_file"]);
    }

    #[tokio::test]
    async fn an_immutable_change_keeps_the_old_snapshot_and_blocks_publication() {
        let dir = TempDir::new("immutable");
        let path = dir.config_path();
        write_atomic(&path, BASE)
            .await
            .expect("base config should be written");
        let (_watcher, mut receiver) = watch(&path).await;

        let with_events = "\
[budget]
max_rounds = 9
tool_timeout_ms = 9000

[policy]
allowed_tools = [\"bash\", \"read_file\"]

[events]
path = \"events.sqlite\"
";
        write_atomic(&path, with_events)
            .await
            .expect("immutable-change config should be written");
        tokio::time::sleep(SETTLE).await;

        assert!(
            !receiver.has_changed().expect("channel should stay open"),
            "an immutable section must reject the whole reload"
        );
        assert_eq!(receiver.borrow().budget.max_rounds, 4);

        // The watcher must keep working after a rejection.
        let reloadable = BASE.replace("max_rounds = 4", "max_rounds = 15");
        write_atomic(&path, &reloadable)
            .await
            .expect("reloadable config should be written");
        let snapshot = next_snapshot(&mut receiver).await;
        assert_eq!(snapshot.budget.max_rounds, 15);
    }

    #[tokio::test]
    async fn an_invalid_file_keeps_the_old_snapshot_and_does_not_stop_the_watcher() {
        let dir = TempDir::new("invalid");
        let path = dir.config_path();
        write_atomic(&path, BASE)
            .await
            .expect("base config should be written");
        let (_watcher, mut receiver) = watch(&path).await;

        write_atomic(&path, "[budget\nmax_rounds = nope")
            .await
            .expect("invalid config should be written");
        tokio::time::sleep(SETTLE).await;
        assert!(
            !receiver.has_changed().expect("channel should stay open"),
            "a parse failure must leave the old snapshot in place"
        );
        assert_eq!(receiver.borrow().budget.max_rounds, 4);

        let reloadable = BASE.replace("max_rounds = 4", "max_rounds = 11");
        write_atomic(&path, &reloadable)
            .await
            .expect("reloadable config should be written");
        let snapshot = next_snapshot(&mut receiver).await;
        assert_eq!(snapshot.budget.max_rounds, 11);
    }

    #[tokio::test]
    async fn an_invalid_delegation_allowlist_is_rejected_as_a_whole() {
        let dir = TempDir::new("delegation");
        let path = dir.config_path();
        write_atomic(&path, BASE)
            .await
            .expect("base config should be written");
        let (_watcher, mut receiver) = watch(&path).await;

        // Enabling delegation without `delegate_task` in the allowlist fails
        // cross-field validation, so the whole document is rejected even
        // though the delegation section itself is otherwise reloadable.
        let invalid = "\
[budget]
max_rounds = 4
tool_timeout_ms = 9000

[policy]
allowed_tools = [\"bash\", \"read_file\"]

[delegation]
enabled = true
";
        write_atomic(&path, invalid)
            .await
            .expect("invalid delegation config should be written");
        tokio::time::sleep(SETTLE).await;
        assert!(
            !receiver.has_changed().expect("channel should stay open"),
            "an invalid allowlist must not replace the snapshot"
        );

        let valid = "\
[budget]
max_rounds = 4
tool_timeout_ms = 9000

[policy]
allowed_tools = [\"bash\", \"read_file\", \"delegate_task\"]

[delegation]
enabled = true
";
        write_atomic(&path, valid)
            .await
            .expect("valid delegation config should be written");
        let snapshot = next_snapshot(&mut receiver).await;
        assert!(snapshot.delegation.enabled);
        assert!(
            snapshot
                .policy
                .allowed_tools
                .iter()
                .any(|name| name == "delegate_task")
        );
    }

    #[tokio::test]
    async fn only_reloadable_sections_are_accepted() {
        let current = HarnessConfig::from_toml_str(BASE).expect("base config should parse");
        let reloaded = HarnessConfig::from_toml_str(
            "[budget]\nmax_rounds = 8\n[policy]\nallowed_tools = [\"bash\"]\n\
             [sensor]\nname = \"check\"\ncommand = \"/bin/true\"\n",
        )
        .expect("reloadable config should parse");
        assert_eq!(changed_immutable_section(&current, &reloaded), None);
    }

    #[test]
    fn every_immutable_section_is_named_in_rejection_order() {
        let current = HarnessConfig::default();
        let cases = [
            ("profile", "[profile]\npreset = \"ci\"\n", "profile"),
            ("events", "[events]\npath = \"events.sqlite\"\n", "events"),
            (
                "memory",
                "[memory]\nenabled = true\npath = \".jessica/memory.db\"\n",
                "memory",
            ),
            (
                "skills",
                "[skills]\nenabled = true\npath = \".jessica/skills.db\"\n",
                "skills",
            ),
            ("wasm", "[wasm]\nmanifests = [\"plugin.toml\"]\n", "wasm"),
            (
                "reliability",
                "[reliability]\nenabled = true\n",
                "reliability",
            ),
            ("acp", "[acp]\nmax_prompt_bytes = 4096\n", "acp"),
        ];
        for (label, toml, expected) in cases {
            let next = HarnessConfig::from_toml_str(toml)
                .unwrap_or_else(|error| panic!("{label} config should parse: {error}"));
            assert_eq!(
                changed_immutable_section(&current, &next),
                Some(expected),
                "{label} must be reported by name"
            );
        }
    }

    #[test]
    fn the_first_changed_immutable_section_wins() {
        let current = HarnessConfig::default();
        let next = HarnessConfig::from_toml_str(
            "[profile]\npreset = \"safe\"\n\
             [events]\npath = \"events.sqlite\"\n\
             [memory]\nenabled = true\npath = \".jessica/memory.db\"\n",
        )
        .expect("config should parse");
        assert_eq!(changed_immutable_section(&current, &next), Some("profile"));

        let events_before_memory = HarnessConfig::from_toml_str(
            "[events]\npath = \"events.sqlite\"\n\
             [memory]\nenabled = true\npath = \".jessica/memory.db\"\n",
        )
        .expect("config should parse");
        assert_eq!(
            changed_immutable_section(&current, &events_before_memory),
            Some("events")
        );
    }
}
