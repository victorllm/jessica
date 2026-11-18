//! Bridging the CLI event stream to the SQLite event store.
//!
//! The runtime and harness are synchronous observers of the agent's event
//! stream, while [`jessica_memory::EventStore`] is blocking. [`EventRecorder`]
//! owns the store behind a mutex and performs every database operation in
//! `spawn_blocking`, so the pump never blocks a Tokio worker.
//!
//! The recorder is also the read side of `--resume`: it loads the immutable
//! bootstrap messages plus the recorded events and converts the stored rows into
//! the runtime's [`RecordedEvent`]s.
//!
//! [`EventJournal`] is the narrow write interface used by the pump and turn
//! orchestration. Tests inject deterministic failures through it; production
//! code uses [`EventRecorder`], which performs every database operation on the
//! blocking pool.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use jessica_core::trace::TraceEvent;
use jessica_core::types::{AgentEvent, Message, SessionId};
use jessica_memory::{EventStore, EventStoreError, Timestamp};
use jessica_runtime::RecordedEvent;
use thiserror::Error;
use tokio::task::JoinError;

/// Write-only interface for session metadata and versioned traces.
///
/// The pump owns one implementation for a whole turn. Keeping the interface
/// narrow lets behavior tests force storage failures without touching the
/// filesystem, while production uses [`EventRecorder`].
#[async_trait]
pub(crate) trait EventJournal: Send + Sync {
    /// Records the immutable bootstrap messages of a new session.
    async fn record_session_start(
        &self,
        session_id: &SessionId,
        messages: &[Message],
    ) -> Result<(), RecorderError>;

    /// Atomically appends one trace entry and its optional legacy mirror.
    ///
    /// `legacy` must be present for agent events that belong to a session,
    /// except audit-only facts that replay cannot reconstruct: acceptance
    /// failures, permission decisions, and child-session lifecycle.
    async fn record_traced(
        &self,
        trace: TraceEvent,
        legacy: Option<(SessionId, AgentEvent)>,
    ) -> Result<(), RecorderError>;
}

/// A persisted session ready to be replayed.
pub(crate) struct StoredSession {
    /// Messages recorded before the session's first model call.
    pub(crate) initial_messages: Vec<Message>,
    /// Recorded events in sequence order.
    pub(crate) events: Vec<RecordedEvent>,
}

/// Shared handle to the append-only event store.
#[derive(Clone)]
pub(crate) struct EventRecorder {
    /// SQLite store; a blocking mutex is safe because no lock is held across
    /// an await point (all access happens inside `spawn_blocking`).
    store: Arc<Mutex<EventStore>>,
}

impl EventRecorder {
    /// Opens or creates the database at `path`.
    pub(crate) async fn open(path: PathBuf) -> Result<Self, RecorderError> {
        let store = tokio::task::spawn_blocking(move || EventStore::open(path))
            .await
            .map_err(RecorderError::Task)?
            .map_err(RecorderError::Store)?;
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
        })
    }

    /// Records the immutable bootstrap messages of a new session.
    ///
    /// Must be awaited before the session's first event is appended so a
    /// recoverable session always has metadata.
    pub(crate) async fn record_session_start(
        &self,
        session_id: &SessionId,
        messages: &[Message],
    ) -> Result<(), RecorderError> {
        let store = Arc::clone(&self.store);
        let session_id = session_id.clone();
        let messages = messages.to_vec();
        let timestamp = now();
        tokio::task::spawn_blocking(move || {
            lock_store(&store).record_session(&session_id, &messages, timestamp)
        })
        .await
        .map_err(RecorderError::Task)?
        .map_err(RecorderError::Store)
    }

    /// Appends one event to its session's stream.
    ///
    /// Production writes go through [`EventJournal::record_traced`] so the
    /// legacy mirror and the trace entry commit together. This direct helper
    /// exists for recovery-focused tests that do not build a full turn.
    #[cfg(test)]
    pub(crate) async fn append(
        &self,
        session_id: &SessionId,
        event: &AgentEvent,
    ) -> Result<(), RecorderError> {
        let store = Arc::clone(&self.store);
        let session_id = session_id.clone();
        let event = event.clone();
        let timestamp = now();
        tokio::task::spawn_blocking(move || {
            lock_store(&store).append(&session_id, &event, timestamp)?;
            Ok(())
        })
        .await
        .map_err(RecorderError::Task)?
        .map_err(RecorderError::Store)
    }

    /// Loads a session's bootstrap messages and events.
    ///
    /// The event-first source is the non-empty `messages` field on
    /// [`AgentEvent::SessionStarted`]. Version-1 recordings have an empty field
    /// and fall back to the `sessions.messages_json` metadata row written by
    /// older builds. When both sources are present they must agree; a conflict
    /// is an integrity failure, because resuming with either value would change
    /// the conversation the model originally received.
    ///
    /// Returns `None` only when neither source has messages.
    pub(crate) async fn load(
        &self,
        session_id: SessionId,
    ) -> Result<Option<StoredSession>, RecorderError> {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || {
            let store = lock_store(&store);
            let stored_events = store.events(&session_id)?;
            let event_messages = session_start_messages(&session_id, &stored_events)?;
            let metadata_messages = store.session_messages(&session_id)?;

            let initial_messages = match (event_messages, metadata_messages) {
                (Some(event_messages), Some(metadata_messages))
                    if event_messages != metadata_messages =>
                {
                    return Err(RecorderError::Integrity {
                        message: format!(
                            "session `{session_id}` has conflicting bootstrap messages in \
                                 its session_started event and metadata row"
                        ),
                    });
                }
                (Some(event_messages), _) => event_messages,
                (None, Some(metadata_messages)) => metadata_messages,
                (None, None) => return Ok(None),
            };
            let events = stored_events
                .into_iter()
                .map(|stored| RecordedEvent::new(stored.seq(), stored.event().clone()))
                .collect();
            Ok(Some(StoredSession {
                initial_messages,
                events,
            }))
        })
        .await
        .map_err(RecorderError::Task)?
    }
}

/// Returns the bootstrap messages carried by the event stream, if any.
///
/// Version-1 streams store an empty vector, so this returns `None` and lets the
/// caller fall back to metadata. Multiple non-empty `SessionStarted` events
/// must carry identical messages; anything else cannot be resumed
/// deterministically.
fn session_start_messages(
    session_id: &SessionId,
    events: &[jessica_memory::StoredEvent],
) -> Result<Option<Vec<Message>>, RecorderError> {
    let mut found: Option<Vec<Message>> = None;
    for stored in events {
        let AgentEvent::SessionStarted {
            session_id: event_session,
            messages,
        } = stored.event()
        else {
            continue;
        };
        if event_session != session_id {
            return Err(RecorderError::Integrity {
                message: format!(
                    "session `{session_id}` contains a session_started event for `{event_session}`"
                ),
            });
        }
        if messages.is_empty() {
            continue;
        }
        match &found {
            Some(existing) if existing != messages => {
                return Err(RecorderError::Integrity {
                    message: format!(
                        "session `{session_id}` has conflicting session_started messages"
                    ),
                });
            }
            Some(_) => {}
            None => found = Some(messages.clone()),
        }
    }
    Ok(found)
}

#[async_trait]
impl EventJournal for EventRecorder {
    async fn record_session_start(
        &self,
        session_id: &SessionId,
        messages: &[Message],
    ) -> Result<(), RecorderError> {
        EventRecorder::record_session_start(self, session_id, messages).await
    }

    async fn record_traced(
        &self,
        trace: TraceEvent,
        legacy: Option<(SessionId, AgentEvent)>,
    ) -> Result<(), RecorderError> {
        let store = Arc::clone(&self.store);
        let timestamp = now();
        tokio::task::spawn_blocking(move || {
            let legacy = legacy
                .as_ref()
                .map(|(session_id, event)| (session_id, event));
            lock_store(&store).append_traced(&trace, legacy, timestamp)?;
            Ok(())
        })
        .await
        .map_err(RecorderError::Task)?
        .map_err(RecorderError::Store)
    }
}

/// Locks the store, recovering from a poisoned mutex.
fn lock_store(store: &Mutex<EventStore>) -> MutexGuard<'_, EventStore> {
    store
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// Persistence is the only place the CLI reads the host clock; replay itself
/// uses the recorded values.
fn now() -> Timestamp {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    Timestamp::from_millis(millis)
}

/// Errors raised while recording or loading events.
#[derive(Debug, Error)]
pub(crate) enum RecorderError {
    /// The underlying event store failed.
    #[error("event store error: {0}")]
    Store(#[from] EventStoreError),
    /// The blocking database task failed.
    #[error("event store task failed: {0}")]
    Task(#[source] JoinError),
    /// A trace lifecycle or attribution rule was violated.
    ///
    /// This is always a failed turn: the trace cannot be completed without
    /// guessing about missing or misattributed entries.
    #[error("trace integrity failure: {message}")]
    Integrity {
        /// Description of the violated rule.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Database path removed together with its SQLite sidecars.
    struct TempPath(PathBuf);

    impl TempPath {
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "jessica-events-{label}-{}-{unique}.sqlite",
                std::process::id()
            )))
        }

        fn as_path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            for suffix in ["-journal", "-wal", "-shm"] {
                let mut sidecar = self.0.as_os_str().to_owned();
                sidecar.push(suffix);
                let _ = std::fs::remove_file(PathBuf::from(sidecar));
            }
        }
    }

    #[tokio::test]
    async fn session_started_messages_supply_the_bootstrap_without_metadata() {
        let database = TempPath::new("event-first");
        let recorder = EventRecorder::open(database.as_path().to_path_buf())
            .await
            .expect("recorder should open");
        let session = SessionId::new("events-event-first");
        let messages = vec![Message::system("event system"), Message::user("event user")];
        recorder
            .append(
                &session,
                &AgentEvent::SessionStarted {
                    session_id: session.clone(),
                    messages: messages.clone(),
                },
            )
            .await
            .expect("session_started should append");

        let stored = recorder
            .load(session.clone())
            .await
            .expect("load should succeed")
            .expect("the session should exist");
        assert_eq!(
            stored.initial_messages, messages,
            "a version-2 session_started must provide the bootstrap messages"
        );
        assert_eq!(stored.events.len(), 1);
    }

    #[tokio::test]
    async fn an_empty_session_started_falls_back_to_metadata() {
        let database = TempPath::new("metadata-fallback");
        let recorder = EventRecorder::open(database.as_path().to_path_buf())
            .await
            .expect("recorder should open");
        let session = SessionId::new("events-fallback");
        let metadata = vec![
            Message::system("metadata system"),
            Message::user("metadata user"),
        ];
        recorder
            .record_session_start(&session, &metadata)
            .await
            .expect("metadata should record");
        recorder
            .append(
                &session,
                &AgentEvent::SessionStarted {
                    session_id: session.clone(),
                    messages: Vec::new(),
                },
            )
            .await
            .expect("version-1 session_started should append");

        let stored = recorder
            .load(session.clone())
            .await
            .expect("load should succeed")
            .expect("the session should exist");
        assert_eq!(
            stored.initial_messages, metadata,
            "an empty version-1 field must fall back to metadata"
        );
    }

    #[tokio::test]
    async fn conflicting_bootstrap_sources_fail_closed() {
        let database = TempPath::new("conflict");
        let recorder = EventRecorder::open(database.as_path().to_path_buf())
            .await
            .expect("recorder should open");
        let session = SessionId::new("events-conflict");
        recorder
            .record_session_start(
                &session,
                &[
                    Message::system("metadata system"),
                    Message::user("metadata user"),
                ],
            )
            .await
            .expect("metadata should record");
        recorder
            .append(
                &session,
                &AgentEvent::SessionStarted {
                    session_id: session.clone(),
                    messages: vec![Message::system("event system"), Message::user("event user")],
                },
            )
            .await
            .expect("session_started should append");

        let error = match recorder.load(session.clone()).await {
            Ok(_) => panic!("conflicting bootstrap messages must be rejected"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, RecorderError::Integrity { message } if message.contains("conflicting bootstrap messages")),
            "unexpected error: {error:?}"
        );
    }
}
