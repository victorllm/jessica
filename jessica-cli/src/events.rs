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
    /// except acceptance failures, which are trace-only.
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
    /// Returns `None` when the session has no metadata row; such a session
    /// cannot be replayed because the user messages are unknown.
    pub(crate) async fn load(
        &self,
        session_id: SessionId,
    ) -> Result<Option<StoredSession>, RecorderError> {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || {
            let store = lock_store(&store);
            let Some(initial_messages) = store.session_messages(&session_id)? else {
                return Ok(None);
            };
            let events = store
                .events(&session_id)?
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
        .map_err(RecorderError::Store)
    }
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
