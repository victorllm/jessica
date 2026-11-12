//! Async layered recall built on the synchronous [`MemoryLog`].
//!
//! [`SqliteMemoryStore`] implements the frozen
//! [`jessica_core::memory::MemoryStore`] contract by combining three
//! deterministic candidate paths:
//!
//! - **short term:** the newest entries for the session and workspace;
//! - **mid term:** workspace-wide FTS5 keyword matches;
//! - **long term:** brute-force cosine matches over stored vectors.
//!
//! The paths are fused with reciprocal rank fusion (RRF):
//! `sum(weight_i / (k + rank_i))`, where ranks are one-based. Fused results are
//! ordered by descending score and then ascending entry id, and the returned
//! content is bounded by a byte cap that never splits a UTF-8 character.
//!
//! A failing candidate path is skipped with a `tracing::warn`; only an
//! unavailable database is reported as [`MemoryError::Backend`]. Every SQLite
//! call runs inside [`tokio::task::spawn_blocking`], and the single connection
//! is serialized by a mutex.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use jessica_core::memory::{MemoryEntry, MemoryError, MemoryStore};
use jessica_core::types::SessionId;
use thiserror::Error;

use crate::Timestamp;
use crate::embedding::{BruteForceIndex, Embedder, VectorIndex};
use crate::memory_log::{
    MemoryEntryId, MemoryLog, MemoryLogError, MemoryRecord, NewMemoryEntry, WorkspaceScope,
};
use crate::workspace::{WorkspaceScopeError, workspace_scope_for};

/// Default RRF constant added to every one-based rank.
const DEFAULT_RRF_K: usize = 60;

/// Default maximum number of content bytes returned by one recall.
const DEFAULT_MAX_BYTES: usize = 4096;

/// Default weight applied to each candidate path.
const DEFAULT_WEIGHT: f32 = 1.0;

/// Number of bytes occupied by the `---` separator between recalled entries.
const SEPARATOR_BYTES: usize = 3;

/// Timestamp used by the frozen [`MemoryStore::write`] method.
///
/// The core trait has no timestamp parameter, so the compatibility method uses
/// the Unix epoch and relies on row-id order as its deterministic tie-breaker.
/// Callers that need a meaningful creation time must call
/// [`SqliteMemoryStore::write_at`].
const TRAIT_WRITE_TIMESTAMP: Timestamp = Timestamp::from_millis(0);

/// Parameters for one layered recall.
///
/// The workspace scope is fixed by [`SqliteMemoryStore`] and is intentionally
/// absent from this request, so a caller cannot widen a store beyond its
/// configured workspace.
#[derive(Debug, Clone)]
pub struct RecallRequest {
    /// Session whose short-term entries are eligible.
    pub session: SessionId,
    /// Query text used by the keyword and vector paths.
    pub query: String,
    /// Maximum number of fused entries to return.
    pub limit: usize,
    /// RRF weight for the short-term (recent session) path.
    pub short_weight: f32,
    /// RRF weight for the mid-term (workspace keyword) path.
    pub keyword_weight: f32,
    /// RRF weight for the long-term (vector) path.
    pub vector_weight: f32,
    /// RRF constant added to every one-based rank; must be greater than zero.
    pub rrf_k: usize,
    /// Maximum total content bytes returned, including `---` separators.
    pub max_bytes: usize,
}

impl RecallRequest {
    /// Creates a request with weight `1.0` for every path, `rrf_k = 60`, and
    /// `max_bytes = 4096`.
    pub fn new(session: SessionId, query: impl Into<String>, limit: usize) -> Self {
        Self {
            session,
            query: query.into(),
            limit,
            short_weight: DEFAULT_WEIGHT,
            keyword_weight: DEFAULT_WEIGHT,
            vector_weight: DEFAULT_WEIGHT,
            rrf_k: DEFAULT_RRF_K,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }

    /// Rejects non-finite or negative weights and a zero RRF constant.
    fn validate(&self) -> Result<(), MemoryError> {
        for (name, weight) in [
            ("short", self.short_weight),
            ("keyword", self.keyword_weight),
            ("vector", self.vector_weight),
        ] {
            if !weight.is_finite() || weight < 0.0 {
                return Err(backend_error(format!(
                    "memory recall {name} weight must be finite and non-negative"
                )));
            }
        }
        if self.rrf_k == 0 {
            return Err(backend_error(
                "memory recall rrf_k must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Async layered memory backed by SQLite.
///
/// The store fixes one [`WorkspaceScope`] at construction from the caller's
/// canonical workspace path. It never reads the current directory later, so a
/// process-wide `chdir` cannot change which entries it sees.
///
/// Writes append the raw entry to the memory log and, when an [`Embedder`] is
/// configured, best-effort store its vector. Embedding failures are logged and
/// do not roll back the entry.
#[derive(Clone)]
pub struct SqliteMemoryStore {
    /// Serialized SQLite connection shared by every async operation.
    log: Arc<Mutex<MemoryLog>>,
    /// Workspace partition fixed when the store was built.
    workspace_scope: WorkspaceScope,
    /// Optional long-term vector producer.
    embedder: Option<Arc<dyn Embedder>>,
}

impl fmt::Debug for SqliteMemoryStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteMemoryStore")
            .field("workspace_scope", &self.workspace_scope)
            .field(
                "embedder",
                &self.embedder.as_ref().map(|embedder| embedder.model_id()),
            )
            .finish_non_exhaustive()
    }
}

impl SqliteMemoryStore {
    /// Opens or creates a memory database at `database_path` and fixes the
    /// workspace scope to `canonical_workspace`.
    ///
    /// The caller must supply an already-canonicalized workspace path. This
    /// constructor deliberately does not call `std::fs::canonicalize`, which is
    /// blocking filesystem I/O and could change the scope if the process later
    /// changes its working directory. Passing `None` disables the long-term
    /// vector path while keeping short- and mid-term recall available.
    pub fn open(
        database_path: impl AsRef<Path>,
        canonical_workspace: impl AsRef<Path>,
        embedder: Option<Arc<dyn Embedder>>,
    ) -> Result<Self, SqliteMemoryStoreError> {
        let log = MemoryLog::open(database_path)?;
        Self::with_log(log, canonical_workspace.as_ref(), embedder)
    }

    /// Opens a private in-memory database with the same workspace semantics as
    /// [`SqliteMemoryStore::open`].
    pub fn open_in_memory(
        canonical_workspace: impl AsRef<Path>,
        embedder: Option<Arc<dyn Embedder>>,
    ) -> Result<Self, SqliteMemoryStoreError> {
        let log = MemoryLog::open_in_memory()?;
        Self::with_log(log, canonical_workspace.as_ref(), embedder)
    }

    /// Wraps an already-open log and derives its fixed workspace scope.
    fn with_log(
        log: MemoryLog,
        canonical_workspace: &Path,
        embedder: Option<Arc<dyn Embedder>>,
    ) -> Result<Self, SqliteMemoryStoreError> {
        let workspace_scope =
            workspace_scope_for(canonical_workspace).map_err(|error| match error {
                WorkspaceScopeError::EmptyPath => SqliteMemoryStoreError::EmptyWorkspacePath,
                WorkspaceScopeError::InvalidScope => SqliteMemoryStoreError::WorkspaceScope,
            })?;
        Ok(Self {
            log: Arc::new(Mutex::new(log)),
            workspace_scope,
            embedder,
        })
    }

    /// Returns the fixed workspace scope used by this store.
    pub fn workspace_scope(&self) -> &WorkspaceScope {
        &self.workspace_scope
    }

    /// Appends `entry` for `session` at the caller-supplied `created_at`.
    ///
    /// Empty or whitespace-only content is rejected. Metadata is preserved
    /// verbatim. Identical live content for the same session and workspace is
    /// deduplicated by the underlying log and returns `Ok(())` without creating
    /// a second row or a second embedding.
    ///
    /// When an embedder is configured, the vector is produced after the entry
    /// is committed. An embedding or vector-storage failure only logs a warning;
    /// the entry remains stored and recallable through the other paths.
    pub async fn write_at(
        &self,
        session: &SessionId,
        entry: MemoryEntry,
        created_at: Timestamp,
    ) -> Result<(), MemoryError> {
        if entry.content.trim().is_empty() {
            return Err(backend_error("memory entry content must not be empty"));
        }

        let session = session.clone();
        let workspace_scope = self.workspace_scope.clone();
        let content_for_embedding = entry.content.clone();
        let content = entry.content;
        let metadata = entry.metadata;
        let log = Arc::clone(&self.log);

        let appended = tokio::task::spawn_blocking(move || {
            let log = lock_log(&log)?;
            let new_entry = NewMemoryEntry::new(session, workspace_scope, content, created_at)
                .map_err(|error| backend_error(format!("failed to build memory entry: {error}")))?
                .with_metadata(metadata);
            log.append(new_entry)
                .map_err(|error| backend_error(format!("failed to append memory entry: {error}")))
        })
        .await
        .map_err(|error| backend_error(format!("memory write task failed: {error}")))??;

        let Some(entry_id) = appended else {
            return Ok(());
        };
        let Some(embedder) = self.embedder.clone() else {
            return Ok(());
        };

        let model = embedder.model_id().to_owned();
        let expected_dims = embedder.dims();
        let vector = match embedder.embed(&content_for_embedding).await {
            Ok(vector) => vector,
            Err(error) => {
                tracing::warn!(
                    model = %model,
                    error = %error,
                    "memory embedding failed; entry stored without a vector"
                );
                return Ok(());
            }
        };
        if vector.len() != expected_dims {
            tracing::warn!(
                model = %model,
                expected_dims,
                actual_dims = vector.len(),
                "memory embedder returned an unexpected dimension count; entry stored without a vector"
            );
            return Ok(());
        }

        let log = Arc::clone(&self.log);
        let stored = tokio::task::spawn_blocking(move || {
            let log = lock_log(&log)?;
            BruteForceIndex::new(&log)
                .add(entry_id, &model, &vector, created_at)
                .map_err(|error| {
                    backend_error(format!("failed to store memory embedding: {error}"))
                })
        })
        .await;

        match stored {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    error = %error,
                    "memory embedding could not be stored; entry remains available to other recall paths"
                );
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "memory embedding task failed; entry remains available to other recall paths"
                );
            }
        }
        Ok(())
    }

    /// Recalls entries using the explicit parameters in `request`.
    ///
    /// Weights must be finite and non-negative, and `rrf_k` must be greater
    /// than zero; invalid values return [`MemoryError::Backend`]. A zero
    /// `limit` or `max_bytes` returns an empty result without touching the
    /// database.
    ///
    /// Failing candidate paths are skipped with a warning. If at least one path
    /// failed, a cheap health query distinguishes a degraded database from an
    /// available one; only an unavailable database fails the whole recall.
    pub async fn recall_with(
        &self,
        request: RecallRequest,
    ) -> Result<Vec<MemoryEntry>, MemoryError> {
        request.validate()?;
        if request.limit == 0 || request.max_bytes == 0 {
            return Ok(Vec::new());
        }

        let RecallRequest {
            session,
            query,
            limit,
            short_weight,
            keyword_weight,
            vector_weight,
            rrf_k,
            max_bytes,
        } = request;

        let vector_query = if vector_weight > 0.0 {
            match self.embedder.clone() {
                Some(embedder) => {
                    let model = embedder.model_id().to_owned();
                    let expected_dims = embedder.dims();
                    match embedder.embed(&query).await {
                        Ok(vector) if vector.len() == expected_dims => Some((model, vector)),
                        Ok(vector) => {
                            tracing::warn!(
                                model = %model,
                                expected_dims,
                                actual_dims = vector.len(),
                                "memory embedder returned an unexpected dimension count; skipping vector recall"
                            );
                            None
                        }
                        Err(error) => {
                            tracing::warn!(
                                model = %model,
                                error = %error,
                                "memory embedder failed; skipping vector recall"
                            );
                            None
                        }
                    }
                }
                None => None,
            }
        } else {
            None
        };

        let plan = RecallPlan {
            session,
            query,
            limit,
            short_weight,
            keyword_weight,
            vector_weight,
            rrf_k,
            max_bytes,
            workspace: self.workspace_scope.clone(),
            vector_query,
        };
        let log = Arc::clone(&self.log);

        tokio::task::spawn_blocking(move || recall_blocking(&log, plan))
            .await
            .map_err(|error| backend_error(format!("memory recall task failed: {error}")))?
    }
}

#[async_trait]
impl MemoryStore for SqliteMemoryStore {
    async fn write(&self, session: &SessionId, entry: MemoryEntry) -> Result<(), MemoryError> {
        self.write_at(session, entry, TRAIT_WRITE_TIMESTAMP).await
    }

    async fn recall(
        &self,
        session: &SessionId,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>, MemoryError> {
        self.recall_with(RecallRequest::new(session.clone(), query, limit))
            .await
    }
}

/// Everything needed by the blocking recall phase.
struct RecallPlan {
    /// Session whose short-term entries are eligible.
    session: SessionId,
    /// Query text for the keyword path.
    query: String,
    /// Per-path and final result limit.
    limit: usize,
    /// Weight of the short-term path.
    short_weight: f32,
    /// Weight of the keyword path.
    keyword_weight: f32,
    /// Weight of the vector path.
    vector_weight: f32,
    /// RRF constant.
    rrf_k: usize,
    /// Content byte cap.
    max_bytes: usize,
    /// Fixed workspace partition.
    workspace: WorkspaceScope,
    /// Precomputed query vector and its model id, when available.
    vector_query: Option<(String, Vec<f32>)>,
}

/// Runs all candidate paths and fuses them. Executed inside `spawn_blocking`.
fn recall_blocking(
    log: &Mutex<MemoryLog>,
    plan: RecallPlan,
) -> Result<Vec<MemoryEntry>, MemoryError> {
    let RecallPlan {
        session,
        query,
        limit,
        short_weight,
        keyword_weight,
        vector_weight,
        rrf_k,
        max_bytes,
        workspace,
        vector_query,
    } = plan;

    let log = lock_log(log)?;
    let mut scores: HashMap<MemoryEntryId, f64> = HashMap::new();
    let mut records: HashMap<MemoryEntryId, MemoryRecord> = HashMap::new();
    let mut path_failed = false;

    if short_weight > 0.0 {
        match log.recent_for_session(&session, &workspace, limit) {
            Ok(recent) => {
                accumulate(&mut scores, &mut records, recent, short_weight, rrf_k);
            }
            Err(error) => {
                tracing::warn!(
                    path = "short",
                    error = %error,
                    "memory recall path failed; degrading"
                );
                path_failed = true;
            }
        }
    }

    if keyword_weight > 0.0 {
        match log.search_keyword(&workspace, &query, limit) {
            Ok(keyword) => {
                accumulate(&mut scores, &mut records, keyword, keyword_weight, rrf_k);
            }
            Err(error) => {
                tracing::warn!(
                    path = "keyword",
                    error = %error,
                    "memory recall path failed; degrading"
                );
                path_failed = true;
            }
        }
    }

    if let Some((model, vector)) = vector_query {
        match BruteForceIndex::new(&log).search(&vector, &model, &workspace, limit) {
            Ok(matches) => {
                for (index, matched) in matches.into_iter().enumerate() {
                    let entry_id = matched.entry_id();
                    if let Entry::Vacant(slot) = records.entry(entry_id) {
                        match log.get(entry_id) {
                            Ok(Some(record)) if record.deleted_at().is_none() => {
                                slot.insert(record);
                            }
                            Ok(Some(_)) | Ok(None) => continue,
                            Err(error) => {
                                tracing::warn!(
                                    path = "vector",
                                    entry_id = %entry_id,
                                    error = %error,
                                    "memory record lookup failed; skipping candidate"
                                );
                                path_failed = true;
                                continue;
                            }
                        }
                    }
                    add_score(&mut scores, entry_id, vector_weight, index + 1, rrf_k);
                }
            }
            Err(error) => {
                tracing::warn!(
                    path = "vector",
                    error = %error,
                    "memory recall path failed; degrading"
                );
                path_failed = true;
            }
        }
    }

    if path_failed && let Err(error) = log.recent_for_session(&session, &workspace, 0) {
        return Err(backend_error(format!(
            "memory backend is unavailable: {error}"
        )));
    }

    let mut fused: Vec<(MemoryEntryId, f64)> = scores.into_iter().collect();
    fused.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });

    Ok(apply_byte_cap(fused, &records, limit, max_bytes))
}

/// Adds one path's candidates to the fused score and record maps.
fn accumulate(
    scores: &mut HashMap<MemoryEntryId, f64>,
    records: &mut HashMap<MemoryEntryId, MemoryRecord>,
    found: Vec<MemoryRecord>,
    weight: f32,
    rrf_k: usize,
) {
    for (index, record) in found.into_iter().enumerate() {
        let entry_id = record.id();
        add_score(scores, entry_id, weight, index + 1, rrf_k);
        records.entry(entry_id).or_insert(record);
    }
}

/// Adds one RRF contribution.
fn add_score(
    scores: &mut HashMap<MemoryEntryId, f64>,
    entry_id: MemoryEntryId,
    weight: f32,
    rank: usize,
    rrf_k: usize,
) {
    let contribution = f64::from(weight) / (rrf_k as f64 + rank as f64);
    *scores.entry(entry_id).or_insert(0.0) += contribution;
}

/// Truncates fused entries to `limit` and the total content byte budget.
///
/// Separators count toward the budget but are not included in individual entry
/// content. A partially fitting entry is truncated on a UTF-8 character
/// boundary; once an entry is truncated, later entries are dropped so the
/// caller never receives a reordered or fragmented suffix.
fn apply_byte_cap(
    fused: Vec<(MemoryEntryId, f64)>,
    records: &HashMap<MemoryEntryId, MemoryRecord>,
    limit: usize,
    max_bytes: usize,
) -> Vec<MemoryEntry> {
    let mut entries = Vec::new();
    let mut used = 0_usize;

    for (entry_id, _score) in fused {
        if entries.len() >= limit {
            break;
        }
        let Some(record) = records.get(&entry_id) else {
            continue;
        };

        let separator_bytes = if entries.is_empty() {
            0
        } else {
            SEPARATOR_BYTES
        };
        let Some(after_separator) = used.checked_add(separator_bytes) else {
            break;
        };
        if after_separator >= max_bytes {
            break;
        }

        let remaining = max_bytes - after_separator;
        let content = truncate_on_char_boundary(record.content(), remaining);
        if content.is_empty() {
            break;
        }

        used = after_separator + content.len();
        let truncated = content.len() < record.content().len();
        entries.push(MemoryEntry {
            content: content.to_owned(),
            metadata: record.metadata().clone(),
        });
        if truncated {
            break;
        }
    }

    entries
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

/// Maps a poisoned connection mutex to a backend error.
fn lock_log(log: &Mutex<MemoryLog>) -> Result<MutexGuard<'_, MemoryLog>, MemoryError> {
    log.lock()
        .map_err(|_| backend_error("memory store connection mutex is poisoned"))
}

/// Builds a [`MemoryError::Backend`] with `message`.
fn backend_error(message: impl Into<String>) -> MemoryError {
    MemoryError::Backend {
        message: message.into(),
    }
}

/// Errors raised while constructing a [`SqliteMemoryStore`].
#[derive(Debug, Error)]
pub enum SqliteMemoryStoreError {
    /// The underlying memory database could not be opened or initialized.
    #[error(transparent)]
    Open(#[from] MemoryLogError),
    /// The canonical workspace path was empty.
    #[error("canonical workspace path must not be empty")]
    EmptyWorkspacePath,
    /// The derived 16-character scope was rejected unexpectedly.
    #[error("failed to derive a deterministic workspace scope")]
    WorkspaceScope,
}
