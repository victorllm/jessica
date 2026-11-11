//! Deterministic embeddings and a brute-force vector index.
//!
//! [`DeterministicEmbedder`] is a fixed, reproducible byte n-gram embedding
//! used by tests and demonstrations. It deliberately has no semantic
//! understanding; [`BruteForceIndex`] scans the vectors stored in the memory
//! database and ranks them by cosine similarity.
//!
//! Both layers are synchronous and never read the host clock or the network.
//! Callers must supply embedding timestamps explicitly and run SQLite work
//! inside `tokio::task::spawn_blocking` when they are on an async worker.

use async_trait::async_trait;
use thiserror::Error;

use crate::{MemoryEntryId, MemoryLog, MemoryLogError, Timestamp, WorkspaceScope};

/// Number of dimensions produced by [`DeterministicEmbedder`].
const DETERMINISTIC_DIMS: usize = 256;

/// Stable identifier reported by [`DeterministicEmbedder`].
const DETERMINISTIC_MODEL_ID: &str = "deterministic-v1";

/// Bit mask used to select the sign of one hashed n-gram.
const SIGN_BIT: u64 = 1 << 63;

/// Errors raised while producing an embedding.
#[derive(Debug, Error)]
pub enum EmbedError {
    /// The embedder could not produce a vector for the supplied text.
    #[error("embedding failed: {message}")]
    Failed {
        /// Human-readable failure description.
        message: String,
    },
}

/// Produces a vector representation of text.
///
/// Implementations are asynchronous so a future HTTP or local-model backend
/// can replace the deterministic demo embedder without changing the memory
/// store. Implementations should report a stable [`Embedder::model_id`] and a
/// fixed [`Embedder::dims`] for every text they accept.
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Embeds `text` into a finite vector of length [`Embedder::dims`].
    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError>;

    /// Returns the stable identifier stored next to every produced vector.
    fn model_id(&self) -> &str;

    /// Returns the number of dimensions produced by this embedder.
    fn dims(&self) -> usize;
}

/// A deterministic, non-semantic embedding used for tests and demonstrations.
///
/// The algorithm hashes every UTF-8 byte n-gram of length 2, 3, and 4 with
/// 64-bit FNV-1a. The low eight bits select one of 256 buckets, the highest bit
/// selects the sign, and the resulting vector is L2-normalized. Empty or
/// whitespace-only input produces an all-zero vector.
///
/// **This embedder is not a semantic model.** It has no language
/// understanding, and hash collisions inject similarity noise. It must never be
/// used to judge production semantic-retrieval quality.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeterministicEmbedder;

impl DeterministicEmbedder {
    /// Creates the deterministic embedder.
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Embedder for DeterministicEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(deterministic_embed(text))
    }

    fn model_id(&self) -> &str {
        DETERMINISTIC_MODEL_ID
    }

    fn dims(&self) -> usize {
        DETERMINISTIC_DIMS
    }
}

/// Returns the fixed vector for `text`.
fn deterministic_embed(text: &str) -> Vec<f32> {
    let mut vector = vec![0.0_f32; DETERMINISTIC_DIMS];
    if text.trim().is_empty() {
        return vector;
    }

    let bytes = text.as_bytes();
    for width in 2..=4 {
        for ngram in bytes.windows(width) {
            let hash = fnv1a64(ngram);
            let bucket = (hash % DETERMINISTIC_DIMS as u64) as usize;
            let sign = if hash & SIGN_BIT == 0 { 1.0 } else { -1.0 };
            vector[bucket] += sign;
        }
    }

    let norm_sq: f32 = vector.iter().map(|value| value * value).sum();
    if norm_sq == 0.0 {
        return vector;
    }
    let norm = norm_sq.sqrt();
    for value in &mut vector {
        *value /= norm;
    }
    vector
}

/// Computes 64-bit FNV-1a over `bytes`.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Errors raised while encoding or decoding vectors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum VectorError {
    /// The byte length is not a multiple of four.
    #[error("vector byte length {len} is not a multiple of 4")]
    InvalidByteLength {
        /// Observed byte length.
        len: usize,
    },
    /// The decoded vector length differs from the requested dimension count.
    #[error("vector has {actual} dimensions but {expected} were expected")]
    DimensionMismatch {
        /// Dimension count requested by the caller.
        expected: usize,
        /// Dimension count implied by the byte length.
        actual: usize,
    },
    /// A vector component is NaN or infinite.
    #[error("vector component {index} is not finite")]
    NonFinite {
        /// Position of the invalid component.
        index: usize,
    },
    /// The dimension count cannot be represented by SQLite's signed integer.
    #[error("vector has {dims} dimensions, which cannot be stored")]
    DimensionsTooLarge {
        /// Rejected dimension count.
        dims: usize,
    },
}

/// Encodes `vector` as little-endian `f32` bytes.
///
/// Returns [`VectorError::NonFinite`] if any component is NaN or infinite.
pub fn encode_vector(vector: &[f32]) -> Result<Vec<u8>, VectorError> {
    ensure_finite(vector)?;
    let mut encoded = Vec::new();
    for value in vector {
        encoded.extend_from_slice(&value.to_le_bytes());
    }
    Ok(encoded)
}

/// Decodes `bytes` as `dims` little-endian `f32` values.
///
/// Returns [`VectorError::InvalidByteLength`] when the byte length is not a
/// multiple of four, [`VectorError::DimensionMismatch`] when it does not equal
/// `dims * 4`, and [`VectorError::NonFinite`] when a component is NaN or
/// infinite.
pub fn decode_vector(bytes: &[u8], dims: usize) -> Result<Vec<f32>, VectorError> {
    if !bytes.len().is_multiple_of(4) {
        return Err(VectorError::InvalidByteLength { len: bytes.len() });
    }
    let actual = bytes.len() / 4;
    if actual != dims {
        return Err(VectorError::DimensionMismatch {
            expected: dims,
            actual,
        });
    }

    let mut vector = Vec::with_capacity(dims);
    let (chunks, _) = bytes.as_chunks::<4>();
    for (index, chunk) in chunks.iter().enumerate() {
        let value = f32::from_le_bytes(*chunk);
        if !value.is_finite() {
            return Err(VectorError::NonFinite { index });
        }
        vector.push(value);
    }
    Ok(vector)
}

/// Rejects NaN and infinite components.
fn ensure_finite(vector: &[f32]) -> Result<(), VectorError> {
    for (index, value) in vector.iter().enumerate() {
        if !value.is_finite() {
            return Err(VectorError::NonFinite { index });
        }
    }
    Ok(())
}

/// Converts a dimension count into the signed integer SQLite stores.
fn dims_to_i64(dims: usize) -> Result<i64, VectorError> {
    i64::try_from(dims).map_err(|_| VectorError::DimensionsTooLarge { dims })
}

/// One entry returned by a vector search.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorMatch {
    /// Matching memory entry.
    entry_id: MemoryEntryId,
    /// Cosine similarity between the query and the stored vector.
    score: f32,
}

impl VectorMatch {
    /// Returns the matching entry identifier.
    pub const fn entry_id(self) -> MemoryEntryId {
        self.entry_id
    }

    /// Returns the cosine similarity score.
    pub const fn score(self) -> f32 {
        self.score
    }
}

/// Errors raised by a [`VectorIndex`].
#[derive(Debug, Error)]
pub enum VectorIndexError {
    /// A vector could not be encoded or decoded.
    #[error(transparent)]
    Vector(#[from] VectorError),
    /// The backing memory database failed.
    #[error(transparent)]
    Storage(#[from] MemoryLogError),
}

/// A synchronous vector index.
///
/// `add` replaces any existing vector for the same entry and model. `search`
/// returns at most `limit` live entries from one workspace and model whose
/// stored dimension count equals the query length.
pub trait VectorIndex {
    /// Stores or replaces `vector` for `entry_id` under `model`.
    fn add(
        &self,
        entry_id: MemoryEntryId,
        model: &str,
        vector: &[f32],
        created_at: Timestamp,
    ) -> Result<(), VectorIndexError>;

    /// Ranks live entries in `workspace` by cosine similarity to `query`.
    ///
    /// Results are sorted by descending score and then by ascending entry id,
    /// making ties deterministic. An empty query, a dimension mismatch, or a
    /// model with no stored vectors returns an empty result rather than an
    /// error. A zero-norm similarity is defined as `0.0`.
    fn search(
        &self,
        query: &[f32],
        model: &str,
        workspace: &WorkspaceScope,
        limit: usize,
    ) -> Result<Vec<VectorMatch>, VectorIndexError>;
}

/// A [`VectorIndex`] that scans every matching row in the memory database.
///
/// This is intentionally simple: it has no approximate-nearest-neighbor
/// structure and its cost grows linearly with the number of stored vectors.
#[derive(Debug, Clone, Copy)]
pub struct BruteForceIndex<'a> {
    /// Memory database that owns the entries and vectors.
    log: &'a MemoryLog,
}

impl<'a> BruteForceIndex<'a> {
    /// Creates an index over `log`.
    pub const fn new(log: &'a MemoryLog) -> Self {
        Self { log }
    }
}

impl VectorIndex for BruteForceIndex<'_> {
    fn add(
        &self,
        entry_id: MemoryEntryId,
        model: &str,
        vector: &[f32],
        created_at: Timestamp,
    ) -> Result<(), VectorIndexError> {
        let encoded = encode_vector(vector)?;
        let dims = dims_to_i64(vector.len())?;
        self.log
            .put_embedding(entry_id, model, dims, &encoded, created_at)?;
        Ok(())
    }

    fn search(
        &self,
        query: &[f32],
        model: &str,
        workspace: &WorkspaceScope,
        limit: usize,
    ) -> Result<Vec<VectorMatch>, VectorIndexError> {
        if query.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        ensure_finite(query)?;

        let dims = dims_to_i64(query.len())?;
        let stored = self.log.scan_embeddings(workspace, model, dims)?;
        let mut matches = Vec::with_capacity(stored.len());
        for (entry_id, encoded) in stored {
            let vector = decode_vector(&encoded, query.len())?;
            matches.push(VectorMatch {
                entry_id,
                score: cosine_similarity(query, &vector),
            });
        }

        matches.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.entry_id.cmp(&right.entry_id))
        });
        matches.truncate(limit);
        Ok(matches)
    }
}

/// Returns the cosine similarity of two equally sized vectors.
///
/// Either vector having zero norm produces `0.0`. A non-finite intermediate
/// result also degrades to `0.0` so callers never receive NaN.
fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        dot += left_value * right_value;
        left_norm += left_value * left_value;
        right_norm += right_value * right_value;
    }

    if left_norm == 0.0 || right_norm == 0.0 {
        return 0.0;
    }
    let score = dot / (left_norm.sqrt() * right_norm.sqrt());
    if score.is_finite() { score } else { 0.0 }
}
