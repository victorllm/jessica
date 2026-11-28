//! Content-addressed artifact storage, result management, and pagination.
//!
//! Large tool results are stored before they enter the transcript or model
//! context. The model sees a bounded preview and a digest; the stored bytes are
//! available through [`ArtifactStore::read_page`] and the [`ReadArtifactTool`].
//!
//! Writes use a same-directory temporary file, `fsync`, and rename, and never
//! leave a partially written artifact under its final name. Every artifact has
//! a v1 JSON sidecar at `<hex>.json` recording the digest, stored size, tool
//! name, creation time, original size, and truncation flag. Reads require and
//! validate that sidecar; artifacts written by older engine builds without a
//! sidecar are rejected. This is the strict CLI sidecar v1 format.
//!
//! Cleanup runs at most once per [`ResultManager`] (one turn). Retention uses
//! the sidecar's `created_at` timestamp, and the byte budget charges the data
//! file plus its sidecar. [`ReadArtifactTool`] results bypass result management
//! so pagination can never recursively create another artifact.

use std::fs::{self, DirBuilder, OpenOptions};
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use jessica_core::tool::{Tool, ToolSchema};
use jessica_core::types::ToolResult;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::warn;

use crate::config::ResultConfig;
use crate::ports::ToolResultManager;
use crate::tooling::ResultFormat;

/// Stable name of the artifact pagination tool.
pub const READ_ARTIFACT_TOOL: &str = "read_artifact";
/// Sidecar schema version written by this build.
const SIDECAR_VERSION: u32 = 1;
/// Bytes reserved in a `read_artifact` response for the text header.
const READ_PAGE_HEADER_RESERVE: usize = 512;
/// Maximum directory entries inspected by one cleanup pass.
const MAX_CLEANUP_ENTRIES: usize = 4096;

/// Errors raised by artifact reads, writes, and cleanup.
///
/// The enum is non-exhaustive in practice across compatible releases: new
/// variants may be added in a minor release, so downstream `match` expressions
/// should keep a wildcard arm.
#[derive(Debug, Error)]
pub enum ArtifactError {
    /// The supplied digest is not 64 hexadecimal characters.
    #[error(
        "artifact digest must be 64 hexadecimal characters, optionally prefixed with `sha256:`"
    )]
    InvalidDigest {
        /// Rejected input.
        digest: String,
    },
    /// No artifact exists for the digest.
    #[error("artifact `{digest}` does not exist")]
    Unknown {
        /// Normalized digest in `sha256:<hex>` form.
        digest: String,
    },
    /// The artifact file exists but is not a readable regular file or does not
    /// match its content digest.
    #[error("artifact `{digest}` is corrupt: {reason}")]
    Corrupt {
        /// Normalized digest in `sha256:<hex>` form.
        digest: String,
        /// Observed integrity failure.
        reason: String,
    },
    /// The sidecar is missing, unreadable, or inconsistent.
    #[error("artifact `{digest}` has a corrupt sidecar: {reason}")]
    CorruptSidecar {
        /// Normalized digest in `sha256:<hex>` form.
        digest: String,
        /// Observed sidecar failure.
        reason: String,
    },
    /// The requested offset is beyond the artifact.
    #[error("artifact offset {offset} is out of range for `{digest}` ({total} bytes)")]
    OffsetOutOfRange {
        /// Normalized digest in `sha256:<hex>` form.
        digest: String,
        /// Requested byte offset.
        offset: u64,
        /// Total stored bytes.
        total: u64,
    },
    /// A configured path is unsafe to use as an artifact directory.
    #[error("artifact path `{path}` is unsafe: {reason}")]
    UnsafePath {
        /// Rejected path.
        path: PathBuf,
        /// Why the path was rejected.
        reason: String,
    },
    /// A filesystem operation failed.
    #[error("artifact I/O failed for `{path}`: {source}")]
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Sidecar serialization failed.
    #[error("artifact sidecar could not be serialized: {0}")]
    Serialize(#[from] serde_json::Error),
    /// The blocking artifact task failed.
    #[error("artifact task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl ArtifactError {
    /// Builds an I/O error for `path`.
    fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

/// Result of storing one tool result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredArtifact {
    /// Digest in `sha256:<64 lowercase hex>` form.
    pub digest: String,
    /// Original result byte length before artifact truncation.
    pub original_bytes: usize,
    /// Bytes actually stored in the artifact.
    pub stored_bytes: usize,
    /// Whether the stored bytes are a truncated prefix of the original.
    pub truncated: bool,
    /// Absolute path of the stored artifact file.
    pub path: PathBuf,
}

/// One bounded artifact page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPage {
    /// Digest in `sha256:<64 lowercase hex>` form.
    pub digest: String,
    /// Actual offset after adjusting down to a UTF-8 boundary.
    pub offset: u64,
    /// Total stored bytes.
    pub total_bytes: u64,
    /// Offset of the next page, or `None` at the end.
    pub next_offset: Option<u64>,
    /// Page contents.
    pub content: String,
}

/// Observable outcome of one cleanup pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// Artifacts found with a valid data file and sidecar.
    pub scanned: usize,
    /// Artifacts whose data file was removed.
    pub deleted: usize,
    /// Removal failures.
    pub failed: usize,
    /// Bytes freed by successful removals, including sidecar bytes.
    pub freed_bytes: u64,
    /// Bytes retained after cleanup, including sidecar bytes.
    pub retained_bytes: u64,
}

/// Content-addressed artifact store rooted in one workspace.
#[derive(Debug, Clone)]
pub struct ArtifactStore {
    /// Workspace root supplied by the caller.
    workspace_root: PathBuf,
    /// Lexically validated relative artifact directory.
    artifact_dir: PathBuf,
    /// Maximum bytes stored for one artifact.
    max_artifact_bytes: usize,
    /// Age beyond which artifacts are removed, when retention is configured.
    retention: Option<Duration>,
    /// Total byte budget for retained artifacts, when configured.
    max_bytes: Option<u64>,
}

/// One artifact file considered for cleanup.
struct ArtifactRecord {
    /// 64 hex-character artifact name.
    digest: String,
    /// Data file path.
    data_path: PathBuf,
    /// Sidecar path.
    sidecar_path: PathBuf,
    /// Sidecar creation time in Unix milliseconds.
    created_at: u64,
    /// Data file length.
    data_size: u64,
    /// Sidecar file length.
    sidecar_size: u64,
}

impl ArtifactRecord {
    /// Total bytes charged to the artifact budget.
    fn total_size(&self) -> u64 {
        self.data_size.saturating_add(self.sidecar_size)
    }
}

/// Sidecar metadata for one artifact.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sidecar {
    /// Sidecar schema version.
    version: u32,
    /// `sha256:<hex>` digest of the stored bytes.
    digest: String,
    /// Stored byte length.
    size: u64,
    /// Tool that produced the result.
    tool: String,
    /// Creation time in Unix milliseconds.
    created_at: u64,
    /// Original result byte length.
    original_bytes: u64,
    /// Whether the artifact stores a truncated prefix.
    truncated: bool,
}

impl ArtifactStore {
    /// Creates a store. No filesystem access occurs until an operation runs.
    ///
    /// A store created this way has no retention or total-byte cleanup until
    /// [`Self::with_retention`] is called.
    pub fn new(
        workspace_root: impl Into<PathBuf>,
        artifact_dir: impl Into<PathBuf>,
        max_artifact_bytes: usize,
    ) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            artifact_dir: artifact_dir.into(),
            max_artifact_bytes,
            retention: None,
            max_bytes: None,
        }
    }

    /// Creates a store from a resolved result configuration.
    pub fn from_result_config(workspace_root: &Path, config: &ResultConfig) -> Self {
        Self::new(
            workspace_root.to_path_buf(),
            config.artifact_dir.clone(),
            config.max_artifact_bytes,
        )
        .with_retention(config.artifact_retention_days, config.artifact_max_bytes)
    }

    /// Configures retention and total-byte cleanup.
    ///
    /// Retention is measured against the sidecar's `created_at` timestamp. A
    /// zero retention removes every artifact created before the current
    /// millisecond; a zero byte budget removes every artifact after the
    /// retention pass. The budget charges the data file plus its sidecar.
    pub fn with_retention(mut self, retention_days: u64, max_bytes: u64) -> Self {
        self.retention = Some(Duration::from_secs(retention_days.saturating_mul(86_400)));
        self.max_bytes = Some(max_bytes);
        self
    }

    /// Runs one retention and size cleanup pass.
    ///
    /// The pass is best effort: unreadable entries and failed removals are
    /// counted in the report and never abort the remaining work. An absent
    /// artifact directory is a successful no-op. Only artifacts with a valid
    /// data file and sidecar are considered, and their sidecar `created_at`
    /// decides retention.
    pub async fn cleanup(&self) -> Result<CleanupReport, ArtifactError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.cleanup_sync()).await?
    }

    /// Writes `bytes` and returns their content address.
    ///
    /// Content longer than the configured artifact limit is truncated at a
    /// UTF-8 boundary. The digest always addresses the bytes actually stored.
    /// A v1 sidecar is written next to the data file.
    pub async fn write(&self, bytes: &[u8], tool: &str) -> Result<StoredArtifact, ArtifactError> {
        let store = self.clone();
        let bytes = bytes.to_vec();
        let tool = tool.to_owned();
        tokio::task::spawn_blocking(move || store.write_sync(&bytes, &tool)).await?
    }

    /// Reads at most `limit` bytes starting at `offset`.
    ///
    /// The offset and the returned page are adjusted down to UTF-8 boundaries.
    /// The artifact must be a regular file with a valid v1 sidecar whose digest,
    /// stored size, and truncation fields agree with the data file.
    pub async fn read_page(
        &self,
        digest: &str,
        offset: u64,
        limit: u64,
    ) -> Result<ArtifactPage, ArtifactError> {
        let store = self.clone();
        let digest = digest.to_owned();
        tokio::task::spawn_blocking(move || store.read_page_sync(&digest, offset, limit)).await?
    }

    /// Writes an artifact synchronously.
    fn write_sync(&self, bytes: &[u8], tool: &str) -> Result<StoredArtifact, ArtifactError> {
        self.write_at(bytes, tool, now_millis())
    }

    /// Writes an artifact with an explicit creation timestamp.
    ///
    /// Retention tests use this to construct sidecars at real age boundaries;
    /// production writes always use the current clock.
    fn write_at(
        &self,
        bytes: &[u8],
        tool: &str,
        created_at: u64,
    ) -> Result<StoredArtifact, ArtifactError> {
        let stored_len = floor_byte_boundary(bytes, self.max_artifact_bytes);
        let stored = &bytes[..stored_len];
        let truncated = stored_len < bytes.len();
        let hex = hex_digest(stored);
        let digest = format!("sha256:{hex}");

        let root = self.resolve_create_root()?;
        let shard = root.join(&hex[..2]);
        ensure_directory(&shard, true)?;
        let path = shard.join(&hex);
        let sidecar_path = shard.join(format!("{hex}.json"));

        let sidecar = Sidecar {
            version: SIDECAR_VERSION,
            digest: digest.clone(),
            size: u64::try_from(stored.len()).unwrap_or(u64::MAX),
            tool: tool.to_owned(),
            created_at,
            original_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            truncated,
        };
        let sidecar_bytes = serde_json::to_vec(&sidecar)?;

        atomic_write(&path, stored)?;
        atomic_write(&sidecar_path, &sidecar_bytes)?;

        Ok(StoredArtifact {
            digest,
            original_bytes: bytes.len(),
            stored_bytes: stored.len(),
            truncated,
            path,
        })
    }

    /// Reads a validated page synchronously.
    fn read_page_sync(
        &self,
        digest: &str,
        requested_offset: u64,
        limit: u64,
    ) -> Result<ArtifactPage, ArtifactError> {
        let hex = normalize_digest(digest)?;
        let normalized = format!("sha256:{hex}");
        let Some(root) = self.resolve_existing_root()? else {
            return Err(ArtifactError::Unknown { digest: normalized });
        };

        let shard = root.join(&hex[..2]);
        match fs::symlink_metadata(&shard) {
            Ok(metadata) if !metadata.file_type().is_symlink() && metadata.is_dir() => {}
            Ok(_) => {
                return Err(ArtifactError::Corrupt {
                    digest: normalized,
                    reason: "artifact shard is not a regular directory".to_owned(),
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ArtifactError::Unknown { digest: normalized });
            }
            Err(error) => return Err(ArtifactError::io(&shard, error)),
        }
        let path = shard.join(&hex);
        let sidecar_path = shard.join(format!("{hex}.json"));

        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ArtifactError::Unknown { digest: normalized });
            }
            Err(error) => return Err(ArtifactError::io(&path, error)),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(ArtifactError::Corrupt {
                digest: normalized,
                reason: "artifact path is not a regular file".to_owned(),
            });
        }

        let sidecar = read_sidecar(&sidecar_path, &hex)?;
        let bytes = fs::read(&path).map_err(|error| ArtifactError::io(&path, error))?;
        let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if sidecar.size != actual {
            return Err(ArtifactError::Corrupt {
                digest: normalized,
                reason: format!(
                    "sidecar records {} bytes but the artifact has {actual}",
                    sidecar.size
                ),
            });
        }
        if hex_digest(&bytes) != hex {
            return Err(ArtifactError::Corrupt {
                digest: normalized,
                reason: "content does not match the digest".to_owned(),
            });
        }

        if requested_offset > actual {
            return Err(ArtifactError::OffsetOutOfRange {
                digest: normalized,
                offset: requested_offset,
                total: actual,
            });
        }

        let text = String::from_utf8(bytes).map_err(|_| ArtifactError::Corrupt {
            digest: normalized.clone(),
            reason: "artifact content is not valid UTF-8".to_owned(),
        })?;
        let offset = floor_char_boundary(
            &text,
            usize::try_from(requested_offset).unwrap_or(usize::MAX),
        );
        let requested_end = offset.saturating_add(usize::try_from(limit).unwrap_or(usize::MAX));
        let end = floor_char_boundary(&text, requested_end.min(text.len()));
        let content = text[offset..end].to_owned();
        let next_offset = (end < text.len()).then_some(u64::try_from(end).unwrap_or(u64::MAX));

        Ok(ArtifactPage {
            digest: normalized,
            offset: u64::try_from(offset).unwrap_or(u64::MAX),
            total_bytes: u64::try_from(text.len()).unwrap_or(u64::MAX),
            next_offset,
            content,
        })
    }

    /// Resolves the artifact directory, creating it with `0700` permissions.
    fn resolve_create_root(&self) -> Result<PathBuf, ArtifactError> {
        let workspace = fs::canonicalize(&self.workspace_root)
            .map_err(|error| ArtifactError::io(&self.workspace_root, error))?;
        let mut current = workspace.clone();
        let components: Vec<_> = self.artifact_dir.components().collect();
        if components.is_empty() {
            return Err(ArtifactError::UnsafePath {
                path: self.artifact_dir.clone(),
                reason: "artifact directory must not be empty".to_owned(),
            });
        }
        let last = components.len() - 1;
        for (index, component) in components.into_iter().enumerate() {
            let Component::Normal(part) = component else {
                return Err(ArtifactError::UnsafePath {
                    path: self.artifact_dir.clone(),
                    reason: "must be relative and contain only normal components".to_owned(),
                });
            };
            current.push(part);
            ensure_directory(&current, index == last)?;
        }
        let canonical =
            fs::canonicalize(&current).map_err(|error| ArtifactError::io(&current, error))?;
        if !canonical.starts_with(&workspace) {
            return Err(ArtifactError::UnsafePath {
                path: canonical,
                reason: "resolves outside the workspace".to_owned(),
            });
        }
        Ok(canonical)
    }

    /// Resolves an existing artifact directory without creating or modifying
    /// it, returning `None` when the artifact directory is absent.
    fn resolve_existing_root(&self) -> Result<Option<PathBuf>, ArtifactError> {
        let workspace = fs::canonicalize(&self.workspace_root)
            .map_err(|error| ArtifactError::io(&self.workspace_root, error))?;
        if self.artifact_dir.components().next().is_none() {
            return Err(ArtifactError::UnsafePath {
                path: self.artifact_dir.clone(),
                reason: "artifact directory must not be empty".to_owned(),
            });
        }
        let mut current = workspace.clone();
        for component in self.artifact_dir.components() {
            let Component::Normal(part) = component else {
                return Err(ArtifactError::UnsafePath {
                    path: self.artifact_dir.clone(),
                    reason: "must be relative and contain only normal components".to_owned(),
                });
            };
            current.push(part);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(ArtifactError::UnsafePath {
                        path: current,
                        reason: "must not be a symlink".to_owned(),
                    });
                }
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => {
                    return Err(ArtifactError::UnsafePath {
                        path: current,
                        reason: "must be a directory".to_owned(),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(ArtifactError::io(&current, error)),
            }
        }
        let canonical =
            fs::canonicalize(&current).map_err(|error| ArtifactError::io(&current, error))?;
        if !canonical.starts_with(&workspace) {
            return Err(ArtifactError::UnsafePath {
                path: canonical,
                reason: "resolves outside the workspace".to_owned(),
            });
        }
        Ok(Some(canonical))
    }

    /// Removes expired and over-budget artifacts synchronously.
    fn cleanup_sync(&self) -> Result<CleanupReport, ArtifactError> {
        let Some(root) = self.resolve_existing_root()? else {
            return Ok(CleanupReport::default());
        };

        let now = now_millis();
        let cutoff = self.retention.map(|retention| {
            let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
            now.saturating_sub(retention_ms)
        });
        let mut artifacts = Vec::new();
        let mut inspected = 0usize;

        'shards: for shard in
            fs::read_dir(&root).map_err(|error| ArtifactError::io(&root, error))?
        {
            if inspected >= MAX_CLEANUP_ENTRIES {
                break;
            }
            let shard = shard.map_err(|error| ArtifactError::io(&root, error))?;
            let shard_path = shard.path();
            let Some(shard_name) = shard_path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !is_hex(shard_name, 2) {
                continue;
            }
            let shard_metadata = match fs::symlink_metadata(&shard_path) {
                Ok(metadata) => metadata,
                Err(error) => {
                    warn!(path = %shard_path.display(), error = %error, "artifact cleanup could not inspect a shard directory");
                    continue;
                }
            };
            if shard_metadata.file_type().is_symlink() || !shard_metadata.is_dir() {
                continue;
            }

            let entries = match fs::read_dir(&shard_path) {
                Ok(entries) => entries,
                Err(error) => {
                    warn!(path = %shard_path.display(), error = %error, "artifact cleanup could not read a shard directory");
                    continue;
                }
            };
            for entry in entries {
                if inspected >= MAX_CLEANUP_ENTRIES {
                    break 'shards;
                }
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        warn!(path = %shard_path.display(), error = %error, "artifact cleanup could not read an entry");
                        continue;
                    }
                };
                inspected += 1;
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                if !is_hex(name, 64) {
                    continue;
                }
                let metadata = match fs::symlink_metadata(&path) {
                    Ok(metadata) => metadata,
                    Err(error) => {
                        warn!(path = %path.display(), error = %error, "artifact cleanup could not inspect an artifact");
                        continue;
                    }
                };
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    continue;
                }
                let sidecar_path = shard_path.join(format!("{name}.json"));
                let sidecar = match read_sidecar(&sidecar_path, name) {
                    Ok(sidecar) => sidecar,
                    Err(error) => {
                        warn!(path = %sidecar_path.display(), error = %error, "artifact cleanup skipped an artifact with an unusable sidecar");
                        continue;
                    }
                };
                let sidecar_size = match fs::metadata(&sidecar_path) {
                    Ok(metadata) => metadata.len(),
                    Err(error) => {
                        warn!(path = %sidecar_path.display(), error = %error, "artifact cleanup could not size a sidecar");
                        continue;
                    }
                };
                if sidecar.size != metadata.len() {
                    continue;
                }
                artifacts.push(ArtifactRecord {
                    digest: name.to_owned(),
                    data_path: path,
                    sidecar_path,
                    created_at: sidecar.created_at,
                    data_size: metadata.len(),
                    sidecar_size,
                });
            }
        }

        artifacts.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.digest.cmp(&right.digest))
        });
        let mut report = CleanupReport {
            scanned: artifacts.len(),
            ..CleanupReport::default()
        };
        let mut remaining = Vec::new();
        for artifact in artifacts {
            let expired = cutoff.is_some_and(|cutoff| artifact.created_at < cutoff);
            if expired && remove_record(&artifact, &mut report) {
                continue;
            }
            remaining.push(artifact);
        }

        let mut total: u64 = remaining.iter().map(ArtifactRecord::total_size).sum();
        if let Some(max_bytes) = self.max_bytes {
            for artifact in &remaining {
                if total <= max_bytes {
                    break;
                }
                if remove_record(artifact, &mut report) {
                    total = total.saturating_sub(artifact.total_size());
                }
            }
        }
        report.retained_bytes = total;
        Ok(report)
    }
}

/// Manages tool results before they enter the transcript or model context.
///
/// Results above the inline threshold are stored as artifacts and replaced by
/// a bounded preview. The manager's output never exceeds `max_inline`, so the
/// result-size guard after it sees the managed content rather than the original
/// tool output. At most one cleanup pass runs per manager (one turn), and
/// [`READ_ARTIFACT_TOOL`] results bypass management entirely.
#[derive(Debug)]
pub struct ResultManager {
    /// Content-addressed storage.
    store: Arc<ArtifactStore>,
    /// Results at or below this length are returned unchanged.
    inline_threshold: usize,
    /// Hard cap for a managed result.
    max_inline: usize,
    /// Whether this turn has already attempted cleanup.
    cleaned: AtomicBool,
}

impl ResultManager {
    /// Builds a manager from the engine result configuration.
    ///
    /// `max_result_bytes` is the result-size guard's hard cap. Both the inline
    /// threshold and the managed envelope are clamped to it, so managing a
    /// result cannot itself produce content the guard must reject.
    pub fn new(workspace_root: &Path, config: &ResultConfig, max_result_bytes: usize) -> Self {
        let max_inline = config.max_inline_bytes.min(max_result_bytes);
        Self {
            store: Arc::new(ArtifactStore::from_result_config(workspace_root, config)),
            inline_threshold: config.inline_threshold_bytes.min(max_inline),
            max_inline,
            cleaned: AtomicBool::new(false),
        }
    }

    /// Replaces an oversized result with a bounded artifact preview.
    ///
    /// `read_artifact` results bypass management so pagination can never
    /// recursively create another artifact. A failed artifact write degrades
    /// to a bounded inline preview without failing the turn.
    pub async fn manage(
        &self,
        tool: &str,
        format: ResultFormat,
        mut result: ToolResult,
    ) -> ToolResult {
        if tool == READ_ARTIFACT_TOOL || result.content.len() <= self.inline_threshold {
            return result;
        }

        self.cleanup_once().await;
        let content = std::mem::take(&mut result.content);
        match self.store.write(content.as_bytes(), tool).await {
            Ok(stored) => {
                tracing::debug!(
                    tool,
                    digest = %stored.digest,
                    stored_bytes = stored.stored_bytes,
                    truncated = stored.truncated,
                    path = %stored.path.display(),
                    "stored oversized tool result as an artifact"
                );
                result.content = self.render_artifact(format, tool, &content, &stored);
            }
            Err(error) => {
                warn!(
                    tool,
                    error = %error,
                    "could not persist a tool result artifact; returning a bounded inline preview"
                );
                result.content = self.render_fallback(format, tool, &content, &error.to_string());
            }
        }
        result
    }

    /// Runs at most one cleanup per manager (one turn).
    async fn cleanup_once(&self) {
        if self.cleaned.swap(true, Ordering::SeqCst) {
            return;
        }
        match self.store.cleanup().await {
            Ok(report) if report.failed > 0 => {
                warn!(
                    scanned = report.scanned,
                    deleted = report.deleted,
                    failed = report.failed,
                    freed_bytes = report.freed_bytes,
                    retained_bytes = report.retained_bytes,
                    "artifact cleanup completed with failures"
                );
            }
            Ok(report) => {
                tracing::debug!(
                    scanned = report.scanned,
                    deleted = report.deleted,
                    freed_bytes = report.freed_bytes,
                    retained_bytes = report.retained_bytes,
                    "artifact cleanup completed"
                );
            }
            Err(error) => {
                warn!(error = %error, "artifact cleanup failed; continuing the turn");
            }
        }
    }

    /// Renders a successful artifact envelope.
    fn render_artifact(
        &self,
        format: ResultFormat,
        tool: &str,
        content: &str,
        stored: &StoredArtifact,
    ) -> String {
        let stored_text = &content[..stored.stored_bytes.min(content.len())];
        let initial = self.inline_threshold.min(stored_text.len());
        match format {
            ResultFormat::Text => self.fit_preview(stored_text, initial, |preview, next| {
                format!(
                    "[result artifact]\n\
                     tool: {tool}\n\
                     digest: {}\n\
                     original_bytes: {}\n\
                     stored_bytes: {}\n\
                     truncated: {}\n\
                     next_offset: {}\n\
                     preview:\n{preview}",
                    stored.digest,
                    stored.original_bytes,
                    stored.stored_bytes,
                    stored.truncated,
                    next.map_or_else(|| "none".to_owned(), |value| value.to_string()),
                )
            }),
            ResultFormat::Json => self.fit_preview(stored_text, initial, |preview, next| {
                json!({
                    "result_artifact": {
                        "tool": tool,
                        "digest": stored.digest,
                        "original_bytes": stored.original_bytes,
                        "stored_bytes": stored.stored_bytes,
                        "truncated": stored.truncated,
                        "next_offset": next,
                    },
                    "preview": preview,
                })
                .to_string()
            }),
        }
    }

    /// Renders a bounded preview when the artifact write failed.
    fn render_fallback(
        &self,
        format: ResultFormat,
        tool: &str,
        content: &str,
        error: &str,
    ) -> String {
        let initial = self.max_inline.min(content.len());
        match format {
            ResultFormat::Text => self.fit_preview(content, initial, |preview, _| {
                format!(
                    "[result truncated]\n\
                     artifact: unavailable ({error})\n\
                     tool: {tool}\n\
                     original_bytes: {}\n\
                     max_inline_bytes: {}\n\
                     preview:\n{preview}",
                    content.len(),
                    self.max_inline,
                )
            }),
            ResultFormat::Json => self.fit_preview(content, initial, |preview, _| {
                json!({
                    "result_artifact": {
                        "tool": tool,
                        "digest": Value::Null,
                        "original_bytes": content.len(),
                        "stored_bytes": 0,
                        "truncated": true,
                        "next_offset": Value::Null,
                        "error": error,
                    },
                    "preview": preview,
                })
                .to_string()
            }),
        }
    }

    /// Shrinks `preview` until the rendered envelope fits `max_inline`.
    fn fit_preview(
        &self,
        source: &str,
        initial: usize,
        render: impl Fn(&str, Option<u64>) -> String,
    ) -> String {
        let mut cap = initial.min(source.len());
        loop {
            let boundary = floor_char_boundary(source, cap);
            let preview = &source[..boundary];
            let next = (boundary < source.len()).then_some(boundary as u64);
            let rendered = render(preview, next);
            if rendered.len() <= self.max_inline || boundary == 0 {
                return if rendered.len() <= self.max_inline {
                    rendered
                } else {
                    truncate_text(&rendered, self.max_inline)
                };
            }
            let overflow = rendered.len() - self.max_inline;
            cap = boundary.saturating_sub(overflow.max(64));
        }
    }
}

#[async_trait]
impl ToolResultManager for ResultManager {
    async fn manage(&self, tool: &str, format: ResultFormat, result: ToolResult) -> ToolResult {
        ResultManager::manage(self, tool, format, result).await
    }
}

/// Reads artifacts created by the result manager.
///
/// The tool accepts only an artifact digest, never a filesystem path, and its
/// own results bypass [`ResultManager`] so pagination cannot create artifacts
/// recursively.
pub struct ReadArtifactTool {
    /// Shared artifact store.
    store: Arc<ArtifactStore>,
    /// Effective hard cap for one page and response.
    max_inline: usize,
}

impl ReadArtifactTool {
    /// Builds the tool from an existing store and effective inline cap.
    pub fn new(store: Arc<ArtifactStore>, max_inline: usize) -> Self {
        Self { store, max_inline }
    }
}

/// Arguments accepted by [`ReadArtifactTool`].
#[derive(Deserialize)]
struct ReadArtifactArgs {
    /// Digest of the artifact to read.
    digest: String,
    /// Byte offset, defaulting to zero.
    #[serde(default)]
    offset: u64,
    /// Requested page length.
    limit: Option<u64>,
}

#[async_trait]
impl Tool for ReadArtifactTool {
    fn name(&self) -> &str {
        READ_ARTIFACT_TOOL
    }

    fn schema(&self) -> ToolSchema {
        json!({
            "type": "function",
            "function": {
                "name": READ_ARTIFACT_TOOL,
                "description": "Read a byte range from an artifact created by a large tool result. Pass the digest shown in the result, not a filesystem path.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "digest": {
                            "type": "string",
                            "description": "Artifact digest, with or without the `sha256:` prefix."
                        },
                        "offset": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "Byte offset. Adjusts down to a UTF-8 boundary."
                        },
                        "limit": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Maximum bytes to return, capped by tools.max_inline_bytes."
                        }
                    },
                    "required": ["digest"]
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> ToolResult {
        let args: ReadArtifactArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => {
                return ToolResult::error(
                    String::new(),
                    format!("invalid read_artifact arguments: {error}"),
                );
            }
        };
        if args.limit == Some(0) {
            return ToolResult::error(String::new(), "read_artifact limit must be at least 1");
        }
        let page_cap = u64::try_from(
            self.max_inline
                .saturating_sub(READ_PAGE_HEADER_RESERVE)
                .max(1),
        )
        .unwrap_or(u64::MAX);
        let limit = args.limit.unwrap_or(page_cap).min(page_cap);

        match self.store.read_page(&args.digest, args.offset, limit).await {
            Ok(page) => {
                let next = page
                    .next_offset
                    .map_or_else(|| "none".to_owned(), |value| value.to_string());
                let content = format!(
                    "[artifact]\n\
                     digest: {}\n\
                     offset: {}\n\
                     bytes: {}\n\
                     total_bytes: {}\n\
                     next_offset: {next}\n\
                     content:\n{}",
                    page.digest,
                    page.offset,
                    page.content.len(),
                    page.total_bytes,
                    page.content,
                );
                ToolResult::success(String::new(), content)
            }
            Err(error) => ToolResult::error(String::new(), error.to_string()),
        }
    }
}

/// Reads and validates a sidecar against its expected artifact name.
fn read_sidecar(path: &Path, expected_hex: &str) -> Result<Sidecar, ArtifactError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| ArtifactError::CorruptSidecar {
        digest: format!("sha256:{expected_hex}"),
        reason: error.to_string(),
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ArtifactError::CorruptSidecar {
            digest: format!("sha256:{expected_hex}"),
            reason: "sidecar is not a regular file".to_owned(),
        });
    }
    let bytes = fs::read(path).map_err(|error| ArtifactError::CorruptSidecar {
        digest: format!("sha256:{expected_hex}"),
        reason: error.to_string(),
    })?;
    let sidecar: Sidecar =
        serde_json::from_slice(&bytes).map_err(|error| ArtifactError::CorruptSidecar {
            digest: format!("sha256:{expected_hex}"),
            reason: error.to_string(),
        })?;
    if sidecar.version != SIDECAR_VERSION {
        return Err(ArtifactError::CorruptSidecar {
            digest: format!("sha256:{expected_hex}"),
            reason: format!("unsupported sidecar version {}", sidecar.version),
        });
    }
    let sidecar_hex =
        normalize_digest(&sidecar.digest).map_err(|_| ArtifactError::CorruptSidecar {
            digest: format!("sha256:{expected_hex}"),
            reason: "sidecar digest is invalid".to_owned(),
        })?;
    if sidecar_hex != expected_hex {
        return Err(ArtifactError::CorruptSidecar {
            digest: format!("sha256:{expected_hex}"),
            reason: "sidecar digest does not match the artifact name".to_owned(),
        });
    }
    if sidecar.tool.trim().is_empty() {
        return Err(ArtifactError::CorruptSidecar {
            digest: format!("sha256:{expected_hex}"),
            reason: "sidecar tool name is empty".to_owned(),
        });
    }
    if sidecar.original_bytes < sidecar.size
        || sidecar.truncated != (sidecar.original_bytes > sidecar.size)
    {
        return Err(ArtifactError::CorruptSidecar {
            digest: format!("sha256:{expected_hex}"),
            reason: "sidecar size and truncation fields are inconsistent".to_owned(),
        });
    }
    Ok(sidecar)
}

/// Normalizes a digest to 64 lowercase hexadecimal characters.
fn normalize_digest(digest: &str) -> Result<String, ArtifactError> {
    let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ArtifactError::InvalidDigest {
            digest: digest.to_owned(),
        });
    }
    Ok(hex.to_ascii_lowercase())
}

/// Returns the lowercase SHA-256 digest of `bytes`.
fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

/// Creates a directory with `0700` permissions, optionally tightening an
/// existing directory.
fn ensure_directory(path: &Path, enforce_mode: bool) -> Result<(), ArtifactError> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(ArtifactError::UnsafePath {
            path: path.to_path_buf(),
            reason: "artifact directories must not be symlinks".to_owned(),
        }),
        Ok(metadata) if metadata.is_dir() => {
            if enforce_mode {
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                    .map_err(|error| ArtifactError::io(path, error))?;
            }
            Ok(())
        }
        Ok(_) => Err(ArtifactError::UnsafePath {
            path: path.to_path_buf(),
            reason: "artifact path component is not a directory".to_owned(),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = DirBuilder::new();
            builder.mode(0o700);
            builder
                .create(path)
                .map_err(|error| ArtifactError::io(path, error))
        }
        Err(error) => Err(ArtifactError::io(path, error)),
    }
}

/// Atomically writes `bytes` to `path` with mode `0600`.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), ArtifactError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    let parent = path.parent().ok_or_else(|| ArtifactError::UnsafePath {
        path: path.to_path_buf(),
        reason: "artifact path has no parent directory".to_owned(),
    })?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ArtifactError::UnsafePath {
            path: path.to_path_buf(),
            reason: "artifact path has no UTF-8 file name".to_owned(),
        })?;
    let unique = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(".{name}.tmp-{}-{unique}", std::process::id()));

    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)
            .map_err(|error| ArtifactError::io(&temp, error))?;
        file.write_all(bytes)
            .map_err(|error| ArtifactError::io(&temp, error))?;
        file.sync_all()
            .map_err(|error| ArtifactError::io(&temp, error))?;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|error| ArtifactError::io(&temp, error))?;
        drop(file);
        fs::rename(&temp, path).map_err(|error| ArtifactError::io(path, error))
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Returns the largest UTF-8 boundary at or below `limit`.
fn floor_byte_boundary(bytes: &[u8], limit: usize) -> usize {
    let mut index = limit.min(bytes.len());
    while index > 0 && !is_utf8_boundary(bytes, index) {
        index -= 1;
    }
    index
}

/// Returns the largest UTF-8 boundary at or below `limit` in `text`.
fn floor_char_boundary(text: &str, limit: usize) -> usize {
    floor_byte_boundary(text.as_bytes(), limit)
}

/// Returns whether `index` is a valid UTF-8 boundary.
fn is_utf8_boundary(bytes: &[u8], index: usize) -> bool {
    index == 0 || index == bytes.len() || (bytes[index] & 0b1100_0000) != 0b1000_0000
}

/// Truncates `text` to at most `max_bytes` without splitting a character.
fn truncate_text(text: &str, max_bytes: usize) -> String {
    text[..floor_char_boundary(text, max_bytes)].to_owned()
}

/// Returns the current Unix time in milliseconds.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Returns whether `text` is exactly `len` lowercase hexadecimal characters.
fn is_hex(text: &str, len: usize) -> bool {
    text.len() == len
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// Removes one record, logging failures without aborting the pass.
fn remove_record(record: &ArtifactRecord, report: &mut CleanupReport) -> bool {
    match fs::remove_file(&record.data_path) {
        Ok(()) => {
            report.deleted += 1;
            report.freed_bytes = report.freed_bytes.saturating_add(record.data_size);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            report.failed += 1;
            warn!(path = %record.data_path.display(), error = %error, "artifact cleanup could not remove an artifact");
            return false;
        }
    }

    match fs::remove_file(&record.sidecar_path) {
        Ok(()) => {
            report.freed_bytes = report.freed_bytes.saturating_add(record.sidecar_size);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            report.failed += 1;
            warn!(path = %record.sidecar_path.display(), error = %error, "artifact cleanup could not remove a sidecar");
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt, symlink};

    use super::*;

    /// Unique temporary workspace removed on drop.
    struct TempDir {
        /// Directory path.
        path: PathBuf,
    }

    impl TempDir {
        /// Creates a unique temporary directory.
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "jessica-engine-artifacts-{label}-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).expect("temporary directory must be created");
            Self {
                path: fs::canonicalize(&path).expect("temporary directory must resolve"),
            }
        }

        /// Returns the workspace root.
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// Returns a small-budget configuration for focused result-manager tests.
    fn config() -> ResultConfig {
        ResultConfig {
            inline_threshold_bytes: 8,
            max_inline_bytes: 80,
            max_artifact_bytes: 1024,
            artifact_dir: PathBuf::from(".jessica/artifacts"),
            artifact_retention_days: 7,
            artifact_max_bytes: 1024 * 1024,
        }
    }

    /// Returns the CLI's production-sized configuration.
    fn large_config() -> ResultConfig {
        ResultConfig {
            inline_threshold_bytes: 32_768,
            max_inline_bytes: 65_536,
            max_artifact_bytes: 16_777_216,
            artifact_dir: PathBuf::from(".jessica/artifacts"),
            artifact_retention_days: 7,
            artifact_max_bytes: 268_435_456,
        }
    }

    /// Returns the mode bits of `path`.
    fn mode(path: &Path) -> u32 {
        fs::metadata(path)
            .expect("path metadata must read")
            .permissions()
            .mode()
            & 0o777
    }

    /// Extracts the digest header from a text envelope.
    fn envelope_digest(content: &str) -> &str {
        content
            .lines()
            .find_map(|line| line.strip_prefix("digest: "))
            .expect("envelope must carry a digest")
    }

    /// Builds the sidecar path for one stored artifact.
    fn sidecar_path(artifact: &StoredArtifact) -> PathBuf {
        let hex = artifact
            .digest
            .strip_prefix("sha256:")
            .expect("digest must be namespaced");
        artifact
            .path
            .parent()
            .expect("artifact must have a shard")
            .join(format!("{hex}.json"))
    }

    /// Returns every non-sidecar artifact file under the default directory.
    fn artifact_files(root: &Path) -> Vec<PathBuf> {
        let artifacts = root.join(".jessica/artifacts");
        let mut files = Vec::new();
        let Ok(shards) = fs::read_dir(&artifacts) else {
            return files;
        };
        for shard in shards.flatten() {
            let Ok(entries) = fs::read_dir(shard.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let name = entry.file_name();
                if name.to_str().is_some_and(|name| is_hex(name, 64)) {
                    files.push(entry.path());
                }
            }
        }
        files.sort();
        files
    }

    /// Returns a v1 sidecar value for on-disk fixture tests.
    fn v1_sidecar(hex: &str, size: u64, original: u64, truncated: bool) -> Value {
        json!({
            "version": 1,
            "digest": format!("sha256:{hex}"),
            "size": size,
            "tool": "bash",
            "created_at": 1_700_000_000_000_u64,
            "original_bytes": original,
            "truncated": truncated,
        })
    }

    /// Writes a hand-built v1 artifact and returns its hex digest.
    fn write_fixture(root: &Path, bytes: &[u8]) -> String {
        let hex = hex_digest(bytes);
        let shard = root.join(".jessica/artifacts").join(&hex[..2]);
        fs::create_dir_all(&shard).expect("fixture shard must be created");
        fs::write(shard.join(&hex), bytes).expect("fixture data must be written");
        let sidecar = v1_sidecar(
            &hex,
            u64::try_from(bytes.len()).expect("fixture must fit u64"),
            u64::try_from(bytes.len()).expect("fixture must fit u64"),
            false,
        );
        fs::write(
            shard.join(format!("{hex}.json")),
            serde_json::to_vec(&sidecar).expect("fixture sidecar must serialize"),
        )
        .expect("fixture sidecar must be written");
        hex
    }

    #[tokio::test]
    async fn stored_bytes_round_trip_through_bounded_pages() {
        let temp = TempDir::new("round-trip");
        let store = ArtifactStore::from_result_config(&temp.path, &config());
        let stored = store
            .write("hello world".as_bytes(), "echo")
            .await
            .expect("artifact write must succeed");

        assert!(stored.digest.starts_with("sha256:"));
        assert_eq!(stored.stored_bytes, 11);
        assert!(!stored.truncated);
        assert!(sidecar_path(&stored).is_file(), "a sidecar must be written");

        let page = store
            .read_page(&stored.digest, 6, 5)
            .await
            .expect("artifact page must be readable");
        assert_eq!(page.content, "world");
        assert_eq!(page.next_offset, None);
        assert_eq!(page.total_bytes, 11);
    }

    #[tokio::test]
    async fn artifact_content_is_truncated_at_a_character_boundary() {
        let temp = TempDir::new("truncate");
        let mut configured = config();
        configured.max_artifact_bytes = 4;
        let store = ArtifactStore::from_result_config(&temp.path, &configured);
        let stored = store
            .write("aéé".as_bytes(), "echo")
            .await
            .expect("artifact write must succeed");

        assert_eq!(stored.stored_bytes, 3);
        assert!(stored.truncated);
        let sidecar: Value =
            serde_json::from_slice(&fs::read(sidecar_path(&stored)).expect("sidecar must read"))
                .expect("sidecar must be JSON");
        assert_eq!(sidecar["size"], 3);
        assert_eq!(sidecar["original_bytes"], 5);
        assert_eq!(sidecar["truncated"], true);
        let page = store
            .read_page(&stored.digest, 0, 10)
            .await
            .expect("page must be readable");
        assert_eq!(page.content, "aé");
    }

    #[tokio::test]
    async fn small_results_bypass_the_artifact_store() {
        let temp = TempDir::new("small");
        let configured = ResultConfig {
            inline_threshold_bytes: 8,
            ..large_config()
        };
        let manager = ResultManager::new(&temp.path, &configured, 1_048_576);
        let result = manager
            .manage(
                "read_file",
                ResultFormat::Text,
                ToolResult::success("call", "12345678"),
            )
            .await;

        assert_eq!(result.content, "12345678");
        assert!(
            !temp.path().join(".jessica").exists(),
            "a small result must not create the artifact directory"
        );
    }

    #[tokio::test]
    async fn large_read_file_result_is_stored_with_a_verifiable_digest() {
        let temp = TempDir::new("large-read");
        let configured = large_config();
        let manager = ResultManager::new(&temp.path, &configured, 1_048_576);
        let content = "a".repeat(100 * 1024);

        let result = manager
            .manage(
                "read_file",
                ResultFormat::Text,
                ToolResult::success("call", content.clone()),
            )
            .await;

        assert!(!result.is_error);
        assert!(result.content.starts_with("[result artifact]"));
        assert!(
            result.content.len() <= 64 * 1024,
            "managed result must fit the inline cap: {}",
            result.content.len()
        );
        assert!(result.content.contains("original_bytes: 102400"));
        assert!(result.content.contains("stored_bytes: 102400"));
        assert!(result.content.contains("truncated: false"));
        assert!(result.content.contains("next_offset: 32768"));

        let expected = format!("sha256:{}", hex_digest(content.as_bytes()));
        assert_eq!(envelope_digest(&result.content), expected);
        let hex = expected
            .strip_prefix("sha256:")
            .expect("digest must be namespaced");
        let artifact = temp
            .path()
            .join(".jessica/artifacts")
            .join(&hex[..2])
            .join(hex);
        assert_eq!(
            fs::read(&artifact).expect("artifact must exist"),
            content.as_bytes()
        );
        assert_eq!(mode(&artifact), 0o600, "artifact files must be private");
        assert_eq!(
            mode(&temp.path().join(".jessica/artifacts")),
            0o700,
            "artifact directories must be private"
        );
        assert_eq!(
            mode(&temp.path().join(".jessica/artifacts").join(&hex[..2])),
            0o700,
            "shard directories must be private"
        );

        let sidecar_file = artifact.with_extension("json");
        assert_eq!(mode(&sidecar_file), 0o600);
        let sidecar: Value =
            serde_json::from_slice(&fs::read(&sidecar_file).expect("sidecar must exist"))
                .expect("sidecar must be JSON");
        assert_eq!(sidecar["version"], 1);
        assert_eq!(sidecar["size"], 102_400);
        assert_eq!(sidecar["tool"], "read_file");
        assert_eq!(sidecar["digest"], expected);
        assert_eq!(sidecar["original_bytes"], 102_400);
        assert_eq!(sidecar["truncated"], false);
        assert!(
            sidecar["created_at"]
                .as_u64()
                .is_some_and(|value| value > 0),
            "created_at must be a Unix millisecond timestamp"
        );
        assert!(
            !fs::read_dir(artifact.parent().expect("artifact must have a shard"))
                .expect("shard must read")
                .flatten()
                .any(|entry| entry.file_name().to_string_lossy().contains(".tmp-")),
            "no temporary file may survive a successful write"
        );
    }

    #[tokio::test]
    async fn artifact_truncation_respects_utf8_boundaries_and_the_preview_budget() {
        let temp = TempDir::new("truncate-preview");
        let configured = ResultConfig {
            inline_threshold_bytes: 100,
            max_inline_bytes: 1024,
            max_artifact_bytes: 1024,
            ..large_config()
        };
        let manager = ResultManager::new(&temp.path, &configured, 1_048_576);
        let content = format!("{}{}", "a".repeat(999), "é".repeat(101));

        let result = manager
            .manage(
                "bash",
                ResultFormat::Text,
                ToolResult::success("call", content.clone()),
            )
            .await;

        assert!(
            result.content.len() <= 1024,
            "truncated envelope must fit the inline cap: {}",
            result.content.len()
        );
        assert!(result.content.contains("original_bytes: 1201"));
        assert!(result.content.contains("truncated: true"));

        let digest = envelope_digest(&result.content).to_owned();
        let hex = digest
            .strip_prefix("sha256:")
            .expect("digest must be namespaced");
        let artifact = temp
            .path()
            .join(".jessica/artifacts")
            .join(&hex[..2])
            .join(hex);
        let stored = fs::read(&artifact).expect("truncated artifact must exist");
        assert_eq!(stored.len(), 1023, "truncation must back up to a boundary");
        assert_eq!(stored, content.as_bytes()[..1023]);
        assert_eq!(hex_digest(&stored), hex);
        assert!(
            String::from_utf8(stored).is_ok(),
            "truncated artifact must remain valid UTF-8"
        );
    }

    #[tokio::test]
    async fn json_tools_receive_a_json_envelope_within_the_inline_cap() {
        let temp = TempDir::new("json");
        let configured = large_config();
        let manager = ResultManager::new(&temp.path, &configured, 1_048_576);
        let content = "x".repeat(100 * 1024);

        let result = manager
            .manage(
                "echo_json",
                ResultFormat::Json,
                ToolResult::success("call", content),
            )
            .await;

        let envelope: Value =
            serde_json::from_str(&result.content).expect("JSON tools must receive JSON");
        assert_eq!(envelope["result_artifact"]["tool"], "echo_json");
        assert_eq!(envelope["result_artifact"]["original_bytes"], 102_400);
        assert_eq!(envelope["result_artifact"]["truncated"], false);
        assert_eq!(envelope["result_artifact"]["next_offset"], 32_768);
        assert!(
            result.content.len() <= 64 * 1024,
            "JSON escaping must not exceed the inline cap"
        );
    }

    #[tokio::test]
    async fn read_artifact_paginates_and_never_creates_another_artifact() {
        let temp = TempDir::new("paginate");
        let configured = large_config();
        let manager = ResultManager::new(&temp.path, &configured, 1_048_576);
        let content = "0123456789".repeat(10 * 1024);
        let managed = manager
            .manage(
                "read_file",
                ResultFormat::Text,
                ToolResult::success("call", content.clone()),
            )
            .await;
        let digest = envelope_digest(&managed.content).to_owned();
        let store = Arc::new(ArtifactStore::from_result_config(&temp.path, &configured));
        let tool = ReadArtifactTool::new(store, configured.max_inline_bytes);
        let before = artifact_files(temp.path());

        let first = tool.execute(json!({"digest": digest, "limit": 1000})).await;
        assert!(!first.is_error, "{}", first.content);
        assert!(first.content.contains("offset: 0"));
        assert!(first.content.contains("bytes: 1000"));
        assert!(first.content.contains("next_offset: 1000"));
        assert!(first.content.ends_with(&content[..1000]));

        let second = tool
            .execute(json!({"digest": digest, "offset": 1000, "limit": 1000}))
            .await;
        assert!(!second.is_error, "{}", second.content);
        assert!(second.content.contains("offset: 1000"));
        assert!(
            second.content.ends_with(&content[1000..2000]),
            "pagination must resume exactly at the requested byte"
        );

        let end = tool
            .execute(json!({"digest": digest, "offset": content.len()}))
            .await;
        assert!(!end.is_error, "{}", end.content);
        assert!(end.content.contains("bytes: 0"));
        assert!(end.content.contains("next_offset: none"));

        let beyond = tool
            .execute(json!({"digest": digest, "offset": content.len() as u64 + 1}))
            .await;
        assert!(beyond.is_error);
        assert!(
            beyond.content.contains("out of range"),
            "{}",
            beyond.content
        );

        let zero = tool.execute(json!({"digest": digest, "limit": 0})).await;
        assert!(zero.is_error);
        assert!(zero.content.contains("at least 1"));

        let managed_read = manager
            .manage(
                READ_ARTIFACT_TOOL,
                ResultFormat::Text,
                ToolResult::success("call", content),
            )
            .await;
        assert!(
            managed_read.content.starts_with("0123456789"),
            "read_artifact results must bypass management"
        );
        assert_eq!(
            artifact_files(temp.path()),
            before,
            "pagination and the manager bypass must not create artifacts"
        );
    }

    #[tokio::test]
    async fn read_artifact_reports_unknown_corrupt_and_escaping_paths() {
        let temp = TempDir::new("errors");
        let outside = TempDir::new("outside");
        let configured = config();
        let store = ArtifactStore::from_result_config(&temp.path, &configured);
        let stored = store
            .write_at(b"artifact content", "bash", now_millis())
            .expect("artifact must write");
        let hex = stored
            .digest
            .strip_prefix("sha256:")
            .expect("digest must be namespaced");
        let tool = ReadArtifactTool::new(Arc::new(store.clone()), configured.max_inline_bytes);

        let unknown = tool.execute(json!({"digest": "a".repeat(64)})).await;
        assert!(unknown.is_error);
        assert!(unknown.content.contains("does not exist"));

        let invalid = tool.execute(json!({"digest": "../escape"})).await;
        assert!(invalid.is_error);
        assert!(invalid.content.contains("64 hexadecimal"));

        let sidecar = stored.path.with_extension("json");
        fs::write(&sidecar, "{not json").expect("sidecar must be corruptible");
        let corrupt = tool.execute(json!({"digest": hex})).await;
        assert!(corrupt.is_error);
        assert!(
            corrupt.content.contains("corrupt sidecar"),
            "{}",
            corrupt.content
        );

        let second = store
            .write_at(b"second artifact", "bash", now_millis())
            .expect("second artifact must write");
        let second_hex = second
            .digest
            .strip_prefix("sha256:")
            .expect("digest must be namespaced");
        let shard = second
            .path
            .parent()
            .expect("artifact must have a shard")
            .to_path_buf();
        let outside_shard = outside.path().join("shard");
        fs::create_dir_all(&outside_shard).expect("outside shard must be created");
        fs::copy(&second.path, outside_shard.join(second_hex))
            .expect("artifact must be copied outside");
        fs::remove_dir_all(&shard).expect("shard must be removable");
        symlink(&outside_shard, &shard).expect("shard symlink must be created");

        let escaped = tool.execute(json!({"digest": second_hex})).await;
        assert!(escaped.is_error, "a symlinked shard must not be read");
        assert!(
            escaped.content.contains("not a regular directory"),
            "{}",
            escaped.content
        );
        assert!(
            !escaped.content.contains("second artifact"),
            "escaping the artifact root must never return outside content"
        );
    }

    #[tokio::test]
    async fn a_cli_v1_sidecar_fixture_is_read_by_the_engine_store() {
        let temp = TempDir::new("fixture");
        let content = b"artifact written by the old CLI store";
        let hex = write_fixture(temp.path(), content);
        let store = ArtifactStore::from_result_config(temp.path(), &config());

        let page = store
            .read_page(&format!("sha256:{hex}"), 0, 100)
            .await
            .expect("a hand-built CLI v1 artifact must be readable");

        assert_eq!(page.content, "artifact written by the old CLI store");
        assert_eq!(page.total_bytes, u64::try_from(content.len()).unwrap());
        assert_eq!(page.next_offset, None);
    }

    #[tokio::test]
    async fn read_page_rejects_missing_corrupt_and_inconsistent_sidecars() {
        let configured = config();
        let read = |temp: &TempDir, hex: String| {
            let store = ArtifactStore::from_result_config(temp.path(), &configured);
            async move { store.read_page(&hex, 0, 100).await }
        };

        let missing = TempDir::new("sidecar-missing");
        let hex = write_fixture(missing.path(), b"missing sidecar");
        fs::remove_file(
            missing
                .path()
                .join(".jessica/artifacts")
                .join(&hex[..2])
                .join(format!("{hex}.json")),
        )
        .expect("sidecar must be removable");
        let error = read(&missing, hex)
            .await
            .expect_err("a missing sidecar must be rejected");
        assert!(error.to_string().contains("corrupt sidecar"), "{error}");

        let malformed = TempDir::new("sidecar-malformed");
        let hex = write_fixture(malformed.path(), b"malformed sidecar");
        fs::write(
            malformed
                .path()
                .join(".jessica/artifacts")
                .join(&hex[..2])
                .join(format!("{hex}.json")),
            b"{not json",
        )
        .expect("sidecar must be corruptible");
        let error = read(&malformed, hex)
            .await
            .expect_err("malformed JSON must be rejected");
        assert!(error.to_string().contains("corrupt sidecar"), "{error}");

        let versioned = TempDir::new("sidecar-version");
        let hex = write_fixture(versioned.path(), b"future sidecar");
        let mut sidecar = v1_sidecar(&hex, 13, 13, false);
        sidecar["version"] = json!(2);
        fs::write(
            versioned
                .path()
                .join(".jessica/artifacts")
                .join(&hex[..2])
                .join(format!("{hex}.json")),
            serde_json::to_vec(&sidecar).expect("sidecar must serialize"),
        )
        .expect("sidecar must be writable");
        let error = read(&versioned, hex)
            .await
            .expect_err("an unsupported version must be rejected");
        assert!(
            error.to_string().contains("unsupported sidecar version 2"),
            "{error}"
        );

        let digests = TempDir::new("sidecar-digest");
        let hex = write_fixture(digests.path(), b"digest sidecar");
        let mut sidecar = v1_sidecar(&hex, 13, 13, false);
        sidecar["digest"] = json!("b".repeat(64));
        fs::write(
            digests
                .path()
                .join(".jessica/artifacts")
                .join(&hex[..2])
                .join(format!("{hex}.json")),
            serde_json::to_vec(&sidecar).expect("sidecar must serialize"),
        )
        .expect("sidecar must be writable");
        let error = read(&digests, hex)
            .await
            .expect_err("a mismatched sidecar digest must be rejected");
        assert!(
            error
                .to_string()
                .contains("sidecar digest does not match the artifact name"),
            "{error}"
        );

        let sized = TempDir::new("sidecar-size");
        let hex = write_fixture(sized.path(), b"size sidecar");
        let mut sidecar = v1_sidecar(&hex, 99, 99, false);
        sidecar["truncated"] = json!(false);
        fs::write(
            sized
                .path()
                .join(".jessica/artifacts")
                .join(&hex[..2])
                .join(format!("{hex}.json")),
            serde_json::to_vec(&sidecar).expect("sidecar must serialize"),
        )
        .expect("sidecar must be writable");
        let error = read(&sized, hex)
            .await
            .expect_err("a sidecar size mismatch must be rejected");
        assert!(
            error.to_string().contains("sidecar records 99 bytes"),
            "{error}"
        );

        let inconsistent = TempDir::new("sidecar-inconsistent");
        let hex = write_fixture(inconsistent.path(), b"inconsistent sidecar");
        let sidecar = v1_sidecar(&hex, 20, 20, true);
        fs::write(
            inconsistent
                .path()
                .join(".jessica/artifacts")
                .join(&hex[..2])
                .join(format!("{hex}.json")),
            serde_json::to_vec(&sidecar).expect("sidecar must serialize"),
        )
        .expect("sidecar must be writable");
        let error = read(&inconsistent, hex)
            .await
            .expect_err("inconsistent truncation fields must be rejected");
        assert!(
            error
                .to_string()
                .contains("size and truncation fields are inconsistent"),
            "{error}"
        );

        let linked = TempDir::new("sidecar-symlink");
        let hex = write_fixture(linked.path(), b"symlinked sidecar");
        let sidecar_file = linked
            .path()
            .join(".jessica/artifacts")
            .join(&hex[..2])
            .join(format!("{hex}.json"));
        let target = linked.path().join("outside-sidecar.json");
        fs::rename(&sidecar_file, &target).expect("sidecar must move outside");
        symlink(&target, &sidecar_file).expect("sidecar symlink must be created");
        let error = read(&linked, hex)
            .await
            .expect_err("a symlinked sidecar must be rejected");
        assert!(
            error.to_string().contains("sidecar is not a regular file"),
            "{error}"
        );

        let tampered = TempDir::new("data-digest");
        let hex = write_fixture(tampered.path(), b"tampered data");
        fs::write(
            tampered
                .path()
                .join(".jessica/artifacts")
                .join(&hex[..2])
                .join(&hex),
            b"evil content!",
        )
        .expect("fixture data must be replaceable");
        let error = read(&tampered, hex)
            .await
            .expect_err("data must match its digest");
        assert!(
            error
                .to_string()
                .contains("content does not match the digest"),
            "{error}"
        );
    }

    #[test]
    fn read_artifact_schema_requires_only_the_digest() {
        let temp = TempDir::new("schema");
        let tool = ReadArtifactTool::new(
            Arc::new(ArtifactStore::from_result_config(temp.path(), &config())),
            config().max_inline_bytes,
        );
        let schema = tool.schema();

        assert_eq!(schema["function"]["name"], READ_ARTIFACT_TOOL);
        assert_eq!(
            schema["function"]["parameters"]["required"],
            json!(["digest"])
        );
        assert_eq!(
            schema["function"]["parameters"]["properties"]["offset"]["minimum"],
            0
        );
        assert_eq!(
            schema["function"]["parameters"]["properties"]["limit"]["minimum"],
            1
        );
        assert_eq!(tool.name(), READ_ARTIFACT_TOOL);
    }

    #[tokio::test]
    async fn cleanup_removes_expired_artifacts_and_oldest_over_budget_artifacts() {
        let temp = TempDir::new("cleanup");
        let now = now_millis();
        let day = 86_400_000;

        let retention =
            ArtifactStore::new(temp.path(), "artifacts", 1024).with_retention(1, u64::MAX);
        let old = retention
            .write_at(b"old", "bash", now.saturating_sub(2 * day))
            .expect("old artifact must write");
        let fresh = retention
            .write_at(b"fresh", "bash", now)
            .expect("fresh artifact must write");
        let report = retention.cleanup_sync().expect("cleanup must succeed");
        assert_eq!(report.deleted, 1);
        assert!(!old.path.exists(), "expired artifact must be removed");
        assert!(fresh.path.exists(), "fresh artifact must be retained");
        assert!(
            !sidecar_path(&old).exists(),
            "the old sidecar must be removed"
        );
        assert!(sidecar_path(&fresh).exists());

        let mut capped =
            ArtifactStore::new(temp.path(), "capped-artifacts", 1024).with_retention(7, u64::MAX);
        let first = capped
            .write_at(b"first", "bash", now.saturating_sub(3000))
            .expect("first artifact must write");
        let second = capped
            .write_at(b"second", "bash", now.saturating_sub(2000))
            .expect("second artifact must write");
        let third = capped
            .write_at(b"third", "bash", now.saturating_sub(1000))
            .expect("third artifact must write");
        let newest_only = third.path.metadata().expect("third metadata").len()
            + sidecar_path(&third)
                .metadata()
                .expect("third sidecar")
                .len();
        capped.max_bytes = Some(newest_only);
        let report = capped.cleanup_sync().expect("capped cleanup must succeed");
        assert_eq!(report.deleted, 2);
        assert_eq!(report.failed, 0);
        assert!(!first.path.exists() && !second.path.exists());
        assert!(third.path.exists(), "the newest artifact must survive");
        assert!(report.retained_bytes <= newest_only);
        assert_eq!(report.scanned, 3);
    }

    #[tokio::test]
    async fn cleanup_runs_once_per_manager() {
        let temp = TempDir::new("cleanup-once");
        let configured = large_config();
        let manager = ResultManager::new(&temp.path, &configured, 1_048_576);
        let expired_at = now_millis().saturating_sub(8 * 86_400_000);
        let old = manager
            .store
            .write_at(b"old", "bash", expired_at)
            .expect("old artifact must write");
        let content = "m".repeat(70_000);

        manager
            .manage(
                "read_file",
                ResultFormat::Text,
                ToolResult::success("call", content.clone()),
            )
            .await;
        assert!(
            !old.path.exists(),
            "the first oversized result must run cleanup"
        );

        let late = manager
            .store
            .write_at(b"late", "bash", expired_at)
            .expect("late artifact must write");
        manager
            .manage(
                "read_file",
                ResultFormat::Text,
                ToolResult::success("call", content),
            )
            .await;
        assert!(
            late.path.exists(),
            "later results must not run a second cleanup in the same manager"
        );
    }

    #[tokio::test]
    async fn cleanup_and_write_failures_degrade_to_a_bounded_preview() {
        let temp = TempDir::new("degrade");
        fs::write(temp.path().join("blocked"), "not a directory")
            .expect("blocking file must be created");
        let configured = ResultConfig {
            artifact_dir: PathBuf::from("blocked"),
            inline_threshold_bytes: 8,
            ..large_config()
        };
        let manager = ResultManager::new(&temp.path, &configured, 1_048_576);
        let content = "z".repeat(100 * 1024);

        let result = manager
            .manage(
                "bash",
                ResultFormat::Text,
                ToolResult::success("call", content),
            )
            .await;

        assert!(
            result.content.contains("artifact: unavailable"),
            "{}",
            result.content
        );
        assert!(
            result.content.len() <= 64 * 1024,
            "the degraded preview must stay bounded: {}",
            result.content.len()
        );
        assert_eq!(
            fs::read_to_string(temp.path().join("blocked")).expect("blocking file must remain"),
            "not a directory"
        );
    }

    #[tokio::test]
    async fn an_absolute_artifact_directory_is_rejected() {
        let temp = TempDir::new("unsafe");
        let store = ArtifactStore::new(&temp.path, "/tmp/not-a-relative-artifact-dir", 1024);
        let error = store
            .write(b"content", "echo")
            .await
            .expect_err("an absolute artifact directory must be rejected");
        assert!(
            matches!(error, ArtifactError::UnsafePath { .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn result_manager_output_is_bounded_before_the_result_guard() {
        let temp = TempDir::new("manager");
        let mut configured = config();
        configured.max_inline_bytes = 240;
        let manager = ResultManager::new(&temp.path, &configured, 320);
        let original = "x".repeat(512);
        let managed = manager
            .manage(
                "echo",
                ResultFormat::Text,
                ToolResult::success("call-1", original),
            )
            .await;

        assert!(managed.content.len() <= 240, "{}", managed.content.len());
        assert!(managed.content.contains("[result artifact]"));
        assert!(managed.content.contains("digest: sha256:"));
        assert!(managed.content.contains("preview:"));
    }

    #[tokio::test]
    async fn cleanup_without_an_artifact_directory_is_a_no_op() {
        let temp = TempDir::new("cleanup-absent");
        let store =
            ArtifactStore::new(&temp.path, ".jessica/artifacts", 1024).with_retention(7, 1024);

        let report = store.cleanup().await.expect("cleanup must succeed");

        assert_eq!(report, CleanupReport::default());
        assert!(!temp.path.join(".jessica/artifacts").exists());
    }

    #[tokio::test]
    async fn cleanup_skips_entries_that_are_not_artifact_files() {
        let temp = TempDir::new("cleanup-filter");
        let store = ArtifactStore::new(&temp.path, ".jessica/artifacts", 1024)
            .with_retention(7, 1024 * 1024);
        let artifact = store
            .write_at(
                b"artifact",
                "echo",
                now_millis().saturating_sub(8 * 86_400_000),
            )
            .expect("artifact write must succeed");
        let shard = artifact
            .path
            .parent()
            .expect("artifact must have a shard")
            .to_path_buf();
        let junk = shard.join("notes.txt");
        fs::write(&junk, b"keep me").expect("junk file must be written");
        let fake_dir = shard.join("0".repeat(64));
        fs::create_dir(&fake_dir).expect("fake artifact directory must be created");
        let link = shard.join("f".repeat(64));
        symlink(&artifact.path, &link).expect("symlink must be created");

        let report = store.cleanup().await.expect("cleanup must succeed");

        assert_eq!(report.scanned, 1);
        assert_eq!(report.deleted, 1);
        assert_eq!(report.failed, 0);
        assert!(!artifact.path.exists());
        assert_eq!(fs::read(&junk).expect("junk file must remain"), b"keep me");
        assert!(fake_dir.is_dir());
        assert!(
            fs::symlink_metadata(&link).is_ok(),
            "symlink entries must not be removed"
        );
    }
}
