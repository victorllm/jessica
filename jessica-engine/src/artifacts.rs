//! Content-addressed artifact storage and bounded result management.
//!
//! Large tool results are stored before they enter the transcript or model
//! context. The model sees a bounded preview and a digest; the stored bytes are
//! available through [`ArtifactStore::read_page`]. Writes use a same-directory
//! temporary file, `fsync`, and rename, and never leave a partially written
//! artifact under its final name.
//!
//! E1 stores artifacts but does not yet run retention cleanup or expose the
//! `read_artifact` tool. Those remain adapter/later-phase concerns.

use std::fs::{self, DirBuilder, OpenOptions};
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use jessica_core::types::ToolResult;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::warn;

use crate::config::ResultConfig;
use crate::ports::ToolResultManager;
use crate::tooling::ResultFormat;

/// Errors raised by artifact reads and writes.
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
        /// Normalized digest.
        digest: String,
    },
    /// The artifact file exists but is not a readable regular file or does not
    /// match its content digest.
    #[error("artifact `{digest}` is corrupt: {reason}")]
    Corrupt {
        /// Normalized digest.
        digest: String,
        /// Observed integrity failure.
        reason: String,
    },
    /// The requested offset is beyond the artifact.
    #[error("artifact offset {offset} is out of range for `{digest}` ({total} bytes)")]
    OffsetOutOfRange {
        /// Normalized digest.
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

/// Content-addressed artifact store rooted in one workspace.
#[derive(Debug, Clone)]
pub struct ArtifactStore {
    /// Workspace root supplied by the caller.
    workspace_root: PathBuf,
    /// Lexically validated relative artifact directory.
    artifact_dir: PathBuf,
    /// Maximum bytes stored for one artifact.
    max_artifact_bytes: usize,
}

impl ArtifactStore {
    /// Creates a store. No filesystem access occurs until an operation runs.
    pub fn new(
        workspace_root: impl Into<PathBuf>,
        artifact_dir: impl Into<PathBuf>,
        max_artifact_bytes: usize,
    ) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            artifact_dir: artifact_dir.into(),
            max_artifact_bytes,
        }
    }

    /// Creates a store from a resolved result configuration.
    pub fn from_result_config(workspace_root: &Path, config: &ResultConfig) -> Self {
        Self::new(
            workspace_root.to_path_buf(),
            config.artifact_dir.clone(),
            config.max_artifact_bytes,
        )
    }

    /// Writes `bytes` and returns their content address.
    ///
    /// Content longer than the configured artifact limit is truncated at a
    /// UTF-8 boundary. The digest always addresses the bytes actually stored.
    pub async fn write(&self, bytes: &[u8], tool: &str) -> Result<StoredArtifact, ArtifactError> {
        let store = self.clone();
        let bytes = bytes.to_vec();
        let tool = tool.to_owned();
        tokio::task::spawn_blocking(move || store.write_sync(&bytes, &tool)).await?
    }

    /// Reads at most `limit` bytes starting at `offset`.
    ///
    /// The offset and the returned page are adjusted down to UTF-8 boundaries.
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
    fn write_sync(&self, bytes: &[u8], _tool: &str) -> Result<StoredArtifact, ArtifactError> {
        let stored_len = floor_byte_boundary(bytes, self.max_artifact_bytes);
        let stored = &bytes[..stored_len];
        let truncated = stored_len < bytes.len();
        let hex = hex_digest(stored);
        let digest = format!("sha256:{hex}");

        let root = self.resolve_create_root()?;
        let shard = root.join(&hex[..2]);
        ensure_directory(&shard, true)?;
        let path = shard.join(&hex);
        atomic_write(&path, stored)?;

        Ok(StoredArtifact {
            digest,
            original_bytes: bytes.len(),
            stored_bytes: stored.len(),
            truncated,
            path,
        })
    }

    /// Reads a page synchronously.
    fn read_page_sync(
        &self,
        digest: &str,
        offset: u64,
        limit: u64,
    ) -> Result<ArtifactPage, ArtifactError> {
        let hex = normalize_digest(digest)?;
        let root = self.resolve_existing_root()?;
        let path = root.join(&hex[..2]).join(&hex);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ArtifactError::Unknown {
                    digest: format!("sha256:{hex}"),
                }
            } else {
                ArtifactError::io(&path, error)
            }
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(ArtifactError::Corrupt {
                digest: format!("sha256:{hex}"),
                reason: "artifact is not a regular file".to_owned(),
            });
        }
        let bytes = fs::read(&path).map_err(|error| ArtifactError::io(&path, error))?;
        if hex_digest(&bytes) != hex {
            return Err(ArtifactError::Corrupt {
                digest: format!("sha256:{hex}"),
                reason: "artifact content does not match its digest".to_owned(),
            });
        }
        let total = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if offset > total {
            return Err(ArtifactError::OffsetOutOfRange {
                digest: format!("sha256:{hex}"),
                offset,
                total,
            });
        }
        let mut start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        while start > 0 && !is_utf8_boundary(&bytes, start) {
            start -= 1;
        }
        let requested_end = start
            .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
            .min(bytes.len());
        let mut end = requested_end;
        while end > start && !is_utf8_boundary(&bytes, end) {
            end -= 1;
        }
        let content = std::str::from_utf8(&bytes[start..end])
            .map_err(|error| ArtifactError::Corrupt {
                digest: format!("sha256:{hex}"),
                reason: error.to_string(),
            })?
            .to_owned();
        Ok(ArtifactPage {
            digest: format!("sha256:{hex}"),
            offset: u64::try_from(start).unwrap_or(u64::MAX),
            total_bytes: total,
            next_offset: (end < bytes.len()).then_some(u64::try_from(end).unwrap_or(u64::MAX)),
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
    /// it.
    fn resolve_existing_root(&self) -> Result<PathBuf, ArtifactError> {
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
                        reason: "artifact directories must not be symlinks".to_owned(),
                    });
                }
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => {
                    return Err(ArtifactError::UnsafePath {
                        path: current,
                        reason: "artifact path component is not a directory".to_owned(),
                    });
                }
                Err(error) => {
                    return Err(ArtifactError::io(&current, error));
                }
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
        Ok(canonical)
    }
}

/// Manages tool results before they enter the transcript or model context.
///
/// Results above the inline threshold are stored as artifacts and replaced by
/// a bounded preview. The manager's output never exceeds `max_inline`, so the
/// result-size guard after it sees the managed content rather than the original
/// tool output.
#[derive(Debug)]
pub struct ResultManager {
    /// Content-addressed storage.
    store: Arc<ArtifactStore>,
    /// Results at or below this length are returned unchanged.
    inline_threshold: usize,
    /// Hard cap for a managed result.
    max_inline: usize,
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
        }
    }

    /// Replaces an oversized result with a bounded artifact preview.
    ///
    /// A failed artifact write degrades to a bounded inline preview without
    /// failing the turn.
    pub async fn manage(
        &self,
        tool: &str,
        format: ResultFormat,
        mut result: ToolResult,
    ) -> ToolResult {
        if result.content.len() <= self.inline_threshold {
            return result;
        }

        let content = std::mem::take(&mut result.content);
        match self.store.write(content.as_bytes(), tool).await {
            Ok(stored) => {
                tracing::debug!(
                    tool,
                    digest = %stored.digest,
                    stored_bytes = stored.stored_bytes,
                    truncated = stored.truncated,
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
            cap = boundary.saturating_sub(overflow.max(32));
        }
    }
}

#[async_trait]
impl ToolResultManager for ResultManager {
    async fn manage(&self, tool: &str, format: ResultFormat, result: ToolResult) -> ToolResult {
        ResultManager::manage(self, tool, format, result).await
    }
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Creates a unique directory and removes it on drop.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "jessica-engine-artifacts-{label}-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temp dir must be created");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

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
        let page = store
            .read_page(&stored.digest, 0, 10)
            .await
            .expect("page must be readable");
        assert_eq!(page.content, "aé");
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
}
