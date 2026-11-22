//! Crash-safe replacement for workspace files.
//!
//! Existing file tools write directly. The editor tools added in Phase 9 use a
//! temporary file in the target directory, flush it to disk, and rename it over
//! the target so a failed validation or partial write cannot leave a truncated
//! file. The temporary file is removed on every error path.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::AsyncWriteExt;

use crate::workspace::{canonical_root, resolve_existing};

/// Disambiguates temporary files created by concurrent calls.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
/// Number of names to try before reporting a collision.
const TEMP_ATTEMPTS: u64 = 64;

/// Atomically replaces an existing regular file inside `root`.
///
/// The target must already exist and stay inside the workspace after symlink
/// resolution. The new contents are written to a sibling temporary file,
/// flushed with `sync_all`, and renamed over the target. The original file is
/// untouched until the rename.
pub(crate) async fn atomic_replace_existing(
    root: &Path,
    requested: &str,
    content: &[u8],
) -> Result<PathBuf, String> {
    let root = canonical_root(root).await?;
    let path = resolve_existing(&root, requested).await?;
    let metadata = tokio::fs::metadata(&path)
        .await
        .map_err(|error| format!("failed to inspect `{requested}`: {error}"))?;
    if !metadata.is_file() {
        return Err(format!("`{requested}` is not a regular file"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| format!("path `{requested}` has no parent"))?;
    let permissions = metadata.permissions();

    let (temporary, mut file) = create_temporary(parent, requested).await?;
    let written: std::io::Result<()> = async {
        file.write_all(content).await?;
        file.flush().await?;
        file.sync_all().await
    }
    .await;
    drop(file);

    if let Err(error) = written {
        remove_temporary(&temporary).await;
        return Err(format!("failed to write `{requested}`: {error}"));
    }

    if let Err(error) = tokio::fs::set_permissions(&temporary, permissions).await {
        remove_temporary(&temporary).await;
        return Err(format!(
            "failed to preserve permissions for `{requested}`: {error}"
        ));
    }

    if let Err(error) = tokio::fs::rename(&temporary, &path).await {
        remove_temporary(&temporary).await;
        return Err(format!(
            "failed to replace `{requested}` atomically: {error}"
        ));
    }

    sync_parent(parent).await;
    Ok(path)
}

/// Creates an exclusive temporary file in `parent`.
async fn create_temporary(
    parent: &Path,
    requested: &str,
) -> Result<(PathBuf, tokio::fs::File), String> {
    let mut last_error = None;
    for _ in 0..TEMP_ATTEMPTS {
        let sequence = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(".jessica-tmp-{}-{sequence}", std::process::id()));
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
            .await
        {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                last_error = Some(error);
            }
            Err(error) => {
                return Err(format!(
                    "failed to create a temporary file for `{requested}`: {error}"
                ));
            }
        }
    }
    Err(format!(
        "failed to create a unique temporary file for `{requested}`: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "name collision limit reached".to_owned())
    ))
}

/// Removes a temporary file, ignoring errors from an already-absent file.
async fn remove_temporary(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

/// Flushes the directory entry after a rename where the platform supports it.
async fn sync_parent(parent: &Path) {
    #[cfg(unix)]
    {
        if let Ok(directory) = tokio::fs::File::open(parent).await {
            let _ = directory.sync_all().await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
    }
}
