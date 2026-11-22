//! Shared workspace-bound path resolution for native tools.
//!
//! Every path a tool receives is joined to the canonical workspace root and
//! canonicalized before use. A canonical path that does not start with the root
//! is rejected, so `..`, absolute paths outside the workspace, and symlinks that
//! escape it cannot be followed. A time-of-check / time-of-use race remains
//! possible until the Phase 3 sandbox provides stronger containment.

use std::path::{Path, PathBuf};

/// Resolves the workspace root to an absolute, symlink-free path.
pub(crate) async fn canonical_root(root: &Path) -> Result<PathBuf, String> {
    tokio::fs::canonicalize(root).await.map_err(|error| {
        format!(
            "failed to resolve workspace root `{}`: {error}",
            root.display()
        )
    })
}

/// Resolves an existing path and checks that its canonical form stays inside
/// `root`.
pub(crate) async fn resolve_existing(root: &Path, requested: &str) -> Result<PathBuf, String> {
    let candidate = root.join(requested);
    let canonical = tokio::fs::canonicalize(&candidate)
        .await
        .map_err(|error| format!("failed to resolve `{requested}`: {error}"))?;
    ensure_inside(root, &canonical, requested)?;
    Ok(canonical)
}

/// Resolves a write target, allowing the final component not to exist while
/// still rejecting escapes through `..` or symlinks.
pub(crate) async fn resolve_for_write(root: &Path, requested: &str) -> Result<PathBuf, String> {
    let candidate = root.join(requested);

    match tokio::fs::symlink_metadata(&candidate).await {
        Ok(_) => {
            let canonical = tokio::fs::canonicalize(&candidate)
                .await
                .map_err(|error| format!("failed to resolve `{requested}`: {error}"))?;
            ensure_inside(root, &canonical, requested)?;
            Ok(canonical)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = candidate
                .parent()
                .ok_or_else(|| format!("path `{requested}` has no parent"))?;
            let canonical_parent = tokio::fs::canonicalize(parent).await.map_err(|error| {
                format!("failed to resolve the parent of `{requested}`: {error}")
            })?;
            ensure_inside(root, &canonical_parent, requested)?;
            let name = candidate
                .file_name()
                .ok_or_else(|| format!("path `{requested}` has no file name"))?;
            Ok(canonical_parent.join(name))
        }
        Err(error) => Err(format!("failed to inspect `{requested}`: {error}")),
    }
}

/// Rejects canonical paths outside the workspace root.
pub(crate) fn ensure_inside(root: &Path, path: &Path, requested: &str) -> Result<(), String> {
    if path.starts_with(root) {
        Ok(())
    } else {
        Err(format!("path `{requested}` is outside the workspace"))
    }
}
