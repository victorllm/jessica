//! Shared workspace-scope derivation.
//!
//! A workspace scope is the first 16 hexadecimal characters of
//! `sha256(canonical_workspace_path)`. The memory and skill stores both bind
//! themselves to one scope at construction time so callers cannot widen a
//! store to another workspace by supplying a different scope on each call.

use std::path::Path;

use sha2::{Digest, Sha256};

use crate::memory_log::WorkspaceScope;

/// Number of hexadecimal characters kept from the workspace path digest.
const WORKSPACE_SCOPE_HEX_LEN: usize = 16;

/// Why a workspace path could not be converted into a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspaceScopeError {
    /// The path's encoded bytes were empty.
    EmptyPath,
    /// The derived value was unexpectedly rejected by [`WorkspaceScope::new`].
    InvalidScope,
}

/// Derives `hex(sha256(canonical_workspace))[..16]`.
///
/// The caller must supply an already-canonicalized path. This helper
/// deliberately does not call `std::fs::canonicalize`, which is blocking
/// filesystem I/O and could change the scope if the process later changes its
/// working directory.
pub(crate) fn workspace_scope_for(
    canonical_workspace: &Path,
) -> Result<WorkspaceScope, WorkspaceScopeError> {
    let bytes = canonical_workspace.as_os_str().as_encoded_bytes();
    if bytes.is_empty() {
        return Err(WorkspaceScopeError::EmptyPath);
    }

    let digest = Sha256::digest(bytes);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut scope = String::with_capacity(WORKSPACE_SCOPE_HEX_LEN);
    for byte in &digest[..WORKSPACE_SCOPE_HEX_LEN / 2] {
        scope.push(char::from(HEX[usize::from(byte >> 4)]));
        scope.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }

    WorkspaceScope::new(scope).map_err(|_| WorkspaceScopeError::InvalidScope)
}
