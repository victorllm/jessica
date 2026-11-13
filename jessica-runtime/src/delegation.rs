//! Structural permission narrowing for delegated child agents.
//!
//! A parent delegates work by requesting a subset of its own tools. This module
//! computes the only set of tools the child may receive:
//!
//! - the request must not repeat a tool name;
//! - every requested name must be in the parent allowlist;
//! - the result is the request intersected with tools the parent can execute
//!   natively, minus [`DELEGATE_TASK_TOOL_NAME`].
//!
//! WASM tools and other non-native tools are silently dropped even when the
//! parent allowlist permits them: v1 has no isolated child WASM instances, so an
//! exposed instance would share mutable guest state between children. Delegation
//! is single-level, so `delegate_task` is always dropped even if a caller lists
//! it as native.
//!
//! The module is pure and deterministic: it performs no I/O, makes no model
//! call, and does not depend on the CLI or any concrete tool. The resulting
//! [`NarrowedTools`] is the structural input of the child tool factory; later
//! stages must build exactly these names and nothing else.

use std::collections::BTreeSet;
use std::time::Duration;

use thiserror::Error;

/// Name of the tool that delegates work to a child agent.
///
/// The child never receives this tool: [`narrow_tools`] always removes it, and
/// a child that forges a call gets the agent loop's unknown-tool error.
pub const DELEGATE_TASK_TOOL_NAME: &str = "delegate_task";

/// Wall-clock timeout applied to each individual child tool call.
///
/// v1 fixes this value instead of exposing a child-specific configuration. It
/// must stay strictly below [`DelegationLimits::timeout`], which in turn stays
/// below the parent's tool timeout; the CLI validates the full nesting.
pub const CHILD_TOOL_TIMEOUT: Duration = Duration::from_millis(5_000);

/// Smallest accepted [`DelegationLimits::max_result_bytes`].
///
/// The limit applies to the final serialized JSON. The empty-answer truncated
/// skeleton and the empty-error skeleton must both fit, so a positive limit
/// alone is not sufficient.
pub const MIN_RESULT_BYTES: usize = 128;

/// Tool names the parent agent is allowed to use.
///
/// Both sets are stored de-duplicated. `native` describes tools the parent can
/// execute without WASM; names outside `allowed` are harmless here because
/// [`narrow_tools`] checks the allowlist first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentCapabilities {
    /// Parent policy allowlist.
    allowed: BTreeSet<String>,
    /// Tools the parent can execute natively.
    native: BTreeSet<String>,
}

impl ParentCapabilities {
    /// Creates parent capabilities from an allowlist and a native tool set.
    ///
    /// Duplicate names in either input are collapsed. The constructor does not
    /// validate that `native` is a subset of `allowed`: a requested name must
    /// pass the allowlist check regardless, and a native-but-not-allowed name
    /// is therefore never exposed to a child.
    pub fn new(
        allowed: impl IntoIterator<Item = String>,
        native: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            allowed: allowed.into_iter().collect(),
            native: native.into_iter().collect(),
        }
    }
}

/// Deterministic set of tool names a child agent may receive.
///
/// Names are stored sorted and de-duplicated. The type exposes read-only
/// iteration only; once narrowing has produced it, the set cannot be widened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NarrowedTools {
    /// Permitted names in ascending order.
    names: BTreeSet<String>,
}

impl NarrowedTools {
    /// Returns the permitted names in ascending order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(String::as_str)
    }

    /// Returns whether no tool is permitted.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Returns how many tools are permitted.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Returns whether `name` is permitted.
    pub fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }
}

/// Narrows `requested` to the tools a child agent may receive.
///
/// Duplicate names are rejected before permission checks, so an invalid request
/// always yields the same error regardless of which names happen to be
/// authorized. Every requested name must appear in the parent allowlist;
/// unauthorized names are an error rather than being silently omitted.
///
/// After validation, the result is `requested ∩ parent.native` with
/// [`DELEGATE_TASK_TOOL_NAME`] removed unconditionally. Non-native tools such as
/// WASM plugins are therefore dropped even when the parent allowlist contains
/// them. An empty request and an empty result are both valid: the child then
/// runs with no tools.
///
/// # Errors
///
/// Returns [`DelegationError::DuplicateTool`] when a name repeats in
/// `requested`, and [`DelegationError::ToolNotPermitted`] when a requested name
/// is absent from the parent allowlist.
pub fn narrow_tools(
    parent: &ParentCapabilities,
    requested: &[String],
) -> Result<NarrowedTools, DelegationError> {
    let mut seen = BTreeSet::new();
    for name in requested {
        if !seen.insert(name.as_str()) {
            return Err(DelegationError::DuplicateTool { name: name.clone() });
        }
    }
    for name in requested {
        if !parent.allowed.contains(name) {
            return Err(DelegationError::ToolNotPermitted { name: name.clone() });
        }
    }

    let names = requested
        .iter()
        .filter(|name| parent.native.contains(name.as_str()))
        .filter(|name| name.as_str() != DELEGATE_TASK_TOOL_NAME)
        .cloned()
        .collect();

    Ok(NarrowedTools { names })
}

/// Resource limits for one delegated child run.
///
/// The runtime validates every limit that can be checked without the parent
/// budget. The CLI owns the remaining nesting check because only it knows
/// `Budget::tool_timeout`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelegationLimits {
    /// Maximum number of model rounds the child may run.
    pub max_rounds: u32,
    /// Wall-clock limit for the whole child run.
    pub timeout: Duration,
    /// Maximum UTF-8 byte length of the delegated task.
    pub max_task_bytes: usize,
    /// Maximum byte length of the serialized result JSON.
    pub max_result_bytes: usize,
    /// Maximum number of tool calls the child may request.
    pub max_child_tool_calls: u32,
}

impl Default for DelegationLimits {
    /// Returns the fixed v1 defaults: 4 rounds, 8000 ms, 8192 task bytes,
    /// 32768 result bytes, and 20 child tool calls.
    fn default() -> Self {
        Self {
            max_rounds: 4,
            timeout: Duration::from_millis(8_000),
            max_task_bytes: 8_192,
            max_result_bytes: 32_768,
            max_child_tool_calls: 20,
        }
    }
}

impl DelegationLimits {
    /// Validates every limit that does not depend on the parent budget.
    ///
    /// The child tool timeout is compared against [`CHILD_TOOL_TIMEOUT`], not
    /// against the parent's tool timeout: `timeout` must be strictly greater
    /// than 5000 ms. The CLI additionally checks that `timeout` is strictly
    /// below the parent tool timeout when delegation is enabled.
    ///
    /// # Errors
    ///
    /// Returns a structured error for every zero round/tool-call/task-byte
    /// limit, a result limit below [`MIN_RESULT_BYTES`], or a timeout that does
    /// not exceed [`CHILD_TOOL_TIMEOUT`].
    pub fn validate(&self) -> Result<(), DelegationError> {
        if self.max_rounds == 0 {
            return Err(DelegationError::ZeroMaxRounds);
        }
        if self.max_child_tool_calls == 0 {
            return Err(DelegationError::ZeroMaxChildToolCalls);
        }
        if self.max_task_bytes == 0 {
            return Err(DelegationError::ZeroMaxTaskBytes);
        }
        if self.max_result_bytes < MIN_RESULT_BYTES {
            return Err(DelegationError::ResultBytesTooSmall {
                actual: self.max_result_bytes,
            });
        }
        if self.timeout <= CHILD_TOOL_TIMEOUT {
            return Err(DelegationError::TimeoutTooShort {
                timeout: self.timeout,
            });
        }
        Ok(())
    }
}

/// Structured errors from permission narrowing and limit validation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DelegationError {
    /// A tool name was requested more than once.
    #[error("tool `{name}` was requested more than once")]
    DuplicateTool {
        /// Repeated tool name.
        name: String,
    },
    /// A requested tool is absent from the parent allowlist.
    #[error("tool `{name}` is not permitted by the parent allowlist")]
    ToolNotPermitted {
        /// Tool name that was not allowed.
        name: String,
    },
    /// `max_rounds` was zero.
    #[error("delegation max_rounds must be at least 1")]
    ZeroMaxRounds,
    /// `max_child_tool_calls` was zero.
    #[error("delegation max_child_tool_calls must be at least 1")]
    ZeroMaxChildToolCalls,
    /// `max_task_bytes` was zero.
    #[error("delegation max_task_bytes must be at least 1")]
    ZeroMaxTaskBytes,
    /// `max_result_bytes` was below the minimum fixed JSON skeleton size.
    #[error("delegation max_result_bytes must be at least 128 bytes, got {actual}")]
    ResultBytesTooSmall {
        /// Configured limit.
        actual: usize,
    },
    /// The child run timeout did not exceed the fixed child tool timeout.
    #[error(
        "delegation timeout {timeout:?} must be greater than the fixed 5000ms child tool timeout"
    )]
    TimeoutTooShort {
        /// Configured child-run timeout.
        timeout: Duration,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds capabilities from name slices.
    fn parent(allowed: &[&str], native: &[&str]) -> ParentCapabilities {
        ParentCapabilities::new(
            allowed.iter().map(|name| (*name).to_owned()),
            native.iter().map(|name| (*name).to_owned()),
        )
    }

    /// Builds a request from name slices.
    fn request(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn narrowing_keeps_only_the_requested_native_subset_in_sorted_order() {
        let capabilities = parent(
            &["write_file", "bash", "read_file"],
            &["write_file", "bash", "read_file"],
        );

        let result = narrow_tools(&capabilities, &request(&["write_file", "bash"]))
            .expect("authorized native tools must narrow successfully");

        assert_eq!(
            result.names().collect::<Vec<_>>(),
            vec!["bash", "write_file"]
        );
        assert_eq!(result.len(), 2);
        assert!(!result.is_empty());
        assert!(result.contains("bash"));
        assert!(!result.contains("read_file"));
    }

    #[test]
    fn duplicate_request_names_are_rejected_even_when_allowed() {
        let capabilities = parent(&["bash"], &["bash"]);

        let error = narrow_tools(&capabilities, &request(&["bash", "bash"]))
            .expect_err("duplicate names must be rejected");

        assert_eq!(
            error,
            DelegationError::DuplicateTool {
                name: "bash".to_owned()
            }
        );
    }

    #[test]
    fn duplicate_detection_precedes_permission_checks() {
        let capabilities = parent(&["bash"], &["bash"]);

        let error = narrow_tools(&capabilities, &request(&["missing", "missing"]))
            .expect_err("duplicate names must be rejected");

        assert_eq!(
            error,
            DelegationError::DuplicateTool {
                name: "missing".to_owned()
            }
        );
    }

    #[test]
    fn requesting_a_tool_outside_the_parent_allowlist_is_an_error() {
        let capabilities = parent(&["bash"], &["bash"]);

        let error = narrow_tools(&capabilities, &request(&["bash", "read_file"]))
            .expect_err("an unauthorized tool must not be silently dropped");

        assert_eq!(
            error,
            DelegationError::ToolNotPermitted {
                name: "read_file".to_owned()
            }
        );
    }

    #[test]
    fn an_unauthorized_non_native_tool_is_not_silently_filtered() {
        let capabilities = parent(&["bash"], &["bash"]);

        let error = narrow_tools(&capabilities, &request(&["wasm_echo"]))
            .expect_err("allowlist validation must run before native filtering");

        assert_eq!(
            error,
            DelegationError::ToolNotPermitted {
                name: "wasm_echo".to_owned()
            }
        );
    }

    #[test]
    fn delegate_task_and_non_native_tools_are_removed_even_when_authorized() {
        let capabilities = parent(
            &["bash", DELEGATE_TASK_TOOL_NAME, "wasm_echo"],
            &["bash", DELEGATE_TASK_TOOL_NAME],
        );

        let result = narrow_tools(
            &capabilities,
            &request(&["bash", DELEGATE_TASK_TOOL_NAME, "wasm_echo"]),
        )
        .expect("authorized tools must narrow successfully");

        assert_eq!(result.names().collect::<Vec<_>>(), vec!["bash"]);
    }

    #[test]
    fn a_request_for_only_filtered_tools_yields_an_empty_set() {
        let capabilities = parent(
            &[DELEGATE_TASK_TOOL_NAME, "wasm_echo"],
            &[DELEGATE_TASK_TOOL_NAME],
        );

        let result = narrow_tools(
            &capabilities,
            &request(&[DELEGATE_TASK_TOOL_NAME, "wasm_echo"]),
        )
        .expect("authorized tools must narrow successfully");

        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
        assert_eq!(result.names().count(), 0);
        assert!(!result.contains(DELEGATE_TASK_TOOL_NAME));
    }

    #[test]
    fn an_empty_request_yields_an_empty_set() {
        let capabilities = parent(&["bash"], &["bash"]);

        let result = narrow_tools(&capabilities, &[]).expect("an empty request must be accepted");

        assert!(result.is_empty());
        assert_eq!(result.names().count(), 0);
    }

    #[test]
    fn parent_capability_sets_are_deduplicated() {
        let capabilities = parent(&["bash", "bash"], &["bash", "bash"]);

        let result = narrow_tools(&capabilities, &request(&["bash"]))
            .expect("authorized tools must narrow successfully");

        assert_eq!(result.names().collect::<Vec<_>>(), vec!["bash"]);
    }

    #[test]
    fn default_limits_match_the_v1_delegation_contract() {
        let limits = DelegationLimits::default();

        assert_eq!(limits.max_rounds, 4);
        assert_eq!(limits.timeout, Duration::from_millis(8_000));
        assert_eq!(limits.max_task_bytes, 8_192);
        assert_eq!(limits.max_result_bytes, 32_768);
        assert_eq!(limits.max_child_tool_calls, 20);
        assert_eq!(CHILD_TOOL_TIMEOUT, Duration::from_millis(5_000));
        assert_eq!(MIN_RESULT_BYTES, 128);
        assert_eq!(limits.validate(), Ok(()));
    }

    #[test]
    fn validate_rejects_each_invalid_boundary() {
        let cases = [
            (
                DelegationLimits {
                    max_rounds: 0,
                    ..DelegationLimits::default()
                },
                DelegationError::ZeroMaxRounds,
            ),
            (
                DelegationLimits {
                    max_child_tool_calls: 0,
                    ..DelegationLimits::default()
                },
                DelegationError::ZeroMaxChildToolCalls,
            ),
            (
                DelegationLimits {
                    max_task_bytes: 0,
                    ..DelegationLimits::default()
                },
                DelegationError::ZeroMaxTaskBytes,
            ),
            (
                DelegationLimits {
                    max_result_bytes: MIN_RESULT_BYTES - 1,
                    ..DelegationLimits::default()
                },
                DelegationError::ResultBytesTooSmall {
                    actual: MIN_RESULT_BYTES - 1,
                },
            ),
            (
                DelegationLimits {
                    timeout: CHILD_TOOL_TIMEOUT,
                    ..DelegationLimits::default()
                },
                DelegationError::TimeoutTooShort {
                    timeout: CHILD_TOOL_TIMEOUT,
                },
            ),
            (
                DelegationLimits {
                    timeout: Duration::from_millis(4_999),
                    ..DelegationLimits::default()
                },
                DelegationError::TimeoutTooShort {
                    timeout: Duration::from_millis(4_999),
                },
            ),
        ];

        for (limits, expected) in cases {
            assert_eq!(limits.validate(), Err(expected));
        }
    }

    #[test]
    fn validate_accepts_the_smallest_allowed_limits() {
        let limits = DelegationLimits {
            max_rounds: 1,
            timeout: CHILD_TOOL_TIMEOUT + Duration::from_millis(1),
            max_task_bytes: 1,
            max_result_bytes: MIN_RESULT_BYTES,
            max_child_tool_calls: 1,
        };

        assert_eq!(limits.validate(), Ok(()));
    }
}
