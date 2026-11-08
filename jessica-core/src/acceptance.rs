//! Acceptance check contract: verifies the final result of a session.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::types::Message;

/// Result of an [`AcceptanceCheck`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceResult {
    /// Whether the acceptance criteria were met.
    pub passed: bool,
    /// Feedback injected into the conversation when the check fails.
    pub feedback: String,
}

impl AcceptanceResult {
    /// Creates a passing result.
    pub fn pass(feedback: impl Into<String>) -> Self {
        Self {
            passed: true,
            feedback: feedback.into(),
        }
    }

    /// Creates a failing result.
    pub fn fail(feedback: impl Into<String>) -> Self {
        Self {
            passed: false,
            feedback: feedback.into(),
        }
    }
}

/// Inputs available to an acceptance check once the model stops.
#[derive(Debug, Clone, Copy)]
pub struct AcceptanceContext<'a> {
    /// Conversation produced so far.
    pub messages: &'a [Message],
    /// Final text produced by the model.
    pub last_output: &'a str,
}

/// Verifies that the session's result satisfies external acceptance criteria.
#[async_trait]
pub trait AcceptanceCheck: Send + Sync {
    /// Human-readable name used in diagnostics.
    fn name(&self) -> &str;

    /// Runs the check and returns pass/fail plus model-facing feedback.
    async fn check(&self, context: &AcceptanceContext<'_>) -> AcceptanceResult;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_round_trip<T>(value: &T)
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_string(value).expect("value should serialize");
        let decoded: T = serde_json::from_str(&encoded).expect("value should deserialize");
        assert_eq!(&decoded, value);
    }

    struct FinalOutputContains {
        needle: &'static str,
    }

    #[async_trait]
    impl AcceptanceCheck for FinalOutputContains {
        fn name(&self) -> &str {
            "final_output_contains"
        }

        async fn check(&self, context: &AcceptanceContext<'_>) -> AcceptanceResult {
            if context.last_output.contains(self.needle) {
                AcceptanceResult::pass("accepted")
            } else {
                AcceptanceResult::fail(format!("final output did not contain `{}`", self.needle))
            }
        }
    }

    #[test]
    fn results_construct_and_round_trip() {
        let pass = AcceptanceResult::pass("accepted");
        assert!(pass.passed);
        assert_round_trip(&pass);

        let fail = AcceptanceResult::fail("rejected");
        assert!(!fail.passed);
        assert_round_trip(&fail);
    }

    #[test]
    fn check_receives_transcript_and_final_output() {
        let messages = vec![Message::user("fix the typo")];
        let context = AcceptanceContext {
            messages: &messages,
            last_output: "fixed",
        };
        let check: &dyn AcceptanceCheck = &FinalOutputContains { needle: "fixed" };

        let result = futures::executor::block_on(check.check(&context));
        assert!(result.passed);

        let failing_context = AcceptanceContext {
            messages: &messages,
            last_output: "nothing happened",
        };
        let failing = FinalOutputContains { needle: "fixed" };
        let result = futures::executor::block_on(failing.check(&failing_context));
        assert!(!result.passed);
        assert!(result.feedback.contains("fixed"));
    }
}
