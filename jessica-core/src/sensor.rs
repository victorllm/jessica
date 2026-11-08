//! Sensor contract: post-tool observations.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::types::{ToolCall, ToolResult};

/// Result of running a [`Sensor`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SensorResult {
    /// Whether the observed state passed the sensor's checks.
    pub passed: bool,
    /// Human-readable output used for diagnostics and model feedback.
    pub output: String,
}

impl SensorResult {
    /// Creates a passing result.
    pub fn pass(output: impl Into<String>) -> Self {
        Self {
            passed: true,
            output: output.into(),
        }
    }

    /// Creates a failing result.
    pub fn fail(output: impl Into<String>) -> Self {
        Self {
            passed: false,
            output: output.into(),
        }
    }
}

/// Inputs available to a sensor after a tool executes.
#[derive(Debug, Clone, Copy)]
pub struct SensorContext<'a> {
    /// The tool call that was executed.
    pub tool_call: &'a ToolCall,
    /// The result returned by the tool.
    pub tool_result: &'a ToolResult,
}

/// Observes tool execution and reports whether the observed state is acceptable.
#[async_trait]
pub trait Sensor: Send + Sync {
    /// Human-readable name used in diagnostics.
    fn name(&self) -> &str;

    /// Runs the sensor against a completed tool execution.
    async fn observe(&self, context: &SensorContext<'_>) -> SensorResult;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assert_round_trip<T>(value: &T)
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_string(value).expect("value should serialize");
        let decoded: T = serde_json::from_str(&encoded).expect("value should deserialize");
        assert_eq!(&decoded, value);
    }

    struct OutputContains {
        needle: &'static str,
    }

    #[async_trait]
    impl Sensor for OutputContains {
        fn name(&self) -> &str {
            "output_contains"
        }

        async fn observe(&self, context: &SensorContext<'_>) -> SensorResult {
            if context.tool_result.content.contains(self.needle) {
                SensorResult::pass(context.tool_result.content.clone())
            } else {
                SensorResult::fail(format!(
                    "output did not contain `{}`: {}",
                    self.needle, context.tool_result.content
                ))
            }
        }
    }

    #[test]
    fn results_construct_and_round_trip() {
        let pass = SensorResult::pass("all good");
        assert!(pass.passed);
        assert_round_trip(&pass);

        let fail = SensorResult::fail("not good");
        assert!(!fail.passed);
        assert_round_trip(&fail);
    }

    #[test]
    fn sensor_observes_tool_result_through_object_safe_trait() {
        let call = ToolCall::new("call-1", "bash", json!({"command": "echo ok"}));
        let result = ToolResult::success("call-1", "ok");
        let context = SensorContext {
            tool_call: &call,
            tool_result: &result,
        };
        let sensor: &dyn Sensor = &OutputContains { needle: "ok" };

        let passing = futures::executor::block_on(sensor.observe(&context));
        assert!(passing.passed);
        assert_eq!(passing.output, "ok");

        let failing_sensor = OutputContains { needle: "missing" };
        let failing = futures::executor::block_on(failing_sensor.observe(&context));
        assert!(!failing.passed);
        assert!(failing.output.contains("missing"));
    }
}
