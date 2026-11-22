//! Sensor pipelines and command-backed sensors.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jessica_core::sensor::{Sensor, SensorContext, SensorResult};

use crate::supervisor::{
    CapturedStream, CommandSpec, DEFAULT_COMMAND_TIMEOUT, DEFAULT_OUTPUT_LIMIT, SupervisedOutput,
    cap_rendered_output, cap_rendered_output_with_total, run_supervised,
};

/// One sensor's named result within a [`SensorReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SensorObservation {
    /// Name reported by the sensor.
    pub sensor: String,
    /// Pass/fail result and diagnostic output.
    pub result: SensorResult,
}

/// Results of running every sensor in a [`SensorPipeline`].
///
/// Observations preserve sensor registration order. A pipeline runs every
/// sensor even after one fails, so the report contains the complete evidence
/// for a tool execution.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SensorReport {
    observations: Vec<SensorObservation>,
}

impl SensorReport {
    /// Returns every observation in registration order.
    pub fn observations(&self) -> &[SensorObservation] {
        &self.observations
    }

    /// Returns whether every sensor passed.
    ///
    /// An empty report passes vacuously.
    pub fn passed(&self) -> bool {
        self.observations
            .iter()
            .all(|observation| observation.result.passed)
    }

    /// Iterates over failing observations in registration order.
    pub fn failures(&self) -> impl Iterator<Item = &SensorObservation> {
        self.observations
            .iter()
            .filter(|observation| !observation.result.passed)
    }
}

/// Runs sensors in deterministic registration order after a tool executes.
#[derive(Default)]
pub struct SensorPipeline {
    sensors: Vec<Arc<dyn Sensor>>,
}

impl SensorPipeline {
    /// Creates a pipeline from the configured sensors.
    pub fn new(sensors: Vec<Arc<dyn Sensor>>) -> Self {
        Self { sensors }
    }

    /// Returns whether no sensors are configured.
    pub fn is_empty(&self) -> bool {
        self.sensors.is_empty()
    }

    /// Observes one completed tool execution with every configured sensor.
    ///
    /// Sensors are awaited sequentially in registration order. A failing sensor
    /// does not prevent later sensors from running.
    pub async fn run(&self, context: &SensorContext<'_>) -> SensorReport {
        let mut observations = Vec::with_capacity(self.sensors.len());
        for sensor in &self.sensors {
            let result = sensor.observe(context).await;
            observations.push(SensorObservation {
                sensor: sensor.name().to_owned(),
                result,
            });
        }
        SensorReport { observations }
    }
}

/// Runs a fixed command after tool execution and uses its exit status as the
/// pass/fail verdict.
///
/// Exit status zero is a pass; a nonzero exit, a signal termination, a timeout,
/// or a spawn failure is a failure. The command receives no stdin, runs without
/// a shell unless the caller explicitly configures a shell program, and starts
/// with an empty environment containing only the supervisor allowlist. Both
/// stdout and stderr are included in the returned output, capped at the
/// configured output limit with an explicit truncation notice.
#[derive(Debug, Clone)]
pub struct CommandSensor {
    name: String,
    program: String,
    args: Vec<String>,
    current_dir: Option<PathBuf>,
    env_allowlist: Vec<String>,
    timeout: Duration,
    output_limit: usize,
}

impl CommandSensor {
    /// Creates a sensor named `name` that runs `program`.
    ///
    /// The default deadline is [`DEFAULT_COMMAND_TIMEOUT`] and each captured
    /// stream is capped at [`DEFAULT_OUTPUT_LIMIT`] bytes.
    pub fn new(name: impl Into<String>, program: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            program: program.into(),
            args: Vec::new(),
            current_dir: None,
            env_allowlist: Vec::new(),
            timeout: DEFAULT_COMMAND_TIMEOUT,
            output_limit: DEFAULT_OUTPUT_LIMIT,
        }
    }

    /// Appends one program argument.
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Appends several program arguments in iteration order.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Sets the command's working directory.
    pub fn with_current_dir(mut self, directory: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(directory.into());
        self
    }

    /// Sets the wall-clock deadline for the complete spawn, read, and wait
    /// sequence.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the retained byte limit for each captured stream and for the
    /// rendered sensor output.
    pub fn with_output_limit(mut self, limit: usize) -> Self {
        self.output_limit = limit;
        self
    }

    /// Adds environment variable names on top of the supervisor defaults.
    ///
    /// Credential-bearing names are always filtered, even when listed here.
    pub fn with_env_allowlist<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.env_allowlist.extend(names.into_iter().map(Into::into));
        self
    }
}

#[async_trait]
impl Sensor for CommandSensor {
    fn name(&self) -> &str {
        &self.name
    }

    async fn observe(&self, _context: &SensorContext<'_>) -> SensorResult {
        let spec = CommandSpec {
            program: self.program.clone(),
            args: self.args.clone(),
            current_dir: self.current_dir.clone(),
            env_allowlist: self.env_allowlist.clone(),
            output_limit: self.output_limit,
        };

        let output = match run_supervised(&spec, self.timeout).await {
            Ok(output) => output,
            Err(error) => {
                let message = format!("command sensor `{}` failed: {error}", self.name);
                return SensorResult::fail(cap_rendered_output(&message, self.output_limit));
            }
        };

        let rendered = render_output(&output);
        let source_total = output
            .stdout
            .total_bytes
            .saturating_add(output.stderr.total_bytes);
        if output.status.success() {
            SensorResult::pass(cap_rendered_output_with_total(
                &rendered,
                self.output_limit,
                source_total,
            ))
        } else {
            let message = format!(
                "command sensor `{}` failed with {}: {rendered}",
                self.name, output.status
            );
            SensorResult::fail(cap_rendered_output_with_total(
                &message,
                self.output_limit,
                source_total,
            ))
        }
    }
}

/// Renders captured streams with stable labels and truncation notices.
fn render_output(output: &SupervisedOutput) -> String {
    let mut rendered = String::new();
    append_stream(&mut rendered, "stdout", &output.stdout);
    append_stream(&mut rendered, "stderr", &output.stderr);
    if rendered.is_empty() {
        rendered.push_str("(no output)");
    }
    rendered
}

/// Appends one non-empty stream with its truncation state.
fn append_stream(rendered: &mut String, label: &str, stream: &CapturedStream) {
    if stream.bytes.is_empty() && !stream.truncated {
        return;
    }
    if !rendered.is_empty() {
        rendered.push('\n');
    }
    rendered.push_str(label);
    rendered.push_str(":\n");
    rendered.push_str(&String::from_utf8_lossy(&stream.bytes));
    if stream.truncated {
        rendered.push_str(&format!(
            "\n[{label} truncated at {} bytes: {} bytes total]",
            stream.bytes.len(),
            stream.total_bytes
        ));
    }
}
