//! Sensor pipelines and command-backed sensors.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use jessica_core::sensor::{Sensor, SensorContext, SensorResult};
use tokio::process::Command;

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
/// Exit status zero is a pass; a nonzero exit, a signal termination, or a spawn
/// failure is a failure. The command receives no stdin and runs without a shell
/// unless the caller explicitly configures a shell program. Both stdout and
/// stderr are included in the returned output.
#[derive(Debug, Clone)]
pub struct CommandSensor {
    name: String,
    program: String,
    args: Vec<String>,
    current_dir: Option<PathBuf>,
}

impl CommandSensor {
    /// Creates a sensor named `name` that runs `program`.
    pub fn new(name: impl Into<String>, program: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            program: program.into(),
            args: Vec::new(),
            current_dir: None,
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
}

#[async_trait]
impl Sensor for CommandSensor {
    fn name(&self) -> &str {
        &self.name
    }

    async fn observe(&self, _context: &SensorContext<'_>) -> SensorResult {
        let mut command = Command::new(&self.program);
        command
            .args(&self.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(directory) = &self.current_dir {
            command.current_dir(directory);
        }

        let output = match command.output().await {
            Ok(output) => output,
            Err(error) => {
                return SensorResult::fail(format!(
                    "command sensor `{}` could not start `{}`: {error}",
                    self.name, self.program
                ));
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let rendered = render_output(&stdout, &stderr);

        if output.status.success() {
            SensorResult::pass(rendered)
        } else {
            SensorResult::fail(format!(
                "command sensor `{}` failed with {}: {rendered}",
                self.name, output.status
            ))
        }
    }
}

/// Renders captured streams with stable labels.
fn render_output(stdout: &str, stderr: &str) -> String {
    let mut rendered = String::new();
    if !stdout.is_empty() {
        rendered.push_str("stdout:\n");
        rendered.push_str(stdout);
    }
    if !stderr.is_empty() {
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str("stderr:\n");
        rendered.push_str(stderr);
    }
    if rendered.is_empty() {
        rendered.push_str("(no output)");
    }
    rendered
}
