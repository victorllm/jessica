//! Assembly of the governed CLI runtime and one-turn execution.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jessica_core::acceptance::AcceptanceCheck;
use jessica_core::guard::Guard;
use jessica_core::llm::{LlmError, LlmProvider};
use jessica_core::policy::Policy;
use jessica_core::sensor::Sensor;
use jessica_core::tool::{Tool, ToolRegistry, ToolRegistryError};
use jessica_core::types::{AgentEvent, Message, SessionEndReason, SessionId};
use jessica_harness::{
    AcceptanceRunner, AllowlistPolicy, CommandSensor, DEFAULT_EVENT_CAPACITY, Harness,
    ResultSizeGuard, SessionAttempt, StuckDetectionGuard, TokenBudgetGuard, ToolTimeoutGuard,
};
use jessica_runtime::AgentLoop;
use jessica_tools::{BashTool, ReadFileTool, WriteFileTool};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::acceptance_check::CommandAcceptanceCheck;
use crate::config::{ConfigError, HarnessConfig};
use crate::governance::{GovernanceState, GovernedTool, TokenCountingProvider};
use crate::pump::{EventPump, SessionSnapshot};

/// Maximum time to wait for the event pump to observe a terminal event.
const PUMP_TIMEOUT: Duration = Duration::from_secs(30);

/// Owns the provider, configuration, and workspace shared by every turn.
pub(crate) struct CliRuntime {
    provider: Arc<dyn LlmProvider>,
    config: Arc<HarnessConfig>,
    root: PathBuf,
    system_prompt: String,
}

impl CliRuntime {
    /// Creates a runtime for the current workspace.
    pub(crate) fn new(
        provider: Arc<dyn LlmProvider>,
        config: HarnessConfig,
        root: PathBuf,
    ) -> Self {
        let system_prompt = system_prompt(&root, &config);
        Self {
            provider,
            config: Arc::new(config),
            root,
            system_prompt,
        }
    }

    /// Runs one user request with the configured governance.
    pub(crate) async fn run_turn(&self, input: String) -> Result<SessionEndReason, CliError> {
        let timeout = self.config.budget.tool_timeout();
        let token_guard = Arc::new(TokenBudgetGuard::new(self.config.budget.max_tokens));
        let timeout_guard = Arc::new(ToolTimeoutGuard::new(timeout));
        let stuck_guard = Arc::new(StuckDetectionGuard::new(
            self.config.budget.max_stuck_rounds,
        ));
        let result_guard = Arc::new(ResultSizeGuard::new(self.config.budget.max_result_bytes));

        let guards: Vec<Arc<dyn Guard>> = vec![
            Arc::clone(&token_guard) as Arc<dyn Guard>,
            Arc::clone(&timeout_guard) as Arc<dyn Guard>,
            Arc::clone(&stuck_guard) as Arc<dyn Guard>,
            Arc::clone(&result_guard) as Arc<dyn Guard>,
        ];
        let policy: Arc<dyn Policy> = Arc::new(AllowlistPolicy::new(
            self.config.policy.allowed_tools.clone(),
        ));
        let sensors = self.build_sensors();
        let harness = Arc::new(
            Harness::with_event_capacity(Vec::new(), DEFAULT_EVENT_CAPACITY)
                .with_guards(guards)
                .with_policy(policy)
                .with_sensors(sensors),
        );

        let events = harness.events();
        let cancel = CancellationToken::new();
        let governance = Arc::new(GovernanceState::new(events.clone(), cancel.clone()));
        let provider: Arc<dyn LlmProvider> = Arc::new(TokenCountingProvider::new(
            Arc::clone(&self.provider),
            Arc::clone(&governance),
            Arc::clone(&token_guard),
        ));

        let registry = self.build_tool_registry(
            Arc::clone(&harness),
            Arc::clone(&governance),
            timeout,
            Arc::clone(&timeout_guard),
            Arc::clone(&result_guard),
        )?;
        let agent = Arc::new(AgentLoop::new(
            provider,
            Arc::new(registry),
            self.config.budget.to_budget(),
            events.clone(),
        ));
        let pump = Arc::new(EventPump::new(
            Arc::clone(&harness),
            Arc::clone(&governance),
        ));
        let pump_task = tokio::spawn(Arc::clone(&pump).run(harness.subscribe()));
        let signal_task = spawn_ctrl_c_listener(cancel.clone());

        let initial = vec![
            Message::system(self.system_prompt.clone()),
            Message::user(input),
        ];

        let (reason, emit_outer) = if let Some(acceptance) = &self.config.acceptance {
            let check = CommandAcceptanceCheck::new(
                acceptance.name.clone(),
                acceptance.command.clone(),
                acceptance.args.clone(),
                resolve_dir(&self.root, acceptance.working_dir.as_deref()),
            );
            let runner = AcceptanceRunner::new(
                Arc::new(check) as Arc<dyn AcceptanceCheck>,
                acceptance.max_retries,
                events.clone(),
            );
            let outcome = runner
                .run(initial, |messages| {
                    let agent = Arc::clone(&agent);
                    let harness = Arc::clone(&harness);
                    let pump = Arc::clone(&pump);
                    let governance = Arc::clone(&governance);
                    let stuck_guard = Arc::clone(&stuck_guard);
                    let cancel = cancel.clone();
                    async move {
                        run_attempt(
                            agent,
                            harness,
                            pump,
                            governance,
                            stuck_guard,
                            cancel,
                            messages,
                        )
                        .await
                    }
                })
                .await;

            (outcome.reason, true)
        } else {
            let attempt = run_attempt(
                agent,
                harness,
                Arc::clone(&pump),
                governance.clone(),
                Arc::clone(&stuck_guard),
                cancel.clone(),
                initial,
            )
            .await;
            (attempt.reason, governance.tripped())
        };

        // A budget trip overrides the runtime's Completed/Cancelled reason.
        // Emit an outer session event so the final observable reason is the
        // governed one rather than the last attempt's reason.
        if emit_outer {
            emit_outer_session(&pump, &events, reason).await;
        }

        signal_task.abort();
        pump_task.abort();
        Ok(reason)
    }

    fn build_sensors(&self) -> Vec<Arc<dyn Sensor>> {
        let Some(sensor) = &self.config.sensor else {
            return Vec::new();
        };
        let command = CommandSensor::new(&sensor.name, &sensor.command)
            .args(sensor.args.clone())
            .with_current_dir(resolve_dir(&self.root, sensor.working_dir.as_deref()));
        vec![Arc::new(command) as Arc<dyn Sensor>]
    }

    fn build_tool_registry(
        &self,
        harness: Arc<Harness>,
        governance: Arc<GovernanceState>,
        timeout: Duration,
        timeout_guard: Arc<ToolTimeoutGuard>,
        result_guard: Arc<ResultSizeGuard>,
    ) -> Result<ToolRegistry, CliError> {
        let mut registry = ToolRegistry::new();

        let read: Arc<dyn Tool> = Arc::new(ReadFileTool::new(self.root.clone()));
        let read = GovernedTool::new(
            read,
            Arc::clone(&harness),
            Arc::clone(&governance),
            timeout,
            Arc::clone(&timeout_guard),
            Arc::clone(&result_guard),
        );
        registry
            .register(Arc::new(read))
            .map_err(|source| CliError::RegisterTool {
                name: "read_file",
                source,
            })?;

        let write: Arc<dyn Tool> = Arc::new(WriteFileTool::new(self.root.clone()));
        let write = GovernedTool::new(
            write,
            Arc::clone(&harness),
            Arc::clone(&governance),
            timeout,
            Arc::clone(&timeout_guard),
            Arc::clone(&result_guard),
        );
        registry
            .register(Arc::new(write))
            .map_err(|source| CliError::RegisterTool {
                name: "write_file",
                source,
            })?;

        let bash: Arc<dyn Tool> = Arc::new(BashTool::new());
        let bash = GovernedTool::new(
            bash,
            harness,
            governance,
            timeout,
            timeout_guard,
            result_guard,
        );
        registry
            .register(Arc::new(bash))
            .map_err(|source| CliError::RegisterTool {
                name: "bash",
                source,
            })?;

        Ok(registry)
    }
}

/// Runs one agent attempt and waits until its transcript is complete.
///
/// The stuck guard is stateful per session, so it is reset before every
/// acceptance retry; token usage remains cumulative across the whole turn.
async fn run_attempt(
    agent: Arc<AgentLoop>,
    harness: Arc<Harness>,
    pump: Arc<EventPump>,
    governance: Arc<GovernanceState>,
    stuck_guard: Arc<StuckDetectionGuard>,
    cancel: CancellationToken,
    messages: Vec<Message>,
) -> SessionAttempt {
    stuck_guard.reset();
    let session_id = next_session_id();
    pump.register(session_id.clone(), messages.clone());
    let fallback_messages = messages.clone();

    let raw_reason = harness
        .run(&session_id, agent.run(session_id.clone(), messages, cancel))
        .await;
    let reason = if governance.tripped() && raw_reason != SessionEndReason::BudgetExceeded {
        SessionEndReason::BudgetExceeded
    } else {
        raw_reason
    };

    let (snapshot, pump_failed) =
        match tokio::time::timeout(PUMP_TIMEOUT, pump.wait_for(&session_id)).await {
            Ok(Some(snapshot)) => (snapshot, false),
            Ok(None) => {
                eprintln!("[warn] event pump lost session {session_id}");
                (
                    SessionSnapshot {
                        messages: fallback_messages,
                        last_output: String::new(),
                    },
                    true,
                )
            }
            Err(_) => {
                eprintln!("[warn] event pump timed out for session {session_id}");
                (
                    SessionSnapshot {
                        messages: fallback_messages,
                        last_output: String::new(),
                    },
                    true,
                )
            }
        };

    SessionAttempt {
        messages: snapshot.messages,
        last_output: snapshot.last_output,
        reason: if pump_failed {
            SessionEndReason::Error
        } else {
            reason
        },
    }
}

/// Emits a final outer session event after acceptance orchestration.
///
/// Individual attempts already emitted their own terminal events. The outer
/// event carries the governed turn's final reason, so a retry-exhausted turn is
/// observable as [`SessionEndReason::AcceptanceFailed`] rather than ending with
/// the last attempt's [`SessionEndReason::Completed`].
async fn emit_outer_session(
    pump: &EventPump,
    events: &broadcast::Sender<AgentEvent>,
    reason: SessionEndReason,
) {
    let session_id = next_session_id();
    pump.register(session_id.clone(), Vec::new());
    let _ = events.send(AgentEvent::SessionStarted {
        session_id: session_id.clone(),
    });
    let _ = events.send(AgentEvent::SessionEnded {
        session_id: session_id.clone(),
        reason,
    });

    match tokio::time::timeout(PUMP_TIMEOUT, pump.wait_for(&session_id)).await {
        Ok(Some(_)) => {}
        Ok(None) => eprintln!("[warn] outer session was not registered with the event pump"),
        Err(_) => eprintln!("[warn] timed out while flushing the outer session event"),
    }
}

fn spawn_ctrl_c_listener(cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                break;
            }
            cancel.cancel();
        }
    })
}

fn resolve_dir(root: &Path, configured: Option<&Path>) -> PathBuf {
    match configured {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => root.join(path),
        None => root.to_path_buf(),
    }
}

fn system_prompt(root: &Path, config: &HarnessConfig) -> String {
    let allowed = if config.policy.allowed_tools.is_empty() {
        "none".to_owned()
    } else {
        config.policy.allowed_tools.join(", ")
    };
    format!(
        "You are Jessica, a deterministic coding agent. The workspace root is {}. \
         Use read_file and write_file with paths relative to the workspace root; bash is also \
         available when allowed. Tool calls are subject to this allowlist: {allowed}. A denied \
         call returns an error result, so adapt instead of repeating it. A command sensor runs \
         after every tool result. If an acceptance check fails, its feedback is injected as a \
         user message: fix the underlying problem and retry. Verify your work with tools instead \
         of claiming success. When the task is complete, reply with a concise summary.",
        root.display()
    )
}

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a process-local session id.
fn next_session_id() -> SessionId {
    let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    SessionId::new(format!("cli-{}-{counter}", std::process::id()))
}

/// Errors produced while assembling or running a CLI turn.
#[derive(Debug, Error)]
pub(crate) enum CliError {
    /// The LLM provider could not be configured.
    #[error("provider setup failed: {0}")]
    Provider(#[from] LlmError),
    /// The harness configuration could not be loaded or validated.
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),
    /// A tool could not be registered.
    #[error("failed to register the `{name}` tool: {source}")]
    RegisterTool {
        /// Name of the tool that could not be registered.
        name: &'static str,
        /// Underlying registry error.
        #[source]
        source: ToolRegistryError,
    },
    /// `--config` was given without a path.
    #[error("`--config` requires a path")]
    MissingConfigPath,
    /// `--config` was given more than once.
    #[error("`--config` may be given only once")]
    DuplicateConfig,
    /// The current directory could not be determined.
    #[error("failed to determine the current directory: {0}")]
    CurrentDir(#[source] io::Error),
    /// The blocking current-directory task failed.
    #[error("current-directory task failed: {0}")]
    CurrentDirTask(#[source] tokio::task::JoinError),
}
