//! Governed one-shot / REPL front end for Jessica.

mod acceptance_check;
mod acp;
mod acp_events;
mod acp_session;
mod acp_store;
mod app;
mod config;
mod delegation;
mod events;
mod governance;
mod hot_reload;
mod journal;
mod mcp;
mod memory;
mod pump;
mod replay;
mod shutdown;
mod skills;
mod tools;
mod trace_bundle;
mod trace_digest;
mod trace_replay;
mod trace_verify;

use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::sync::Arc;

use jessica_core::llm::LlmProvider;
use jessica_core::types::{SessionEndReason, SessionId};
use jessica_memory::{SkillId, SkillStatus};
use jessica_providers::{AnthropicProvider, ClassifiedProvider, OpenAiProvider, ReliableProvider};
use tokio::io::{AsyncBufReadExt, BufReader, Stdin};
use tokio::sync::watch;

use crate::app::{CliError, CliRuntime, load_wasm_tools};
use crate::config::{HarnessConfig, ReliabilityConfig as CliReliabilityConfig};
use crate::events::EventRecorder;
use crate::hot_reload::ConfigWatcher;
use crate::memory::MemoryAssembly;
use crate::shutdown::{ShutdownController, ShutdownSignal, SignalListener};
use crate::skills::{SkillCommand, SkillsAssembly, default_list_limit, run_skill_command};
use crate::tools::{ToolsCommand, run_tools_command};
use crate::trace_verify::{TraceCommand, TraceSelection, run_command as run_trace_command};

#[tokio::main]
async fn main() {
    let outcome = run().await;
    if let Err(error) = &outcome.result {
        eprintln!("jessica: {error}");
    }
    // Turns persist through the event pump and the skill worker is drained
    // before `run` returns. Exit explicitly instead of returning from
    // `#[tokio::main]`: while the REPL waits for a line, an interactive stdin
    // has a blocking read pending on the blocking pool, and dropping the
    // runtime would wait for that read until the terminal closes. Flush both
    // streams first so no user-visible output is lost.
    let _ = io::stdout().flush();
    let _ = io::stderr().flush();
    std::process::exit(i32::from(exit_code(&outcome)));
}

/// Result of one CLI invocation.
struct RunOutcome {
    /// Result of the selected mode; `Ok(None)` means no turn was executed.
    result: Result<Option<SessionEndReason>, CliError>,
    /// First graceful-shutdown signal, if the turn path installed a listener.
    signal: Option<ShutdownSignal>,
}

impl RunOutcome {
    /// Successful invocation that did not run a turn.
    fn success() -> Self {
        Self {
            result: Ok(None),
            signal: None,
        }
    }

    /// Failure raised before the turn path installed a signal listener.
    fn error(error: CliError) -> Self {
        Self {
            result: Err(error),
            signal: None,
        }
    }
}

async fn run() -> RunOutcome {
    let options = match parse_args() {
        Ok(Some(options)) => options,
        Ok(None) => return RunOutcome::success(),
        Err(error) => return RunOutcome::error(error),
    };
    let Options {
        config: config_path,
        mode,
    } = options;

    let config = match &config_path {
        Some(path) => match HarnessConfig::load(path).await {
            Ok(config) => config,
            Err(error) => return RunOutcome::error(error.into()),
        },
        None => HarnessConfig::default(),
    };
    let root = match tokio::task::spawn_blocking(std::env::current_dir).await {
        Ok(Ok(root)) => root,
        Ok(Err(error)) => return RunOutcome::error(CliError::CurrentDir(error)),
        Err(error) => return RunOutcome::error(CliError::CurrentDirTask(error)),
    };
    let watch_path = watched_config_path(&mode, &config_path);

    match mode {
        Mode::Replay { session, out } => {
            let result: Result<Option<SessionEndReason>, CliError> = async {
                let recorder = open_recorder(&config, &root)
                    .await?
                    .ok_or(CliError::ReplayRequiresEventStore)?;
                let stored = recorder.load(session.clone()).await?.ok_or_else(|| {
                    CliError::UnknownSession {
                        session_id: session.to_string(),
                    }
                })?;
                let events = replay::replay_session(&session, &stored).await?;
                let count = events.len();
                let output_path = out.display().to_string();
                replay::write_jsonl(out, events).await?;
                println!("[replay] wrote {count} events for session {session} to {output_path}");
                Ok(None)
            }
            .await;
            RunOutcome {
                result,
                signal: None,
            }
        }
        Mode::Skills { command } => {
            // Manual review never initializes a provider, loads wasm tools, or
            // opens the event store.
            let result: Result<Option<SessionEndReason>, CliError> = async {
                run_skill_command(&config.skills, &root, command).await?;
                Ok(None)
            }
            .await;
            RunOutcome {
                result,
                signal: None,
            }
        }
        Mode::Tools { command } => {
            // Catalog inspection loads configuration and, if configured, wasm
            // manifests. It never creates a provider, event store, memory or
            // skill database, MCP connection, or `.jessica` directory.
            let result: Result<Option<SessionEndReason>, CliError> = async {
                run_tools_command(&config, &root, command).await?;
                Ok(None)
            }
            .await;
            RunOutcome {
                result,
                signal: None,
            }
        }
        Mode::Trace { command } => {
            // Verification only opens the event store read-only. It never
            // initializes a provider, wasm tool, memory store, or signal
            // listener; an audit must not change the evidence.
            let result: Result<Option<SessionEndReason>, CliError> = async {
                run_trace_command(&config, &root, command).await?;
                Ok(None)
            }
            .await;
            RunOutcome {
                result,
                signal: None,
            }
        }
        Mode::Config => {
            // Configuration inspection has no side effects: it never opens a
            // provider, event store, memory database, skill database, or wasm
            // module.
            let result = match config.to_effective_json() {
                Ok(json) => {
                    println!("{json}");
                    Ok(None)
                }
                Err(error) => Err(CliError::ConfigSerialization(error)),
            };
            RunOutcome {
                result,
                signal: None,
            }
        }
        Mode::Turn { resume, prompt } => {
            // The listener is installed once at startup, before the event
            // store is opened. Turns never install their own listener.
            let shutdown = Arc::new(ShutdownController::new());
            let listener = SignalListener::spawn(Arc::clone(&shutdown));
            let result = run_turn_mode(config, root, watch_path, resume, prompt, &shutdown).await;
            let signal = shutdown.signal();
            // A forced exit happened inside the listener and never reaches
            // this point. On the normal path the listener is now quiescent.
            drop(listener);
            RunOutcome { result, signal }
        }
        Mode::Acp => {
            // ACP owns stdout, so this branch must not touch the REPL, replay,
            // or skills output paths. The signal listener is installed before
            // the transport starts so an early SIGTERM still shuts down
            // gracefully. The watcher starts when the lazy runtime is
            // assembled, so every later prompt reads the newest reloadable
            // snapshot.
            let shutdown = Arc::new(ShutdownController::new());
            let listener = SignalListener::spawn(Arc::clone(&shutdown));
            let result = acp::run(config, root, watch_path, Arc::clone(&shutdown)).await;
            let signal = shutdown.signal();
            drop(listener);
            RunOutcome {
                result: result.map(|()| None),
                signal,
            }
        }
    }
}

/// Runs every turn mode: one-shot, resume, or the interactive REPL.
///
/// The skill drain happens exactly once, after the last turn, without changing
/// the exit code when it times out.
async fn run_turn_mode(
    config: HarnessConfig,
    root: PathBuf,
    watch_path: Option<PathBuf>,
    resume: Option<SessionId>,
    prompt: Option<String>,
    shutdown: &Arc<ShutdownController>,
) -> Result<Option<SessionEndReason>, CliError> {
    let (runtime, config_sender) =
        assemble_runtime(Arc::new(config), root, Arc::clone(shutdown)).await?;

    let result = if let Some(session_id) = resume {
        runtime.resume_turn(session_id).await.map(Some)
    } else {
        match prompt {
            Some(input) => runtime.run_turn(input).await.map(Some),
            None => {
                // One-shot, resume, replay, and skills never reach this
                // branch, so they never start a configuration watcher or read
                // the file a second time.
                let watcher = match watch_path {
                    Some(path) => Some(ConfigWatcher::spawn(path, config_sender).await),
                    None => None,
                };
                let outcome = repl(&runtime, shutdown).await;
                // Dropping the watcher aborts its polling task before the
                // skill worker drain begins.
                drop(watcher);
                outcome.map(|()| None)
            }
        }
    };
    runtime.shutdown().await;
    result
}

/// Assembles the provider, recorder, wasm tools, memory, and skills once.
///
/// Turn modes and the ACP server share this assembly so they observe the same
/// startup configuration. The returned sender keeps the configuration channel
/// open for the REPL watcher; callers that never reload may drop it, leaving
/// the receiver with the startup snapshot. Memory and skills are fail-open and
/// may be absent. Replay and skills commands never call this function and
/// therefore never open either database.
pub(crate) async fn assemble_runtime(
    config: Arc<HarnessConfig>,
    root: PathBuf,
    shutdown: Arc<ShutdownController>,
) -> Result<(CliRuntime, watch::Sender<Arc<HarnessConfig>>), CliError> {
    let recorder = open_recorder(&config, &root).await?;
    let provider = build_provider(&config.reliability)?;
    let wasm_tools = load_wasm_tools(&config, &root).await?;
    let memory = MemoryAssembly::open(&config.memory, &root).await;
    let skills = SkillsAssembly::open(&config.skills, &root, Arc::clone(&provider)).await;
    // The startup configuration is loaded and validated once. The receiver is
    // the per-turn snapshot source; only the REPL and ACP server install a
    // watcher capable of publishing a newer value.
    let (config_sender, config_receiver) = watch::channel(config);
    let runtime = CliRuntime::new(
        provider,
        config_receiver,
        root,
        wasm_tools,
        recorder,
        memory,
        skills,
        shutdown,
    );
    Ok((runtime, config_sender))
}

/// Opens the configured event store, resolving relative paths against `root`.
async fn open_recorder(
    config: &HarnessConfig,
    root: &std::path::Path,
) -> Result<Option<Arc<EventRecorder>>, CliError> {
    let Some(path) = config.events.path.clone() else {
        return Ok(None);
    };
    let path = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    Ok(Some(Arc::new(EventRecorder::open(path).await?)))
}

/// Environment variable selecting which provider the CLI assembles.
const PROVIDER_ENV: &str = "JESSICA_PROVIDER";

/// Provider families the CLI can assemble.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderKind {
    /// OpenAI-compatible chat completions, configured by `OPENAI_*`.
    OpenAi,
    /// Anthropic-compatible Messages API, configured by `ANTHROPIC_*`.
    Anthropic,
}

impl ProviderKind {
    /// Parses a `JESSICA_PROVIDER` value; `None` means the variable is unset.
    ///
    /// Unset defaults to OpenAI so existing deployments keep their behavior.
    fn from_env_value(value: Option<&str>) -> Result<Self, CliError> {
        match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            None | Some("openai") => Ok(Self::OpenAi),
            Some("anthropic") => Ok(Self::Anthropic),
            Some(other) => Err(CliError::InvalidProvider {
                name: other.to_owned(),
            }),
        }
    }
}

/// Builds the provider selected by `JESSICA_PROVIDER`.
///
/// The same returned instance is used by the skill distiller, every turn, and
/// any delegated child agent. When `[reliability]` is enabled, the base
/// provider is wrapped exactly once here; callers must not wrap it again.
fn build_provider(reliability: &CliReliabilityConfig) -> Result<Arc<dyn LlmProvider>, CliError> {
    let selection = match std::env::var(PROVIDER_ENV) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(CliError::InvalidProvider {
                name: "<non-Unicode>".to_owned(),
            });
        }
    };

    match ProviderKind::from_env_value(selection.as_deref())? {
        ProviderKind::OpenAi => {
            assemble_provider(Arc::new(OpenAiProvider::from_env()?), reliability)
        }
        ProviderKind::Anthropic => {
            assemble_provider(Arc::new(AnthropicProvider::from_env()?), reliability)
        }
    }
}

/// Optionally wraps a concrete base provider with the reliability layer.
///
/// Keeping this generic lets the disabled path return the base `Arc` directly
/// instead of constructing a pass-through wrapper. The enabled path coerces
/// the same `Arc` into `Arc<dyn ClassifiedProvider>` for [`ReliableProvider`].
fn assemble_provider<T>(
    base: Arc<T>,
    reliability: &CliReliabilityConfig,
) -> Result<Arc<dyn LlmProvider>, CliError>
where
    T: LlmProvider + ClassifiedProvider + 'static,
{
    if !reliability.enabled {
        return Ok(base);
    }

    let wrapped = ReliableProvider::new(base, reliability.to_provider_config())?;
    Ok(Arc::new(wrapped))
}

/// Parsed command line.
#[derive(Debug)]
struct Options {
    /// Optional TOML configuration path.
    config: Option<PathBuf>,
    /// Selected execution mode.
    mode: Mode,
}

/// Command selected by the arguments.
#[derive(Debug)]
enum Mode {
    /// Runs one governed turn, resumes a session, or starts the REPL.
    Turn {
        /// Session to resume instead of starting a new turn.
        resume: Option<SessionId>,
        /// One-shot prompt; absent means REPL mode.
        prompt: Option<String>,
    },
    /// Replays a recorded session without calling the model or tools.
    Replay {
        /// Session to replay.
        session: SessionId,
        /// JSONL output path.
        out: PathBuf,
    },
    /// Lists, shows, activates, or soft-deletes distilled skills.
    Skills {
        /// Review command to execute.
        command: SkillCommand,
    },
    /// Lists or inspects the configured tool catalog.
    Tools {
        /// Catalog command to execute.
        command: ToolsCommand,
    },
    /// Verifies recorded turn traces offline.
    Trace {
        /// Parsed trace command.
        command: TraceCommand,
    },
    /// Prints the effective resolved configuration as JSON.
    Config,
    /// Serves stable ACP v1 over stdin/stdout.
    Acp,
}

/// Returns the configuration path watched by this execution path.
///
/// An interactive REPL (a `Turn` with neither `--resume` nor a prompt) and
/// the long-lived ACP server own a config watcher. The ACP watcher starts when
/// the lazy runtime is assembled, so every later prompt reads the newest
/// reloadable snapshot. One-shot, resume, replay, skills, tools, trace, and
/// config return `None`, so they load the startup configuration exactly once.
fn watched_config_path(mode: &Mode, config_path: &Option<PathBuf>) -> Option<PathBuf> {
    match mode {
        Mode::Turn {
            resume: None,
            prompt: None,
        }
        | Mode::Acp => config_path.clone(),
        _ => None,
    }
}

/// Parses process arguments, returning `None` after printing help.
fn parse_args() -> Result<Option<Options>, CliError> {
    parse_args_from(std::env::args().skip(1))
}

/// Parses arguments from an iterator, returning `None` after printing help.
fn parse_args_from(mut args: impl Iterator<Item = String>) -> Result<Option<Options>, CliError> {
    let mut config = None;
    let mut resume = None;
    let mut prompt_parts = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                return Ok(None);
            }
            "--version" | "-V" => {
                // The workspace version is the single source: the package
                // inherits it, so this reports the same value as the manifests.
                println!("jessica {}", env!("CARGO_PKG_VERSION"));
                return Ok(None);
            }
            "--config" => {
                let path = args.next().ok_or(CliError::MissingConfigPath)?;
                set_config(&mut config, PathBuf::from(path))?;
            }
            _ if arg.starts_with("--config=") => {
                let path = arg
                    .strip_prefix("--config=")
                    .filter(|path| !path.is_empty())
                    .ok_or(CliError::MissingConfigPath)?;
                set_config(&mut config, PathBuf::from(path))?;
            }
            "replay" if resume.is_none() && prompt_parts.is_empty() => {
                let Some((session, out)) = parse_replay_args(args)? else {
                    return Ok(None);
                };
                return Ok(Some(Options {
                    config,
                    mode: Mode::Replay { session, out },
                }));
            }
            "skills" if resume.is_none() && prompt_parts.is_empty() => {
                let Some(command) = parse_skill_args(args)? else {
                    return Ok(None);
                };
                return Ok(Some(Options {
                    config,
                    mode: Mode::Skills { command },
                }));
            }
            "tools" if resume.is_none() && prompt_parts.is_empty() => {
                let Some(command) = parse_tools_args(args)? else {
                    return Ok(None);
                };
                return Ok(Some(Options {
                    config,
                    mode: Mode::Tools { command },
                }));
            }
            "trace" if resume.is_none() && prompt_parts.is_empty() => {
                let Some(command) = parse_trace_args(args)? else {
                    return Ok(None);
                };
                return Ok(Some(Options {
                    config,
                    mode: Mode::Trace { command },
                }));
            }
            "config" if resume.is_none() && prompt_parts.is_empty() => {
                if let Some(argument) = args.next() {
                    return Err(CliError::InvalidConfigCommand { argument });
                }
                return Ok(Some(Options {
                    config,
                    mode: Mode::Config,
                }));
            }
            "acp" if resume.is_none() && prompt_parts.is_empty() => {
                return parse_acp_args(config, args);
            }
            "--resume" => {
                let id = args.next().ok_or(CliError::MissingResumeSessionId)?;
                set_resume(&mut resume, &id)?;
            }
            _ if arg.starts_with("--resume=") => {
                let id = arg
                    .strip_prefix("--resume=")
                    .filter(|id| !id.is_empty())
                    .ok_or(CliError::MissingResumeSessionId)?;
                set_resume(&mut resume, id)?;
            }
            _ => prompt_parts.push(arg),
        }
    }

    if resume.is_some() && !prompt_parts.is_empty() {
        return Err(CliError::ResumeWithPrompt);
    }

    let prompt = if prompt_parts.is_empty() {
        None
    } else {
        Some(prompt_parts.join(" "))
    };

    Ok(Some(Options {
        config,
        mode: Mode::Turn { resume, prompt },
    }))
}

/// Parses the arguments following the `acp` subcommand.
///
/// The only accepted argument is the configuration flag, in either form. Any
/// other argument is rejected instead of being treated as a prompt: a typo
/// must not silently start a long-lived protocol server with defaults.
fn parse_acp_args(
    mut config: Option<PathBuf>,
    mut args: impl Iterator<Item = String>,
) -> Result<Option<Options>, CliError> {
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                return Ok(None);
            }
            "--config" => {
                let path = args.next().ok_or(CliError::MissingConfigPath)?;
                set_config(&mut config, PathBuf::from(path))?;
            }
            _ if arg.starts_with("--config=") => {
                let path = arg
                    .strip_prefix("--config=")
                    .filter(|path| !path.is_empty())
                    .ok_or(CliError::MissingConfigPath)?;
                set_config(&mut config, PathBuf::from(path))?;
            }
            _ => {
                return Err(CliError::UnknownAcpArgument { argument: arg });
            }
        }
    }

    Ok(Some(Options {
        config,
        mode: Mode::Acp,
    }))
}

/// Parses the arguments following the `replay` subcommand.
fn parse_replay_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<(SessionId, PathBuf)>, CliError> {
    let mut session = None;
    let mut out = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                return Ok(None);
            }
            "--session" => {
                let id = args.next().ok_or(CliError::MissingReplaySession)?;
                set_replay_session(&mut session, &id)?;
            }
            _ if arg.starts_with("--session=") => {
                let id = arg
                    .strip_prefix("--session=")
                    .filter(|id| !id.is_empty())
                    .ok_or(CliError::MissingReplaySession)?;
                set_replay_session(&mut session, id)?;
            }
            "--out" => {
                let path = args.next().ok_or(CliError::MissingReplayOut)?;
                set_replay_out(&mut out, PathBuf::from(path))?;
            }
            _ if arg.starts_with("--out=") => {
                let path = arg
                    .strip_prefix("--out=")
                    .filter(|path| !path.is_empty())
                    .ok_or(CliError::MissingReplayOut)?;
                set_replay_out(&mut out, PathBuf::from(path))?;
            }
            _ => return Err(CliError::UnknownReplayArgument { argument: arg }),
        }
    }

    let session = session.ok_or(CliError::MissingReplaySession)?;
    let out = out.ok_or(CliError::MissingReplayOut)?;
    Ok(Some((session, out)))
}

/// Parses the arguments following the `skills` subcommand.
fn parse_skill_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<SkillCommand>, CliError> {
    let Some(command) = args.next() else {
        print_skill_usage();
        return Err(CliError::InvalidSkillsCommand {
            message: "missing subcommand".to_owned(),
        });
    };
    if command == "--help" || command == "-h" {
        print_skill_usage();
        return Ok(None);
    }

    match command.as_str() {
        "list" => {
            let mut status = None;
            let mut limit = default_list_limit();
            while let Some(arg) = args.next() {
                if arg == "--help" || arg == "-h" {
                    print_skill_usage();
                    return Ok(None);
                }
                if let Some(value) = arg.strip_prefix("--status=") {
                    set_skill_status(&mut status, value)?;
                    continue;
                }
                if arg == "--status" {
                    let value = args.next().ok_or_else(|| CliError::InvalidSkillsCommand {
                        message: "`--status` requires `draft` or `active`".to_owned(),
                    })?;
                    set_skill_status(&mut status, &value)?;
                    continue;
                }
                if let Some(value) = arg.strip_prefix("--limit=") {
                    set_skill_limit(&mut limit, value)?;
                    continue;
                }
                if arg == "--limit" {
                    let value = args.next().ok_or_else(|| CliError::InvalidSkillsCommand {
                        message: "`--limit` requires a positive integer".to_owned(),
                    })?;
                    set_skill_limit(&mut limit, &value)?;
                    continue;
                }
                return Err(CliError::InvalidSkillsCommand {
                    message: format!("unexpected `skills list` argument `{arg}`"),
                });
            }
            Ok(Some(SkillCommand::List { status, limit }))
        }
        "show" | "activate" | "delete" => {
            let mut id = None;
            for arg in args {
                if arg == "--help" || arg == "-h" {
                    print_skill_usage();
                    return Ok(None);
                }
                if arg.starts_with('-') {
                    return Err(CliError::InvalidSkillsCommand {
                        message: format!("unexpected `skills {command}` argument `{arg}`"),
                    });
                }
                if id.is_some() {
                    return Err(CliError::InvalidSkillsCommand {
                        message: format!("`skills {command}` accepts exactly one id"),
                    });
                }
                id = Some(parse_skill_id(&arg)?);
            }
            let id = id.ok_or_else(|| CliError::InvalidSkillsCommand {
                message: format!("`skills {command}` requires a skill id"),
            })?;
            Ok(Some(match command.as_str() {
                "show" => SkillCommand::Show { id },
                "activate" => SkillCommand::Activate { id },
                _ => SkillCommand::Delete { id },
            }))
        }
        other => Err(CliError::InvalidSkillsCommand {
            message: format!(
                "unknown subcommand `{other}`; expected list, show, activate, or delete"
            ),
        }),
    }
}

/// Parses the arguments following the `tools` subcommand.
///
/// Only `list [--all]` and `inspect <name>` are accepted. Unexpected
/// arguments fail instead of being ignored, so a typo cannot produce a
/// misleading catalog report.
fn parse_tools_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<ToolsCommand>, CliError> {
    let Some(command) = args.next() else {
        return Err(CliError::InvalidToolsCommand {
            message: "missing subcommand; expected `list` or `inspect <name>`".to_owned(),
        });
    };
    if command == "--help" || command == "-h" {
        print_usage();
        return Ok(None);
    }

    match command.as_str() {
        "list" => {
            let mut all = false;
            for arg in args {
                match arg.as_str() {
                    "--help" | "-h" => {
                        print_usage();
                        return Ok(None);
                    }
                    "--all" => all = true,
                    other => {
                        return Err(CliError::InvalidToolsCommand {
                            message: format!("unexpected `tools list` argument `{other}`"),
                        });
                    }
                }
            }
            Ok(Some(ToolsCommand::List { all }))
        }
        "inspect" => {
            let mut name = None;
            for arg in args {
                if arg == "--help" || arg == "-h" {
                    print_usage();
                    return Ok(None);
                }
                if arg.starts_with('-') {
                    return Err(CliError::InvalidToolsCommand {
                        message: format!("unexpected `tools inspect` argument `{arg}`"),
                    });
                }
                if name.is_some() {
                    return Err(CliError::InvalidToolsCommand {
                        message: "`tools inspect` accepts exactly one tool name".to_owned(),
                    });
                }
                name = Some(arg);
            }
            let name = name.ok_or_else(|| CliError::InvalidToolsCommand {
                message: "`tools inspect` requires a tool name".to_owned(),
            })?;
            Ok(Some(ToolsCommand::Inspect { name }))
        }
        other => Err(CliError::InvalidToolsCommand {
            message: format!("unknown subcommand `{other}`; expected `list` or `inspect`"),
        }),
    }
}

/// Parses the arguments following the `trace` subcommand.
fn parse_trace_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<TraceCommand>, CliError> {
    let Some(subcommand) = args.next() else {
        print_trace_usage();
        return Err(CliError::InvalidTraceCommand {
            message: "missing subcommand".to_owned(),
        });
    };
    if subcommand == "--help" || subcommand == "-h" {
        print_trace_usage();
        return Ok(None);
    }

    match subcommand.as_str() {
        "verify" => parse_trace_verify_args(args),
        "digest" => parse_trace_digest_args(args),
        "export" => parse_trace_export_args(args),
        "verify-bundle" => parse_trace_verify_bundle_args(args),
        other => Err(CliError::InvalidTraceCommand {
            message: format!(
                "unknown subcommand `{other}`; expected verify, digest, export, or verify-bundle"
            ),
        }),
    }
}

/// Parses the arguments following `trace verify`.
///
/// `--turn` and `--all` are mutually exclusive; exactly one is required.
/// `--allow-open` is accepted only with `--all`, because a single requested turn
/// is either complete or not and must not be silently downgraded. Replay accepts
/// either selection, but not `--allow-open`: replay requires a closed trace.
fn parse_trace_verify_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<TraceCommand>, CliError> {
    let mut turn = None;
    let mut all = false;
    let mut json = false;
    let mut allow_open = false;
    let mut replay = false;
    let mut expect = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_trace_usage();
                return Ok(None);
            }
            "--turn" => {
                let id = args.next().ok_or(CliError::MissingTraceTurn)?;
                set_trace_turn(&mut turn, id)?;
            }
            _ if arg.starts_with("--turn=") => {
                let id = arg
                    .strip_prefix("--turn=")
                    .filter(|id| !id.is_empty())
                    .ok_or(CliError::MissingTraceTurn)?;
                set_trace_turn(&mut turn, id.to_owned())?;
            }
            "--expect" => {
                let digest = args.next().ok_or_else(|| CliError::InvalidTraceCommand {
                    message: "`--expect` requires a digest".to_owned(),
                })?;
                set_trace_expect(&mut expect, &digest)?;
            }
            _ if arg.starts_with("--expect=") => {
                let digest =
                    arg.strip_prefix("--expect=")
                        .ok_or_else(|| CliError::InvalidTraceCommand {
                            message: "`--expect` requires a digest".to_owned(),
                        })?;
                set_trace_expect(&mut expect, digest)?;
            }
            "--all" => all = true,
            "--json" => json = true,
            "--allow-open" => allow_open = true,
            "--replay" => replay = true,
            _ => {
                return Err(CliError::InvalidTraceCommand {
                    message: format!("unexpected `trace verify` argument `{arg}`"),
                });
            }
        }
    }

    if all && turn.is_some() {
        return Err(CliError::TraceSelectionConflict);
    }
    if allow_open && !all {
        return Err(CliError::InvalidTraceCommand {
            message: "`--allow-open` requires `--all`".to_owned(),
        });
    }

    let selection = match (turn, all) {
        (Some(turn), false) => TraceSelection::Turn(turn),
        (None, true) => TraceSelection::All,
        _ => return Err(CliError::MissingTraceSelection),
    };
    if replay && allow_open {
        return Err(CliError::InvalidTraceCommand {
            message: "`--allow-open` cannot be combined with `--replay`".to_owned(),
        });
    }
    if expect.is_some() && !matches!(selection, TraceSelection::Turn(_)) {
        return Err(CliError::InvalidTraceCommand {
            message: "`--expect` requires `--turn`".to_owned(),
        });
    }
    Ok(Some(TraceCommand::Verify {
        selection,
        json,
        allow_open,
        replay,
        expect,
    }))
}

/// Parses the arguments following `trace digest`.
///
/// `--turn` and `--all` are mutually exclusive; exactly one is required.
fn parse_trace_digest_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<TraceCommand>, CliError> {
    let mut turn = None;
    let mut all = false;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_trace_usage();
                return Ok(None);
            }
            "--turn" => {
                let id = args.next().ok_or(CliError::MissingTraceTurn)?;
                set_trace_turn(&mut turn, id)?;
            }
            _ if arg.starts_with("--turn=") => {
                let id = arg
                    .strip_prefix("--turn=")
                    .filter(|id| !id.is_empty())
                    .ok_or(CliError::MissingTraceTurn)?;
                set_trace_turn(&mut turn, id.to_owned())?;
            }
            "--all" => all = true,
            "--json" => json = true,
            _ => {
                return Err(CliError::InvalidTraceCommand {
                    message: format!("unexpected `trace digest` argument `{arg}`"),
                });
            }
        }
    }

    if all && turn.is_some() {
        return Err(CliError::TraceSelectionConflict);
    }
    let selection = match (turn, all) {
        (Some(turn), false) => TraceSelection::Turn(turn),
        (None, true) => TraceSelection::All,
        _ => return Err(CliError::MissingTraceSelection),
    };
    Ok(Some(TraceCommand::Digest { selection, json }))
}

/// Parses the arguments following `trace export`.
///
/// `--turn` and `--all` are mutually exclusive; `--out` is required. As with
/// verification, `--allow-open` is accepted only with `--all`.
fn parse_trace_export_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<TraceCommand>, CliError> {
    let mut turn = None;
    let mut all = false;
    let mut out = None;
    let mut allow_open = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_trace_usage();
                return Ok(None);
            }
            "--turn" => {
                let id = args.next().ok_or(CliError::MissingTraceTurn)?;
                set_trace_turn(&mut turn, id)?;
            }
            _ if arg.starts_with("--turn=") => {
                let id = arg
                    .strip_prefix("--turn=")
                    .filter(|id| !id.is_empty())
                    .ok_or(CliError::MissingTraceTurn)?;
                set_trace_turn(&mut turn, id.to_owned())?;
            }
            "--out" => {
                let path = args.next().ok_or(CliError::MissingTraceExportOut)?;
                set_trace_export_out(&mut out, PathBuf::from(path))?;
            }
            _ if arg.starts_with("--out=") => {
                let path = arg
                    .strip_prefix("--out=")
                    .filter(|path| !path.is_empty())
                    .ok_or(CliError::MissingTraceExportOut)?;
                set_trace_export_out(&mut out, PathBuf::from(path))?;
            }
            "--all" => all = true,
            "--allow-open" => allow_open = true,
            _ => {
                return Err(CliError::InvalidTraceCommand {
                    message: format!("unexpected `trace export` argument `{arg}`"),
                });
            }
        }
    }

    if all && turn.is_some() {
        return Err(CliError::TraceSelectionConflict);
    }
    if allow_open && !all {
        return Err(CliError::InvalidTraceCommand {
            message: "`--allow-open` requires `--all`".to_owned(),
        });
    }
    let selection = match (turn, all) {
        (Some(turn), false) => TraceSelection::Turn(turn),
        (None, true) => TraceSelection::All,
        _ => return Err(CliError::MissingTraceSelection),
    };
    let out = out.ok_or(CliError::MissingTraceExportOut)?;
    Ok(Some(TraceCommand::Export {
        selection,
        out,
        allow_open,
    }))
}

/// Parses the arguments following `trace verify-bundle`.
///
/// The command is offline: `--bundle` names the portable evidence package and
/// no `[events]` path is consulted. `--replay` is deliberately rejected until
/// bundle replay is implemented.
fn parse_trace_verify_bundle_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<TraceCommand>, CliError> {
    let mut bundle = None;
    let mut json = false;
    let mut allow_open = false;
    let mut expect = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_trace_usage();
                return Ok(None);
            }
            "--bundle" => {
                let path = args.next().ok_or(CliError::MissingTraceBundle)?;
                set_trace_bundle(&mut bundle, PathBuf::from(path))?;
            }
            _ if arg.starts_with("--bundle=") => {
                let path = arg
                    .strip_prefix("--bundle=")
                    .filter(|path| !path.is_empty())
                    .ok_or(CliError::MissingTraceBundle)?;
                set_trace_bundle(&mut bundle, PathBuf::from(path))?;
            }
            "--json" => json = true,
            "--allow-open" => allow_open = true,
            "--expect" => {
                let digest = args.next().ok_or_else(|| CliError::InvalidTraceCommand {
                    message: "`--expect` requires a digest".to_owned(),
                })?;
                set_trace_expect(&mut expect, &digest)?;
            }
            _ if arg.starts_with("--expect=") => {
                let digest =
                    arg.strip_prefix("--expect=")
                        .ok_or_else(|| CliError::InvalidTraceCommand {
                            message: "`--expect` requires a digest".to_owned(),
                        })?;
                set_trace_expect(&mut expect, digest)?;
            }
            "--replay" => {
                return Err(CliError::InvalidTraceCommand {
                    message: "`--replay` is not supported with `trace verify-bundle`".to_owned(),
                });
            }
            _ => {
                return Err(CliError::InvalidTraceCommand {
                    message: format!("unexpected `trace verify-bundle` argument `{arg}`"),
                });
            }
        }
    }

    let bundle = bundle.ok_or(CliError::MissingTraceBundle)?;
    Ok(Some(TraceCommand::VerifyBundle {
        bundle,
        json,
        allow_open,
        expect,
    }))
}

fn set_trace_export_out(slot: &mut Option<PathBuf>, path: PathBuf) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::DuplicateTraceExportOut);
    }
    *slot = Some(path);
    Ok(())
}

fn set_trace_bundle(slot: &mut Option<PathBuf>, path: PathBuf) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::DuplicateTraceBundle);
    }
    *slot = Some(path);
    Ok(())
}

/// Validates and normalizes an expected digest.
fn set_trace_expect(slot: &mut Option<String>, digest: &str) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::InvalidTraceCommand {
            message: "`--expect` may be given only once".to_owned(),
        });
    }
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CliError::InvalidTraceCommand {
            message: "`--expect` requires exactly 64 hexadecimal characters".to_owned(),
        });
    }
    *slot = Some(digest.to_ascii_lowercase());
    Ok(())
}

fn set_trace_turn(slot: &mut Option<String>, id: String) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::DuplicateTraceTurn);
    }
    if id.is_empty() {
        return Err(CliError::MissingTraceTurn);
    }
    *slot = Some(id);
    Ok(())
}

fn set_skill_status(slot: &mut Option<SkillStatus>, value: &str) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::InvalidSkillsCommand {
            message: "`--status` may be given only once".to_owned(),
        });
    }
    *slot = Some(match value {
        "draft" => SkillStatus::Draft,
        "active" => SkillStatus::Active,
        other => {
            return Err(CliError::InvalidSkillsCommand {
                message: format!("unknown skill status `{other}`; expected `draft` or `active`"),
            });
        }
    });
    Ok(())
}

fn set_skill_limit(slot: &mut usize, value: &str) -> Result<(), CliError> {
    if *slot != default_list_limit() {
        return Err(CliError::InvalidSkillsCommand {
            message: "`--limit` may be given only once".to_owned(),
        });
    }
    let parsed = value
        .parse::<usize>()
        .ok()
        .filter(|limit| *limit > 0)
        .ok_or_else(|| CliError::InvalidSkillsCommand {
            message: format!("invalid `--limit` value `{value}`; expected a positive integer"),
        })?;
    *slot = parsed;
    Ok(())
}

fn parse_skill_id(value: &str) -> Result<SkillId, CliError> {
    let parsed = value
        .parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| CliError::InvalidSkillsCommand {
            message: format!("invalid skill id `{value}`; expected a positive integer"),
        })?;
    Ok(SkillId::from_raw(parsed))
}

fn print_skill_usage() {
    println!(
        "Usage: jessica [--config <path>] skills <command>\n\
         \n\
         Commands:\n\
         \x20 list [--status draft|active] [--limit <n>]  List live skills\n\
         \x20 show <id>                                 Show one skill\n\
         \x20 activate <id>                             Activate a draft\n\
         \x20 delete <id>                               Soft-delete a skill\n\
         \n\
         Review commands require `[skills] enabled = true`. They never call the\n\
         model, load wasm tools, or use the network."
    );
}

fn print_trace_usage() {
    println!(
        "Usage: jessica [--config <path>] trace verify --turn <turn_id> [--json] [--replay] [--expect <digest>]\n\
         \x20      jessica [--config <path>] trace verify --all [--json] [--allow-open] [--replay]\n\
         \x20      jessica [--config <path>] trace digest --turn <turn_id> [--json]\n\
         \x20      jessica [--config <path>] trace digest --all [--json]\n\
         \x20      jessica [--config <path>] trace export --turn <turn_id> --out <path>\n\
         \x20      jessica [--config <path>] trace export --all --out <path> [--allow-open]\n\
         \x20      jessica trace verify-bundle --bundle <path> [--json] [--allow-open] [--expect <digest>]\n\
         \n\
         Verifies, digests, or exports recorded turn traces without creating,\n\
         migrating, or modifying the [events] database. Verification checks\n\
         structural completeness, version-2 semantics, trace-to-legacy mirror\n\
         consistency, and the SHA-256 hash chain. It never calls the model or a\n\
         tool. `verify-bundle` is fully offline and does not read [events].\n\
         \n\
         --turn <turn_id>  Verify, digest, or export one turn; unknown is an error\n\
         --all             Select every stored turn\n\
         --json            Emit the stable JSON report instead of text\n\
         --allow-open      Treat traces without `turn_ended` as verified (with --all;\n\
         \x20                 not with --replay)\n\
         --replay          Re-run each fresh or resumed attempt with recorded inputs\n\
         \x20                 and require the event sequence to match (verify only)\n\
         --expect <digest> Require a turn chain digest (verify) or the bundle\n\
         \x20                 aggregate digest (verify-bundle) to match 64 hex chars\n\
         --out <path>      Write the evidence bundle; an existing file is replaced\n\
         --bundle <path>   Read a portable evidence bundle for offline verification\n\
         \n\
         `trace digest --turn` prints only the lowercase hex digest; --all prints\n\
         one `turn_id<TAB>digest` line per turn in ascending turn-id order.\n\
         `trace export` accepts only traces that pass structural, semantic, and\n\
         mirror verification. Invalid traces and unallowed open traces abort the\n\
         export without writing a bundle.\n\
         \n\
         Exit code 0 means every selected trace passed; 1 means a trace was\n\
         invalid, an open trace was not allowed, replay found a mismatch, the\n\
         trace is not replayable, a digest was unavailable, or the command could\n\
         not run."
    );
}

fn set_resume(slot: &mut Option<SessionId>, id: &str) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::DuplicateResume);
    }
    *slot = Some(SessionId::new(id));
    Ok(())
}

fn set_replay_session(slot: &mut Option<SessionId>, id: &str) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::DuplicateReplaySession);
    }
    *slot = Some(SessionId::new(id));
    Ok(())
}

fn set_replay_out(slot: &mut Option<PathBuf>, path: PathBuf) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::DuplicateReplayOut);
    }
    *slot = Some(path);
    Ok(())
}

fn set_config(slot: &mut Option<PathBuf>, path: PathBuf) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::DuplicateConfig);
    }
    *slot = Some(path);
    Ok(())
}

fn print_usage() {
    println!(
        "Usage: jessica [--config <path>] [--resume <session_id>] [prompt...]\n\
         \n\
         Runs one governed agent turn when a prompt is given; otherwise starts a REPL.\n\
         The configuration file controls budgets, tool allowlist, command sensor, and\n\
         acceptance retries. See examples/harness.toml.\n\
         \n\
         JESSICA_PROVIDER selects `openai` (default) or `anthropic`; the matching\n\
         OPENAI_* or ANTHROPIC_* variables configure it.\n\
         \n\
         --resume <session_id> continues an interrupted session recorded in the\n\
         trace event store (default .jessica/events.db). It cannot be combined\n\
         with a prompt and requires trace persistence (profile.trace = \"strict\").\n\
         \n\
         Usage: jessica [--config <path>] config\n\
         \n\
         Prints the effective configuration, including profile preset defaults and\n\
         the resolved trace path, as pretty JSON. It has no runtime side effects.\n\
         \n\
         Usage: jessica [--config <path>] replay --session <session_id> --out <path>\n\
         \n\
         Replays a recorded session offline: the recorded model responses and tool\n\
         results drive the loop, and the resulting events are written as JSONL. It\n\
         requires trace persistence, never calls the model or a tool, and never\n\
         reads the clock or the network.\n\
         \n\
         Usage: jessica [--config <path>] trace verify --turn <turn_id> [--json] [--replay] [--expect <digest>]\n\
         \x20      jessica [--config <path>] trace verify --all [--json] [--allow-open] [--replay]\n\
         \x20      jessica [--config <path>] trace digest --turn <turn_id> [--json]\n\
         \x20      jessica [--config <path>] trace digest --all [--json]\n\
         \x20      jessica [--config <path>] trace export --turn <turn_id> --out <path>\n\
         \x20      jessica [--config <path>] trace export --all --out <path> [--allow-open]\n\
         \x20      jessica trace verify-bundle --bundle <path> [--json] [--allow-open] [--expect <digest>]\n\
         \n\
         Verifies, digests, or exports recorded turn traces against a recorded\n\
         trace store (default .jessica/events.db). The store is opened read-only:\n\
         the command never creates or migrates it. `trace verify-bundle` audits a\n\
         portable bundle\n\
         entirely offline. See `jessica trace --help` for problem kinds and exit\n\
         codes.\n\
         \n\
         Usage: jessica [--config <path>] tools list [--all]\n\
         \x20      jessica [--config <path>] tools inspect <name>\n\
         \n\
         Lists the configured tool catalog in stable name order, with each\n\
         exposed tool's source, effective timeout, and schema digest. `--all`\n\
         also shows hidden tools and why they are hidden. `tools inspect` prints\n\
         one tool's descriptor, capabilities, limits, policy hints, and schema.\n\
         Both commands are side-effect free: they do not create a provider,\n\
         event store, memory or skill database, or MCP connection.\n\
         \n\
         Usage: jessica [--config <path>] acp\n\
         \n\
         Runs a stable Agent Client Protocol (ACP) v1 agent over stdin/stdout.\n\
         Stdout carries only newline-delimited JSON-RPC; diagnostics go to stderr.\n\
         The process exits on stdin EOF, SIGINT, or SIGTERM. Implements\n\
         `initialize`, `session/new`, `session/prompt`, and `session/cancel`."
    );
}

/// Reads user requests until EOF, an explicit exit command, or shutdown.
///
/// Once the first signal is observed no new turn is started, even if the signal
/// arrived while a line was being read.
async fn repl(runtime: &CliRuntime, shutdown: &ShutdownController) -> Result<(), CliError> {
    let mut stdin = BufReader::new(tokio::io::stdin());
    loop {
        if shutdown.is_shutting_down() {
            break;
        }
        let Some(line) = read_line(&mut stdin, shutdown).await else {
            break;
        };
        if shutdown.is_shutting_down() {
            break;
        }
        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if input == "exit" || input == "quit" {
            break;
        }
        if let Err(error) = runtime.run_turn(input.to_owned()).await {
            eprintln!("jessica: {error}");
        }
    }
    Ok(())
}

/// Reads one line from stdin, returning `None` on EOF or shutdown.
///
/// A shutdown signal wakes the pending read immediately instead of waiting for
/// the next newline or EOF.
async fn read_line(stdin: &mut BufReader<Stdin>, shutdown: &ShutdownController) -> Option<String> {
    if io::stdin().is_terminal() {
        print!("> ");
        let _ = io::stdout().flush();
    }

    let mut line = String::new();
    let read = stdin.read_line(&mut line);
    tokio::pin!(read);

    tokio::select! {
        biased;
        _ = shutdown.cancelled() => {
            if io::stdin().is_terminal() {
                println!();
            }
            None
        }
        result = &mut read => match result {
            Ok(0) => None,
            Ok(_) => Some(line),
            Err(error) => {
                eprintln!("[error] failed to read stdin: {error}");
                None
            }
        },
    }
}

/// Maps a completed invocation to a process exit code.
///
/// A received signal always determines the exit code: SIGINT is 130 and
/// SIGTERM is 143. A skill drain timeout never changes the code. Without a
/// signal, completed and cancelled turns are success, while budget,
/// acceptance, and internal failures are failure.
fn exit_code(outcome: &RunOutcome) -> u8 {
    if let Some(signal) = outcome.signal {
        return signal.exit_code();
    }
    match &outcome.result {
        Err(_) => 1,
        Ok(None) => 0,
        Ok(Some(SessionEndReason::Completed | SessionEndReason::Cancelled)) => 0,
        Ok(Some(
            SessionEndReason::BudgetExceeded
            | SessionEndReason::AcceptanceFailed
            | SessionEndReason::Error,
        )) => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;
    use futures::StreamExt;
    use jessica_core::llm::{LlmError, LlmEvent, LlmStream};
    use jessica_core::tool::ToolSchema;
    use jessica_core::types::Message;
    use jessica_providers::{ClassifiedStream, ProviderFailure};

    #[test]
    fn a_received_signal_determines_the_exit_code() {
        for (signal, expected) in [
            (ShutdownSignal::SigInt, 130),
            (ShutdownSignal::SigTerm, 143),
        ] {
            let completed = RunOutcome {
                result: Ok(Some(SessionEndReason::Completed)),
                signal: Some(signal),
            };
            assert_eq!(exit_code(&completed), expected);

            let failed = RunOutcome {
                result: Err(CliError::ResumeRequiresEventStore),
                signal: Some(signal),
            };
            assert_eq!(
                exit_code(&failed),
                expected,
                "an internal error must not hide the signal exit code"
            );
        }
    }

    #[test]
    fn without_a_signal_cancelled_is_success_and_failures_are_not() {
        assert_eq!(exit_code(&RunOutcome::success()), 0);
        assert_eq!(
            exit_code(&RunOutcome {
                result: Ok(Some(SessionEndReason::Cancelled)),
                signal: None,
            }),
            0
        );
        for reason in [
            SessionEndReason::BudgetExceeded,
            SessionEndReason::AcceptanceFailed,
            SessionEndReason::Error,
        ] {
            assert_eq!(
                exit_code(&RunOutcome {
                    result: Ok(Some(reason)),
                    signal: None,
                }),
                1
            );
        }
    }

    #[test]
    fn only_long_lived_modes_watch_the_config_file() {
        let config = Some(PathBuf::from("harness.toml"));
        let repl = Mode::Turn {
            resume: None,
            prompt: None,
        };
        assert_eq!(watched_config_path(&repl, &config), config);
        assert_eq!(watched_config_path(&repl, &None), None);

        let one_shot = Mode::Turn {
            resume: None,
            prompt: Some("hi".to_owned()),
        };
        assert_eq!(watched_config_path(&one_shot, &config), None);

        let resume = Mode::Turn {
            resume: Some(SessionId::new("session")),
            prompt: None,
        };
        assert_eq!(watched_config_path(&resume, &config), None);

        let replay = Mode::Replay {
            session: SessionId::new("session"),
            out: PathBuf::from("out.jsonl"),
        };
        assert_eq!(watched_config_path(&replay, &config), None);

        let skills = Mode::Skills {
            command: SkillCommand::List {
                status: None,
                limit: 10,
            },
        };
        assert_eq!(watched_config_path(&skills, &config), None);

        let trace = Mode::Trace {
            command: TraceCommand::Verify {
                selection: TraceSelection::All,
                json: false,
                allow_open: false,
                replay: false,
                expect: None,
            },
        };
        assert_eq!(watched_config_path(&trace, &config), None);
        assert_eq!(watched_config_path(&Mode::Config, &config), None);

        assert_eq!(
            watched_config_path(&Mode::Acp, &config),
            config,
            "ACP serves long-lived prompts and must observe reloadable config"
        );
        assert_eq!(watched_config_path(&Mode::Acp, &None), None);
    }

    #[test]
    fn config_mode_parses_a_config_path_and_rejects_extra_arguments() {
        let options = parse_args_from(skill_args(&["--config", "harness.toml", "config"]))
            .expect("config arguments must parse")
            .expect("not a help request");
        assert_eq!(options.config, Some(PathBuf::from("harness.toml")));
        assert!(matches!(options.mode, Mode::Config));

        let error = parse_args_from(skill_args(&["config", "--json"]))
            .expect_err("config has no extra arguments");
        assert!(error.to_string().contains("--json"), "{error}");
    }

    #[test]
    fn acp_accepts_the_config_flag_on_either_side() {
        let config = PathBuf::from("harness.toml");
        for (initial, arguments) in [
            (None, vec!["--config", "harness.toml"]),
            (None, vec!["--config=harness.toml"]),
            (Some(config.clone()), vec![]),
        ] {
            let options = parse_acp_args(initial, skill_args(&arguments))
                .expect("acp arguments must parse")
                .expect("not a help request");
            assert_eq!(options.config, Some(config.clone()));
            assert!(matches!(options.mode, Mode::Acp));
        }
    }

    #[test]
    fn trace_verify_parses_turn_and_all_selections() {
        assert_eq!(
            parse_trace_args(skill_args(&["verify", "--turn", "turn-a", "--json"]))
                .expect("turn selection must parse"),
            Some(TraceCommand::Verify {
                selection: TraceSelection::Turn("turn-a".to_owned()),
                json: true,
                allow_open: false,
                replay: false,
                expect: None,
            })
        );

        assert_eq!(
            parse_trace_args(skill_args(&["verify", "--all", "--json", "--allow-open"]))
                .expect("all selection must parse"),
            Some(TraceCommand::Verify {
                selection: TraceSelection::All,
                json: true,
                allow_open: true,
                replay: false,
                expect: None,
            })
        );

        assert_eq!(
            parse_trace_args(skill_args(&["verify", "--turn=turn-b"]))
                .expect("equals-form turn selection must parse"),
            Some(TraceCommand::Verify {
                selection: TraceSelection::Turn("turn-b".to_owned()),
                json: false,
                allow_open: false,
                replay: false,
                expect: None,
            })
        );

        assert_eq!(
            parse_trace_args(skill_args(&[
                "verify", "--turn", "turn-c", "--replay", "--json"
            ]))
            .expect("replay with one turn must parse"),
            Some(TraceCommand::Verify {
                selection: TraceSelection::Turn("turn-c".to_owned()),
                json: true,
                allow_open: false,
                replay: true,
                expect: None,
            })
        );

        assert_eq!(
            parse_trace_args(skill_args(&["verify", "--all", "--replay", "--json"]))
                .expect("batch replay must parse"),
            Some(TraceCommand::Verify {
                selection: TraceSelection::All,
                json: true,
                allow_open: false,
                replay: true,
                expect: None,
            })
        );
    }

    #[test]
    fn trace_digest_parses_turn_and_all_selections() {
        assert_eq!(
            parse_trace_args(skill_args(&["digest", "--turn", "turn-a", "--json"]))
                .expect("turn digest must parse"),
            Some(TraceCommand::Digest {
                selection: TraceSelection::Turn("turn-a".to_owned()),
                json: true,
            })
        );

        assert_eq!(
            parse_trace_args(skill_args(&["digest", "--all"]))
                .expect("all-turns digest must parse"),
            Some(TraceCommand::Digest {
                selection: TraceSelection::All,
                json: false,
            })
        );

        assert_eq!(
            parse_trace_args(skill_args(&["digest", "--turn=turn-b"]))
                .expect("equals-form turn digest must parse"),
            Some(TraceCommand::Digest {
                selection: TraceSelection::Turn("turn-b".to_owned()),
                json: false,
            })
        );
    }

    #[test]
    fn trace_expect_is_validated_and_normalized() {
        let uppercase = "AB".repeat(32);
        let lowercase = "ab".repeat(32);
        assert_eq!(
            parse_trace_args(skill_args(&[
                "verify",
                "--turn",
                "turn-a",
                "--expect",
                uppercase.as_str(),
            ]))
            .expect("an uppercase digest must be accepted"),
            Some(TraceCommand::Verify {
                selection: TraceSelection::Turn("turn-a".to_owned()),
                json: false,
                allow_open: false,
                replay: false,
                expect: Some(lowercase.clone()),
            })
        );

        assert_eq!(
            parse_trace_args(skill_args(&[
                "verify",
                "--turn=turn-b",
                format!("--expect={uppercase}").as_str(),
            ]))
            .expect("the equals form must be accepted"),
            Some(TraceCommand::Verify {
                selection: TraceSelection::Turn("turn-b".to_owned()),
                json: false,
                allow_open: false,
                replay: false,
                expect: Some(lowercase),
            })
        );
    }

    #[test]
    fn malformed_digest_arguments_are_rejected() {
        for (arguments, expected) in [
            (vec!["digest"], "requires"),
            (vec!["digest", "--turn"], "requires a turn id"),
            (vec!["digest", "--turn="], "requires a turn id"),
            (vec!["digest", "--turn", "a", "--turn", "b"], "only once"),
            (vec!["digest", "--turn", "a", "--all"], "cannot be combined"),
            (vec!["digest", "--expect", "00"], "--expect"),
            (vec!["digest", "--unknown"], "--unknown"),
        ] {
            let error = parse_trace_args(skill_args(&arguments))
                .expect_err("malformed digest arguments must be rejected");
            assert!(
                error.to_string().contains(expected),
                "error `{error}` must mention `{expected}`"
            );
        }
    }

    #[test]
    fn malformed_expect_arguments_are_rejected() {
        let short = "a".repeat(63);
        let long = "a".repeat(65);
        let non_hex = "g".repeat(64);
        let valid = "a".repeat(64);
        for (arguments, expected) in [
            (
                vec!["verify", "--turn", "a", "--expect"],
                "requires a digest",
            ),
            (
                vec!["verify", "--turn", "a", "--expect", short.as_str()],
                "64 hexadecimal",
            ),
            (
                vec!["verify", "--turn", "a", "--expect", long.as_str()],
                "64 hexadecimal",
            ),
            (
                vec!["verify", "--turn", "a", "--expect", non_hex.as_str()],
                "64 hexadecimal",
            ),
            (
                vec![
                    "verify",
                    "--turn",
                    "a",
                    "--expect",
                    valid.as_str(),
                    "--expect",
                    valid.as_str(),
                ],
                "only once",
            ),
            (
                vec!["verify", "--all", "--expect", valid.as_str()],
                "requires `--turn`",
            ),
        ] {
            let error = parse_trace_args(skill_args(&arguments))
                .expect_err("malformed expect arguments must be rejected");
            assert!(
                error.to_string().contains(expected),
                "error `{error}` must mention `{expected}`"
            );
        }
    }

    #[test]
    fn trace_export_parses_turn_all_out_and_allow_open() {
        assert_eq!(
            parse_trace_args(skill_args(&[
                "export",
                "--turn",
                "turn-a",
                "--out",
                "evidence.json",
            ]))
            .expect("single-turn export must parse"),
            Some(TraceCommand::Export {
                selection: TraceSelection::Turn("turn-a".to_owned()),
                out: PathBuf::from("evidence.json"),
                allow_open: false,
            })
        );

        assert_eq!(
            parse_trace_args(skill_args(&[
                "export",
                "--all",
                "--allow-open",
                "--out=all.json",
            ]))
            .expect("all-turns export must parse"),
            Some(TraceCommand::Export {
                selection: TraceSelection::All,
                out: PathBuf::from("all.json"),
                allow_open: true,
            })
        );
    }

    #[test]
    fn trace_verify_bundle_parses_offline_flags() {
        let uppercase = "AB".repeat(32);
        let lowercase = "ab".repeat(32);
        assert_eq!(
            parse_trace_args(skill_args(&[
                "verify-bundle",
                "--bundle",
                "evidence.json",
                "--json",
                "--allow-open",
                "--expect",
                uppercase.as_str(),
            ]))
            .expect("bundle verification must parse without an event store"),
            Some(TraceCommand::VerifyBundle {
                bundle: PathBuf::from("evidence.json"),
                json: true,
                allow_open: true,
                expect: Some(lowercase.clone()),
            })
        );

        assert_eq!(
            parse_trace_args(skill_args(&[
                "verify-bundle",
                "--bundle=evidence.json",
                format!("--expect={uppercase}").as_str(),
            ]))
            .expect("equals forms must parse"),
            Some(TraceCommand::VerifyBundle {
                bundle: PathBuf::from("evidence.json"),
                json: false,
                allow_open: false,
                expect: Some(lowercase),
            })
        );
    }

    #[test]
    fn malformed_bundle_arguments_are_rejected() {
        for (arguments, expected) in [
            (vec!["export", "--turn", "a"], "requires a path"),
            (
                vec!["export", "--turn", "a", "--out", "a", "--out", "b"],
                "only once",
            ),
            (
                vec!["export", "--turn", "a", "--all", "--out", "a"],
                "cannot be combined",
            ),
            (
                vec!["export", "--turn", "a", "--allow-open", "--out", "a"],
                "requires `--all`",
            ),
            (vec!["export", "--all", "--out"], "requires a path"),
            (vec!["verify-bundle"], "requires a path"),
            (
                vec!["verify-bundle", "--bundle", "a", "--bundle", "b"],
                "only once",
            ),
            (vec!["verify-bundle", "--bundle="], "requires a path"),
            (
                vec!["verify-bundle", "--bundle", "a", "--replay"],
                "not supported",
            ),
            (
                vec!["verify-bundle", "--bundle", "a", "--turn", "t"],
                "--turn",
            ),
            (
                vec!["verify-bundle", "--bundle", "a", "--expect", "00"],
                "64 hexadecimal",
            ),
        ] {
            let error = parse_trace_args(skill_args(&arguments))
                .expect_err("malformed bundle arguments must be rejected");
            assert!(
                error.to_string().contains(expected),
                "error `{error}` must mention `{expected}`"
            );
        }
    }

    #[test]
    fn trace_help_is_not_an_error_and_prints_usage() {
        assert_eq!(
            parse_trace_args(skill_args(&["--help"])).expect("trace help must parse"),
            None
        );
        assert_eq!(
            parse_trace_args(skill_args(&["verify", "-h"])).expect("verify help must parse"),
            None
        );
    }

    #[test]
    fn malformed_trace_arguments_are_rejected_with_the_offending_value() {
        for (arguments, expected) in [
            (vec!["publish"], "publish"),
            (vec!["verify"], "requires"),
            (vec!["verify", "--turn"], "requires a turn id"),
            (vec!["verify", "--turn="], "requires a turn id"),
            (vec!["verify", "--turn", "a", "--turn", "b"], "only once"),
            (vec!["verify", "--turn", "a", "--all"], "cannot be combined"),
            (
                vec!["verify", "--all", "--allow-open=true"],
                "--allow-open=true",
            ),
            (
                vec!["verify", "--turn", "a", "--allow-open"],
                "requires `--all`",
            ),
            (
                vec!["verify", "--all", "--replay", "--allow-open"],
                "cannot be combined",
            ),
            (vec!["verify", "--unknown"], "--unknown"),
        ] {
            let error = parse_trace_args(skill_args(&arguments))
                .expect_err("malformed trace arguments must be rejected");
            assert!(
                error.to_string().contains(expected),
                "error `{error}` must mention `{expected}`"
            );
        }

        let error =
            parse_trace_args(skill_args(&[])).expect_err("a missing subcommand must be rejected");
        assert!(error.to_string().contains("missing subcommand"));
    }

    #[test]
    fn acp_rejects_prompts_and_malformed_flags() {
        let error = parse_acp_args(None, skill_args(&["hello"]))
            .expect_err("a prompt must not be accepted in ACP mode");
        assert!(error.to_string().contains("hello"));

        let error = parse_acp_args(None, skill_args(&["--unknown"]))
            .expect_err("unknown flags must be rejected");
        assert!(error.to_string().contains("--unknown"));

        let error = parse_acp_args(None, skill_args(&["--config"]))
            .expect_err("a missing path is rejected");
        assert!(error.to_string().contains("path"));
    }

    /// Concrete provider that distinguishes the legacy and classified paths.
    #[derive(Default)]
    struct DualProvider {
        legacy_calls: AtomicU32,
        classified_calls: AtomicU32,
    }

    #[async_trait]
    impl LlmProvider for DualProvider {
        async fn stream_chat(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<LlmStream, LlmError> {
            self.legacy_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter(vec![
                LlmEvent::TextDelta {
                    text: "legacy".to_owned(),
                },
                LlmEvent::Done { usage: None },
            ])))
        }
    }

    #[async_trait]
    impl ClassifiedProvider for DualProvider {
        async fn stream_chat_classified(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
        ) -> Result<ClassifiedStream, ProviderFailure> {
            self.classified_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(LlmEvent::TextDelta {
                    text: "classified".to_owned(),
                }),
                Ok(LlmEvent::Done { usage: None }),
            ])))
        }
    }

    fn skill_args(arguments: &[&str]) -> std::vec::IntoIter<String> {
        arguments
            .iter()
            .map(|argument| (*argument).to_owned())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn skills_list_defaults_to_every_status_and_the_default_limit() {
        let command = parse_skill_args(skill_args(&["list"]))
            .expect("list must parse")
            .expect("list is not a help request");

        assert_eq!(
            command,
            SkillCommand::List {
                status: None,
                limit: default_list_limit(),
            }
        );
    }

    #[test]
    fn skills_list_parses_status_and_limit_in_both_forms() {
        assert_eq!(
            parse_skill_args(skill_args(&["list", "--status", "draft", "--limit", "5"]))
                .expect("list must parse"),
            Some(SkillCommand::List {
                status: Some(SkillStatus::Draft),
                limit: 5,
            })
        );
        assert_eq!(
            parse_skill_args(skill_args(&["list", "--status=active", "--limit=9"]))
                .expect("list must parse"),
            Some(SkillCommand::List {
                status: Some(SkillStatus::Active),
                limit: 9,
            })
        );
    }

    #[test]
    fn malformed_skills_list_arguments_are_rejected_with_the_offending_value() {
        for (arguments, expected) in [
            (vec!["list", "--status", "archived"], "archived"),
            (vec!["list", "--status="], "unknown skill status"),
            (vec!["list", "--limit", "0"], "0"),
            (vec!["list", "--limit", "many"], "many"),
            (
                vec!["list", "--status=draft", "--status=active"],
                "--status",
            ),
            (vec!["list", "--limit=1", "--limit=2"], "--limit"),
            (vec!["list", "--unknown"], "--unknown"),
        ] {
            let error = parse_skill_args(skill_args(&arguments))
                .expect_err("malformed list arguments must fail");
            assert!(
                error.to_string().contains(expected),
                "error for {arguments:?} should mention {expected:?}: {error}"
            );
        }
    }

    fn skill_command_variant(command: &str, id: SkillId) -> SkillCommand {
        match command {
            "show" => SkillCommand::Show { id },
            "activate" => SkillCommand::Activate { id },
            _ => SkillCommand::Delete { id },
        }
    }

    #[test]
    fn skills_show_activate_and_delete_require_exactly_one_positive_id() {
        for command in ["show", "activate", "delete"] {
            assert_eq!(
                parse_skill_args(skill_args(&[command, "7"])).expect("id must parse"),
                Some(skill_command_variant(command, SkillId::from_raw(7)))
            );
            for (arguments, expected) in [
                (vec![command], command),
                (vec![command, "0"], "skill id"),
                (vec![command, "x"], "skill id"),
                (vec![command, "1", "2"], command),
            ] {
                let error =
                    parse_skill_args(skill_args(&arguments)).expect_err("invalid ids must fail");
                assert!(
                    error.to_string().contains(expected),
                    "error for {arguments:?} should mention {expected:?}: {error}"
                );
            }
        }
    }

    #[test]
    fn unknown_skills_subcommands_fail_and_help_returns_none() {
        let error =
            parse_skill_args(skill_args(&["publish"])).expect_err("unknown subcommands must fail");
        assert!(error.to_string().contains("publish"));

        assert_eq!(
            parse_skill_args(skill_args(&["--help"])).expect("help must parse"),
            None
        );
        assert_eq!(
            parse_skill_args(skill_args(&["list", "--help"])).expect("help must parse"),
            None
        );
    }

    #[test]
    fn provider_selection_defaults_to_openai_when_unset() {
        assert_eq!(
            ProviderKind::from_env_value(None).expect("unset selection must parse"),
            ProviderKind::OpenAi
        );
        assert_eq!(
            ProviderKind::from_env_value(Some("openai")).expect("explicit selection must parse"),
            ProviderKind::OpenAi
        );
    }

    #[test]
    fn provider_selection_accepts_anthropic_case_insensitively() {
        assert_eq!(
            ProviderKind::from_env_value(Some(" Anthropic ")).expect("selection must parse"),
            ProviderKind::Anthropic
        );
    }

    #[test]
    fn provider_selection_rejects_unknown_names() {
        let error = ProviderKind::from_env_value(Some("gemini"))
            .expect_err("unsupported providers must be rejected");
        let message = error.to_string();
        assert!(message.contains("JESSICA_PROVIDER"));
        assert!(message.contains("gemini"));
    }

    #[tokio::test]
    async fn disabled_reliability_returns_the_base_provider_without_wrapping() {
        let base = Arc::new(DualProvider::default());
        let observer = Arc::clone(&base);
        let reliability = CliReliabilityConfig {
            enabled: false,
            ..CliReliabilityConfig::default()
        };

        let provider = assemble_provider(base, &reliability).expect("assembly should succeed");
        let events = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("base provider should stream")
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "legacy".to_owned()
                },
                LlmEvent::Done { usage: None },
            ]
        );
        assert_eq!(observer.legacy_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            observer.classified_calls.load(Ordering::SeqCst),
            0,
            "disabled reliability must not route through ClassifiedProvider"
        );
    }

    #[tokio::test]
    async fn enabled_reliability_wraps_the_base_provider_once() {
        let base = Arc::new(DualProvider::default());
        let observer = Arc::clone(&base);
        let reliability = CliReliabilityConfig {
            enabled: true,
            max_attempts: 1,
            ..CliReliabilityConfig::default()
        };

        let provider = assemble_provider(base, &reliability).expect("assembly should succeed");
        let events = provider
            .stream_chat(&[Message::user("hello")], &[])
            .await
            .expect("wrapped provider should stream")
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta {
                    text: "classified".to_owned()
                },
                LlmEvent::Done { usage: None },
            ]
        );
        assert_eq!(observer.classified_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observer.legacy_calls.load(Ordering::SeqCst), 0);
    }
}
