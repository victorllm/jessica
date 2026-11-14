//! Governed one-shot / REPL front end for Jessica.

mod acceptance_check;
mod app;
mod config;
mod delegation;
mod events;
mod governance;
mod hot_reload;
mod memory;
mod pump;
mod replay;
mod shutdown;
mod skills;

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
    let recorder = open_recorder(&config, &root).await?;
    let provider = build_provider(&config.reliability)?;
    let wasm_tools = load_wasm_tools(&config, &root).await?;
    // Memory and skills are assembled only on the turn path. Replay returns
    // above and therefore never opens, creates, or reads either database. Both
    // assemblies are fail-open.
    let memory = MemoryAssembly::open(&config.memory, &root).await;
    let skills = SkillsAssembly::open(&config.skills, &root, Arc::clone(&provider)).await;
    // The startup configuration is loaded and validated once. The receiver is
    // the per-turn snapshot source; only the REPL branch installs a watcher
    // capable of publishing a newer value.
    let (config_sender, config_receiver) = watch::channel(Arc::new(config));
    let runtime = CliRuntime::new(
        provider,
        config_receiver,
        root,
        wasm_tools,
        recorder,
        memory,
        skills,
        Arc::clone(shutdown),
    );

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
struct Options {
    /// Optional TOML configuration path.
    config: Option<PathBuf>,
    /// Selected execution mode.
    mode: Mode,
}

/// Command selected by the arguments.
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
}

/// Returns the configuration path watched by this execution path.
///
/// Only an interactive REPL (a `Turn` with neither `--resume` nor a prompt)
/// owns a long-lived config watcher. One-shot, resume, replay, and skills
/// return `None`, so they load the startup configuration exactly once.
fn watched_config_path(mode: &Mode, config_path: &Option<PathBuf>) -> Option<PathBuf> {
    match mode {
        Mode::Turn {
            resume: None,
            prompt: None,
        } => config_path.clone(),
        _ => None,
    }
}

/// Parses arguments, returning `None` after printing help.
fn parse_args() -> Result<Option<Options>, CliError> {
    let mut config = None;
    let mut resume = None;
    let mut prompt_parts = Vec::new();
    let mut args = std::env::args().skip(1);

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
         [events] database. It cannot be combined with a prompt and requires an\n\
         [events] path in the configuration.\n\
         \n\
         Usage: jessica [--config <path>] replay --session <session_id> --out <path>\n\
         \n\
         Replays a recorded session offline: the recorded model responses and tool\n\
         results drive the loop, and the resulting events are written as JSONL. It\n\
         requires an [events] path, never calls the model or a tool, and never reads\n\
         the clock or the network."
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
    fn only_the_interactive_repl_watches_the_config_file() {
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
