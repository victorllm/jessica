//! Governed one-shot / REPL front end for Jessica.

mod acceptance_check;
mod app;
mod config;
mod events;
mod governance;
mod pump;
mod replay;

use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use jessica_core::llm::LlmProvider;
use jessica_core::types::{SessionEndReason, SessionId};
use jessica_providers::{AnthropicProvider, OpenAiProvider};
use tokio::io::{AsyncBufReadExt, BufReader, Stdin};

use crate::app::{CliError, CliRuntime, load_wasm_tools};
use crate::config::HarnessConfig;
use crate::events::EventRecorder;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(Some(reason)) => exit_code(reason),
        Ok(None) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("jessica: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<Option<SessionEndReason>, CliError> {
    let Some(options) = parse_args()? else {
        return Ok(None);
    };
    let Options {
        config: config_path,
        mode,
    } = options;

    let config = match &config_path {
        Some(path) => HarnessConfig::load(path).await?,
        None => HarnessConfig::default(),
    };
    let root = tokio::task::spawn_blocking(std::env::current_dir)
        .await
        .map_err(CliError::CurrentDirTask)?
        .map_err(CliError::CurrentDir)?;
    let recorder = match config.events.path.clone() {
        Some(path) => {
            let path = if path.is_absolute() {
                path
            } else {
                root.join(path)
            };
            Some(Arc::new(EventRecorder::open(path).await?))
        }
        None => None,
    };

    match mode {
        Mode::Replay { session, out } => {
            let recorder = recorder.ok_or(CliError::ReplayRequiresEventStore)?;
            let stored =
                recorder
                    .load(session.clone())
                    .await?
                    .ok_or_else(|| CliError::UnknownSession {
                        session_id: session.to_string(),
                    })?;
            let events = replay::replay_session(&session, &stored).await?;
            let count = events.len();
            let output_path = out.display().to_string();
            replay::write_jsonl(out, events).await?;
            println!("[replay] wrote {count} events for session {session} to {output_path}");
            Ok(None)
        }
        Mode::Turn { resume, prompt } => {
            let provider = build_provider()?;
            let wasm_tools = load_wasm_tools(&config, &root).await?;
            let runtime = CliRuntime::new(provider, config, root, wasm_tools, recorder);

            if let Some(session_id) = resume {
                return Ok(Some(runtime.resume_turn(session_id).await?));
            }

            match prompt {
                Some(input) => Ok(Some(runtime.run_turn(input).await?)),
                None => {
                    repl(&runtime).await?;
                    Ok(None)
                }
            }
        }
    }
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
fn build_provider() -> Result<Arc<dyn LlmProvider>, CliError> {
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
        ProviderKind::OpenAi => Ok(Arc::new(OpenAiProvider::from_env()?)),
        ProviderKind::Anthropic => Ok(Arc::new(AnthropicProvider::from_env()?)),
    }
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

/// Reads user requests until EOF, Ctrl+C, or an explicit exit command.
async fn repl(runtime: &CliRuntime) -> Result<(), CliError> {
    let mut stdin = BufReader::new(tokio::io::stdin());
    loop {
        let Some(line) = read_line(&mut stdin).await else {
            break;
        };
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

/// Reads one line from stdin, returning `None` on EOF or Ctrl+C.
async fn read_line(stdin: &mut BufReader<Stdin>) -> Option<String> {
    if io::stdin().is_terminal() {
        print!("> ");
        let _ = io::stdout().flush();
    }

    let mut line = String::new();
    let read = stdin.read_line(&mut line);
    tokio::pin!(read);

    tokio::select! {
        result = &mut read => match result {
            Ok(0) => None,
            Ok(_) => Some(line),
            Err(error) => {
                eprintln!("[error] failed to read stdin: {error}");
                None
            }
        },
        _ = tokio::signal::ctrl_c() => {
            if io::stdin().is_terminal() {
                println!();
            }
            None
        }
    }
}

fn exit_code(reason: SessionEndReason) -> ExitCode {
    match reason {
        SessionEndReason::Completed | SessionEndReason::Cancelled => ExitCode::SUCCESS,
        SessionEndReason::BudgetExceeded
        | SessionEndReason::AcceptanceFailed
        | SessionEndReason::Error => ExitCode::FAILURE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
