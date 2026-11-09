//! Governed one-shot / REPL front end for Jessica.

mod acceptance_check;
mod app;
mod config;
mod events;
mod governance;
mod pump;

use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use jessica_core::llm::LlmProvider;
use jessica_core::types::{SessionEndReason, SessionId};
use jessica_providers::OpenAiProvider;
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

    let config = match &options.config {
        Some(path) => HarnessConfig::load(path).await?,
        None => HarnessConfig::default(),
    };
    let provider: Arc<dyn LlmProvider> = Arc::new(OpenAiProvider::from_env()?);
    let root = tokio::task::spawn_blocking(std::env::current_dir)
        .await
        .map_err(CliError::CurrentDirTask)?
        .map_err(CliError::CurrentDir)?;
    let wasm_tools = load_wasm_tools(&config, &root).await?;
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
    let runtime = CliRuntime::new(provider, config, root, wasm_tools, recorder);

    if let Some(session_id) = options.resume {
        return Ok(Some(runtime.resume_turn(session_id).await?));
    }

    match options.prompt {
        Some(input) => Ok(Some(runtime.run_turn(input).await?)),
        None => {
            repl(&runtime).await?;
            Ok(None)
        }
    }
}

/// Parsed command line.
struct Options {
    /// Optional TOML configuration path.
    config: Option<PathBuf>,
    /// Session to resume instead of starting a new turn.
    resume: Option<SessionId>,
    /// One-shot prompt; absent means REPL mode.
    prompt: Option<String>,
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
        resume,
        prompt,
    }))
}

fn set_resume(slot: &mut Option<SessionId>, id: &str) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::DuplicateResume);
    }
    *slot = Some(SessionId::new(id));
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
         --resume <session_id> continues an interrupted session recorded in the\n\
         [events] database. It cannot be combined with a prompt and requires an\n\
         [events] path in the configuration."
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
