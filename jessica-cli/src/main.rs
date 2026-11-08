//! Minimal one-shot / REPL front end for Jessica.

use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jessica_core::llm::LlmError;
use jessica_core::tool::{ToolRegistry, ToolRegistryError};
use jessica_core::types::{AgentEvent, Budget, Message, SessionEndReason, SessionId};
use jessica_providers::OpenAiProvider;
use jessica_runtime::AgentLoop;
use jessica_tools::{BashTool, ReadFileTool, WriteFileTool};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader, Stdin};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Number of model rounds allowed per user request.
const MAX_ROUNDS: u32 = 16;
/// Token budget carried into the loop; enforced by Phase 2 guards later.
const TOKEN_BUDGET: u64 = 1_000_000;
/// Default tool timeout carried into the loop; Phase 2 guards enforce it later.
const TOOL_TIMEOUT: Duration = Duration::from_secs(10);

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
    let provider = Arc::new(OpenAiProvider::from_env()?);
    let root = tokio::task::spawn_blocking(std::env::current_dir)
        .await
        .map_err(CliError::CurrentDirTask)?
        .map_err(CliError::CurrentDir)?;

    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(ReadFileTool::new(root.clone())))
        .map_err(|source| CliError::RegisterTool {
            name: "read_file",
            source,
        })?;
    registry
        .register(Arc::new(WriteFileTool::new(root.clone())))
        .map_err(|source| CliError::RegisterTool {
            name: "write_file",
            source,
        })?;
    registry
        .register(Arc::new(BashTool::new()))
        .map_err(|source| CliError::RegisterTool {
            name: "bash",
            source,
        })?;

    let (events, mut receiver) = broadcast::channel(1024);
    let agent = AgentLoop::new(
        provider,
        Arc::new(registry),
        Budget::new(TOKEN_BUDGET, TOOL_TIMEOUT, MAX_ROUNDS),
        events,
    );

    let system_prompt = format!(
        "You are Jessica, a deterministic coding agent. The workspace root is {}. \
         Use read_file and write_file with paths relative to the workspace root, and bash \
         for commands (10 second timeout, bounded output). Verify your work with tools \
         instead of claiming success. When the task is complete, reply with a concise summary.",
        root.display()
    );

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        repl(&agent, &system_prompt, &mut receiver).await;
        Ok(None)
    } else {
        let input = args.join(" ");
        let reason = run_turn(&agent, &system_prompt, input, &mut receiver).await;
        Ok(Some(reason))
    }
}

/// Reads user requests until EOF, Ctrl+C, or an explicit exit command.
async fn repl(
    agent: &AgentLoop,
    system_prompt: &str,
    receiver: &mut broadcast::Receiver<AgentEvent>,
) {
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
        run_turn(agent, system_prompt, input.to_owned(), receiver).await;
    }
}

/// Runs one user request and prints events until the session ends.
async fn run_turn(
    agent: &AgentLoop,
    system_prompt: &str,
    input: String,
    receiver: &mut broadcast::Receiver<AgentEvent>,
) -> SessionEndReason {
    let messages = vec![
        Message::system(system_prompt.to_owned()),
        Message::user(input),
    ];
    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();

    // A persistent listener turns every Ctrl+C during the turn into a
    // cancellation request; aborting the task after the turn stops it.
    let signal_task = tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                break;
            }
            signal_cancel.cancel();
        }
    });

    let run = agent.run(next_session_id(), messages, cancel);
    tokio::pin!(run);

    let reason = loop {
        tokio::select! {
            reason = &mut run => break reason,
            event = receiver.recv() => match event {
                Ok(event) => print_event(&event),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    eprintln!("[warn] skipped {skipped} events");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // The loop owns a sender, so this cannot happen while it runs.
                    break SessionEndReason::Error;
                }
            },
        }
    };

    signal_task.abort();
    drain_events(receiver);
    reason
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

/// Prints buffered events that were not consumed by the select loop.
fn drain_events(receiver: &mut broadcast::Receiver<AgentEvent>) {
    loop {
        match receiver.try_recv() {
            Ok(event) => print_event(&event),
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                break;
            }
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                eprintln!("[warn] skipped {skipped} events");
            }
        }
    }
}

/// Renders one structured event for the terminal.
fn print_event(event: &AgentEvent) {
    match event {
        AgentEvent::SessionStarted { session_id } => {
            println!("[session] started {session_id}");
        }
        AgentEvent::ModelDelta { text } => {
            print!("{text}");
            let _ = io::stdout().flush();
        }
        AgentEvent::ToolCallStarted { call } => {
            println!(
                "\n[tool] {} {}",
                call.name,
                summarize(&call.arguments.to_string(), 120)
            );
        }
        AgentEvent::ToolCallFinished { result } => {
            let status = if result.is_error { "error" } else { "ok" };
            println!("[tool:{status}] {}", summarize(&result.content, 200));
        }
        AgentEvent::PolicyDenied { call, reason } => {
            println!("[policy] denied {}: {reason}", call.name);
        }
        AgentEvent::BudgetExceeded { kind, message } => {
            println!("[budget] {kind:?}: {message}");
        }
        AgentEvent::AcceptanceFailed { attempt, feedback } => {
            println!(
                "[acceptance] attempt {attempt} failed: {}",
                summarize(feedback, 200)
            );
        }
        AgentEvent::Error { message } => {
            eprintln!("\n[error] {message}");
        }
        AgentEvent::SessionEnded { session_id, reason } => {
            println!("\n[session] ended {session_id}: {reason:?}");
        }
    }
}

/// Flattens a value to one line and truncates it to `limit` characters.
fn summarize(text: &str, limit: usize) -> String {
    let flattened = text.replace('\n', " ");
    if flattened.chars().count() <= limit {
        flattened
    } else {
        let mut truncated: String = flattened.chars().take(limit).collect();
        truncated.push('…');
        truncated
    }
}

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a process-local session id.
fn next_session_id() -> SessionId {
    let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    SessionId::new(format!("cli-{}-{counter}", std::process::id()))
}

fn exit_code(reason: SessionEndReason) -> ExitCode {
    match reason {
        SessionEndReason::Completed | SessionEndReason::Cancelled => ExitCode::SUCCESS,
        SessionEndReason::BudgetExceeded
        | SessionEndReason::AcceptanceFailed
        | SessionEndReason::Error => ExitCode::FAILURE,
    }
}

#[derive(Debug, Error)]
enum CliError {
    #[error("provider setup failed: {0}")]
    Provider(#[from] LlmError),
    #[error("failed to determine the current directory: {0}")]
    CurrentDir(#[source] io::Error),
    #[error("current-directory task failed: {0}")]
    CurrentDirTask(#[source] tokio::task::JoinError),
    #[error("failed to register the `{name}` tool: {source}")]
    RegisterTool {
        name: &'static str,
        #[source]
        source: ToolRegistryError,
    },
}
