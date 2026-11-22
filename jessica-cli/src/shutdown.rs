//! Process-level graceful shutdown for the CLI.
//!
//! The controller owns one root [`CancellationToken`] for the whole process and
//! hands every turn a child token. The signal listener is installed once at
//! startup (never per turn) and is the only component that observes SIGINT and,
//! on Unix, SIGTERM.
//!
//! # Shutdown sequence
//!
//! 1. The first signal writes its diagnostic line, then records its kind,
//!    cancels the root token (and therefore the active turn's child token), and
//!    wakes the REPL's `read_line`. Writing the line first prevents `main` from
//!    exiting with the signal code before the diagnostic reaches stderr.
//! 2. The REPL creates no further turns. The active turn observes its token and
//!    ends with [`SessionEndReason::Cancelled`](jessica_core::types::SessionEndReason::Cancelled).
//!    The existing turn path waits up to its pump timeout (30 seconds) for the
//!    terminal [`AgentEvent::SessionEnded`](jessica_core::types::AgentEvent::SessionEnded)
//!    to be persisted and then returns.
//! 3. The runtime drains the skill worker, bounded by
//!    `skills.drain_timeout_ms`; a drain timeout only warns.
//! 4. Main flushes stdout and exits with 130 for SIGINT or 143 for SIGTERM.
//!
//! A second signal forces an immediate exit with the second signal's code:
//! there is no further drain or flush beyond the warning written to stderr.
//!
//! # Known limitations
//!
//! - A cancelled session ends with `SessionEnded(Cancelled)`, which recovery
//!   treats as terminal: `--resume` reports the session as already ended.
//!   Recoverable shutdown (a checkpoint instead of a terminal cancellation)
//!   requires a separate task.
//! - `CommandSensor` and `CommandAcceptanceCheck` are bounded by
//!   `[tools.supervisor]`, but the first signal still waits for an in-flight
//!   command to reach its deadline before the terminal-event wait and skill
//!   drain can finish. Process-group cleanup and cancellation-aware command
//!   teardown require a separate task.
//! - Only SIGINT and SIGTERM are handled. SIGHUP and other signals keep their
//!   default disposition.

use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

/// Signal kinds handled by the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownSignal {
    /// Interactive interrupt (Ctrl+C).
    SigInt,
    /// Unix termination request.
    SigTerm,
}

impl ShutdownSignal {
    /// Shell-conventional process exit code for this signal.
    pub(crate) const fn exit_code(self) -> u8 {
        match self {
            Self::SigInt => 130,
            Self::SigTerm => 143,
        }
    }

    /// Human-readable signal name used in shutdown logs.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::SigInt => "SIGINT",
            Self::SigTerm => "SIGTERM",
        }
    }
}

/// Effect of delivering one signal to the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalAction {
    /// First signal: start the graceful shutdown and cancel the active turn.
    Cancel,
    /// Second signal: exit immediately instead of waiting for the drain.
    Force,
    /// Further signals after a forced shutdown: nothing left to do.
    Ignore,
}

/// Process-wide shutdown state.
///
/// Every turn calls [`child_token`](Self::child_token) before it starts work.
/// Cancelling the root token therefore reaches the active turn without
/// endpoint-specific signal plumbing inside the turn itself.
pub(crate) struct ShutdownController {
    /// Root token; per-turn tokens are children of this token.
    root: CancellationToken,
    /// Kind of the first signal, if one has been received.
    signal: Mutex<Option<ShutdownSignal>>,
    /// Whether an immediate forced exit was requested.
    forced: AtomicBool,
}

impl ShutdownController {
    /// Creates a controller that has not observed a signal.
    pub(crate) fn new() -> Self {
        Self {
            root: CancellationToken::new(),
            signal: Mutex::new(None),
            forced: AtomicBool::new(false),
        }
    }

    /// Creates a token for one turn.
    ///
    /// After the first signal the root is already cancelled, so any token
    /// created later starts cancelled and the turn cannot issue model requests.
    pub(crate) fn child_token(&self) -> CancellationToken {
        self.root.child_token()
    }

    /// Resolves once shutdown has been requested.
    ///
    /// The REPL selects on this future while waiting for input so a signal
    /// wakes `read_line` instead of waiting for the next line or EOF.
    pub(crate) fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.root.cancelled()
    }

    /// Records `signal` and cancels every turn token.
    ///
    /// Returns `true` only for the first request. Later requests leave the
    /// recorded signal unchanged, so the first signal determines the exit code.
    pub(crate) fn request_shutdown(&self, signal: ShutdownSignal) -> bool {
        {
            let mut recorded = self.lock_signal();
            if recorded.is_some() {
                return false;
            }
            *recorded = Some(signal);
        }
        self.root.cancel();
        true
    }

    /// Marks the shutdown as forced and cancels every remaining turn token.
    ///
    /// Returns `true` the first time it is called.
    pub(crate) fn force(&self) -> bool {
        let first = !self.forced.swap(true, Ordering::SeqCst);
        self.root.cancel();
        first
    }

    /// Classifies the next signal without changing any shutdown state.
    ///
    /// The first signal cancels the active turn, the second forces an immediate
    /// exit, and later signals are ignored. Callers must serialize signal
    /// delivery; the listener processes one signal at a time.
    ///
    /// The caller must write its diagnostic line before committing the action
    /// with [`request_shutdown`](Self::request_shutdown) or [`force`](Self::force).
    /// Committing the first signal cancels the root token, which can wake the
    /// REPL and let `main` exit with the signal code; a message written after
    /// that point has no guarantee of reaching stderr.
    pub(crate) fn classify_next(&self) -> SignalAction {
        if self.lock_signal().is_none() {
            SignalAction::Cancel
        } else if !self.forced.load(Ordering::SeqCst) {
            SignalAction::Force
        } else {
            SignalAction::Ignore
        }
    }

    /// Returns whether the first signal has been received.
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.lock_signal().is_some()
    }

    /// Returns whether an immediate forced exit was requested.
    #[cfg(test)]
    pub(crate) fn is_forced(&self) -> bool {
        self.forced.load(Ordering::SeqCst)
    }

    /// Returns the first recorded signal, if any.
    pub(crate) fn signal(&self) -> Option<ShutdownSignal> {
        *self.lock_signal()
    }

    /// Locks the signal slot, recovering from a poisoned mutex.
    fn lock_signal(&self) -> std::sync::MutexGuard<'_, Option<ShutdownSignal>> {
        self.signal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Owns the background task that translates OS signals into controller actions.
///
/// Dropping the listener aborts the task on normal process exit. A forced exit
/// happens inside the task itself, so it does not depend on this cleanup.
pub(crate) struct SignalListener {
    /// Background signal loop.
    task: JoinHandle<()>,
}

impl SignalListener {
    /// Installs SIGINT (and SIGTERM on Unix) for the lifetime of the process.
    ///
    /// Handlers are registered synchronously before this function returns, so
    /// a caller that observes startup side effects (for example the event
    /// database being created) can safely send a signal afterwards.
    pub(crate) fn spawn(controller: std::sync::Arc<ShutdownController>) -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let interrupt = signal(SignalKind::interrupt());
            let terminate = signal(SignalKind::terminate());
            match (interrupt, terminate) {
                (Ok(mut interrupt), Ok(mut terminate)) => Self {
                    task: tokio::spawn(async move {
                        loop {
                            let signal = tokio::select! {
                                _ = interrupt.recv() => ShutdownSignal::SigInt,
                                _ = terminate.recv() => ShutdownSignal::SigTerm,
                            };
                            handle_signal(&controller, signal);
                        }
                    }),
                },
                (Err(error), _) | (_, Err(error)) => {
                    eprintln!("[warn] failed to install signal handlers: {error}");
                    Self {
                        task: tokio::spawn(async {}),
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            Self {
                task: tokio::spawn(async move {
                    loop {
                        if tokio::signal::ctrl_c().await.is_err() {
                            break;
                        }
                        handle_signal(&controller, ShutdownSignal::SigInt);
                    }
                }),
            }
        }
    }
}

impl Drop for SignalListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Applies one OS signal; a forced exit terminates the process from here.
///
/// The diagnostic line is written before committing the state change. The first
/// signal cancels the root token, which can wake the REPL and let `main` exit
/// with the signal code before a later write would run; writing first keeps the
/// message from being lost to that race.
fn handle_signal(controller: &ShutdownController, signal: ShutdownSignal) {
    match controller.classify_next() {
        SignalAction::Cancel => {
            eprintln!(
                "[shutdown] {} received; stopping new turns and draining",
                signal.as_str()
            );
            let _ = std::io::stderr().flush();
            let _ = controller.request_shutdown(signal);
        }
        SignalAction::Force => {
            eprintln!(
                "[shutdown] second {} received; exiting immediately",
                signal.as_str()
            );
            let _ = std::io::stderr().flush();
            let _ = controller.force();
            let _ = std::io::stdout().flush();
            std::process::exit(i32::from(signal.exit_code()));
        }
        SignalAction::Ignore => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_signal_cancels_the_active_turns_child_token() {
        let controller = ShutdownController::new();
        let turn = controller.child_token();

        assert_eq!(controller.classify_next(), SignalAction::Cancel);
        assert!(
            !controller.is_shutting_down() && !turn.is_cancelled(),
            "classification alone must not make shutdown observable"
        );
        assert!(controller.request_shutdown(ShutdownSignal::SigInt));

        assert!(turn.is_cancelled());
        assert!(controller.is_shutting_down());
        assert_eq!(controller.signal(), Some(ShutdownSignal::SigInt));
        assert!(!controller.is_forced());
    }

    #[test]
    fn the_second_signal_forces_and_later_signals_are_ignored() {
        let controller = ShutdownController::new();
        assert!(controller.request_shutdown(ShutdownSignal::SigTerm));

        assert_eq!(controller.classify_next(), SignalAction::Force);
        assert!(
            !controller.is_forced(),
            "classification alone must not force an exit"
        );
        assert!(controller.force());

        assert_eq!(controller.classify_next(), SignalAction::Ignore);
        assert!(controller.is_forced());
        assert_eq!(controller.signal(), Some(ShutdownSignal::SigTerm));
    }

    #[test]
    fn a_turn_token_created_after_shutdown_starts_cancelled() {
        let controller = ShutdownController::new();
        assert!(controller.request_shutdown(ShutdownSignal::SigTerm));

        let late_turn = controller.child_token();

        assert!(
            late_turn.is_cancelled(),
            "no new turn may issue model requests after shutdown"
        );
    }

    #[test]
    fn turn_tokens_are_isolated_from_each_other() {
        let controller = ShutdownController::new();
        let first = controller.child_token();
        let second = controller.child_token();

        first.cancel();

        assert!(first.is_cancelled());
        assert!(!second.is_cancelled());
        assert!(!controller.is_shutting_down());
    }

    #[test]
    fn signal_exit_codes_follow_the_shell_convention() {
        assert_eq!(ShutdownSignal::SigInt.exit_code(), 130);
        assert_eq!(ShutdownSignal::SigTerm.exit_code(), 143);
        assert_eq!(ShutdownSignal::SigInt.as_str(), "SIGINT");
        assert_eq!(ShutdownSignal::SigTerm.as_str(), "SIGTERM");
    }
}
