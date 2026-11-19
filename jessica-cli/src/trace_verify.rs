//! Read-only verification of recorded turn traces.
//!
//! The verifier answers a narrow question: can the stored evidence be trusted?
//! It opens an existing event store without creating or migrating it and checks
//! three layers:
//!
//! 1. structural completeness, by reusing [`EventStore::trace_turn`]'s stored-row
//!    validation;
//! 2. version-2 semantics, including attempt/session attribution, round
//!    lifecycle, permission resolution, and child-session pairing;
//! 3. trace-to-legacy mirror consistency, by matching every mirrored agent event
//!    against the legacy event stream as a multiset.
//!
//! Verification never executes a replay unless `--replay` is requested. That
//! opt-in layer drives the deterministic replay engine over the trace's own
//! bootstrap and runtime events; see [`crate::trace_replay`]. Without it, this
//! module only proves that the events a trace claims to have recorded exist in
//! the legacy log with identical timestamps and payloads.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use jessica_core::trace::{TraceEvent, TracePayload, TraceTurnMode, TurnId};
use jessica_core::types::{AgentEvent, PermissionDecision, SessionEndReason, SessionId};
use jessica_memory::{EventStore, EventStoreError, StoredTraceTurn, Timestamp};
use serde::Serialize;

use crate::app::CliError;
use crate::config::HarnessConfig;
use crate::trace_replay::{self, ReplayOutcome, ReplayStatus, ReplayVerification};

/// Version of the JSON report structure.
const REPORT_VERSION: u32 = 3;

/// A parsed `trace` subcommand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TraceCommand {
    /// Verifies stored turn traces.
    Verify {
        /// Turn selection.
        selection: TraceSelection,
        /// Whether to render the report as JSON.
        json: bool,
        /// Whether open traces are accepted as verified.
        allow_open: bool,
        /// Whether to check execution parity with the replay engine.
        replay: bool,
        /// Optional externally recorded chain digest to require.
        expect: Option<String>,
    },
    /// Prints verified chain digests.
    Digest {
        /// Turn selection.
        selection: TraceSelection,
        /// Whether to render the report as JSON.
        json: bool,
    },
}

/// Which turns a verification run covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TraceSelection {
    /// Exactly one turn id.
    Turn(String),
    /// Every stored turn.
    All,
}

/// Runs one `trace verify` invocation.
///
/// Only the configured event store is opened, read-only. No provider, wasm
/// module, memory database, skill database, runtime, or configuration watcher is
/// constructed.
pub(crate) async fn run_command(
    config: &HarnessConfig,
    root: &Path,
    command: TraceCommand,
) -> Result<(), CliError> {
    match command {
        TraceCommand::Verify {
            selection,
            json,
            allow_open,
            replay,
            expect,
        } => run_verify(config, root, selection, json, allow_open, replay, expect).await,
        TraceCommand::Digest { selection, json } => {
            crate::trace_digest::run_command(config, root, selection, json).await
        }
    }
}

/// Resolves the configured event store path against the workspace root.
pub(crate) fn event_store_path(config: &HarnessConfig, root: &Path) -> Result<PathBuf, CliError> {
    let Some(configured_path) = config.events.path.as_ref() else {
        return Err(CliError::TraceRequiresEventStore);
    };
    Ok(if configured_path.is_absolute() {
        configured_path.clone()
    } else {
        root.join(configured_path)
    })
}

/// Runs one `trace verify` invocation.
async fn run_verify(
    config: &HarnessConfig,
    root: &Path,
    selection: TraceSelection,
    json: bool,
    allow_open: bool,
    replay: bool,
    expect: Option<String>,
) -> Result<(), CliError> {
    if replay && !matches!(selection, TraceSelection::Turn(_)) {
        return Err(CliError::InvalidTraceCommand {
            message: "`--replay` requires `--turn`".to_owned(),
        });
    }

    let database = event_store_path(config, root)?;
    let allow_open_hint = matches!(selection, TraceSelection::All);

    let BuiltReport {
        mut report,
        replay_entries,
    } = tokio::task::spawn_blocking(move || build_report(&database, &selection))
        .await
        .map_err(CliError::TraceTask)??;

    if let Some(expected) = expect.as_deref() {
        {
            let Some(turn) = report.turns.first_mut() else {
                return Err(CliError::TraceVerificationFailed {
                    summary: "the single-turn report is missing its turn".to_owned(),
                });
            };
            match turn.digest.as_deref() {
                Some(actual) if actual == expected => {}
                Some(actual) => {
                    turn.problems.push(TraceProblem::new(
                        None,
                        "digest_mismatch",
                        format!("chain digest mismatch: expected {expected}, found {actual}"),
                    ));
                    turn.status = TraceStatus::Invalid;
                }
                None => {
                    turn.problems.push(TraceProblem::new(
                        None,
                        "digest_unavailable",
                        "cannot check --expect because the trace chain is invalid or unavailable"
                            .to_owned(),
                    ));
                    turn.status = TraceStatus::Invalid;
                }
            }
        }
        report = TraceVerifyReport::new(report.turns);
    }

    let mut unsupported = None;
    if replay {
        let Some(turn) = report.turns.first_mut() else {
            return Err(CliError::TraceVerificationFailed {
                summary: "the single-turn report is missing its turn".to_owned(),
            });
        };
        let outcome = if turn.status == TraceStatus::Ok {
            match replay_entries {
                Some(entries) => trace_replay::verify(&entries).await,
                None => {
                    ReplayOutcome::unsupported("the trace is not structurally valid".to_owned())
                }
            }
        } else {
            let reason = match turn.status {
                TraceStatus::Open => "the trace is not closed; `turn_ended` is missing",
                TraceStatus::Ok | TraceStatus::Invalid => {
                    "the trace is not valid; fix the reported findings first"
                }
            };
            ReplayOutcome::unsupported(reason.to_owned())
        };

        match outcome.verification.status {
            ReplayStatus::Mismatch => {
                for problem in &outcome.problems {
                    turn.problems.push(TraceProblem::new(
                        problem.seq,
                        problem.kind,
                        problem.message.clone(),
                    ));
                }
                turn.status = TraceStatus::Invalid;
            }
            ReplayStatus::Unsupported => {
                unsupported = Some((
                    turn.turn_id.clone(),
                    outcome
                        .verification
                        .reason
                        .clone()
                        .unwrap_or_else(|| "replay is not supported for this trace".to_owned()),
                ));
            }
            ReplayStatus::Ok => {}
        }
        turn.replay = Some(outcome.verification);
        report = TraceVerifyReport::new(report.turns);
    }

    if json {
        let rendered = serde_json::to_string_pretty(&report).map_err(CliError::TraceReport)?;
        println!("{rendered}");
    } else {
        print_text(&report);
    }

    if !report.all_verified(allow_open) {
        let mut summary = report.summary.line();
        if allow_open_hint && !allow_open && report.summary.open > 0 && report.summary.invalid == 0
        {
            summary.push_str("; rerun with --allow-open to accept open traces");
        }
        return Err(CliError::TraceVerificationFailed { summary });
    }

    if let Some((turn_id, reason)) = unsupported {
        return Err(CliError::TraceReplayUnsupported { turn_id, reason });
    }

    Ok(())
}

/// A stored trace as loaded for verification.
enum LoadedTrace {
    /// Structural validation succeeded; the trace is complete or open.
    Complete(StoredTraceTurn),
    /// Stored rows violate the trace contract.
    Structural {
        /// Error rendered for the report.
        message: String,
    },
}

/// Verification report plus the entries a single-turn replay might need.
struct BuiltReport {
    /// Completed verification report.
    report: TraceVerifyReport,
    /// Raw entries for `--replay`; present only for a single loaded turn.
    replay_entries: Option<Vec<(u64, TraceEvent)>>,
}

/// Opens the store read-only and verifies the selected turns.
fn build_report(database: &Path, selection: &TraceSelection) -> Result<BuiltReport, CliError> {
    let store = EventStore::open_read_only(database)?;

    match selection {
        TraceSelection::Turn(turn_id) => {
            let requested = TurnId::new(turn_id.clone());
            let (verification, replay_entries) = match load_trace(&store, &requested)? {
                None => {
                    return Err(CliError::UnknownTurn {
                        turn_id: turn_id.clone(),
                    });
                }
                Some(LoadedTrace::Complete(trace)) => verify_stored_turn(&store, &trace),
                Some(LoadedTrace::Structural { message }) => {
                    (structural_failure(turn_id.clone(), message), Vec::new())
                }
            };
            Ok(BuiltReport {
                report: TraceVerifyReport::new(vec![verification]),
                replay_entries: Some(replay_entries),
            })
        }
        TraceSelection::All => {
            let summaries = store.trace_turns(usize::MAX)?;
            let mut turns = Vec::with_capacity(summaries.len());
            for summary in summaries {
                let turn_id = summary.turn_id().to_string();
                match load_trace(&store, summary.turn_id())? {
                    Some(LoadedTrace::Complete(trace)) => {
                        turns.push(verify_stored_turn(&store, &trace).0);
                    }
                    Some(LoadedTrace::Structural { message }) => {
                        turns.push(structural_failure(turn_id, message));
                    }
                    None => turns.push(structural_failure(
                        turn_id,
                        "the turn head disappeared during verification".to_owned(),
                    )),
                }
            }
            Ok(BuiltReport {
                report: TraceVerifyReport::new(turns),
                replay_entries: None,
            })
        }
    }
}

/// Loads one turn, separating structural corruption from storage failure.
///
/// A corrupt trace is reportable evidence and does not stop an `--all` run.
/// A SQLite failure is a storage error and aborts instead of being presented as
/// a verified-or-invalid trace.
fn load_trace(
    store: &EventStore,
    turn_id: &TurnId,
) -> Result<Option<LoadedTrace>, EventStoreError> {
    match store.trace_turn(turn_id) {
        Ok(Some(trace)) => Ok(Some(LoadedTrace::Complete(trace))),
        Ok(None) => Ok(None),
        Err(error) if is_structural_trace_error(&error) => Ok(Some(LoadedTrace::Structural {
            message: error.to_string(),
        })),
        Err(error) => Err(error),
    }
}

/// Returns whether an error reports corrupt trace evidence rather than storage.
pub(crate) fn is_structural_trace_error(error: &EventStoreError) -> bool {
    matches!(
        error,
        EventStoreError::TraceSequence { .. }
            | EventStoreError::TraceLifecycle { .. }
            | EventStoreError::TraceInvalid { .. }
            | EventStoreError::TraceDecode { .. }
    )
}

/// Builds an invalid report entry for a trace that could not be decoded.
fn structural_failure(turn_id: String, message: String) -> TurnVerification {
    TurnVerification {
        turn_id,
        schema_version: None,
        entries: None,
        digest: None,
        status: TraceStatus::Invalid,
        problems: vec![TraceProblem::new(None, "trace_structure", message)],
        replay: None,
    }
}

/// Verifies one structurally valid stored trace.
///
/// Returns the verification and the raw entries so an opt-in replay can use
/// the trace as its own source of truth.
fn verify_stored_turn(
    store: &EventStore,
    trace: &StoredTraceTurn,
) -> (TurnVerification, Vec<(u64, TraceEvent)>) {
    let entries: Vec<Entry> = trace
        .events()
        .iter()
        .map(|stored| Entry {
            seq: stored.seq(),
            timestamp: stored.timestamp(),
            event: stored.event().clone(),
        })
        .collect();
    let turn_id = trace.turn_id().to_string();

    let mut initial_problems = Vec::new();
    let legacy = match load_legacy_index(store, &entries) {
        Ok(index) => Some(index),
        Err(message) => {
            initial_problems.push(TraceProblem::new(
                None,
                "legacy_unreadable",
                format!("failed to read the legacy event stream: {message}"),
            ));
            None
        }
    };

    let mut verification =
        verify_entries(&turn_id, trace.closed(), &entries, legacy, initial_problems);
    verification.digest = Some(trace.digest().to_owned());
    let replay_entries = entries
        .into_iter()
        .map(|entry| (entry.seq, entry.event))
        .collect();
    (verification, replay_entries)
}

/// Loads the legacy rows needed by the trace-to-legacy mirror check.
fn load_legacy_index(store: &EventStore, entries: &[Entry]) -> Result<LegacyIndex, String> {
    let mut sessions = BTreeSet::new();
    for entry in entries {
        let TracePayload::Agent { event } = &entry.event.payload else {
            continue;
        };
        if is_trace_only(event) {
            continue;
        }
        let Some(session) = entry.event.session_id.as_ref() else {
            continue;
        };
        sessions.insert(session.clone());
    }

    let mut index = LegacyIndex::new();
    for session in sessions {
        let stored = store.events(&session).map_err(|error| error.to_string())?;
        for row in stored {
            let key = (
                session.clone(),
                row.timestamp(),
                row.payload_json().to_owned(),
            );
            *index.entry(key).or_insert(0) += 1;
        }
    }
    Ok(index)
}

/// One trace entry used by the pure verifier.
#[derive(Debug, Clone)]
struct Entry {
    /// Stored one-based sequence number.
    seq: u64,
    /// Stored timestamp, shared by the legacy mirror row.
    timestamp: Timestamp,
    /// Decoded envelope.
    event: TraceEvent,
}

/// Multiset of legacy `(session, timestamp, payload)` rows.
type LegacyIndex = BTreeMap<(SessionId, Timestamp, String), usize>;

/// Attempt and session that own one agent event.
type Attribution = (u32, SessionId);

/// Result of verifying one turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TurnVerification {
    /// Turn that was verified.
    turn_id: String,
    /// Trace schema version, or `None` when stored rows are corrupt.
    schema_version: Option<u16>,
    /// Number of trace entries, or `None` when stored rows are corrupt.
    entries: Option<u64>,
    /// Lowercase hex chain digest, or `None` when the chain is unavailable.
    digest: Option<String>,
    /// Verification status.
    status: TraceStatus,
    /// Findings that make an invalid trace invalid.
    problems: Vec<TraceProblem>,
    /// Execution-parity result, present only when `--replay` was requested.
    replay: Option<ReplayVerification>,
}

/// Status of one turn trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TraceStatus {
    /// The trace is closed and every check passed.
    Ok,
    /// The trace has no `turn_ended` marker but no findings.
    Open,
    /// The trace failed a structural, semantic, or mirror check.
    Invalid,
}

impl TraceStatus {
    /// Returns the stable text label.
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Open => "open",
            Self::Invalid => "invalid",
        }
    }
}

/// One verification finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TraceProblem {
    /// Trace sequence number, or `None` for whole-turn findings.
    seq: Option<u64>,
    /// Stable machine-readable problem kind.
    kind: &'static str,
    /// Human-readable explanation.
    message: String,
}

impl TraceProblem {
    /// Creates a finding.
    fn new(seq: Option<u64>, kind: &'static str, message: String) -> Self {
        Self { seq, kind, message }
    }
}

/// Counts over one verification report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct TraceSummary {
    /// Number of turns checked.
    turns: usize,
    /// Number of verified closed traces.
    ok: usize,
    /// Number of traces without `turn_ended`.
    open: usize,
    /// Number of invalid traces.
    invalid: usize,
    /// Total findings across invalid traces.
    problems: usize,
}

impl TraceSummary {
    /// Renders the one-line summary used by text output and errors.
    fn line(self) -> String {
        format!(
            "{} turns: {} ok, {} open, {} invalid, {} problems",
            self.turns, self.ok, self.open, self.invalid, self.problems
        )
    }
}

/// Stable JSON shape of one verification run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TraceVerifyReport {
    /// Report structure version.
    report_version: u32,
    /// Per-turn results.
    turns: Vec<TurnVerification>,
    /// Aggregate counts.
    summary: TraceSummary,
}

impl TraceVerifyReport {
    /// Builds a report and its summary.
    fn new(turns: Vec<TurnVerification>) -> Self {
        let mut summary = TraceSummary {
            turns: turns.len(),
            ok: 0,
            open: 0,
            invalid: 0,
            problems: 0,
        };
        for turn in &turns {
            match turn.status {
                TraceStatus::Ok => summary.ok += 1,
                TraceStatus::Open => summary.open += 1,
                TraceStatus::Invalid => {
                    summary.invalid += 1;
                    summary.problems += turn.problems.len();
                }
            }
        }
        Self {
            report_version: REPORT_VERSION,
            turns,
            summary,
        }
    }

    /// Returns whether every selected trace is acceptable.
    fn all_verified(&self, allow_open: bool) -> bool {
        self.turns.iter().all(|turn| match turn.status {
            TraceStatus::Ok => true,
            TraceStatus::Open => allow_open,
            TraceStatus::Invalid => false,
        })
    }
}

/// Renders one report as text, one line per turn plus findings and a summary.
fn print_text(report: &TraceVerifyReport) {
    for turn in &report.turns {
        let schema = turn
            .schema_version
            .map(|version| version.to_string())
            .unwrap_or_else(|| "?".to_owned());
        let entries = turn
            .entries
            .map(|count| count.to_string())
            .unwrap_or_else(|| "?".to_owned());
        println!(
            "{:<7} {} schema={schema} entries={entries} problems={}",
            turn.status.as_str(),
            turn.turn_id,
            turn.problems.len()
        );
        if let Some(digest) = &turn.digest {
            println!("  digest {digest}");
        }
        for problem in &turn.problems {
            match problem.seq {
                Some(seq) => println!("  seq {seq} [{}] {}", problem.kind, problem.message),
                None => println!("  [{}] {}", problem.kind, problem.message),
            }
        }
        if let Some(replay) = &turn.replay {
            match (&replay.status, &replay.reason) {
                (ReplayStatus::Unsupported, Some(reason)) => {
                    println!("  replay: unsupported ({reason})");
                }
                _ => println!(
                    "  replay: {} attempts={} events={}",
                    replay.status.as_str(),
                    replay.attempts,
                    replay.events
                ),
            }
        }
    }
    println!("summary: {}", report.summary.line());
}

/// Runs the pure verification layers over one trace.
fn verify_entries(
    turn_id: &str,
    closed: bool,
    entries: &[Entry],
    legacy: Option<LegacyIndex>,
    initial_problems: Vec<TraceProblem>,
) -> TurnVerification {
    let schema_version = entries.first().map(|entry| entry.event.schema_version);
    let turn_mode = entries.iter().find_map(|entry| match &entry.event.payload {
        TracePayload::TurnStarted { mode, .. } => Some(*mode),
        _ => None,
    });
    let entry_count = u64::try_from(entries.len()).unwrap_or(u64::MAX);

    let mut verifier = Verifier::new(turn_mode, legacy, initial_problems);
    verifier.run(entries);

    let status = if verifier.problems.is_empty() {
        if closed {
            TraceStatus::Ok
        } else {
            TraceStatus::Open
        }
    } else {
        TraceStatus::Invalid
    };

    TurnVerification {
        turn_id: turn_id.to_owned(),
        schema_version,
        entries: Some(entry_count),
        digest: None,
        status,
        problems: verifier.problems,
        replay: None,
    }
}

/// Mutable state for the semantic and mirror checks.
struct Verifier {
    /// Turn mode from `turn_started`, when present.
    turn_mode: Option<TraceTurnMode>,
    /// Remaining legacy rows for multiset matching.
    legacy: Option<LegacyIndex>,
    /// Findings accumulated so far.
    problems: Vec<TraceProblem>,
    /// Round lifecycle per `(attempt, session)`.
    rounds: BTreeMap<Attribution, RoundState>,
    /// Permission decisions per `(attempt, session)`.
    permissions: BTreeMap<Attribution, Vec<PermissionRecord>>,
    /// Finished tool-call ids per `(attempt, session)`.
    finished_calls: BTreeMap<Attribution, BTreeSet<String>>,
    /// Child-session lifecycle keyed by `(child_session_id, call_id)`.
    children: BTreeMap<(SessionId, String), ChildRecord>,
    /// Parents whose open children were already checked at session end.
    children_checked_sessions: BTreeSet<Attribution>,
}

impl Verifier {
    /// Creates an empty verifier.
    fn new(
        turn_mode: Option<TraceTurnMode>,
        legacy: Option<LegacyIndex>,
        problems: Vec<TraceProblem>,
    ) -> Self {
        Self {
            turn_mode,
            legacy,
            problems,
            rounds: BTreeMap::new(),
            permissions: BTreeMap::new(),
            finished_calls: BTreeMap::new(),
            children: BTreeMap::new(),
            children_checked_sessions: BTreeSet::new(),
        }
    }

    /// Processes every entry in stored sequence order.
    fn run(&mut self, entries: &[Entry]) {
        let mut terminal_reason = None;
        for entry in entries {
            match &entry.event.payload {
                TracePayload::Gap { skipped, reason } => {
                    self.problems.push(TraceProblem::new(
                        Some(entry.seq),
                        "trace_gap",
                        format!(
                            "the recorder lost {skipped} events before this entry ({reason:?})"
                        ),
                    ));
                }
                TracePayload::TurnStarted { .. } => {}
                TracePayload::TurnEnded { reason } => {
                    terminal_reason = Some(*reason);
                    self.check_children_at_turn_end(entry.seq, *reason);
                }
                TracePayload::Agent { event } => self.on_agent_event(entry, event),
            }
        }

        if terminal_reason == Some(SessionEndReason::Completed) {
            let remaining = self.permissions.keys().cloned().collect::<Vec<_>>();
            for key in remaining {
                self.check_permission_resolution(&key);
            }
        }
    }

    /// Processes one agent event.
    fn on_agent_event(&mut self, entry: &Entry, event: &AgentEvent) {
        self.check_legacy_mirror(entry, event);

        match event {
            AgentEvent::SessionStarted { session_id, .. }
            | AgentEvent::SessionEnded { session_id, .. } => {
                self.check_session_identity(entry, session_id);
            }
            _ => {}
        }

        if entry.event.schema_version < 2 {
            // Version-1 traces predate explicit rounds, permissions, and child
            // sessions. Their absence is expected and must not be reported.
            return;
        }

        match event {
            AgentEvent::RoundStarted { round } => {
                self.on_round_started(entry.seq, entry, *round);
            }
            AgentEvent::RoundFinished { round, .. } => {
                self.on_round_finished(entry.seq, entry, *round);
            }
            AgentEvent::ModelDelta { .. } => {
                self.on_model_delta(entry.seq, entry);
            }
            AgentEvent::ToolCallStarted { .. } => {
                self.on_tool_call_started(entry.seq, entry);
            }
            AgentEvent::ToolCallFinished { result } => {
                if let Some(key) = attribution(&entry.event) {
                    self.finished_calls
                        .entry(key)
                        .or_default()
                        .insert(result.tool_call_id.clone());
                }
            }
            AgentEvent::SessionEnded { reason, .. } => {
                self.on_session_ended(entry.seq, entry, *reason);
            }
            AgentEvent::PermissionDecided { call, decision } => {
                self.on_permission_decided(entry.seq, entry, &call.id, *decision);
            }
            AgentEvent::ChildSessionStarted {
                child_session_id,
                call_id,
            } => {
                self.on_child_started(entry.seq, entry, child_session_id, call_id);
            }
            AgentEvent::ChildSessionEnded {
                child_session_id,
                call_id,
                tool_calls,
                tool_errors,
                ..
            } => {
                self.on_child_ended(
                    entry.seq,
                    entry,
                    child_session_id,
                    call_id,
                    *tool_calls,
                    *tool_errors,
                );
            }
            AgentEvent::SessionStarted { .. }
            | AgentEvent::PolicyDenied { .. }
            | AgentEvent::BudgetExceeded { .. }
            | AgentEvent::AcceptanceFailed { .. }
            | AgentEvent::Error { .. } => {}
        }
    }

    /// Reports missing, duplicate, or unpaired legacy rows.
    fn check_legacy_mirror(&mut self, entry: &Entry, event: &AgentEvent) {
        if is_trace_only(event) {
            return;
        }
        let Some(session) = entry.event.session_id.as_ref() else {
            return;
        };
        let Some(legacy) = self.legacy.as_mut() else {
            return;
        };
        let payload = match serde_json::to_string(event) {
            Ok(payload) => payload,
            Err(error) => {
                self.problems.push(TraceProblem::new(
                    Some(entry.seq),
                    "mirror_serialize",
                    format!("failed to serialize an agent event for mirror matching: {error}"),
                ));
                return;
            }
        };
        let key = (session.clone(), entry.timestamp, payload);
        let matched = match legacy.get_mut(&key) {
            Some(count) if *count > 0 => {
                *count -= 1;
                true
            }
            _ => false,
        };
        if !matched {
            self.problems.push(TraceProblem::new(
                Some(entry.seq),
                "missing_legacy_mirror",
                format!(
                    "agent event `{}` for session `{session}` has no matching legacy row at timestamp {}",
                    agent_event_kind(event),
                    entry.timestamp
                ),
            ));
        }
    }

    /// Checks that a session lifecycle event agrees with its envelope.
    fn check_session_identity(&mut self, entry: &Entry, inner: &SessionId) {
        match entry.event.session_id.as_ref() {
            Some(envelope) if envelope == inner => {}
            Some(envelope) => self.problems.push(TraceProblem::new(
                Some(entry.seq),
                "session_mismatch",
                format!("session event names `{inner}` but the trace envelope names `{envelope}`"),
            )),
            None => self.problems.push(TraceProblem::new(
                Some(entry.seq),
                "session_mismatch",
                format!("session event names `{inner}` but the trace envelope names no session"),
            )),
        }
    }

    /// Validates one `round_started` event.
    fn on_round_started(&mut self, seq: u64, entry: &Entry, round: u32) {
        let Some(key) = self.require_attribution(seq, entry, "round_started") else {
            return;
        };
        if round == 0 {
            self.problems.push(TraceProblem::new(
                Some(seq),
                "invalid_round",
                "rounds are one-based; round zero cannot start".to_owned(),
            ));
            return;
        }

        let (open, last_started) = {
            let state = self.rounds.entry(key.clone()).or_default();
            (state.open, state.last_started)
        };
        if let Some(open) = open {
            self.problems.push(TraceProblem::new(
                Some(seq),
                "nested_round",
                format!("round {round} started while round {open} is still open"),
            ));
            return;
        }

        let expected = match last_started {
            Some(previous) => previous.checked_add(1),
            None => match self.turn_mode {
                Some(TraceTurnMode::Fresh) => Some(1),
                Some(TraceTurnMode::Resumed) | None => Some(round),
            },
        };
        if expected != Some(round) {
            let expected_text = expected
                .map(|value| value.to_string())
                .unwrap_or_else(|| "a representable round".to_owned());
            self.problems.push(TraceProblem::new(
                Some(seq),
                "round_sequence",
                format!("round {round} started but round {expected_text} was expected"),
            ));
        }

        let state = self.rounds.entry(key).or_default();
        state.open = Some(round);
        state.last_started = Some(round);
    }

    /// Validates one `round_finished` event.
    fn on_round_finished(&mut self, seq: u64, entry: &Entry, round: u32) {
        let Some(key) = self.require_attribution(seq, entry, "round_finished") else {
            return;
        };
        let problem = match self.rounds.get_mut(&key) {
            Some(state) => match state.open {
                Some(open) if open == round => {
                    state.open = None;
                    None
                }
                Some(open) => Some(format!(
                    "round {round} finished while round {open} is still open"
                )),
                None => Some(format!("round {round} finished without an open round")),
            },
            None => Some(format!("round {round} finished without an open round")),
        };
        if let Some(message) = problem {
            self.problems.push(TraceProblem::new(
                Some(seq),
                "unexpected_round_finish",
                message,
            ));
        }
    }

    /// Requires model output to belong to an open round.
    fn on_model_delta(&mut self, seq: u64, entry: &Entry) {
        let Some(key) = self.require_attribution(seq, entry, "model_delta") else {
            return;
        };
        let open = self.rounds.get(&key).and_then(|state| state.open);
        if open.is_none() {
            self.problems.push(TraceProblem::new(
                Some(seq),
                "model_delta_outside_round",
                "model output arrived outside an open round".to_owned(),
            ));
        }
    }

    /// Requires tool execution to start after the model round finished.
    fn on_tool_call_started(&mut self, seq: u64, entry: &Entry) {
        let Some(key) = self.require_attribution(seq, entry, "tool_call_started") else {
            return;
        };
        match self.rounds.get(&key) {
            Some(state) if state.open.is_some() => {
                let open = state.open.unwrap_or_default();
                self.problems.push(TraceProblem::new(
                    Some(seq),
                    "tool_call_before_round_finished",
                    format!("tool execution started before round {open} finished"),
                ));
            }
            Some(state) if state.last_started.is_some() => {}
            _ => self.problems.push(TraceProblem::new(
                Some(seq),
                "tool_call_without_round",
                "tool execution started without a preceding model round".to_owned(),
            )),
        }
    }

    /// Validates a session terminal event.
    fn on_session_ended(&mut self, seq: u64, entry: &Entry, reason: SessionEndReason) {
        let key = attribution(&entry.event);

        if reason == SessionEndReason::Completed {
            if let Some(key) = key.as_ref()
                && let Some(open) = self.rounds.get(key).and_then(|state| state.open)
            {
                self.problems.push(TraceProblem::new(
                    Some(seq),
                    "open_round_at_session_end",
                    format!("session ended as completed while round {open} is still open"),
                ));
            }
            if let Some(key) = key.as_ref() {
                self.check_permission_resolution(key);
            }
        } else if let Some(key) = key.as_ref() {
            // Cancellation, budget exhaustion, and errors may abort a call
            // after the permission decision but before its result is recorded.
            // The unresolved decision is expected, not a trace defect.
            self.permissions.remove(key);
        }

        if let Some(key) = key.as_ref()
            && matches!(
                reason,
                SessionEndReason::Completed | SessionEndReason::AcceptanceFailed
            )
        {
            self.children_checked_sessions.insert(key.clone());
            self.check_children_at_session_end(seq, key, reason);
        }
    }

    /// Records one permission decision and validates its call id.
    fn on_permission_decided(
        &mut self,
        seq: u64,
        entry: &Entry,
        call_id: &str,
        decision: PermissionDecision,
    ) {
        let Some(key) = self.require_attribution(seq, entry, "permission_decided") else {
            return;
        };
        if call_id.is_empty() {
            self.problems.push(TraceProblem::new(
                Some(seq),
                "permission_empty_call_id",
                "a permission decision has an empty tool call id".to_owned(),
            ));
            return;
        }
        self.permissions
            .entry(key)
            .or_default()
            .push(PermissionRecord {
                seq,
                call_id: call_id.to_owned(),
                decision,
            });
    }

    /// Requires Allow/Deny decisions to resolve on a normally completed session.
    fn check_permission_resolution(&mut self, key: &Attribution) {
        let records = self.permissions.remove(key).unwrap_or_default();
        let finished = self.finished_calls.get(key);
        for record in records {
            if !matches!(
                record.decision,
                PermissionDecision::Allow | PermissionDecision::Deny
            ) {
                continue;
            }
            if finished.is_some_and(|calls| calls.contains(&record.call_id)) {
                continue;
            }
            self.problems.push(TraceProblem::new(
                Some(record.seq),
                "unresolved_permission",
                format!(
                    "permission decision {:?} for tool call `{}` has no matching tool_call_finished",
                    record.decision, record.call_id
                ),
            ));
        }
    }

    /// Records a child-session start and rejects duplicates.
    fn on_child_started(
        &mut self,
        seq: u64,
        entry: &Entry,
        child_session_id: &SessionId,
        call_id: &str,
    ) {
        let Some(parent) = self.require_attribution(seq, entry, "child_session_started") else {
            return;
        };
        let pair = (child_session_id.clone(), call_id.to_owned());
        if self.children.contains_key(&pair) {
            self.problems.push(TraceProblem::new(
                Some(seq),
                "child_duplicate_start",
                format!(
                    "child session `{child_session_id}` for call `{call_id}` started more than once"
                ),
            ));
            return;
        }
        self.children
            .insert(pair, ChildRecord { parent, open: true });
    }

    /// Pairs a child-session end with its start.
    fn on_child_ended(
        &mut self,
        seq: u64,
        entry: &Entry,
        child_session_id: &SessionId,
        call_id: &str,
        tool_calls: u32,
        tool_errors: u32,
    ) {
        if self
            .require_attribution(seq, entry, "child_session_ended")
            .is_none()
        {
            return;
        }
        let pair = (child_session_id.clone(), call_id.to_owned());
        let problem = match self.children.get_mut(&pair) {
            None => Some((
                "child_end_without_start",
                format!(
                    "child session `{child_session_id}` for call `{call_id}` ended without a start"
                ),
            )),
            Some(record) if !record.open => Some((
                "child_end_without_start",
                format!(
                    "child session `{child_session_id}` for call `{call_id}` ended more than once"
                ),
            )),
            Some(record) => {
                record.open = false;
                if tool_errors > tool_calls {
                    Some((
                        "child_tool_counts",
                        format!(
                            "child session `{child_session_id}` reports {tool_errors} tool errors out of {tool_calls} tool calls"
                        ),
                    ))
                } else {
                    None
                }
            }
        };
        if let Some((kind, message)) = problem {
            self.problems
                .push(TraceProblem::new(Some(seq), kind, message));
        }
    }

    /// Requires children to be closed when their parent session completes.
    fn check_children_at_session_end(
        &mut self,
        seq: u64,
        key: &Attribution,
        reason: SessionEndReason,
    ) {
        let open = self
            .children
            .iter()
            .filter(|(_, record)| record.open && &record.parent == key)
            .map(|((child_session_id, call_id), _)| {
                format!(
                    "child session `{child_session_id}` for call `{call_id}` is still open when its parent session ended with `{reason:?}`"
                )
            })
            .collect::<Vec<_>>();
        for message in open {
            self.problems.push(TraceProblem::new(
                Some(seq),
                "child_open_at_session_end",
                message,
            ));
        }
    }

    /// Requires children to be closed when a completed turn closes.
    fn check_children_at_turn_end(&mut self, seq: u64, reason: SessionEndReason) {
        if !matches!(
            reason,
            SessionEndReason::Completed | SessionEndReason::AcceptanceFailed
        ) {
            return;
        }
        let open = self
            .children
            .iter()
            .filter(|(_, record)| {
                record.open && !self.children_checked_sessions.contains(&record.parent)
            })
            .map(|((child_session_id, call_id), _)| {
                format!(
                    "child session `{child_session_id}` for call `{call_id}` is still open after a `{reason:?}` turn"
                )
            })
            .collect::<Vec<_>>();
        for message in open {
            self.problems.push(TraceProblem::new(
                Some(seq),
                "child_open_at_turn_end",
                message,
            ));
        }
    }

    /// Requires both attribution fields and otherwise records a finding.
    fn require_attribution(&mut self, seq: u64, entry: &Entry, event: &str) -> Option<Attribution> {
        let attribution = attribution(&entry.event);
        if attribution.is_none() {
            self.problems.push(TraceProblem::new(
                Some(seq),
                "missing_attribution",
                format!("`{event}` requires both an attempt and a session id"),
            ));
        }
        attribution
    }
}

/// Round state for one `(attempt, session)`.
#[derive(Debug, Clone, Copy, Default)]
struct RoundState {
    /// Round whose `round_started` has no matching `round_finished`.
    open: Option<u32>,
    /// Most recently started round.
    last_started: Option<u32>,
}

/// One recorded permission decision awaiting resolution.
#[derive(Debug, Clone)]
struct PermissionRecord {
    /// Trace sequence of the decision.
    seq: u64,
    /// Provider tool-call id.
    call_id: String,
    /// Decision returned by the gate.
    decision: PermissionDecision,
}

/// Child-session lifecycle state.
#[derive(Debug, Clone)]
struct ChildRecord {
    /// Parent attempt/session that observed the child.
    parent: Attribution,
    /// Whether `child_session_ended` has not been observed.
    open: bool,
}

/// Returns the attempt/session attribution of an envelope.
fn attribution(trace: &TraceEvent) -> Option<Attribution> {
    match (trace.attempt, trace.session_id.as_ref()) {
        (Some(attempt), Some(session)) => Some((attempt, session.clone())),
        _ => None,
    }
}

/// Returns whether an agent event is intentionally absent from the legacy log.
fn is_trace_only(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::AcceptanceFailed { .. }
            | AgentEvent::PermissionDecided { .. }
            | AgentEvent::ChildSessionStarted { .. }
            | AgentEvent::ChildSessionEnded { .. }
    )
}

/// Returns the stable event kind used in mirror error messages.
fn agent_event_kind(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::SessionStarted { .. } => "session_started",
        AgentEvent::ModelDelta { .. } => "model_delta",
        AgentEvent::ToolCallStarted { .. } => "tool_call_started",
        AgentEvent::ToolCallFinished { .. } => "tool_call_finished",
        AgentEvent::PolicyDenied { .. } => "policy_denied",
        AgentEvent::BudgetExceeded { .. } => "budget_exceeded",
        AgentEvent::AcceptanceFailed { .. } => "acceptance_failed",
        AgentEvent::Error { .. } => "error",
        AgentEvent::SessionEnded { .. } => "session_ended",
        AgentEvent::RoundStarted { .. } => "round_started",
        AgentEvent::RoundFinished { .. } => "round_finished",
        AgentEvent::PermissionDecided { .. } => "permission_decided",
        AgentEvent::ChildSessionStarted { .. } => "child_session_started",
        AgentEvent::ChildSessionEnded { .. } => "child_session_ended",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use jessica_core::trace::TraceGapReason;
    use jessica_core::types::{Message, ToolCall, ToolResult};
    use serde_json::json;

    fn at(seq: u64) -> Timestamp {
        Timestamp::from_millis(i64::try_from(seq).expect("test sequence fits in i64"))
    }

    fn started(seq: u64, mode: TraceTurnMode) -> Entry {
        Entry {
            seq,
            timestamp: at(seq),
            event: TraceEvent::new(
                TurnId::new("turn"),
                None,
                Some(SessionId::new("outer")),
                TracePayload::TurnStarted {
                    mode,
                    messages: vec![Message::user("fix it")],
                },
            ),
        }
    }

    fn ended(seq: u64, reason: SessionEndReason) -> Entry {
        Entry {
            seq,
            timestamp: at(seq),
            event: TraceEvent::new(
                TurnId::new("turn"),
                None,
                None,
                TracePayload::TurnEnded { reason },
            ),
        }
    }

    fn gap(seq: u64) -> Entry {
        Entry {
            seq,
            timestamp: at(seq),
            event: TraceEvent::new(
                TurnId::new("turn"),
                None,
                None,
                TracePayload::Gap {
                    skipped: 2,
                    reason: TraceGapReason::BroadcastLagged,
                },
            ),
        }
    }

    fn agent(
        seq: u64,
        attempt: Option<u32>,
        envelope_session: Option<&str>,
        event: AgentEvent,
    ) -> Entry {
        Entry {
            seq,
            timestamp: at(seq),
            event: TraceEvent::new(
                TurnId::new("turn"),
                attempt,
                envelope_session.map(SessionId::new),
                TracePayload::Agent { event },
            ),
        }
    }

    fn attributed(seq: u64, event: AgentEvent) -> Entry {
        agent(seq, Some(1), Some("outer"), event)
    }

    fn session_started(seq: u64) -> Entry {
        attributed(
            seq,
            AgentEvent::SessionStarted {
                session_id: SessionId::new("outer"),
                messages: Vec::new(),
            },
        )
    }

    fn session_ended(seq: u64, reason: SessionEndReason) -> Entry {
        attributed(
            seq,
            AgentEvent::SessionEnded {
                session_id: SessionId::new("outer"),
                reason,
            },
        )
    }

    fn round_started(seq: u64, round: u32) -> Entry {
        attributed(seq, AgentEvent::RoundStarted { round })
    }

    fn round_finished(seq: u64, round: u32) -> Entry {
        attributed(seq, AgentEvent::RoundFinished { round, usage: None })
    }

    fn model_delta(seq: u64) -> Entry {
        attributed(
            seq,
            AgentEvent::ModelDelta {
                text: "hello".to_owned(),
            },
        )
    }

    fn tool_call(id: &str) -> ToolCall {
        ToolCall::new(id, "read_file", json!({"path": "README.md"}))
    }

    fn tool_started(seq: u64, id: &str) -> Entry {
        attributed(
            seq,
            AgentEvent::ToolCallStarted {
                call: tool_call(id),
            },
        )
    }

    fn tool_finished(seq: u64, id: &str) -> Entry {
        attributed(
            seq,
            AgentEvent::ToolCallFinished {
                result: ToolResult::success(id, "ok"),
            },
        )
    }

    fn permission(seq: u64, id: &str, decision: PermissionDecision) -> Entry {
        attributed(
            seq,
            AgentEvent::PermissionDecided {
                call: tool_call(id),
                decision,
            },
        )
    }

    fn child_started(seq: u64, child: &str, call: &str) -> Entry {
        attributed(
            seq,
            AgentEvent::ChildSessionStarted {
                child_session_id: SessionId::new(child),
                call_id: call.to_owned(),
            },
        )
    }

    fn child_ended(
        seq: u64,
        child: &str,
        call: &str,
        reason: SessionEndReason,
        tool_calls: u32,
        tool_errors: u32,
    ) -> Entry {
        attributed(
            seq,
            AgentEvent::ChildSessionEnded {
                child_session_id: SessionId::new(child),
                call_id: call.to_owned(),
                reason,
                rounds: 1,
                tool_calls,
                tool_errors,
            },
        )
    }

    fn verify(entries: Vec<Entry>, closed: bool) -> TurnVerification {
        verify_entries("turn", closed, &entries, None, Vec::new())
    }

    fn has_kind(result: &TurnVerification, kind: &str) -> bool {
        result.problems.iter().any(|problem| problem.kind == kind)
    }

    fn kinds(result: &TurnVerification) -> Vec<&'static str> {
        result.problems.iter().map(|problem| problem.kind).collect()
    }

    fn legacy_key(entry: &Entry) -> (SessionId, Timestamp, String) {
        let TracePayload::Agent { event } = &entry.event.payload else {
            panic!("legacy keys are only defined for agent payloads");
        };
        let session = entry
            .event
            .session_id
            .clone()
            .expect("mirrored events carry a session");
        let payload = serde_json::to_string(event).expect("test event serializes");
        (session, entry.timestamp, payload)
    }

    fn mirrored_index(entries: &[Entry]) -> LegacyIndex {
        let mut index = LegacyIndex::new();
        for entry in entries {
            let TracePayload::Agent { event } = &entry.event.payload else {
                continue;
            };
            if is_trace_only(event) || entry.event.session_id.is_none() {
                continue;
            }
            *index.entry(legacy_key(entry)).or_insert(0) += 1;
        }
        index
    }

    #[test]
    fn a_complete_v2_turn_passes_every_semantic_check() {
        let result = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                session_started(2),
                round_started(3, 1),
                model_delta(4),
                round_finished(5, 1),
                tool_started(6, "call-1"),
                permission(7, "call-1", PermissionDecision::Allow),
                tool_finished(8, "call-1"),
                session_ended(9, SessionEndReason::Completed),
                ended(10, SessionEndReason::Completed),
            ],
            true,
        );

        assert_eq!(result.status, TraceStatus::Ok);
        assert_eq!(result.problems, Vec::new());
        assert_eq!(result.schema_version, Some(2));
        assert_eq!(result.entries, Some(10));
    }

    #[test]
    fn round_sequence_and_nesting_failures_are_reported() {
        let out_of_order = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 2),
                ended(3, SessionEndReason::Cancelled),
            ],
            true,
        );
        assert_eq!(out_of_order.status, TraceStatus::Invalid);
        assert!(has_kind(&out_of_order, "round_sequence"));
        assert!(
            out_of_order.problems[0].message.contains("round 1"),
            "{:?}",
            out_of_order.problems
        );

        let nested = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 1),
                round_started(3, 2),
                ended(4, SessionEndReason::Cancelled),
            ],
            true,
        );
        assert_eq!(nested.status, TraceStatus::Invalid);
        assert!(has_kind(&nested, "nested_round"));

        let skipped_later = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 1),
                round_finished(3, 1),
                round_started(4, 3),
                ended(5, SessionEndReason::Cancelled),
            ],
            true,
        );
        assert_eq!(skipped_later.status, TraceStatus::Invalid);
        assert!(has_kind(&skipped_later, "round_sequence"));

        let zero = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 0),
                ended(3, SessionEndReason::Cancelled),
            ],
            true,
        );
        assert_eq!(zero.status, TraceStatus::Invalid);
        assert!(has_kind(&zero, "invalid_round"));

        let resumed = verify(
            vec![
                started(1, TraceTurnMode::Resumed),
                round_started(2, 4),
                model_delta(3),
                round_finished(4, 4),
                ended(5, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(resumed.status, TraceStatus::Ok, "{:?}", resumed.problems);
    }

    #[test]
    fn model_and_tool_phases_are_checked() {
        let delta_outside = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                model_delta(2),
                ended(3, SessionEndReason::Cancelled),
            ],
            true,
        );
        assert!(has_kind(&delta_outside, "model_delta_outside_round"));

        let tool_before_finish = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 1),
                tool_started(3, "call-1"),
                ended(4, SessionEndReason::Cancelled),
            ],
            true,
        );
        assert!(has_kind(
            &tool_before_finish,
            "tool_call_before_round_finished"
        ));

        let tool_without_round = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                tool_started(2, "call-1"),
                tool_finished(3, "call-1"),
                ended(4, SessionEndReason::Completed),
            ],
            true,
        );
        assert!(has_kind(&tool_without_round, "tool_call_without_round"));
    }

    #[test]
    fn session_identity_must_agree_with_the_envelope() {
        let mismatch = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                agent(
                    2,
                    Some(1),
                    Some("outer"),
                    AgentEvent::SessionStarted {
                        session_id: SessionId::new("inner"),
                        messages: Vec::new(),
                    },
                ),
                ended(3, SessionEndReason::Completed),
            ],
            true,
        );
        assert!(has_kind(&mismatch, "session_mismatch"));
        assert!(
            mismatch.problems[0].message.contains("inner")
                && mismatch.problems[0].message.contains("outer"),
            "{:?}",
            mismatch.problems
        );

        let missing = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                agent(
                    2,
                    Some(1),
                    None,
                    AgentEvent::SessionStarted {
                        session_id: SessionId::new("inner"),
                        messages: Vec::new(),
                    },
                ),
                ended(3, SessionEndReason::Completed),
            ],
            true,
        );
        assert!(has_kind(&missing, "session_mismatch"));
        assert!(
            missing.problems[0].message.contains("no session"),
            "{:?}",
            missing.problems
        );
    }

    #[test]
    fn completed_sessions_close_rounds_and_resolve_permissions() {
        let open_round = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 1),
                session_ended(3, SessionEndReason::Completed),
                ended(4, SessionEndReason::Completed),
            ],
            true,
        );
        assert!(has_kind(&open_round, "open_round_at_session_end"));

        let unresolved = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 1),
                round_finished(3, 1),
                tool_started(4, "call-1"),
                permission(5, "call-1", PermissionDecision::Allow),
                session_ended(6, SessionEndReason::Completed),
                ended(7, SessionEndReason::Completed),
            ],
            true,
        );
        assert!(has_kind(&unresolved, "unresolved_permission"));

        let resolved = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 1),
                round_finished(3, 1),
                tool_started(4, "call-1"),
                permission(5, "call-1", PermissionDecision::Deny),
                tool_finished(6, "call-1"),
                session_ended(7, SessionEndReason::Completed),
                ended(8, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(resolved.status, TraceStatus::Ok, "{:?}", resolved.problems);

        let non_blocking_decision = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 1),
                round_finished(3, 1),
                permission(4, "call-1", PermissionDecision::Cancelled),
                session_ended(5, SessionEndReason::Completed),
                ended(6, SessionEndReason::Completed),
            ],
            true,
        );
        assert!(
            !has_kind(&non_blocking_decision, "unresolved_permission"),
            "{:?}",
            non_blocking_decision.problems
        );

        let abnormal = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                round_started(2, 1),
                permission(3, "call-1", PermissionDecision::Allow),
                session_ended(4, SessionEndReason::Error),
                ended(5, SessionEndReason::Error),
            ],
            true,
        );
        assert_eq!(abnormal.status, TraceStatus::Ok, "{:?}", abnormal.problems);
    }

    #[test]
    fn child_sessions_must_be_paired_and_closed_on_completion() {
        let complete = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                child_started(2, "child", "call-1"),
                child_ended(3, "child", "call-1", SessionEndReason::Completed, 2, 0),
                session_ended(4, SessionEndReason::Completed),
                ended(5, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(complete.status, TraceStatus::Ok, "{:?}", complete.problems);

        let open_at_session_end = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                child_started(2, "child", "call-1"),
                session_ended(3, SessionEndReason::Completed),
                ended(4, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(
            kinds(&open_at_session_end),
            vec!["child_open_at_session_end"],
            "the same open child must not be reported twice"
        );

        let open_at_turn_end = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                child_started(2, "child", "call-1"),
                ended(3, SessionEndReason::Completed),
            ],
            true,
        );
        assert!(has_kind(&open_at_turn_end, "child_open_at_turn_end"));

        let abnormal = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                child_started(2, "child", "call-1"),
                session_ended(3, SessionEndReason::Error),
                ended(4, SessionEndReason::Error),
            ],
            true,
        );
        assert_eq!(abnormal.status, TraceStatus::Ok, "{:?}", abnormal.problems);

        let duplicate_start = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                child_started(2, "child", "call-1"),
                child_started(3, "child", "call-1"),
                child_ended(4, "child", "call-1", SessionEndReason::Completed, 0, 0),
                session_ended(5, SessionEndReason::Completed),
                ended(6, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(kinds(&duplicate_start), vec!["child_duplicate_start"]);

        let end_without_start = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                child_ended(2, "child", "call-1", SessionEndReason::Completed, 0, 0),
                session_ended(3, SessionEndReason::Completed),
                ended(4, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(kinds(&end_without_start), vec!["child_end_without_start"]);

        let double_end = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                child_started(2, "child", "call-1"),
                child_ended(3, "child", "call-1", SessionEndReason::Completed, 1, 0),
                child_ended(4, "child", "call-1", SessionEndReason::Completed, 1, 0),
                session_ended(5, SessionEndReason::Completed),
                ended(6, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(kinds(&double_end), vec!["child_end_without_start"]);
        assert!(
            double_end.problems[0].message.contains("more than once"),
            "{:?}",
            double_end.problems
        );

        let bad_counts = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                child_started(2, "child", "call-1"),
                child_ended(3, "child", "call-1", SessionEndReason::Completed, 1, 2),
                session_ended(4, SessionEndReason::Completed),
                ended(5, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(kinds(&bad_counts), vec!["child_tool_counts"]);
    }

    #[test]
    fn version_one_traces_are_not_judged_by_version_two_rules() {
        let mut entries = vec![
            started(1, TraceTurnMode::Fresh),
            agent(
                2,
                None,
                None,
                AgentEvent::ModelDelta {
                    text: "hello".to_owned(),
                },
            ),
            agent(
                3,
                None,
                None,
                AgentEvent::ToolCallStarted {
                    call: tool_call("call-1"),
                },
            ),
            agent(
                4,
                None,
                None,
                AgentEvent::ToolCallFinished {
                    result: ToolResult::success("call-1", "ok"),
                },
            ),
            ended(5, SessionEndReason::Completed),
        ];
        for entry in &mut entries {
            entry.event.schema_version = 1;
        }

        let result = verify(entries, true);
        assert_eq!(result.status, TraceStatus::Ok, "{:?}", result.problems);
        assert_eq!(result.schema_version, Some(1));
    }

    #[test]
    fn gaps_make_a_trace_invalid_even_when_it_is_open() {
        let gap_result = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                gap(2),
                ended(3, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(gap_result.status, TraceStatus::Invalid);
        assert_eq!(kinds(&gap_result), vec!["trace_gap"]);

        let open_gap = verify(vec![started(1, TraceTurnMode::Fresh), gap(2)], false);
        assert_eq!(
            open_gap.status,
            TraceStatus::Invalid,
            "a problem must take precedence over the open status"
        );
    }

    #[test]
    fn an_unclosed_trace_without_problems_is_open() {
        let open = verify(vec![started(1, TraceTurnMode::Fresh)], false);
        assert_eq!(open.status, TraceStatus::Open);
        assert_eq!(open.problems, Vec::new());

        let closed = verify(
            vec![
                started(1, TraceTurnMode::Fresh),
                ended(2, SessionEndReason::Completed),
            ],
            true,
        );
        assert_eq!(closed.status, TraceStatus::Ok);
    }

    #[test]
    fn mirror_matching_is_a_multiset_over_session_time_and_payload() {
        let first_delta = model_delta(4);
        let mut second_delta = model_delta(5);
        second_delta.timestamp = first_delta.timestamp;
        let round_finish = round_finished(6, 1);
        let entries = vec![
            started(1, TraceTurnMode::Fresh),
            session_started(2),
            round_started(3, 1),
            first_delta,
            second_delta,
            round_finish.clone(),
            ended(7, SessionEndReason::Completed),
        ];
        let mut legacy = mirrored_index(&entries);

        // The trace recorded the same delta twice at the same timestamp, but
        // the legacy log retained only one of the two rows.
        let delta_key = legacy_key(&entries[3]);
        let count = legacy
            .get_mut(&delta_key)
            .expect("seeded delta row must exist");
        *count -= 1;

        // The legacy row for this round finish is absent entirely.
        legacy
            .remove(&legacy_key(&round_finish))
            .expect("seeded round finish row must exist");

        // Unrelated legacy rows must not affect the result.
        legacy.insert(
            (
                SessionId::new("outer"),
                at(99),
                "{\"type\":\"other\"}".to_owned(),
            ),
            3,
        );

        let result = verify_entries("turn", true, &entries, Some(legacy), Vec::new());

        assert_eq!(result.status, TraceStatus::Invalid);
        assert_eq!(
            result
                .problems
                .iter()
                .map(|problem| (problem.seq, problem.kind))
                .collect::<Vec<_>>(),
            vec![
                (Some(5), "missing_legacy_mirror"),
                (Some(6), "missing_legacy_mirror"),
            ]
        );
    }

    #[test]
    fn trace_only_events_do_not_need_a_legacy_mirror() {
        let audit = permission(4, "call-1", PermissionDecision::Allow);
        let entries = vec![
            started(1, TraceTurnMode::Fresh),
            round_started(2, 1),
            round_finished(3, 1),
            audit,
            tool_started(5, "call-1"),
            tool_finished(6, "call-1"),
            session_ended(7, SessionEndReason::Completed),
            ended(8, SessionEndReason::Completed),
        ];
        let legacy = mirrored_index(&entries);

        let result = verify_entries("turn", true, &entries, Some(legacy), Vec::new());
        assert_eq!(result.status, TraceStatus::Ok, "{:?}", result.problems);
    }

    #[test]
    fn structural_failures_have_no_schema_or_entry_count() {
        let failure = structural_failure("turn".to_owned(), "guard rejected".to_owned());
        assert_eq!(failure.schema_version, None);
        assert_eq!(failure.entries, None);
        assert_eq!(failure.status, TraceStatus::Invalid);
        assert_eq!(failure.problems[0].kind, "trace_structure");
        assert_eq!(failure.problems[0].seq, None);
    }

    #[test]
    fn allow_open_relaxes_only_open_traces() {
        let turn = |status| TurnVerification {
            turn_id: "turn".to_owned(),
            schema_version: Some(2),
            entries: Some(1),
            digest: Some("0".repeat(64)),
            status,
            problems: Vec::new(),
            replay: None,
        };

        let ok = TraceVerifyReport::new(vec![turn(TraceStatus::Ok)]);
        assert!(ok.all_verified(false));
        assert!(ok.all_verified(true));

        let open = TraceVerifyReport::new(vec![turn(TraceStatus::Open)]);
        assert!(!open.all_verified(false));
        assert!(open.all_verified(true));

        let invalid = TraceVerifyReport::new(vec![TurnVerification {
            turn_id: "bad".to_owned(),
            schema_version: Some(2),
            entries: Some(1),
            digest: Some("0".repeat(64)),
            status: TraceStatus::Invalid,
            problems: vec![TraceProblem::new(
                Some(1),
                "trace_structure",
                "broken".to_owned(),
            )],
            replay: None,
        }]);
        assert!(!invalid.all_verified(false));
        assert!(!invalid.all_verified(true));
    }
}
