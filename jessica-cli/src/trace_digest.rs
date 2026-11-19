//! Read-only digest reporting for recorded turn traces.
//!
//! `jessica trace digest` opens the configured event store through the same
//! immutable audit path as `trace verify`, validates each selected turn's
//! hash chain, and prints the resulting digest. It never creates, migrates, or
//! modifies the database and never calls a model or a tool.
//!
//! The digest proves that the stored trace bytes have not changed since the
//! digest was recorded. It is not a signature: a writer able to rewrite the
//! whole database can recompute the chain. Comparing the output with an
//! externally kept value, or passing it to `trace verify --expect`, anchors it.

use std::path::Path;

use jessica_core::trace::TurnId;
use jessica_memory::{EventStore, StoredTraceTurn};
use serde::Serialize;

use crate::app::CliError;
use crate::config::HarnessConfig;
use crate::trace_verify::{TraceSelection, event_store_path, is_structural_trace_error};

/// Version of the digest report structure.
const REPORT_VERSION: u32 = 1;

/// Identifier of the digest encoding.
const DIGEST_FORMAT: &str = "jessica.trace.digest.v1";

/// Stable JSON shape of one digest run.
#[derive(Debug, Serialize)]
struct DigestReport {
    /// Report structure version.
    report_version: u32,
    /// Digest encoding identifier.
    digest_format: &'static str,
    /// Per-turn results, sorted by turn id.
    turns: Vec<TurnDigest>,
}

/// One turn's verified digest.
#[derive(Debug, Serialize)]
struct TurnDigest {
    /// Turn whose chain was digested.
    turn_id: String,
    /// Lowercase hex SHA-256 digest.
    digest: String,
    /// Whether the trace ends with `turn_ended`.
    closed: bool,
    /// Number of entries covered by the digest.
    entries: u64,
}

/// Runs one `trace digest` invocation.
pub(crate) async fn run_command(
    config: &HarnessConfig,
    root: &Path,
    selection: TraceSelection,
    json: bool,
) -> Result<(), CliError> {
    let database = event_store_path(config, root)?;
    let render_selection = selection.clone();
    let report = tokio::task::spawn_blocking(move || build_report(&database, &selection))
        .await
        .map_err(CliError::TraceTask)??;

    if json {
        let rendered = serde_json::to_string_pretty(&report).map_err(CliError::TraceReport)?;
        println!("{rendered}");
    } else {
        print_text(&report, &render_selection);
    }
    Ok(())
}

/// Opens the store read-only and validates every selected chain.
fn build_report(database: &Path, selection: &TraceSelection) -> Result<DigestReport, CliError> {
    let store = EventStore::open_read_only(database)?;
    let traces = match selection {
        TraceSelection::Turn(turn_id) => {
            let requested = TurnId::new(turn_id.clone());
            let trace = load_trace(&store, &requested)?.ok_or_else(|| CliError::UnknownTurn {
                turn_id: turn_id.clone(),
            })?;
            vec![trace]
        }
        TraceSelection::All => {
            let mut summaries = store.trace_turns(usize::MAX)?;
            summaries.sort_by(|left, right| left.turn_id().as_str().cmp(right.turn_id().as_str()));
            let mut traces = Vec::with_capacity(summaries.len());
            for summary in summaries {
                let trace = load_trace(&store, summary.turn_id())?.ok_or_else(|| {
                    CliError::TraceDigestFailed {
                        turn_id: summary.turn_id().to_string(),
                        message: "the turn head disappeared during digesting".to_owned(),
                    }
                })?;
                traces.push(trace);
            }
            traces
        }
    };

    Ok(DigestReport {
        report_version: REPORT_VERSION,
        digest_format: DIGEST_FORMAT,
        turns: traces.iter().map(turn_digest).collect(),
    })
}

/// Loads one turn, converting evidence corruption into a digest failure.
fn load_trace(store: &EventStore, turn_id: &TurnId) -> Result<Option<StoredTraceTurn>, CliError> {
    match store.trace_turn(turn_id) {
        Ok(trace) => Ok(trace),
        Err(error) if is_structural_trace_error(&error) => Err(CliError::TraceDigestFailed {
            turn_id: turn_id.to_string(),
            message: error.to_string(),
        }),
        Err(error) => Err(CliError::TraceStore(error)),
    }
}

/// Projects one validated trace into its report row.
fn turn_digest(trace: &StoredTraceTurn) -> TurnDigest {
    TurnDigest {
        turn_id: trace.turn_id().to_string(),
        digest: trace.digest().to_owned(),
        closed: trace.closed(),
        entries: u64::try_from(trace.events().len()).unwrap_or(u64::MAX),
    }
}

/// Renders the report for humans.
///
/// A single-turn run prints only the digest so it can be captured directly.
/// An all-turns run prints one `turn_id<TAB>digest` line per turn in ascending
/// turn-id order.
fn print_text(report: &DigestReport, selection: &TraceSelection) {
    match selection {
        TraceSelection::Turn(_) => {
            if let Some(turn) = report.turns.first() {
                println!("{}", turn.digest);
            }
        }
        TraceSelection::All => {
            for turn in &report.turns {
                println!("{}\t{}", turn.turn_id, turn.digest);
            }
        }
    }
}
