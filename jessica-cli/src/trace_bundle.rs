//! Portable trace evidence bundles and offline verification.
//!
//! `jessica trace export` writes the validated trace chains and the legacy rows
//! consumed by the mirror check into a self-contained JSON document.
//! `jessica trace verify-bundle` reconstructs each chain in a private in-memory
//! store, re-runs the same structural, semantic, and mirror checks, and verifies
//! the aggregate digest. It never opens, creates, or migrates the configured
//! event store, so a bundle can be audited on a machine that has no database.
//!
//! The aggregate digest is tamper-evident only relative to an externally kept
//! value. A writer able to rewrite the whole bundle can recompute every digest;
//! compare the aggregate with a value recorded elsewhere, for example through
//! `--expect`, to detect that case. The bundle is not a signature.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use jessica_core::trace::TurnId;
use jessica_core::types::SessionId;
use jessica_memory::{
    EventStore, StoredTraceTurn, Timestamp, TraceEvidenceEntry, trace_bundle_digest,
    verify_exported_trace,
};
use serde::{Deserialize, Serialize};

use crate::app::CliError;
use crate::config::HarnessConfig;
use crate::trace_verify::{
    LegacyIndex, TraceProblem, TraceSelection, TraceStatus, TraceSummary, TurnVerification,
    all_verified, entries_for_trace, event_store_path, exported_legacy_rows,
    is_structural_trace_error, print_verification_text, structural_failure, summarize,
    verify_entries, verify_stored_turn,
};

/// Version of the bundle document structure.
const BUNDLE_VERSION: u32 = 1;

/// Identifier of the bundle encoding.
const BUNDLE_FORMAT: &str = "jessica.trace.bundle.v1";

/// Version of the bundle verification report.
const REPORT_VERSION: u32 = 1;

/// Maximum bundle file size accepted by the offline verifier.
const MAX_BUNDLE_BYTES: u64 = 64 * 1024 * 1024;

/// A self-contained trace evidence bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TraceBundle {
    /// Bundle structure version.
    bundle_version: u32,
    /// Bundle encoding identifier.
    bundle_format: String,
    /// Lowercase hex aggregate digest over the sorted turn chains.
    aggregate_digest: String,
    /// Turns in ascending turn-id order.
    turns: Vec<BundleTurn>,
}

/// One turn inside a bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BundleTurn {
    /// Turn identifier.
    turn_id: String,
    /// Whether the trace ends with `turn_ended`.
    closed: bool,
    /// Lowercase hex digest declared by the exporter.
    digest: String,
    /// Trace entries in sequence order.
    entries: Vec<BundleEntry>,
    /// Legacy rows consumed by the mirror check, in trace-entry order.
    legacy: Vec<BundleLegacyRow>,
}

/// One trace entry inside a bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BundleEntry {
    /// One-based sequence number.
    seq: u64,
    /// Milliseconds since the Unix epoch.
    timestamp: i64,
    /// Exact trace envelope JSON.
    payload_json: String,
}

/// One legacy mirror row inside a bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BundleLegacyRow {
    /// Legacy event session id.
    session_id: String,
    /// Legacy event timestamp in milliseconds since the Unix epoch.
    timestamp: i64,
    /// Exact legacy event JSON text.
    payload_json: String,
}

/// Stable JSON shape of one bundle verification run.
#[derive(Debug, Serialize)]
struct BundleVerifyReport {
    /// Report structure version.
    report_version: u32,
    /// Bundle encoding identifier.
    bundle_format: String,
    /// Aggregate-digest outcome.
    bundle: BundleSummary,
    /// Per-turn results.
    turns: Vec<TurnVerification>,
    /// Aggregate counts.
    summary: TraceSummary,
}

/// Aggregate-digest state for one bundle.
#[derive(Debug, Serialize)]
struct BundleSummary {
    /// Aggregate digest declared by the bundle.
    declared_digest: String,
    /// Aggregate recomputed from the verified turn chains, when available.
    computed_digest: Option<String>,
    /// Externally recorded aggregate required by `--expect`.
    expected_digest: Option<String>,
    /// Findings about the bundle as a whole.
    problems: Vec<TraceProblem>,
}

impl BundleVerifyReport {
    /// Returns whether the bundle and every selected trace are acceptable.
    fn all_verified(&self, allow_open: bool) -> bool {
        self.bundle.problems.is_empty() && all_verified(&self.turns, allow_open)
    }

    /// Renders the one-line summary used by text output and failures.
    fn failure_summary(&self) -> String {
        let mut summary = self.summary.line();
        if !self.bundle.problems.is_empty() {
            summary.push_str(&format!("; {} bundle problems", self.bundle.problems.len()));
        }
        summary
    }
}

/// Runs one `trace export` invocation.
pub(crate) async fn run_export(
    config: &HarnessConfig,
    root: &Path,
    selection: TraceSelection,
    out: PathBuf,
    allow_open: bool,
) -> Result<(), CliError> {
    let database = event_store_path(config, root)?;
    let display = out.display().to_string();
    let (count, aggregate) =
        tokio::task::spawn_blocking(move || write_bundle(&database, &selection, &out, allow_open))
            .await
            .map_err(CliError::TraceTask)??;
    println!("exported {count} turn(s) to {display}");
    println!("aggregate {aggregate}");
    Ok(())
}

/// Opens the store read-only and writes one bundle.
fn write_bundle(
    database: &Path,
    selection: &TraceSelection,
    out: &Path,
    allow_open: bool,
) -> Result<(usize, String), CliError> {
    let store = EventStore::open_read_only(database)?;
    let traces = load_export_traces(&store, selection)?;

    let mut turns = Vec::with_capacity(traces.len());
    for trace in &traces {
        let (verification, _) = verify_stored_turn(&store, trace);
        if let Some(reason) = rejection_reason(&verification, allow_open) {
            return Err(CliError::TraceExportFailed { summary: reason });
        }
        let legacy =
            exported_legacy_rows(&store, trace).map_err(|message| CliError::TraceExportFailed {
                summary: format!(
                    "turn `{}` cannot supply legacy mirror evidence: {message}",
                    trace.turn_id()
                ),
            })?;
        turns.push(BundleTurn {
            turn_id: trace.turn_id().to_string(),
            closed: trace.closed(),
            digest: trace.digest().to_owned(),
            entries: trace
                .events()
                .iter()
                .map(|entry| BundleEntry {
                    seq: entry.seq(),
                    timestamp: entry.timestamp().as_millis(),
                    payload_json: entry.payload_json().to_owned(),
                })
                .collect(),
            legacy: legacy
                .into_iter()
                .map(|(session_id, timestamp, payload_json)| BundleLegacyRow {
                    session_id: session_id.to_string(),
                    timestamp: timestamp.as_millis(),
                    payload_json,
                })
                .collect(),
        });
    }

    let references: Vec<&StoredTraceTurn> = traces.iter().collect();
    let aggregate_digest = trace_bundle_digest(&references)?;
    let bundle = TraceBundle {
        bundle_version: BUNDLE_VERSION,
        bundle_format: BUNDLE_FORMAT.to_owned(),
        aggregate_digest: aggregate_digest.clone(),
        turns,
    };
    let mut encoded = serde_json::to_vec_pretty(&bundle).map_err(CliError::TraceReport)?;
    encoded.push(b'\n');
    std::fs::write(out, encoded).map_err(|source| CliError::TraceBundleWrite {
        path: out.to_path_buf(),
        source,
    })?;
    Ok((bundle.turns.len(), aggregate_digest))
}

/// Loads every selected trace, rejecting structural corruption.
fn load_export_traces(
    store: &EventStore,
    selection: &TraceSelection,
) -> Result<Vec<StoredTraceTurn>, CliError> {
    match selection {
        TraceSelection::Turn(turn_id) => {
            let requested = TurnId::new(turn_id.clone());
            match store.trace_turn(&requested) {
                Ok(Some(trace)) => Ok(vec![trace]),
                Ok(None) => Err(CliError::UnknownTurn {
                    turn_id: turn_id.clone(),
                }),
                Err(error) if is_structural_trace_error(&error) => {
                    Err(CliError::TraceExportFailed {
                        summary: format!("turn `{turn_id}` cannot be exported: {error}"),
                    })
                }
                Err(error) => Err(CliError::TraceStore(error)),
            }
        }
        TraceSelection::All => {
            let mut summaries = store.trace_turns(usize::MAX)?;
            summaries.sort_by(|left, right| left.turn_id().as_str().cmp(right.turn_id().as_str()));
            let mut traces = Vec::with_capacity(summaries.len());
            for summary in summaries {
                let turn_id = summary.turn_id();
                match store.trace_turn(turn_id) {
                    Ok(Some(trace)) => traces.push(trace),
                    Ok(None) => {
                        return Err(CliError::TraceExportFailed {
                            summary: format!(
                                "turn `{turn_id}` disappeared while the bundle was being built"
                            ),
                        });
                    }
                    Err(error) if is_structural_trace_error(&error) => {
                        return Err(CliError::TraceExportFailed {
                            summary: format!("turn `{turn_id}` cannot be exported: {error}"),
                        });
                    }
                    Err(error) => return Err(CliError::TraceStore(error)),
                }
            }
            Ok(traces)
        }
    }
}

/// Returns why a verified trace may not be exported, if it may not.
fn rejection_reason(verification: &TurnVerification, allow_open: bool) -> Option<String> {
    let first_problem = verification
        .problems
        .first()
        .map(|problem| format!("{}: {}", problem.kind, problem.message))
        .unwrap_or_else(|| "no problem details were recorded".to_owned());
    match verification.status {
        TraceStatus::Ok => None,
        TraceStatus::Open if allow_open => None,
        TraceStatus::Open => Some(format!(
            "turn `{}` is open; open traces can only be exported with `--all --allow-open`",
            verification.turn_id
        )),
        TraceStatus::Invalid => Some(format!(
            "turn `{}` is invalid ({first_problem})",
            verification.turn_id
        )),
    }
}

/// Runs one `trace verify-bundle` invocation without touching an event store.
pub(crate) async fn run_verify_bundle(
    bundle: PathBuf,
    json: bool,
    allow_open: bool,
    expect: Option<String>,
) -> Result<(), CliError> {
    let display = bundle.display().to_string();
    let report =
        tokio::task::spawn_blocking(move || build_bundle_report(&bundle, expect.as_deref()))
            .await
            .map_err(CliError::TraceTask)??;

    if json {
        let rendered = serde_json::to_string_pretty(&report).map_err(CliError::TraceReport)?;
        println!("{rendered}");
    } else {
        print_bundle_text(&report, &display);
    }

    if !report.all_verified(allow_open) {
        return Err(CliError::TraceVerificationFailed {
            summary: report.failure_summary(),
        });
    }
    Ok(())
}

/// Reads, validates, and verifies one bundle.
fn build_bundle_report(path: &Path, expect: Option<&str>) -> Result<BundleVerifyReport, CliError> {
    let bytes = read_bundle(path)?;
    let mut bundle: TraceBundle =
        serde_json::from_slice(&bytes).map_err(|source| CliError::TraceBundleDecode {
            path: path.to_path_buf(),
            source,
        })?;
    validate_bundle(&bundle)?;
    bundle
        .turns
        .sort_by(|left, right| left.turn_id.cmp(&right.turn_id));

    let mut turns = Vec::with_capacity(bundle.turns.len());
    let mut recomputed = Vec::with_capacity(bundle.turns.len());
    for turn in &bundle.turns {
        let entries: Vec<TraceEvidenceEntry> = turn
            .entries
            .iter()
            .map(|entry| {
                TraceEvidenceEntry::new(
                    entry.seq,
                    Timestamp::from_millis(entry.timestamp),
                    entry.payload_json.clone(),
                )
            })
            .collect();
        match verify_exported_trace(&TurnId::new(turn.turn_id.clone()), turn.closed, &entries) {
            Ok(trace) => {
                turns.push(verify_bundle_turn(turn, &trace));
                recomputed.push(trace);
            }
            Err(error) if is_structural_trace_error(&error) => {
                turns.push(structural_failure(turn.turn_id.clone(), error.to_string()));
            }
            Err(error) => return Err(CliError::TraceStore(error)),
        }
    }

    let computed_digest = if recomputed.len() == bundle.turns.len() {
        let references: Vec<&StoredTraceTurn> = recomputed.iter().collect();
        Some(trace_bundle_digest(&references)?)
    } else {
        None
    };

    let mut problems = Vec::new();
    match &computed_digest {
        Some(computed) if computed == &bundle.aggregate_digest => {}
        Some(computed) => problems.push(TraceProblem::new(
            None,
            "digest_mismatch",
            format!(
                "bundle aggregate digest mismatch: declared {}, computed {computed}",
                bundle.aggregate_digest
            ),
        )),
        None => problems.push(TraceProblem::new(
            None,
            "digest_unavailable",
            "cannot compute the bundle aggregate because one or more turns are structurally invalid"
                .to_owned(),
        )),
    }
    if let Some(expected) = expect {
        match &computed_digest {
            Some(computed) if computed == expected => {}
            Some(computed) => problems.push(TraceProblem::new(
                None,
                "digest_mismatch",
                format!("bundle aggregate digest mismatch: expected {expected}, found {computed}"),
            )),
            None => problems.push(TraceProblem::new(
                None,
                "digest_unavailable",
                "cannot check --expect because the bundle aggregate is unavailable".to_owned(),
            )),
        }
    }

    let summary = summarize(&turns);
    Ok(BundleVerifyReport {
        report_version: REPORT_VERSION,
        bundle_format: bundle.bundle_format.clone(),
        bundle: BundleSummary {
            declared_digest: bundle.aggregate_digest,
            computed_digest,
            expected_digest: expect.map(str::to_owned),
            problems,
        },
        turns,
        summary,
    })
}

/// Verifies one reconstructed trace against the bundle's semantic evidence.
fn verify_bundle_turn(turn: &BundleTurn, trace: &StoredTraceTurn) -> TurnVerification {
    let entries = entries_for_trace(trace);
    let mut legacy = LegacyIndex::new();
    for row in &turn.legacy {
        let key = (
            SessionId::new(row.session_id.clone()),
            Timestamp::from_millis(row.timestamp),
            row.payload_json.clone(),
        );
        *legacy.entry(key).or_insert(0) += 1;
    }

    let mut verification = verify_entries(
        &turn.turn_id,
        trace.closed(),
        &entries,
        Some(legacy),
        Vec::new(),
    );
    verification.digest = Some(trace.digest().to_owned());
    if turn.digest != trace.digest() {
        verification.problems.push(TraceProblem::new(
            None,
            "digest_mismatch",
            format!(
                "bundle turn digest mismatch: declared {}, recomputed {}",
                turn.digest,
                trace.digest()
            ),
        ));
        verification.status = TraceStatus::Invalid;
    }
    verification
}

/// Reads a bundle after enforcing the size limit.
fn read_bundle(path: &Path) -> Result<Vec<u8>, CliError> {
    let metadata = std::fs::metadata(path).map_err(|source| CliError::TraceBundleRead {
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.len() > MAX_BUNDLE_BYTES {
        return Err(bundle_too_large(path, metadata.len()));
    }
    let bytes = std::fs::read(path).map_err(|source| CliError::TraceBundleRead {
        path: path.to_path_buf(),
        source,
    })?;
    let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if length > MAX_BUNDLE_BYTES {
        return Err(bundle_too_large(path, length));
    }
    Ok(bytes)
}

/// Builds the structured size-limit error.
fn bundle_too_large(path: &Path, bytes: u64) -> CliError {
    CliError::TraceBundleTooLarge {
        path: path.to_path_buf(),
        bytes,
        limit: MAX_BUNDLE_BYTES,
    }
}

/// Validates the bundle header, digests, and turn uniqueness.
fn validate_bundle(bundle: &TraceBundle) -> Result<(), CliError> {
    if bundle.bundle_version != BUNDLE_VERSION {
        return Err(CliError::TraceBundleVersion {
            found: bundle.bundle_version,
            supported: BUNDLE_VERSION,
        });
    }
    if bundle.bundle_format != BUNDLE_FORMAT {
        return Err(CliError::TraceBundleFormat {
            found: bundle.bundle_format.clone(),
        });
    }
    if !is_canonical_sha256(&bundle.aggregate_digest) {
        return Err(CliError::TraceBundleDigest {
            field: "aggregate_digest",
            value: bundle.aggregate_digest.clone(),
        });
    }
    let mut seen = BTreeSet::new();
    for turn in &bundle.turns {
        if !is_canonical_sha256(&turn.digest) {
            return Err(CliError::TraceBundleDigest {
                field: "turn digest",
                value: turn.digest.clone(),
            });
        }
        if !seen.insert(turn.turn_id.as_str()) {
            return Err(CliError::TraceBundleDuplicateTurn {
                turn_id: turn.turn_id.clone(),
            });
        }
    }
    Ok(())
}

/// Returns whether `value` is 64 lowercase hexadecimal characters.
fn is_canonical_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Renders a bundle report for humans.
fn print_bundle_text(report: &BundleVerifyReport, path: &str) {
    println!("bundle {path} turns={}", report.turns.len());
    match &report.bundle.computed_digest {
        Some(computed) => println!("  aggregate {computed}"),
        None => println!("  aggregate unavailable"),
    }
    println!("  declared {}", report.bundle.declared_digest);
    if let Some(expected) = &report.bundle.expected_digest {
        println!("  expected {expected}");
    }
    for problem in &report.bundle.problems {
        println!("  [{}] {}", problem.kind, problem.message);
    }
    print_verification_text(&report.turns, report.summary);
}
