//! Execution parity between a stored trace and a scripted agent replay.
//!
//! [`crate::trace_verify`] proves that a trace is structurally valid,
//! semantically consistent, and mirrored into legacy storage. This module adds
//! the execution check: rebuild each fresh attempt from the trace's own
//! bootstrap messages and recorded runtime events, drive the existing
//! deterministic replay engine, and require the produced event sequence to
//! equal the trace's runtime-replayable events.
//!
//! Trace-only audit facts ([`AgentEvent::PermissionDecided`], child-session
//! lifecycle, and [`AgentEvent::AcceptanceFailed`]) and CLI-governance events
//! ([`AgentEvent::PolicyDenied`] and non-round budget trips) are deliberately
//! excluded. Replay never re-runs a permission gate, child session, acceptance
//! check, or CLI policy; their effects are already present in the recorded tool
//! results and terminal reasons.

use std::collections::BTreeMap;

use jessica_core::trace::{TraceEvent, TracePayload, TraceTurnMode};
use jessica_core::types::{AgentEvent, BudgetKind, Message, SessionEndReason, SessionId};
use jessica_engine::StoredSession;
use jessica_engine::replay::{ReplayError, replay_resumed_session, replay_session};
use jessica_runtime::RecordedEvent;
use serde::Serialize;

/// Status of one replay parity check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplayStatus {
    /// Every replayable attempt reproduced its recorded events exactly.
    Ok,
    /// The trace is valid evidence but cannot be exercised by the replay engine.
    Unsupported,
    /// The replay engine produced different events than the trace records.
    Mismatch,
}

impl ReplayStatus {
    /// Returns the stable text label.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Unsupported => "unsupported",
            Self::Mismatch => "mismatch",
        }
    }
}

/// Stable JSON summary of one replay parity check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReplayVerification {
    /// Parity status.
    pub(crate) status: ReplayStatus,
    /// Attempts replayed before the check finished.
    pub(crate) attempts: usize,
    /// Replayable trace events compared.
    pub(crate) events: usize,
    /// Why parity could not run, when the status is `unsupported`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
}

/// One replay parity finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayProblem {
    /// Trace sequence number, when the finding anchors to one event.
    pub(crate) seq: Option<u64>,
    /// Stable machine-readable problem kind.
    pub(crate) kind: &'static str,
    /// Human-readable explanation.
    pub(crate) message: String,
}

impl ReplayProblem {
    /// Creates a parity finding.
    fn new(seq: Option<u64>, message: String) -> Self {
        Self {
            seq,
            kind: "replay_mismatch",
            message,
        }
    }
}

/// Result of checking execution parity for one trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayOutcome {
    /// Stable summary rendered in the verification report.
    pub(crate) verification: ReplayVerification,
    /// Mismatch findings, empty unless the status is `mismatch`.
    pub(crate) problems: Vec<ReplayProblem>,
}

impl ReplayOutcome {
    /// Builds an unsupported outcome.
    pub(crate) fn unsupported(reason: String) -> Self {
        Self {
            verification: ReplayVerification {
                status: ReplayStatus::Unsupported,
                attempts: 0,
                events: 0,
                reason: Some(reason),
            },
            problems: Vec::new(),
        }
    }
}

/// Checks that replaying the trace reproduces its recorded runtime events.
///
/// `entries` are the stored trace entries in sequence order. The function
/// classifies every failure as an outcome instead of returning an error: a
/// trace that merely cannot be replayed is not corrupt evidence, while a trace
/// whose replay differs from its record is.
pub(crate) async fn verify(entries: &[(u64, TraceEvent)]) -> ReplayOutcome {
    let prepared = match prepare(entries) {
        Ok(prepared) => prepared,
        Err(reason) => return ReplayOutcome::unsupported(reason),
    };

    let mut problems = Vec::new();
    let mut compared = 0_usize;
    let mut replayed = 0_usize;

    for attempt in &prepared.attempts {
        replayed += 1;
        compared += attempt.events.len();
        let stored = StoredSession {
            initial_messages: attempt.start.initial_messages().to_vec(),
            events: attempt
                .events
                .iter()
                .map(|(seq, event)| RecordedEvent::new(*seq, event.clone()))
                .collect(),
        };
        let replay = match attempt.start.rounds_completed() {
            Some(rounds_completed) => {
                replay_resumed_session(&attempt.session, &stored, rounds_completed).await
            }
            None => replay_session(&attempt.session, &stored).await,
        };
        match replay {
            Ok(actual) => compare_attempt(attempt, &actual, &mut problems),
            Err(ReplayError::EventLag { skipped }) => {
                return ReplayOutcome {
                    verification: ReplayVerification {
                        status: ReplayStatus::Unsupported,
                        attempts: replayed,
                        events: compared,
                        reason: Some(format!(
                            "attempt {} produced a replay event gap of {skipped} events",
                            attempt.number
                        )),
                    },
                    problems: Vec::new(),
                };
            }
            Err(error) => problems.push(ReplayProblem::new(
                None,
                format!("attempt {} could not be replayed: {error}", attempt.number),
            )),
        }
    }

    check_outer_session(&prepared, &mut problems);

    let status = if problems.is_empty() {
        ReplayStatus::Ok
    } else {
        ReplayStatus::Mismatch
    };
    ReplayOutcome {
        verification: ReplayVerification {
            status,
            attempts: replayed,
            events: compared,
            reason: None,
        },
        problems,
    }
}

/// Starting state of one attempt's replay.
#[derive(Debug, Clone, PartialEq)]
enum AttemptStart {
    /// A fresh attempt whose bootstrap messages come from `SessionStarted`.
    Fresh {
        /// Conversation recorded with the attempt.
        initial_messages: Vec<Message>,
    },
    /// A resumed attempt that starts from an out-of-band checkpoint.
    Resumed {
        /// Conversation the resumed loop must see.
        initial_messages: Vec<Message>,
        /// Model rounds the original session completed before resuming.
        rounds_completed: u32,
    },
}

impl AttemptStart {
    /// Returns the conversation the replay loop must start with.
    fn initial_messages(&self) -> &[Message] {
        match self {
            Self::Fresh { initial_messages }
            | Self::Resumed {
                initial_messages, ..
            } => initial_messages,
        }
    }

    /// Returns the completed rounds when this attempt resumed a checkpoint.
    fn rounds_completed(&self) -> Option<u32> {
        match self {
            Self::Fresh { .. } => None,
            Self::Resumed {
                rounds_completed, ..
            } => Some(*rounds_completed),
        }
    }
}

/// One attempt reconstructed from the trace.
struct PreparedAttempt {
    /// One-based attempt number.
    number: u32,
    /// Session that owns the attempt's events.
    session: SessionId,
    /// Starting state reconstructed from the trace.
    start: AttemptStart,
    /// Terminal reason of the attempt's `SessionEnded`.
    reason: SessionEndReason,
    /// Replayable events with their original trace sequences.
    events: Vec<(u64, AgentEvent)>,
}

/// A trace that passed the replay preconditions.
struct Prepared {
    /// Attempts in one-based order.
    attempts: Vec<PreparedAttempt>,
    /// Agent events outside any attempt, normally the outer session pair.
    outer: Vec<(u64, AgentEvent)>,
    /// Reason recorded on `turn_ended`.
    terminal: SessionEndReason,
}

/// Validates replay preconditions and groups trace events by attempt.
fn prepare(entries: &[(u64, TraceEvent)]) -> Result<Prepared, String> {
    if entries.is_empty() {
        return Err("the trace has no entries".to_owned());
    }
    let version = entries[0].1.schema_version;
    if !(1..=3).contains(&version) {
        return Err(format!(
            "trace schema version {version} is not supported by replay"
        ));
    }
    if entries
        .iter()
        .any(|(_, trace)| trace.schema_version != version)
    {
        return Err("the trace mixes schema versions".to_owned());
    }

    let mut mode = None;
    let mut turn_messages = Vec::new();
    let mut terminal = None;
    let mut attempts: BTreeMap<u32, Vec<(u64, TraceEvent)>> = BTreeMap::new();
    let mut outer = Vec::new();

    for (seq, trace) in entries {
        match &trace.payload {
            TracePayload::Gap { .. } => {
                return Err("the trace contains a gap; replay would be incomplete".to_owned());
            }
            TracePayload::TurnStarted {
                mode: started,
                messages,
            } => {
                mode = Some(*started);
                turn_messages = messages.clone();
            }
            TracePayload::TurnEnded { reason } => terminal = Some(*reason),
            // Child evidence is validated by the verifier before replay, but
            // replay never executes a delegated child again. The parent's own
            // tool events already reproduce the delegation boundary.
            TracePayload::ChildAgent { .. } => {}
            TracePayload::Agent { .. } => match trace.attempt {
                Some(attempt) => attempts
                    .entry(attempt)
                    .or_default()
                    .push((*seq, trace.clone())),
                None => outer.push((*seq, trace.clone())),
            },
        }
    }

    let Some(mode) = mode else {
        return Err("the trace has no `turn_started` entry".to_owned());
    };
    let Some(terminal) = terminal else {
        return Err("the trace has no `turn_ended` entry".to_owned());
    };
    if attempts.is_empty() {
        return Err("the trace contains no attempt".to_owned());
    }
    match mode {
        TraceTurnMode::Fresh => {
            if version < 2 && attempts.len() != 1 {
                return Err(
                    "version-1 traces with multiple attempts do not record each attempt's bootstrap messages"
                        .to_owned(),
                );
            }
        }
        TraceTurnMode::Resumed => {
            if version < 2 {
                return Err("resumed turns require trace schema version 2".to_owned());
            }
            if attempts.len() != 1 {
                return Err(
                    "resumed turns with multiple attempts do not record enough checkpoint state to replay"
                        .to_owned(),
                );
            }
        }
    }

    let numbers: Vec<u32> = attempts.keys().copied().collect();
    for (index, number) in numbers.iter().enumerate() {
        let expected = u32::try_from(index + 1).map_err(|_| "too many attempts".to_owned())?;
        if *number != expected {
            return Err(format!(
                "attempt numbers must be contiguous from one; found {number} where {expected} was expected"
            ));
        }
    }

    let mut prepared = Vec::with_capacity(attempts.len());
    for (number, events) in attempts {
        prepared.push(prepare_attempt(
            number,
            events,
            mode,
            version,
            &turn_messages,
        )?);
    }

    let outer = outer
        .into_iter()
        .filter_map(|(seq, trace)| match trace.payload {
            TracePayload::Agent { event } => Some((seq, event)),
            _ => None,
        })
        .collect();

    Ok(Prepared {
        attempts: prepared,
        outer,
        terminal,
    })
}

/// Validates one attempt and filters its replayable events.
fn prepare_attempt(
    number: u32,
    entries: Vec<(u64, TraceEvent)>,
    mode: TraceTurnMode,
    version: u16,
    turn_messages: &[Message],
) -> Result<PreparedAttempt, String> {
    let mut session: Option<SessionId> = None;
    let mut events = Vec::new();

    for (seq, trace) in entries {
        let Some(envelope_session) = trace.session_id else {
            return Err(format!(
                "attempt {number} has an agent event without a session attribution"
            ));
        };
        match &session {
            Some(existing) if *existing != envelope_session => {
                return Err(format!("attempt {number} spans multiple sessions"));
            }
            None => session = Some(envelope_session.clone()),
            _ => {}
        }
        let TracePayload::Agent { event } = trace.payload else {
            return Err(format!(
                "attempt {number} contains a non-agent payload at sequence {seq}"
            ));
        };

        if let AgentEvent::BudgetExceeded { kind, .. } = &event
            && *kind != BudgetKind::MaxRounds
        {
            return Err(format!(
                "attempt {number} contains a CLI `{kind:?}` budget event that replay cannot reproduce"
            ));
        }
        if is_replayable(&event) {
            events.push((seq, event));
        }
    }

    let Some(session) = session else {
        return Err(format!("attempt {number} contains no agent events"));
    };
    let Some((
        _,
        AgentEvent::SessionEnded {
            session_id: ended,
            reason,
        },
    )) = events.last()
    else {
        return Err(format!(
            "attempt {number} does not end with a `SessionEnded` event"
        ));
    };
    if *ended != session {
        return Err(format!(
            "attempt {number} ends the wrong session: expected `{session}`, found `{ended}`"
        ));
    }
    if events
        .iter()
        .filter(|(_, event)| matches!(event, AgentEvent::SessionEnded { .. }))
        .count()
        != 1
    {
        return Err(format!(
            "attempt {number} contains more than one `SessionEnded` event"
        ));
    }

    let start = match mode {
        TraceTurnMode::Fresh => {
            let Some((
                _,
                AgentEvent::SessionStarted {
                    session_id: started,
                    messages,
                },
            )) = events.first()
            else {
                return Err(format!(
                    "attempt {number} does not begin with a `SessionStarted` event"
                ));
            };
            if *started != session {
                return Err(format!(
                    "attempt {number} starts the wrong session: expected `{session}`, found `{started}`"
                ));
            }
            if events
                .iter()
                .filter(|(_, event)| matches!(event, AgentEvent::SessionStarted { .. }))
                .count()
                != 1
            {
                return Err(format!(
                    "attempt {number} contains more than one `SessionStarted` event"
                ));
            }
            if version < 2 {
                if !messages.is_empty() {
                    return Err(format!(
                        "version-1 attempt {number} carries bootstrap messages on `SessionStarted`"
                    ));
                }
                AttemptStart::Fresh {
                    initial_messages: turn_messages.to_vec(),
                }
            } else {
                AttemptStart::Fresh {
                    initial_messages: messages.clone(),
                }
            }
        }
        TraceTurnMode::Resumed => {
            if events
                .iter()
                .any(|(_, event)| matches!(event, AgentEvent::SessionStarted { .. }))
            {
                return Err(format!(
                    "attempt {number} contains a `SessionStarted` event in a resumed turn"
                ));
            }
            let Some((_, AgentEvent::RoundStarted { round })) = events.first() else {
                return Err(format!(
                    "resumed attempt {number} does not begin with a `RoundStarted` event"
                ));
            };
            let rounds_completed = round
                .checked_sub(1)
                .ok_or_else(|| format!("resumed attempt {number} starts at invalid round 0"))?;
            AttemptStart::Resumed {
                initial_messages: turn_messages.to_vec(),
                rounds_completed,
            }
        }
    };

    match reason {
        SessionEndReason::Completed | SessionEndReason::Error => {}
        SessionEndReason::BudgetExceeded => {
            if !events.iter().any(|(_, event)| {
                matches!(
                    event,
                    AgentEvent::BudgetExceeded {
                        kind: BudgetKind::MaxRounds,
                        ..
                    }
                )
            }) {
                return Err(format!(
                    "attempt {number} ended on a budget without a max-rounds event"
                ));
            }
        }
        SessionEndReason::Cancelled => {
            return Err(format!(
                "attempt {number} was cancelled; replay does not reproduce external cancellation"
            ));
        }
        SessionEndReason::AcceptanceFailed => {
            return Err(format!(
                "attempt {number} ended on acceptance; replay does not re-run acceptance checks"
            ));
        }
    }

    Ok(PreparedAttempt {
        number,
        session,
        start,
        reason: *reason,
        events,
    })
}

/// Returns whether the replay engine reproduces an event.
///
/// Trace-only and CLI-governance events are excluded because replay does not
/// re-run those producers. Their consequences remain in the recorded tool
/// results and terminal reasons that replay does consume.
fn is_replayable(event: &AgentEvent) -> bool {
    match event {
        AgentEvent::SessionStarted { .. }
        | AgentEvent::ModelDelta { .. }
        | AgentEvent::ToolCallStarted { .. }
        | AgentEvent::ToolCallFinished { .. }
        | AgentEvent::Error { .. }
        | AgentEvent::SessionEnded { .. }
        | AgentEvent::RoundStarted { .. }
        | AgentEvent::RoundFinished { .. } => true,
        AgentEvent::BudgetExceeded {
            kind: BudgetKind::MaxRounds,
            ..
        } => true,
        AgentEvent::BudgetExceeded { .. }
        | AgentEvent::PolicyDenied { .. }
        | AgentEvent::AcceptanceFailed { .. }
        | AgentEvent::PermissionDecided { .. }
        | AgentEvent::ChildSessionStarted { .. }
        | AgentEvent::ChildSessionEnded { .. } => false,
    }
}

/// Compares one replay result with its recorded events.
fn compare_attempt(
    attempt: &PreparedAttempt,
    actual: &[AgentEvent],
    problems: &mut Vec<ReplayProblem>,
) {
    let expected = &attempt.events;
    if expected.len() != actual.len() {
        problems.push(ReplayProblem::new(
            None,
            format!(
                "attempt {} replayed {} events but the trace records {}",
                attempt.number,
                actual.len(),
                expected.len()
            ),
        ));
    }
    for (index, ((seq, recorded), replayed)) in expected.iter().zip(actual).enumerate() {
        if recorded != replayed {
            problems.push(ReplayProblem::new(
                Some(*seq),
                format!(
                    "attempt {} event {} differs: trace has `{}`, replay produced `{}`",
                    attempt.number,
                    index + 1,
                    summarize(recorded),
                    summarize(replayed)
                ),
            ));
            return;
        }
    }
}

/// Checks the outer session pair, or the final attempt when no outer session exists.
fn check_outer_session(prepared: &Prepared, problems: &mut Vec<ReplayProblem>) {
    if prepared.outer.is_empty() {
        let Some(last) = prepared.attempts.last() else {
            return;
        };
        if last.reason != prepared.terminal {
            problems.push(ReplayProblem::new(
                None,
                format!(
                    "the final attempt ended as `{:?}` but `turn_ended` says `{:?}`",
                    last.reason, prepared.terminal
                ),
            ));
        }
        return;
    }

    if prepared.outer.len() != 2 {
        problems.push(ReplayProblem::new(
            prepared.outer.get(2).map(|(seq, _)| *seq),
            format!(
                "the outer session must contain exactly `SessionStarted` and `SessionEnded`, found {} events",
                prepared.outer.len()
            ),
        ));
        return;
    }

    let (start_seq, started) = &prepared.outer[0];
    let (end_seq, ended) = &prepared.outer[1];
    let AgentEvent::SessionStarted {
        session_id: started_session,
        messages,
    } = started
    else {
        problems.push(ReplayProblem::new(
            Some(*start_seq),
            "the outer session does not begin with `SessionStarted`".to_owned(),
        ));
        return;
    };
    let AgentEvent::SessionEnded {
        session_id: ended_session,
        reason,
    } = ended
    else {
        problems.push(ReplayProblem::new(
            Some(*end_seq),
            "the outer session does not end with `SessionEnded`".to_owned(),
        ));
        return;
    };
    if !messages.is_empty() {
        problems.push(ReplayProblem::new(
            Some(*start_seq),
            "the outer session must not carry bootstrap messages".to_owned(),
        ));
    }
    if started_session != ended_session {
        problems.push(ReplayProblem::new(
            Some(*end_seq),
            format!(
                "the outer session starts as `{started_session}` but ends as `{ended_session}`"
            ),
        ));
    }
    if *reason != prepared.terminal {
        problems.push(ReplayProblem::new(
            Some(*end_seq),
            format!(
                "the outer session ended as `{reason:?}` but `turn_ended` says `{:?}`",
                prepared.terminal
            ),
        ));
    }
}

/// Renders one event for a mismatch message, bounded in length.
fn summarize(event: &AgentEvent) -> String {
    let rendered =
        serde_json::to_string(event).unwrap_or_else(|error| format!("<unserializable: {error}>"));
    const LIMIT: usize = 320;
    if rendered.chars().count() <= LIMIT {
        return rendered;
    }
    let truncated: String = rendered.chars().take(LIMIT).collect();
    format!("{truncated}…")
}

#[cfg(test)]
mod tests {
    use super::*;

    use jessica_core::trace::{TraceEvent, TurnId};
    use jessica_core::types::{TokenUsage, ToolCall, ToolResult};
    use serde_json::json;

    fn turn() -> TurnId {
        TurnId::new("turn")
    }

    fn session(name: &str) -> SessionId {
        SessionId::new(name)
    }

    fn envelope(
        attempt: Option<u32>,
        session: Option<&SessionId>,
        payload: TracePayload,
    ) -> TraceEvent {
        TraceEvent::new(turn(), attempt, session.cloned(), payload)
    }

    fn started(mode: TraceTurnMode) -> (u64, TraceEvent) {
        (
            1,
            envelope(
                None,
                None,
                TracePayload::TurnStarted {
                    mode,
                    messages: vec![Message::user("do it")],
                },
            ),
        )
    }

    fn ended(seq: u64, reason: SessionEndReason) -> (u64, TraceEvent) {
        (
            seq,
            envelope(None, None, TracePayload::TurnEnded { reason }),
        )
    }

    fn attempt_agent(
        seq: u64,
        attempt: u32,
        session: &SessionId,
        event: AgentEvent,
    ) -> (u64, TraceEvent) {
        (
            seq,
            envelope(Some(attempt), Some(session), TracePayload::Agent { event }),
        )
    }

    fn outer_agent(seq: u64, session: &SessionId, event: AgentEvent) -> (u64, TraceEvent) {
        (
            seq,
            envelope(None, Some(session), TracePayload::Agent { event }),
        )
    }

    fn session_started(
        seq: u64,
        attempt: u32,
        session: &SessionId,
        text: &str,
    ) -> (u64, TraceEvent) {
        attempt_agent(
            seq,
            attempt,
            session,
            AgentEvent::SessionStarted {
                session_id: session.clone(),
                messages: vec![Message::user(text)],
            },
        )
    }

    fn session_ended(
        seq: u64,
        attempt: u32,
        session: &SessionId,
        reason: SessionEndReason,
    ) -> (u64, TraceEvent) {
        attempt_agent(
            seq,
            attempt,
            session,
            AgentEvent::SessionEnded {
                session_id: session.clone(),
                reason,
            },
        )
    }

    /// A completed attempt with one tool round and one final text round.
    fn completed_attempt(start: u64, attempt: u32, name: &str) -> Vec<(u64, TraceEvent)> {
        let session = session(name);
        let call = ToolCall::new("call-1", "echo", json!({"value": 1}));
        let usage = TokenUsage::new(11, 7);
        let mut entries = vec![
            session_started(start, attempt, &session, "do it"),
            attempt_agent(
                start + 1,
                attempt,
                &session,
                AgentEvent::RoundStarted { round: 1 },
            ),
            attempt_agent(
                start + 2,
                attempt,
                &session,
                AgentEvent::ModelDelta {
                    text: "calling".to_owned(),
                },
            ),
            attempt_agent(
                start + 3,
                attempt,
                &session,
                AgentEvent::RoundFinished {
                    round: 1,
                    usage: Some(usage),
                },
            ),
            attempt_agent(
                start + 4,
                attempt,
                &session,
                AgentEvent::ToolCallStarted { call: call.clone() },
            ),
            attempt_agent(
                start + 5,
                attempt,
                &session,
                AgentEvent::PermissionDecided {
                    call: call.clone(),
                    decision: jessica_core::types::PermissionDecision::Allow,
                },
            ),
            attempt_agent(
                start + 6,
                attempt,
                &session,
                AgentEvent::ToolCallFinished {
                    result: ToolResult::success("call-1", "ok"),
                },
            ),
            attempt_agent(
                start + 7,
                attempt,
                &session,
                AgentEvent::RoundStarted { round: 2 },
            ),
            attempt_agent(
                start + 8,
                attempt,
                &session,
                AgentEvent::ModelDelta {
                    text: "done".to_owned(),
                },
            ),
            attempt_agent(
                start + 9,
                attempt,
                &session,
                AgentEvent::RoundFinished {
                    round: 2,
                    usage: None,
                },
            ),
            session_ended(start + 10, attempt, &session, SessionEndReason::Completed),
        ];
        entries.push(ended(start + 11, SessionEndReason::Completed));
        entries
    }

    #[tokio::test]
    async fn a_completed_trace_replays_event_for_event() {
        let mut entries = vec![started(TraceTurnMode::Fresh)];
        entries.extend(completed_attempt(2, 1, "session-a"));
        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Ok, "{outcome:?}");
        assert_eq!(outcome.verification.attempts, 1);
        assert_eq!(
            outcome.verification.events, 10,
            "the permission decision is trace-only and must not be compared"
        );
        assert!(outcome.problems.is_empty());
    }

    #[tokio::test]
    async fn child_evidence_is_ignored_and_never_replayed() {
        let mut entries = vec![started(TraceTurnMode::Fresh)];
        entries.extend(completed_attempt(2, 1, "session-a"));
        let child = session("child");
        let child_entries = vec![
            envelope(
                Some(1),
                Some(&child),
                TracePayload::ChildAgent {
                    call_id: "call-1".to_owned(),
                    event: AgentEvent::SessionStarted {
                        session_id: child.clone(),
                        messages: vec![Message::system("child prompt"), Message::user("do it")],
                    },
                },
            ),
            envelope(
                Some(1),
                Some(&child),
                TracePayload::ChildAgent {
                    call_id: "call-1".to_owned(),
                    event: AgentEvent::RoundStarted { round: 1 },
                },
            ),
            envelope(
                Some(1),
                Some(&child),
                TracePayload::ChildAgent {
                    call_id: "call-1".to_owned(),
                    event: AgentEvent::RoundFinished {
                        round: 1,
                        usage: None,
                    },
                },
            ),
            envelope(
                Some(1),
                Some(&child),
                TracePayload::ChildAgent {
                    call_id: "call-1".to_owned(),
                    event: AgentEvent::SessionEnded {
                        session_id: child.clone(),
                        reason: SessionEndReason::Completed,
                    },
                },
            ),
        ];
        for (offset, trace) in child_entries.into_iter().enumerate() {
            entries.insert(4 + offset, (100 + offset as u64, trace));
        }

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Ok, "{outcome:?}");
        assert_eq!(outcome.verification.attempts, 1);
        assert_eq!(
            outcome.verification.events, 10,
            "child evidence is never executed or compared by replay"
        );
        assert!(outcome.problems.is_empty());
    }

    #[tokio::test]
    async fn trace_only_governance_events_do_not_break_parity() {
        let session = session("session-a");
        let call = ToolCall::new("call-1", "echo", json!({}));
        let entries = vec![
            started(TraceTurnMode::Fresh),
            session_started(2, 1, &session, "do it"),
            attempt_agent(3, 1, &session, AgentEvent::RoundStarted { round: 1 }),
            attempt_agent(
                4,
                1,
                &session,
                AgentEvent::RoundFinished {
                    round: 1,
                    usage: None,
                },
            ),
            attempt_agent(
                5,
                1,
                &session,
                AgentEvent::ToolCallStarted { call: call.clone() },
            ),
            attempt_agent(
                6,
                1,
                &session,
                AgentEvent::PolicyDenied {
                    call: call.clone(),
                    reason: "denied".to_owned(),
                },
            ),
            attempt_agent(
                7,
                1,
                &session,
                AgentEvent::PermissionDecided {
                    call: call.clone(),
                    decision: jessica_core::types::PermissionDecision::Deny,
                },
            ),
            attempt_agent(
                8,
                1,
                &session,
                AgentEvent::ChildSessionStarted {
                    child_session_id: SessionId::new("child"),
                    call_id: "call-1".to_owned(),
                },
            ),
            attempt_agent(
                9,
                1,
                &session,
                AgentEvent::ChildSessionEnded {
                    child_session_id: SessionId::new("child"),
                    call_id: "call-1".to_owned(),
                    reason: SessionEndReason::Completed,
                    rounds: 1,
                    tool_calls: 0,
                    tool_errors: 0,
                },
            ),
            attempt_agent(
                10,
                1,
                &session,
                AgentEvent::ToolCallFinished {
                    result: ToolResult::error("call-1", "denied"),
                },
            ),
            attempt_agent(11, 1, &session, AgentEvent::RoundStarted { round: 2 }),
            attempt_agent(
                12,
                1,
                &session,
                AgentEvent::RoundFinished {
                    round: 2,
                    usage: None,
                },
            ),
            session_ended(13, 1, &session, SessionEndReason::Completed),
            ended(14, SessionEndReason::Completed),
        ];

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Ok);
        assert_eq!(
            outcome.verification.events, 8,
            "policy, permission, and child events are excluded"
        );
    }

    #[tokio::test]
    async fn a_provider_error_is_replayed_exactly() {
        let session = session("session-a");
        let entries = vec![
            started(TraceTurnMode::Fresh),
            session_started(2, 1, &session, "fail"),
            attempt_agent(3, 1, &session, AgentEvent::RoundStarted { round: 1 }),
            attempt_agent(
                4,
                1,
                &session,
                AgentEvent::Error {
                    message: "provider failed".to_owned(),
                },
            ),
            session_ended(5, 1, &session, SessionEndReason::Error),
            ended(6, SessionEndReason::Error),
        ];

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Ok, "{outcome:?}");
        assert!(outcome.problems.is_empty(), "{outcome:?}");
    }

    #[tokio::test]
    async fn max_round_budget_is_replayed_exactly() {
        let session = session("session-a");
        let call = ToolCall::new("call-1", "echo", json!({}));
        let entries = vec![
            started(TraceTurnMode::Fresh),
            session_started(2, 1, &session, "loop"),
            attempt_agent(3, 1, &session, AgentEvent::RoundStarted { round: 1 }),
            attempt_agent(
                4,
                1,
                &session,
                AgentEvent::ModelDelta {
                    text: "calling".to_owned(),
                },
            ),
            attempt_agent(
                5,
                1,
                &session,
                AgentEvent::RoundFinished {
                    round: 1,
                    usage: None,
                },
            ),
            attempt_agent(
                6,
                1,
                &session,
                AgentEvent::ToolCallStarted { call: call.clone() },
            ),
            attempt_agent(
                7,
                1,
                &session,
                AgentEvent::ToolCallFinished {
                    result: ToolResult::success("call-1", "ok"),
                },
            ),
            attempt_agent(
                8,
                1,
                &session,
                AgentEvent::BudgetExceeded {
                    kind: BudgetKind::MaxRounds,
                    message: "maximum rounds (1) exceeded".to_owned(),
                },
            ),
            session_ended(9, 1, &session, SessionEndReason::BudgetExceeded),
            ended(10, SessionEndReason::BudgetExceeded),
        ];

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Ok, "{outcome:?}");
        assert!(outcome.problems.is_empty(), "{outcome:?}");
    }

    #[tokio::test]
    async fn a_tampered_budget_message_is_reported_at_its_sequence() {
        let session = session("session-a");
        let call = ToolCall::new("call-1", "echo", json!({}));
        let entries = vec![
            started(TraceTurnMode::Fresh),
            session_started(2, 1, &session, "loop"),
            attempt_agent(3, 1, &session, AgentEvent::RoundStarted { round: 1 }),
            attempt_agent(
                4,
                1,
                &session,
                AgentEvent::ModelDelta {
                    text: "calling".to_owned(),
                },
            ),
            attempt_agent(
                5,
                1,
                &session,
                AgentEvent::RoundFinished {
                    round: 1,
                    usage: None,
                },
            ),
            attempt_agent(
                6,
                1,
                &session,
                AgentEvent::ToolCallStarted { call: call.clone() },
            ),
            attempt_agent(
                7,
                1,
                &session,
                AgentEvent::ToolCallFinished {
                    result: ToolResult::success("call-1", "ok"),
                },
            ),
            attempt_agent(
                8,
                1,
                &session,
                AgentEvent::BudgetExceeded {
                    kind: BudgetKind::MaxRounds,
                    message: "maximum rounds (9) exceeded".to_owned(),
                },
            ),
            session_ended(9, 1, &session, SessionEndReason::BudgetExceeded),
            ended(10, SessionEndReason::BudgetExceeded),
        ];

        let outcome = verify(&entries).await;

        assert_eq!(
            outcome.verification.status,
            ReplayStatus::Mismatch,
            "{outcome:?}"
        );
        assert_eq!(outcome.problems.len(), 1, "{outcome:?}");
        assert_eq!(outcome.problems[0].seq, Some(8), "{outcome:?}");
        assert_eq!(outcome.problems[0].kind, "replay_mismatch");
    }

    #[tokio::test]
    async fn an_inconsistent_terminal_reason_is_a_mismatch() {
        let mut entries = vec![started(TraceTurnMode::Fresh)];
        entries.extend(completed_attempt(2, 1, "session-a"));
        let last = entries.len() - 1;
        let turn_ended_seq = entries[last].0;
        entries[last] = ended(turn_ended_seq, SessionEndReason::Error);
        // Replace the attempt's terminal reason with `Error` while leaving the
        // event stream completed; the replay loop cannot produce that terminal.
        let session_index = entries
            .iter()
            .position(|(_, trace)| {
                matches!(
                    &trace.payload,
                    TracePayload::Agent {
                        event: AgentEvent::SessionEnded {
                            reason: SessionEndReason::Completed,
                            ..
                        }
                    }
                )
            })
            .expect("fixture has a completed attempt");
        let TracePayload::Agent { event } = &mut entries[session_index].1.payload else {
            unreachable!("the fixture entry is an agent event");
        };
        *event = AgentEvent::SessionEnded {
            session_id: session("session-a"),
            reason: SessionEndReason::Error,
        };

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Mismatch);
        assert!(!outcome.problems.is_empty());
    }

    #[tokio::test]
    async fn acceptance_attempts_and_the_outer_session_are_checked() {
        let second = session("attempt-two");
        let outer = session("outer");
        let mut entries = vec![started(TraceTurnMode::Fresh)];
        entries.extend(completed_attempt(2, 1, "attempt-one"));
        // Drop the first attempt's turn-ended marker; this fixture has one
        // turn-level terminal at the end.
        entries.pop();
        entries.push(attempt_agent(
            20,
            2,
            &second,
            AgentEvent::SessionStarted {
                session_id: second.clone(),
                messages: vec![Message::user("try again")],
            },
        ));
        entries.push(attempt_agent(
            21,
            2,
            &second,
            AgentEvent::RoundStarted { round: 1 },
        ));
        entries.push(attempt_agent(
            22,
            2,
            &second,
            AgentEvent::RoundFinished {
                round: 1,
                usage: None,
            },
        ));
        entries.push(session_ended(23, 2, &second, SessionEndReason::Completed));
        entries.push(outer_agent(
            24,
            &outer,
            AgentEvent::SessionStarted {
                session_id: outer.clone(),
                messages: Vec::new(),
            },
        ));
        entries.push(outer_agent(
            25,
            &outer,
            AgentEvent::SessionEnded {
                session_id: outer.clone(),
                reason: SessionEndReason::AcceptanceFailed,
            },
        ));
        entries.push(ended(26, SessionEndReason::AcceptanceFailed));

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Ok);
        assert_eq!(outcome.verification.attempts, 2);
    }

    #[tokio::test]
    async fn an_outer_session_reason_must_match_turn_ended() {
        let outer = session("outer");
        let mut entries = vec![started(TraceTurnMode::Fresh)];
        entries.extend(completed_attempt(2, 1, "session-a"));
        entries.pop();
        entries.push(outer_agent(
            20,
            &outer,
            AgentEvent::SessionStarted {
                session_id: outer.clone(),
                messages: Vec::new(),
            },
        ));
        entries.push(outer_agent(
            21,
            &outer,
            AgentEvent::SessionEnded {
                session_id: outer.clone(),
                reason: SessionEndReason::Completed,
            },
        ));
        entries.push(ended(22, SessionEndReason::AcceptanceFailed));

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Mismatch);
        assert!(
            outcome
                .problems
                .iter()
                .any(|problem| problem.message.contains("outer session ended"))
        );
    }

    #[tokio::test]
    async fn version_one_traces_replay_from_turn_started_bootstrap() {
        let session = session("session-a");
        let mut entries = vec![
            started(TraceTurnMode::Fresh),
            attempt_agent(
                2,
                1,
                &session,
                AgentEvent::SessionStarted {
                    session_id: session.clone(),
                    messages: Vec::new(),
                },
            ),
            attempt_agent(
                3,
                1,
                &session,
                AgentEvent::ModelDelta {
                    text: "legacy".to_owned(),
                },
            ),
            session_ended(4, 1, &session, SessionEndReason::Completed),
            ended(5, SessionEndReason::Completed),
        ];
        for (_, trace) in &mut entries {
            trace.schema_version = 1;
        }

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Ok, "{outcome:?}");
        assert_eq!(outcome.verification.attempts, 1);
        assert_eq!(outcome.verification.events, 3);
        assert!(outcome.problems.is_empty(), "{outcome:?}");
    }

    #[tokio::test]
    async fn a_resumed_trace_replays_from_its_recorded_checkpoint() {
        let session = session("session-a");
        let entries = vec![
            (
                1,
                envelope(
                    None,
                    None,
                    TracePayload::TurnStarted {
                        mode: TraceTurnMode::Resumed,
                        messages: vec![Message::user("continue")],
                    },
                ),
            ),
            attempt_agent(2, 1, &session, AgentEvent::RoundStarted { round: 3 }),
            attempt_agent(
                3,
                1,
                &session,
                AgentEvent::ModelDelta {
                    text: "done".to_owned(),
                },
            ),
            attempt_agent(
                4,
                1,
                &session,
                AgentEvent::RoundFinished {
                    round: 3,
                    usage: None,
                },
            ),
            session_ended(5, 1, &session, SessionEndReason::Completed),
            ended(6, SessionEndReason::Completed),
        ];

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Ok, "{outcome:?}");
        assert_eq!(outcome.verification.attempts, 1);
        assert_eq!(outcome.verification.events, 4);
        assert!(outcome.problems.is_empty(), "{outcome:?}");
    }

    #[tokio::test]
    async fn resumed_traces_without_a_model_round_are_unsupported() {
        let session = session("session-a");
        let entries = vec![
            (
                1,
                envelope(
                    None,
                    None,
                    TracePayload::TurnStarted {
                        mode: TraceTurnMode::Resumed,
                        messages: vec![Message::user("continue")],
                    },
                ),
            ),
            session_ended(2, 1, &session, SessionEndReason::Completed),
            ended(3, SessionEndReason::Completed),
        ];

        let outcome = verify(&entries).await;

        assert_eq!(outcome.verification.status, ReplayStatus::Unsupported);
        assert!(
            outcome
                .verification
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("does not begin with a `RoundStarted`"))
        );
    }

    #[tokio::test]
    async fn cancelled_and_governance_budget_traces_are_unsupported() {
        let session = session("session-a");

        let cancelled = vec![
            started(TraceTurnMode::Fresh),
            session_started(2, 1, &session, "do it"),
            attempt_agent(3, 1, &session, AgentEvent::RoundStarted { round: 1 }),
            session_ended(4, 1, &session, SessionEndReason::Cancelled),
            ended(5, SessionEndReason::Cancelled),
        ];
        let outcome = verify(&cancelled).await;
        assert_eq!(outcome.verification.status, ReplayStatus::Unsupported);
        assert!(
            outcome
                .verification
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("cancelled"))
        );

        let token_budget = vec![
            started(TraceTurnMode::Fresh),
            session_started(2, 1, &session, "do it"),
            attempt_agent(3, 1, &session, AgentEvent::RoundStarted { round: 1 }),
            attempt_agent(
                4,
                1,
                &session,
                AgentEvent::BudgetExceeded {
                    kind: BudgetKind::TokenLimit,
                    message: "token budget exhausted".to_owned(),
                },
            ),
            session_ended(5, 1, &session, SessionEndReason::BudgetExceeded),
            ended(6, SessionEndReason::BudgetExceeded),
        ];
        let outcome = verify(&token_budget).await;
        assert_eq!(outcome.verification.status, ReplayStatus::Unsupported);
        assert!(
            outcome
                .verification
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("TokenLimit"))
        );
    }

    #[tokio::test]
    async fn malformed_attempts_are_unsupported() {
        let session = session("session-a");

        let missing_start = vec![
            started(TraceTurnMode::Fresh),
            attempt_agent(2, 1, &session, AgentEvent::RoundStarted { round: 1 }),
            session_ended(3, 1, &session, SessionEndReason::Completed),
            ended(4, SessionEndReason::Completed),
        ];
        assert_eq!(
            verify(&missing_start).await.verification.status,
            ReplayStatus::Unsupported
        );

        let gap_in_attempts = vec![
            started(TraceTurnMode::Fresh),
            session_started(2, 1, &session, "do it"),
            session_ended(3, 1, &session, SessionEndReason::Completed),
            attempt_agent(
                4,
                3,
                &session,
                AgentEvent::SessionStarted {
                    session_id: session.clone(),
                    messages: vec![Message::user("third")],
                },
            ),
            session_ended(5, 3, &session, SessionEndReason::Completed),
            ended(6, SessionEndReason::Completed),
        ];
        let outcome = verify(&gap_in_attempts).await;
        assert_eq!(outcome.verification.status, ReplayStatus::Unsupported);
        assert!(
            outcome
                .verification
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("contiguous"))
        );
    }
}
