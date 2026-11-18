//! Versioned trace storage layered beside the legacy event log.
//!
//! A turn trace is identified by a [`TurnId`] and consists of one-based,
//! contiguous entries. [`EventStore::append_traced`] writes an optional legacy
//! [`AgentEvent`] and the trace entry in the same SQLite transaction, so a
//! trace can never claim an event that the legacy log rejected, or vice versa.
//!
//! Reads validate the stored rows rather than trusting them: sequence numbers
//! must be contiguous, the envelope must agree with every mirrored column, the
//! first entry must be `turn_started`, and `closed` must be true exactly when
//! the last entry is `turn_ended`. A trace with no `turn_ended` remains
//! readable and reports `closed = false`; it is never silently upgraded to a
//! complete trace.

use jessica_core::trace::{TRACE_SCHEMA_VERSION, TraceEvent, TracePayload, TurnId};
use jessica_core::types::{AgentEvent, SessionId};
use rusqlite::{Connection, OptionalExtension, params};

use crate::{EventStore, EventStoreError, Timestamp, append_in_transaction, event_type_tag};

/// Trace tables and their append-only guards.
///
/// `trace_heads` is mutable only to advance `last_seq` and to flip `closed`
/// from zero to one; triggers reject deletes, rewinds, and reopening.
pub(crate) const TRACE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS trace_events (
    turn_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    payload_kind TEXT NOT NULL,
    event_type TEXT,
    attempt INTEGER,
    session_id TEXT,
    payload_json TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    PRIMARY KEY (turn_id, seq)
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS trace_events_append_only_update
BEFORE UPDATE ON trace_events
BEGIN
    SELECT RAISE(ABORT, 'trace events are append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trace_events_append_only_delete
BEFORE DELETE ON trace_events
BEGIN
    SELECT RAISE(ABORT, 'trace events are append-only: DELETE is not allowed');
END;

CREATE TABLE IF NOT EXISTS trace_heads (
    turn_id TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL CHECK (last_seq >= 1),
    closed INTEGER NOT NULL CHECK (closed IN (0, 1)),
    created_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS trace_heads_advance_only
BEFORE UPDATE ON trace_heads
WHEN NEW.turn_id <> OLD.turn_id
    OR NEW.last_seq <= OLD.last_seq
    OR NEW.closed < OLD.closed
    OR NEW.created_at <> OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'trace heads only advance');
END;

CREATE TRIGGER IF NOT EXISTS trace_heads_append_only_delete
BEFORE DELETE ON trace_heads
BEGIN
    SELECT RAISE(ABORT, 'trace heads are append-only: DELETE is not allowed');
END;
"#;

/// Installs the trace tables on `connection`.
pub(crate) fn install(connection: &Connection) -> Result<(), EventStoreError> {
    connection
        .execute_batch(TRACE_SCHEMA)
        .map_err(EventStoreError::Schema)
}

/// One trace entry recovered from the store.
///
/// The exact JSON text is retained so audit and replay paths can reproduce the
/// persisted bytes without re-serializing.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTraceEvent {
    /// Turn this entry belongs to.
    turn_id: TurnId,
    /// One-based position within the turn.
    seq: u64,
    /// Exact JSON text persisted in the store.
    payload_json: String,
    /// Caller-supplied entry timestamp.
    timestamp: Timestamp,
    /// Decoded envelope.
    event: TraceEvent,
}

impl StoredTraceEvent {
    /// Returns the turn this entry belongs to.
    pub fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    /// Returns the one-based position within the turn's trace.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Returns the exact JSON text persisted in the store.
    pub fn payload_json(&self) -> &str {
        &self.payload_json
    }

    /// Returns the caller-supplied entry timestamp.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the decoded trace envelope.
    pub fn event(&self) -> &TraceEvent {
        &self.event
    }
}

/// A complete, validated read of one turn trace.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTraceTurn {
    /// Turn that was read.
    turn_id: TurnId,
    /// Whether the trace ended with `turn_ended` at the time of the read.
    closed: bool,
    /// Entries in ascending sequence order.
    events: Vec<StoredTraceEvent>,
}

impl StoredTraceTurn {
    /// Returns the turn that was read.
    pub fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    /// Returns whether the trace ends with `turn_ended`.
    ///
    /// `false` means the trace is incomplete, for example because the process
    /// stopped or a persistence failure prevented the closing marker.
    pub fn closed(&self) -> bool {
        self.closed
    }

    /// Returns the entries in ascending sequence order.
    pub fn events(&self) -> &[StoredTraceEvent] {
        &self.events
    }
}

/// Head metadata for one stored turn, without decoding its entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceTurnSummary {
    /// Turn this summary describes.
    turn_id: TurnId,
    /// Highest stored sequence number.
    last_seq: u64,
    /// Whether the trace ends with `turn_ended`.
    closed: bool,
    /// Timestamp of the turn's first entry.
    created_at: Timestamp,
}

impl TraceTurnSummary {
    /// Returns the turn this summary describes.
    pub fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    /// Returns the highest stored sequence number.
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// Returns whether the trace ends with `turn_ended`.
    pub fn closed(&self) -> bool {
        self.closed
    }

    /// Returns the timestamp of the turn's first entry.
    pub fn created_at(&self) -> Timestamp {
        self.created_at
    }
}

impl EventStore {
    /// Appends one trace entry, optionally mirroring it into the legacy log.
    ///
    /// The first entry for a turn must be [`TracePayload::TurnStarted`], the
    /// last may be [`TracePayload::TurnEnded`], and no entry may follow the
    /// terminal marker. `legacy` is required for agent payloads that belong to
    /// a session, except [`AgentEvent::AcceptanceFailed`], which is deliberately
    /// trace-only: writing it to the legacy stream would place an event after
    /// that attempt's `session_ended` and break recovery. When a legacy event is
    /// supplied, both writes commit in one transaction or neither does.
    ///
    /// The returned entry carries the allocated sequence number and the exact
    /// JSON text persisted.
    pub fn append_traced(
        &mut self,
        trace: &TraceEvent,
        legacy: Option<(&SessionId, &AgentEvent)>,
        timestamp: Timestamp,
    ) -> Result<StoredTraceEvent, EventStoreError> {
        validate_envelope(trace)?;
        let turn_id = trace.turn_id.to_string();
        let payload_json =
            serde_json::to_string(trace).map_err(|source| EventStoreError::TraceSerialize {
                turn_id: turn_id.clone(),
                source,
            })?;
        let payload_kind = trace.payload.kind();
        let event_type = match &trace.payload {
            TracePayload::Agent { event } => Some(event_type_tag(event)),
            TracePayload::Gap { .. }
            | TracePayload::TurnStarted { .. }
            | TracePayload::TurnEnded { .. } => None,
        };
        validate_legacy_mirror(trace, legacy)?;

        let transaction = self
            .connection
            .transaction()
            .map_err(|source| EventStoreError::TraceDatabase { source })?;
        let head = transaction
            .query_row(
                "SELECT last_seq, closed FROM trace_heads WHERE turn_id = ?1",
                [turn_id.as_str()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(|source| store_error(&turn_id, source))?;

        let (seq, closed) = match head {
            None => {
                if !matches!(&trace.payload, TracePayload::TurnStarted { .. }) {
                    return Err(lifecycle_error(
                        &turn_id,
                        "the first trace entry must be `turn_started`",
                    ));
                }
                (1, false)
            }
            Some((last_seq, closed)) => {
                if closed != 0 && closed != 1 {
                    return Err(lifecycle_error(
                        &turn_id,
                        "the stored trace head has an invalid `closed` flag",
                    ));
                }
                if closed == 1 {
                    return Err(lifecycle_error(
                        &turn_id,
                        "the trace already ended and accepts no further entries",
                    ));
                }
                if matches!(&trace.payload, TracePayload::TurnStarted { .. }) {
                    return Err(lifecycle_error(
                        &turn_id,
                        "the trace already has a `turn_started` entry",
                    ));
                }
                let last_seq = u64::try_from(last_seq).map_err(|_| {
                    lifecycle_error(&turn_id, "the stored trace head has a negative sequence")
                })?;
                if last_seq == 0 {
                    return Err(lifecycle_error(
                        &turn_id,
                        "the stored trace head has a zero sequence",
                    ));
                }
                let seq = last_seq.checked_add(1).ok_or_else(|| {
                    lifecycle_error(&turn_id, "the trace sequence would overflow")
                })?;
                (
                    seq,
                    matches!(&trace.payload, TracePayload::TurnEnded { .. }),
                )
            }
        };

        if let Some((session_id, event)) = legacy {
            append_in_transaction(&transaction, session_id, event, timestamp)?;
        }

        let seq_value = i64::try_from(seq)
            .map_err(|_| lifecycle_error(&turn_id, "the trace sequence is out of range"))?;
        transaction
            .execute(
                "INSERT INTO trace_events (\
                     turn_id, seq, schema_version, payload_kind, event_type, attempt, session_id, \
                     payload_json, timestamp\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    turn_id.as_str(),
                    seq_value,
                    i64::from(trace.schema_version),
                    payload_kind,
                    event_type,
                    trace.attempt.map(i64::from),
                    trace.session_id.as_ref().map(SessionId::as_str),
                    payload_json,
                    timestamp.as_millis(),
                ],
            )
            .map_err(|source| store_error(&turn_id, source))?;
        transaction
            .execute(
                "INSERT INTO trace_heads (turn_id, last_seq, closed, created_at) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(turn_id) DO UPDATE SET \
                     last_seq = excluded.last_seq, closed = excluded.closed",
                params![
                    turn_id.as_str(),
                    seq_value,
                    i64::from(closed),
                    timestamp.as_millis()
                ],
            )
            .map_err(|source| store_error(&turn_id, source))?;
        transaction
            .commit()
            .map_err(|source| EventStoreError::TraceDatabase { source })?;

        Ok(StoredTraceEvent {
            turn_id: trace.turn_id.clone(),
            seq,
            payload_json,
            timestamp,
            event: trace.clone(),
        })
    }

    /// Reads and validates a turn trace.
    ///
    /// Returns `None` when the turn has never been recorded. An incomplete
    /// trace is returned with [`StoredTraceTurn::closed`] set to `false`; a
    /// structurally corrupt trace returns an error instead of partial data.
    pub fn trace_turn(&self, turn_id: &TurnId) -> Result<Option<StoredTraceTurn>, EventStoreError> {
        let turn = turn_id.as_str();
        let head: Option<(i64, i64)> = self
            .connection
            .query_row(
                "SELECT last_seq, closed FROM trace_heads WHERE turn_id = ?1",
                [turn],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|source| store_error(turn, source))?;
        let Some((last_seq_value, closed_value)) = head else {
            return Ok(None);
        };
        let last_seq = u64::try_from(last_seq_value)
            .map_err(|_| lifecycle_error(turn, "the stored trace head has a negative sequence"))?;
        let closed = match closed_value {
            0 => false,
            1 => true,
            _ => {
                return Err(lifecycle_error(
                    turn,
                    "the stored trace head has an invalid `closed` flag",
                ));
            }
        };

        let mut statement = self
            .connection
            .prepare(
                "SELECT seq, schema_version, payload_kind, event_type, attempt, session_id, \
                        payload_json, timestamp \
                 FROM trace_events WHERE turn_id = ?1 ORDER BY seq ASC",
            )
            .map_err(|source| store_error(turn, source))?;
        let mut rows = statement
            .query([turn])
            .map_err(|source| store_error(turn, source))?;

        let mut events = Vec::new();
        let mut expected = 1_u64;
        let mut ended_at: Option<u64> = None;
        while let Some(row) = rows.next().map_err(|source| store_error(turn, source))? {
            let seq_value: i64 = row.get(0).map_err(|source| store_error(turn, source))?;
            let seq = u64::try_from(seq_value)
                .map_err(|_| invalid_error(turn, 0, "a stored sequence is negative"))?;
            if seq != expected {
                return Err(EventStoreError::TraceSequence {
                    turn_id: turn.to_owned(),
                    expected,
                    found: seq,
                });
            }
            if ended_at.is_some() {
                return Err(lifecycle_error(turn, "an entry follows `turn_ended`"));
            }

            let schema_version_value: i64 =
                row.get(1).map_err(|source| store_error(turn, source))?;
            let schema_version = u16::try_from(schema_version_value).map_err(|_| {
                invalid_error(turn, seq, "the stored schema version is out of range")
            })?;
            let payload_kind: String = row.get(2).map_err(|source| store_error(turn, source))?;
            let event_type: Option<String> =
                row.get(3).map_err(|source| store_error(turn, source))?;
            let attempt_value: Option<i64> =
                row.get(4).map_err(|source| store_error(turn, source))?;
            let session_value: Option<String> =
                row.get(5).map_err(|source| store_error(turn, source))?;
            let payload_json: String = row.get(6).map_err(|source| store_error(turn, source))?;
            let timestamp_millis: i64 = row.get(7).map_err(|source| store_error(turn, source))?;

            let event: TraceEvent = serde_json::from_str(&payload_json).map_err(|source| {
                EventStoreError::TraceDecode {
                    turn_id: turn.to_owned(),
                    seq,
                    source,
                }
            })?;
            validate_envelope(&event)?;
            if schema_version != event.schema_version {
                return Err(invalid_error(
                    turn,
                    seq,
                    "the stored schema version disagrees with the payload",
                ));
            }
            if event.turn_id.as_str() != turn {
                return Err(invalid_error(
                    turn,
                    seq,
                    "the stored turn id disagrees with the payload",
                ));
            }
            if event.payload.kind() != payload_kind {
                return Err(invalid_error(
                    turn,
                    seq,
                    "the stored payload kind disagrees with the payload",
                ));
            }
            let attempt =
                match attempt_value {
                    None => None,
                    Some(value) => Some(u32::try_from(value).map_err(|_| {
                        invalid_error(turn, seq, "the stored attempt is out of range")
                    })?),
                };
            if attempt != event.attempt {
                return Err(invalid_error(
                    turn,
                    seq,
                    "the stored attempt disagrees with the payload",
                ));
            }
            let session_matches = match (&session_value, &event.session_id) {
                (None, None) => true,
                (Some(stored), Some(decoded)) => stored == decoded.as_str(),
                _ => false,
            };
            if !session_matches {
                return Err(invalid_error(
                    turn,
                    seq,
                    "the stored session id disagrees with the payload",
                ));
            }
            match (&event.payload, &event_type) {
                (TracePayload::Agent { event: agent }, Some(stored)) => {
                    if event_type_tag(agent) != stored.as_str() {
                        return Err(invalid_error(
                            turn,
                            seq,
                            "the stored event type disagrees with the payload",
                        ));
                    }
                }
                (TracePayload::Agent { .. }, None) => {
                    return Err(invalid_error(
                        turn,
                        seq,
                        "an agent payload has no stored event type",
                    ));
                }
                (_, None) => {}
                (_, Some(_)) => {
                    return Err(invalid_error(
                        turn,
                        seq,
                        "a non-agent payload has a stored event type",
                    ));
                }
            }

            match &event.payload {
                TracePayload::TurnStarted { .. } => {
                    if seq != 1 {
                        return Err(lifecycle_error(
                            turn,
                            "`turn_started` must be the first entry",
                        ));
                    }
                }
                TracePayload::TurnEnded { .. } => {
                    ended_at = Some(seq);
                }
                TracePayload::Agent { .. } | TracePayload::Gap { .. } => {}
            }

            events.push(StoredTraceEvent {
                turn_id: event.turn_id.clone(),
                seq,
                payload_json,
                timestamp: Timestamp::from_millis(timestamp_millis),
                event,
            });
            expected += 1;
        }

        let count = u64::try_from(events.len())
            .map_err(|_| lifecycle_error(turn, "the trace has too many entries to count"))?;
        if count != last_seq {
            return Err(lifecycle_error(
                turn,
                "the stored trace head disagrees with the number of entries",
            ));
        }
        if events.is_empty()
            || !events.first().is_some_and(|entry| {
                matches!(&entry.event.payload, TracePayload::TurnStarted { .. })
            })
        {
            return Err(lifecycle_error(
                turn,
                "the trace does not begin with `turn_started`",
            ));
        }
        if ended_at.is_some() != closed {
            return Err(lifecycle_error(
                turn,
                "the stored `closed` flag disagrees with the terminal entry",
            ));
        }

        Ok(Some(StoredTraceTurn {
            turn_id: turn_id.clone(),
            closed,
            events,
        }))
    }

    /// Lists stored turn heads in descending creation order.
    ///
    /// At most `limit` summaries are returned; `limit == 0` returns an empty
    /// vector. Incomplete traces are included and report `closed = false`.
    pub fn trace_turns(&self, limit: usize) -> Result<Vec<TraceTurnSummary>, EventStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut statement = self
            .connection
            .prepare(
                "SELECT turn_id, last_seq, closed, created_at FROM trace_heads \
                 ORDER BY created_at DESC, turn_id DESC LIMIT ?1",
            )
            .map_err(|source| EventStoreError::TraceDatabase { source })?;
        let mut rows = statement
            .query([limit])
            .map_err(|source| EventStoreError::TraceDatabase { source })?;
        let mut turns = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|source| EventStoreError::TraceDatabase { source })?
        {
            let turn_id: String = row
                .get(0)
                .map_err(|source| EventStoreError::TraceDatabase { source })?;
            let last_seq_value: i64 = row.get(1).map_err(|source| store_error(&turn_id, source))?;
            let closed_value: i64 = row.get(2).map_err(|source| store_error(&turn_id, source))?;
            let created_at: i64 = row.get(3).map_err(|source| store_error(&turn_id, source))?;
            let last_seq = u64::try_from(last_seq_value).map_err(|_| {
                lifecycle_error(&turn_id, "the stored trace head has a negative sequence")
            })?;
            let closed = match closed_value {
                0 => false,
                1 => true,
                _ => {
                    return Err(lifecycle_error(
                        &turn_id,
                        "the stored trace head has an invalid `closed` flag",
                    ));
                }
            };
            turns.push(TraceTurnSummary {
                turn_id: TurnId::new(turn_id),
                last_seq,
                closed,
                created_at: Timestamp::from_millis(created_at),
            });
        }
        Ok(turns)
    }
}

/// Validates the context rules an envelope must satisfy before storage.
fn validate_envelope(trace: &TraceEvent) -> Result<(), EventStoreError> {
    let turn_id = trace.turn_id.to_string();
    if turn_id.is_empty() {
        return Err(invalid_error(&turn_id, 0, "the turn id must not be empty"));
    }
    if trace.schema_version != TRACE_SCHEMA_VERSION {
        return Err(invalid_error(
            &turn_id,
            0,
            &format!(
                "unsupported trace schema version {}; this build supports {}",
                trace.schema_version, TRACE_SCHEMA_VERSION
            ),
        ));
    }
    match &trace.payload {
        TracePayload::TurnStarted { .. } => {
            if trace.attempt.is_some() {
                return Err(invalid_error(
                    &turn_id,
                    0,
                    "`turn_started` must not be attributed to an attempt",
                ));
            }
            if trace.session_id.is_none() {
                return Err(invalid_error(
                    &turn_id,
                    0,
                    "`turn_started` must name the first session",
                ));
            }
        }
        TracePayload::TurnEnded { .. } => {
            if trace.attempt.is_some() {
                return Err(invalid_error(
                    &turn_id,
                    0,
                    "`turn_ended` must not be attributed to an attempt",
                ));
            }
            if trace.session_id.is_some() {
                return Err(invalid_error(
                    &turn_id,
                    0,
                    "`turn_ended` must not be attributed to a session",
                ));
            }
        }
        TracePayload::Agent { .. } => {}
        TracePayload::Gap { skipped, .. } => {
            if *skipped == 0 {
                return Err(invalid_error(
                    &turn_id,
                    0,
                    "a gap must report at least one skipped event",
                ));
            }
        }
    }
    Ok(())
}

/// Enforces the relationship between a trace entry and its legacy mirror.
fn validate_legacy_mirror(
    trace: &TraceEvent,
    legacy: Option<(&SessionId, &AgentEvent)>,
) -> Result<(), EventStoreError> {
    let turn_id = trace.turn_id.to_string();
    match (&trace.payload, legacy) {
        (
            TracePayload::Agent {
                event: AgentEvent::AcceptanceFailed { .. },
            },
            Some(_),
        ) => Err(invalid_error(
            &turn_id,
            0,
            "an acceptance failure must be trace-only",
        )),
        (TracePayload::Agent { event }, Some((session_id, legacy_event))) => {
            if Some(session_id) != trace.session_id.as_ref() {
                return Err(invalid_error(
                    &turn_id,
                    0,
                    "the legacy session does not match the trace envelope",
                ));
            }
            if event != legacy_event {
                return Err(invalid_error(
                    &turn_id,
                    0,
                    "the legacy event does not match the trace payload",
                ));
            }
            Ok(())
        }
        (TracePayload::Agent { event }, None) => {
            if matches!(event, AgentEvent::AcceptanceFailed { .. }) {
                return Ok(());
            }
            if trace.session_id.is_some() {
                return Err(invalid_error(
                    &turn_id,
                    0,
                    "an agent payload that names a session must be mirrored into the legacy log",
                ));
            }
            Ok(())
        }
        (_, Some(_)) => Err(invalid_error(
            &turn_id,
            0,
            "only agent payloads may mirror a legacy event",
        )),
        (_, None) => Ok(()),
    }
}

/// Builds a per-turn trace storage error.
fn store_error(turn_id: &str, source: rusqlite::Error) -> EventStoreError {
    EventStoreError::TraceStore {
        turn_id: turn_id.to_owned(),
        source,
    }
}

/// Builds a trace-contract error.
fn invalid_error(turn_id: &str, seq: u64, message: &str) -> EventStoreError {
    EventStoreError::TraceInvalid {
        turn_id: turn_id.to_owned(),
        seq,
        message: message.to_owned(),
    }
}

/// Builds a trace-lifecycle error.
fn lifecycle_error(turn_id: &str, message: &str) -> EventStoreError {
    EventStoreError::TraceLifecycle {
        turn_id: turn_id.to_owned(),
        message: message.to_owned(),
    }
}
