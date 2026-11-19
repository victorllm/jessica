//! Behavioral tests for portable trace evidence validation.

use jessica_core::trace::{TraceEvent, TracePayload, TraceTurnMode, TurnId};
use jessica_core::types::{AgentEvent, SessionEndReason, SessionId};
use jessica_memory::{
    EventStore, EventStoreError, StoredTraceTurn, Timestamp, TraceEvidenceEntry,
    trace_bundle_digest, verify_exported_trace,
};

/// Builds the evidence entries carried by a bundle from a validated trace.
fn evidence(trace: &StoredTraceTurn) -> Vec<TraceEvidenceEntry> {
    trace
        .events()
        .iter()
        .map(|entry| {
            TraceEvidenceEntry::new(
                entry.seq(),
                entry.timestamp(),
                entry.payload_json().to_owned(),
            )
        })
        .collect()
}

/// Appends a complete three-entry turn.
fn append_turn(store: &mut EventStore, turn: &TurnId, session: &SessionId, base: i64) {
    let started = TraceEvent::new(
        turn.clone(),
        None,
        Some(session.clone()),
        TracePayload::TurnStarted {
            mode: TraceTurnMode::Fresh,
            messages: Vec::new(),
        },
    );
    store
        .append_traced(&started, None, Timestamp::from_millis(base))
        .expect("turn_started should append");

    let delta = AgentEvent::ModelDelta {
        text: "portable".to_owned(),
    };
    let agent = TraceEvent::new(
        turn.clone(),
        Some(1),
        Some(session.clone()),
        TracePayload::Agent {
            event: delta.clone(),
        },
    );
    store
        .append_traced(
            &agent,
            Some((session, &delta)),
            Timestamp::from_millis(base + 1),
        )
        .expect("agent entry should append");

    let ended = TraceEvent::new(
        turn.clone(),
        None,
        None,
        TracePayload::TurnEnded {
            reason: SessionEndReason::Completed,
        },
    );
    store
        .append_traced(&ended, None, Timestamp::from_millis(base + 2))
        .expect("turn_ended should append");
}

/// Creates an in-memory store with one closed turn and returns it with the
/// validated trace.
fn seeded_turn(turn: &TurnId, session: &SessionId) -> (EventStore, StoredTraceTurn) {
    let mut store = EventStore::open_in_memory().expect("in-memory store should open");
    append_turn(&mut store, turn, session, 100);
    let trace = store
        .trace_turn(turn)
        .expect("stored trace should validate")
        .expect("stored trace should exist");
    (store, trace)
}

#[test]
fn exported_entries_recompute_the_online_digest() {
    let turn = TurnId::new("turn-export");
    let session = SessionId::new("session-export");
    let (_store, trace) = seeded_turn(&turn, &session);
    let entries = evidence(&trace);

    let offline = verify_exported_trace(&turn, trace.closed(), &entries)
        .expect("exported evidence should validate");

    assert_eq!(
        offline.digest(),
        trace.digest(),
        "the offline chain must reproduce the online digest"
    );
    assert_eq!(offline.events().len(), trace.events().len());
    assert_eq!(
        offline.events()[1].payload_json(),
        trace.events()[1].payload_json()
    );
    assert_eq!(
        offline.events()[1].timestamp(),
        trace.events()[1].timestamp()
    );
}

#[test]
fn timestamps_are_part_of_the_recomputed_digest() {
    let turn = TurnId::new("turn-timestamp");
    let session = SessionId::new("session-timestamp");
    let (_store, trace) = seeded_turn(&turn, &session);
    let mut entries = evidence(&trace);
    let original = verify_exported_trace(&turn, trace.closed(), &entries)
        .expect("the untouched evidence should validate")
        .digest()
        .to_owned();

    entries[1] = TraceEvidenceEntry::new(
        entries[1].seq(),
        Timestamp::from_millis(entries[1].timestamp().as_millis() + 1),
        entries[1].payload_json().to_owned(),
    );
    let changed = verify_exported_trace(&turn, trace.closed(), &entries)
        .expect("a timestamp edit leaves a structurally valid trace")
        .digest()
        .to_owned();

    assert_ne!(
        original, changed,
        "the digest must bind the exported timestamp"
    );
}

#[test]
fn sequence_gaps_and_duplicates_are_rejected() {
    let turn = TurnId::new("turn-sequence");
    let session = SessionId::new("session-sequence");
    let (_store, trace) = seeded_turn(&turn, &session);

    let mut gap = evidence(&trace);
    gap[1] = TraceEvidenceEntry::new(3, gap[1].timestamp(), gap[1].payload_json().to_owned());
    match verify_exported_trace(&turn, trace.closed(), &gap) {
        Err(EventStoreError::TraceSequence {
            expected, found, ..
        }) => {
            assert_eq!(expected, 2);
            assert_eq!(found, 3);
        }
        other => panic!("a sequence gap must be rejected, found {other:?}"),
    }

    let mut duplicate = evidence(&trace);
    duplicate[2] = TraceEvidenceEntry::new(
        2,
        duplicate[2].timestamp(),
        duplicate[2].payload_json().to_owned(),
    );
    assert!(matches!(
        verify_exported_trace(&turn, trace.closed(), &duplicate),
        Err(EventStoreError::TraceSequence { .. })
    ));
}

#[test]
fn malformed_and_cross_turn_payloads_are_rejected() {
    let turn = TurnId::new("turn-payload");
    let session = SessionId::new("session-payload");
    let (_store, trace) = seeded_turn(&turn, &session);

    let mut malformed = evidence(&trace);
    malformed[1] = TraceEvidenceEntry::new(
        malformed[1].seq(),
        malformed[1].timestamp(),
        "{not json".to_owned(),
    );
    assert!(matches!(
        verify_exported_trace(&turn, trace.closed(), &malformed),
        Err(EventStoreError::TraceDecode { .. })
    ));

    let mut cross_turn = evidence(&trace);
    let mut value: serde_json::Value =
        serde_json::from_str(cross_turn[1].payload_json()).expect("payload should be JSON");
    value["turn_id"] = serde_json::Value::String("turn-other".to_owned());
    cross_turn[1] = TraceEvidenceEntry::new(
        cross_turn[1].seq(),
        cross_turn[1].timestamp(),
        serde_json::to_string(&value).expect("edited payload should serialize"),
    );
    match verify_exported_trace(&turn, trace.closed(), &cross_turn) {
        Err(EventStoreError::TraceInvalid { message, .. }) => {
            assert!(
                message.contains("turn id"),
                "the error must name the cross-turn payload: {message}"
            );
        }
        other => panic!("a cross-turn payload must be rejected, found {other:?}"),
    }
}

#[test]
fn lifecycle_disagreements_and_empty_evidence_are_rejected() {
    let turn = TurnId::new("turn-lifecycle");
    let session = SessionId::new("session-lifecycle");
    let (_store, trace) = seeded_turn(&turn, &session);
    let entries = evidence(&trace);

    assert!(matches!(
        verify_exported_trace(&turn, false, &entries),
        Err(EventStoreError::TraceLifecycle { .. })
    ));
    assert!(matches!(
        verify_exported_trace(&turn, true, &[]),
        Err(EventStoreError::TraceLifecycle { .. })
    ));

    let mut after_end = entries.clone();
    after_end.push(TraceEvidenceEntry::new(
        4,
        Timestamp::from_millis(999),
        entries[1].payload_json().to_owned(),
    ));
    assert!(matches!(
        verify_exported_trace(&turn, true, &after_end),
        Err(EventStoreError::TraceLifecycle { .. })
    ));
}

#[test]
fn bundle_digest_is_order_independent_and_set_sensitive() {
    let first_turn = TurnId::new("turn-alpha");
    let second_turn = TurnId::new("turn-beta");
    let first_session = SessionId::new("session-alpha");
    let second_session = SessionId::new("session-beta");
    let (first_store, first) = seeded_turn(&first_turn, &first_session);
    let (second_store, second) = seeded_turn(&second_turn, &second_session);

    let forward =
        trace_bundle_digest(&[&first, &second]).expect("unique turn digests should aggregate");
    let reverse = trace_bundle_digest(&[&second, &first]).expect("caller order must not matter");
    assert_eq!(forward, reverse, "the aggregate must sort turns by turn id");
    assert_eq!(forward.len(), 64);

    let only_first = trace_bundle_digest(&[&first]).expect("one turn should aggregate");
    assert_ne!(
        forward, only_first,
        "removing a turn must change the aggregate"
    );

    match trace_bundle_digest(&[&first, &second, &first]) {
        Err(EventStoreError::TraceInvalid { message, .. }) => {
            assert!(
                message.contains("twice"),
                "the duplicate error must explain the contract: {message}"
            );
        }
        other => panic!("duplicate turns must be rejected, found {other:?}"),
    }

    drop(first_store);
    drop(second_store);
}
