//! SHA-256 hash chain for stored trace entries.
//!
//! Every row in `trace_events` has a matching row in `trace_chain` that binds
//! the exact stored column values. The first entry of a turn starts from an
//! all-zero genesis hash, and every later entry includes the previous entry
//! hash, so deleting, inserting, or editing an entry breaks the chain at a
//! detectable position.
//!
//! The chain is tamper-**evident**, not tamper-proof: a writer with full access
//! to the database can drop the append-only triggers, rewrite the evidence, and
//! recompute the whole chain. Detecting that case requires comparing the turn
//! digest with a value stored outside the database, such as
//! `jessica trace verify --expect`.
//!
//! Domain separation and length-prefixed fields keep the encoding unambiguous.
//! Hashes bind the values SQLite actually stores (including the `i64` column
//! representations and the exact `payload_json` text), never a re-serialized
//! interpretation of them, so migration backfills and live appends agree.

use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};

use crate::EventStoreError;

/// Trace chain table and its append-only guards.
///
/// Rows are inserted once, with the trace entry, and can never be updated or
/// deleted through SQLite's normal trigger enforcement.
pub(crate) const TRACE_CHAIN_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS trace_chain (
    turn_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq >= 1),
    prev_hash TEXT NOT NULL CHECK (length(prev_hash) = 64),
    entry_hash TEXT NOT NULL CHECK (length(entry_hash) = 64),
    PRIMARY KEY (turn_id, seq)
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS trace_chain_append_only_update
BEFORE UPDATE ON trace_chain
BEGIN
    SELECT RAISE(ABORT, 'trace chain is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trace_chain_append_only_delete
BEFORE DELETE ON trace_chain
BEGIN
    SELECT RAISE(ABORT, 'trace chain is append-only: DELETE is not allowed');
END;
"#;

/// Length of a SHA-256 hash in bytes.
pub(crate) const HASH_BYTES: usize = 32;

/// Hash that precedes the first entry of every turn.
pub(crate) const GENESIS_HASH: [u8; HASH_BYTES] = [0; HASH_BYTES];

/// Domain tag for one chained trace entry.
const ENTRY_DOMAIN: &[u8] = b"jessica.trace.entry.v1";

/// Domain tag for a turn digest.
const DIGEST_DOMAIN: &[u8] = b"jessica.trace.digest.v1";

/// Exact stored columns covered by one chain link.
#[derive(Debug)]
pub(crate) struct ChainEntry<'a> {
    /// Stored turn id.
    pub(crate) turn_id: &'a str,
    /// Stored one-based sequence number.
    pub(crate) seq: i64,
    /// Stored trace envelope schema version.
    pub(crate) schema_version: i64,
    /// Stored payload kind.
    pub(crate) payload_kind: &'a str,
    /// Stored event type, present only for agent payloads.
    pub(crate) event_type: Option<&'a str>,
    /// Stored attempt number, when the payload carries one.
    pub(crate) attempt: Option<i64>,
    /// Stored session id, when the payload carries one.
    pub(crate) session_id: Option<&'a str>,
    /// Exact stored JSON text.
    pub(crate) payload_json: &'a str,
    /// Stored timestamp in milliseconds since the Unix epoch.
    pub(crate) timestamp: i64,
}

/// Installs the trace chain table and its triggers.
pub(crate) fn install(connection: &Connection) -> Result<(), EventStoreError> {
    connection
        .execute_batch(TRACE_CHAIN_SCHEMA)
        .map_err(EventStoreError::Schema)
}

/// Backfills chain rows for every trace entry written before the chain existed.
///
/// Runs inside the caller's migration transaction. The backfill is mechanical:
/// it hashes the stored values as-is, including rows that later semantic
/// validation may classify as corrupt. It never rewrites trace entries and
/// never runs during an ordinary reopen of an already-current database.
pub(crate) fn backfill(transaction: &rusqlite::Transaction<'_>) -> Result<(), EventStoreError> {
    let mut select = transaction
        .prepare(
            "SELECT turn_id, seq, schema_version, payload_kind, event_type, attempt, \
                    session_id, payload_json, timestamp \
             FROM trace_events ORDER BY turn_id ASC, seq ASC",
        )
        .map_err(EventStoreError::Schema)?;
    let mut insert = transaction
        .prepare(
            "INSERT INTO trace_chain (turn_id, seq, prev_hash, entry_hash) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .map_err(EventStoreError::Schema)?;

    let mut rows = select.query([]).map_err(EventStoreError::Schema)?;
    let mut current_turn: Option<String> = None;
    let mut previous = GENESIS_HASH;
    while let Some(row) = rows.next().map_err(EventStoreError::Schema)? {
        let turn_id: String = row.get(0).map_err(EventStoreError::Schema)?;
        let seq: i64 = row.get(1).map_err(EventStoreError::Schema)?;
        let schema_version: i64 = row.get(2).map_err(EventStoreError::Schema)?;
        let payload_kind: String = row.get(3).map_err(EventStoreError::Schema)?;
        let event_type: Option<String> = row.get(4).map_err(EventStoreError::Schema)?;
        let attempt: Option<i64> = row.get(5).map_err(EventStoreError::Schema)?;
        let session_id: Option<String> = row.get(6).map_err(EventStoreError::Schema)?;
        let payload_json: String = row.get(7).map_err(EventStoreError::Schema)?;
        let timestamp: i64 = row.get(8).map_err(EventStoreError::Schema)?;

        if current_turn.as_deref() != Some(turn_id.as_str()) {
            current_turn = Some(turn_id.clone());
            previous = GENESIS_HASH;
        }

        let entry = ChainEntry {
            turn_id: &turn_id,
            seq,
            schema_version,
            payload_kind: &payload_kind,
            event_type: event_type.as_deref(),
            attempt,
            session_id: session_id.as_deref(),
            payload_json: &payload_json,
            timestamp,
        };
        let hash = entry_hash(&previous, &entry);
        insert
            .execute(params![
                turn_id.as_str(),
                seq,
                encode_hex(&previous),
                encode_hex(&hash)
            ])
            .map_err(EventStoreError::Schema)?;
        previous = hash;
    }
    Ok(())
}

/// Computes the link hash for one entry.
///
/// Every field is length-prefixed with a big-endian `u64`, optional fields are
/// preceded by a one-byte presence marker, and numbers use fixed-width
/// big-endian encoding. The previous hash is hashed as 32 raw bytes.
pub(crate) fn entry_hash(previous: &[u8; HASH_BYTES], entry: &ChainEntry<'_>) -> [u8; HASH_BYTES] {
    let mut hasher = Sha256::new();
    field(&mut hasher, ENTRY_DOMAIN);
    field(&mut hasher, previous);
    field(&mut hasher, entry.turn_id.as_bytes());
    field(&mut hasher, &entry.seq.to_be_bytes());
    field(&mut hasher, &entry.schema_version.to_be_bytes());
    field(&mut hasher, entry.payload_kind.as_bytes());
    optional_field(&mut hasher, entry.event_type);
    optional_i64_field(&mut hasher, entry.attempt);
    optional_field(&mut hasher, entry.session_id);
    field(&mut hasher, &entry.timestamp.to_be_bytes());
    field(&mut hasher, entry.payload_json.as_bytes());
    finish(hasher)
}

/// Computes the lowercase hex digest that identifies a complete turn chain.
pub(crate) fn turn_digest(turn_id: &str, last: &[u8; HASH_BYTES]) -> String {
    let mut hasher = Sha256::new();
    field(&mut hasher, DIGEST_DOMAIN);
    field(&mut hasher, turn_id.as_bytes());
    field(&mut hasher, last);
    encode_hex(&finish(hasher))
}

/// Encodes bytes as lowercase hexadecimal.
pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Decodes exactly 64 lowercase hexadecimal characters into 32 bytes.
///
/// Uppercase input is rejected because every chain hash written by this crate
/// is canonical lowercase; accepting alternatives would weaken comparisons.
pub(crate) fn decode_hex_32(value: &str) -> Option<[u8; HASH_BYTES]> {
    if value.len() != HASH_BYTES * 2 {
        return None;
    }
    let bytes = value.as_bytes();
    let mut decoded = [0_u8; HASH_BYTES];
    for (index, target) in decoded.iter_mut().enumerate() {
        let high = hex_nibble(bytes[index * 2])?;
        let low = hex_nibble(bytes[index * 2 + 1])?;
        *target = (high << 4) | low;
    }
    Some(decoded)
}

/// Appends one length-prefixed field to the hash.
fn field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

/// Appends an optional string field with an explicit presence marker.
fn optional_field(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            field(hasher, &[1]);
            field(hasher, value.as_bytes());
        }
        None => field(hasher, &[0]),
    }
}

/// Appends an optional integer field with an explicit presence marker.
fn optional_i64_field(hasher: &mut Sha256, value: Option<i64>) {
    match value {
        Some(value) => {
            field(hasher, &[1]);
            field(hasher, &value.to_be_bytes());
        }
        None => field(hasher, &[0]),
    }
}

/// Finalizes a SHA-256 hasher into a fixed-size array without panicking.
fn finish(hasher: Sha256) -> [u8; HASH_BYTES] {
    let digest = hasher.finalize();
    let mut hash = [0_u8; HASH_BYTES];
    for (target, source) in hash.iter_mut().zip(digest.iter()) {
        *target = *source;
    }
    hash
}

/// Converts one lowercase hexadecimal digit to its value.
fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fully populated entry used to pin the encoding.
    fn sample() -> ChainEntry<'static> {
        ChainEntry {
            turn_id: "turn-golden",
            seq: 1,
            schema_version: 2,
            payload_kind: "turn_started",
            event_type: None,
            attempt: None,
            session_id: Some("session-golden"),
            payload_json: "{\"payload\":{\"kind\":\"turn_started\"},\"schema_version\":2,\"session_id\":\"session-golden\",\"turn_id\":\"turn-golden\"}",
            timestamp: 1_700_000_000_000,
        }
    }

    #[test]
    fn the_chain_hash_format_is_locked_by_a_golden_vector() {
        let hash = entry_hash(&GENESIS_HASH, &sample());
        assert_eq!(
            encode_hex(&hash),
            "329b2d04df19c0ee9a4e2cd49cf626f58f37ebfa0c886e1d308a8777ae3d48b5",
            "changing the entry encoding invalidates every recorded digest"
        );
        assert_eq!(
            turn_digest("turn-golden", &hash),
            "3c58a61e766e7a919ad0eaf6518e511751b810e2761f1382eeb9eefcab0635b0",
            "changing the turn digest encoding invalidates every recorded digest"
        );
    }

    #[test]
    fn length_prefixes_prevent_field_boundary_collisions() {
        let mut short_turn = sample();
        short_turn.turn_id = "a";
        short_turn.payload_json = "bc";
        let mut long_turn = sample();
        long_turn.turn_id = "ab";
        long_turn.payload_json = "c";

        assert_ne!(
            entry_hash(&GENESIS_HASH, &short_turn),
            entry_hash(&GENESIS_HASH, &long_turn),
            "moving bytes across a field boundary must change the hash"
        );
    }

    #[test]
    fn optional_field_presence_is_part_of_the_hash() {
        let mut absent = sample();
        absent.event_type = None;
        let mut present_but_empty = sample();
        present_but_empty.event_type = Some("");

        assert_ne!(
            entry_hash(&GENESIS_HASH, &absent),
            entry_hash(&GENESIS_HASH, &present_but_empty),
            "None and Some(\"\") must not collide"
        );
    }

    #[test]
    fn every_field_changes_the_hash() {
        let base = entry_hash(&GENESIS_HASH, &sample());
        let mut variants = Vec::new();

        let mut entry = sample();
        entry.turn_id = "turn-other";
        variants.push(entry_hash(&GENESIS_HASH, &entry));
        let mut entry = sample();
        entry.seq = 2;
        variants.push(entry_hash(&GENESIS_HASH, &entry));
        let mut entry = sample();
        entry.schema_version = 1;
        variants.push(entry_hash(&GENESIS_HASH, &entry));
        let mut entry = sample();
        entry.payload_kind = "agent";
        variants.push(entry_hash(&GENESIS_HASH, &entry));
        let mut entry = sample();
        entry.event_type = Some("model_delta");
        variants.push(entry_hash(&GENESIS_HASH, &entry));
        let mut entry = sample();
        entry.attempt = Some(1);
        variants.push(entry_hash(&GENESIS_HASH, &entry));
        let mut entry = sample();
        entry.session_id = None;
        variants.push(entry_hash(&GENESIS_HASH, &entry));
        let mut entry = sample();
        entry.payload_json = "{}";
        variants.push(entry_hash(&GENESIS_HASH, &entry));
        let mut entry = sample();
        entry.timestamp += 1;
        variants.push(entry_hash(&GENESIS_HASH, &entry));

        for variant in variants {
            assert_ne!(base, variant, "every hashed field must affect the result");
        }
    }

    #[test]
    fn previous_hash_is_part_of_every_link() {
        let mut previous = GENESIS_HASH;
        previous[31] = 1;
        assert_ne!(
            entry_hash(&GENESIS_HASH, &sample()),
            entry_hash(&previous, &sample())
        );
    }

    #[test]
    fn hexadecimal_encoding_round_trips_and_rejects_non_canonical_input() {
        let mut bytes = [0_u8; HASH_BYTES];
        bytes[0] = 0xab;
        bytes[31] = 0x01;
        let encoded = encode_hex(&bytes);
        assert_eq!(encoded.len(), 64);
        assert_eq!(decode_hex_32(&encoded), Some(bytes));
        assert_eq!(decode_hex_32(&encoded.to_uppercase()), None);
        assert_eq!(decode_hex_32(&encoded[1..]), None);
        assert_eq!(decode_hex_32(&format!("{encoded}extra")), None);
        assert_eq!(decode_hex_32(&"z".repeat(64)), None);
    }
}
