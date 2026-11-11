//! Behavioral tests for deterministic embeddings and brute-force vector search.

use jessica_core::types::SessionId;
use jessica_memory::{
    BruteForceIndex, DeterministicEmbedder, Embedder, MemoryEntryId, MemoryLog, NewMemoryEntry,
    Timestamp, VectorError, VectorIndex, VectorIndexError, WorkspaceScope, decode_vector,
    encode_vector,
};

/// Encodes one deterministic embedding for `text`.
fn embed(embedder: &DeterministicEmbedder, text: &str) -> Vec<f32> {
    futures::executor::block_on(embedder.embed(text)).expect("deterministic embedding must succeed")
}

/// Builds a non-empty workspace scope.
fn workspace(scope: &str) -> WorkspaceScope {
    WorkspaceScope::new(scope).expect("test workspace scope should be valid")
}

/// Appends one entry and requires it to be new.
fn append(log: &MemoryLog, scope: &WorkspaceScope, content: &str) -> MemoryEntryId {
    let entry = NewMemoryEntry::new(
        SessionId::new("session"),
        scope.clone(),
        content,
        Timestamp::from_millis(1),
    )
    .expect("test entry should be valid");
    log.append(entry)
        .expect("append should succeed")
        .expect("test content should be unique")
}

/// Asserts two finite scores are equal within a small tolerance.
fn assert_close(actual: f32, expected: f32) {
    assert!(
        (actual - expected).abs() < 1e-6,
        "expected {expected}, got {actual}"
    );
}

/// Decodes an ASCII hex string into bytes.
fn hex_to_bytes(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0, "hex input must have an even length");
    (0..hex.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&hex[index..index + 2], 16).expect("sample must be valid hex")
        })
        .collect()
}

/// Pinned output of `DeterministicEmbedder::embed("abc")`.
///
/// This locks the hash, bucket selection, sign bit, normalization, f32
/// precision, and little-endian encoding. Changing any of them invalidates
/// previously stored vectors under `deterministic-v1`.
const SAMPLE_ABC_HEX: &str = "\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000003acd13bf00000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
00000000000000003acd133f0000000000000000000000000000000000000000\
00000000000000003acd133f0000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000\
0000000000000000000000000000000000000000000000000000000000000000";

#[test]
fn deterministic_embedder_is_reproducible_normalized_and_non_semantic() {
    let embedder = DeterministicEmbedder::new();
    assert_eq!(embedder.model_id(), "deterministic-v1");
    assert_eq!(embedder.dims(), 256);

    let first = embed(&embedder, "the build command is cargo test");
    let second = embed(&embedder, "the build command is cargo test");
    let first_bytes = encode_vector(&first).expect("finite embedding must encode");
    let second_bytes = encode_vector(&second).expect("finite embedding must encode");
    assert_eq!(
        first_bytes, second_bytes,
        "the same text must embed byte-identically"
    );
    assert_eq!(first.len(), 256);

    let norm_sq: f32 = first.iter().map(|value| value * value).sum();
    assert_close(norm_sq.sqrt(), 1.0);

    let different = embed(&embedder, "deploy the staging build");
    assert_ne!(first, different, "different text should differ in practice");

    for text in ["", "   ", "\t\n"] {
        let zero = embed(&embedder, text);
        assert_eq!(zero.len(), 256);
        assert!(
            zero.iter().all(|value| *value == 0.0),
            "empty or whitespace-only text must produce a zero vector"
        );
    }
}

#[test]
fn sample_vector_bytes_are_pinned() {
    let embedder = DeterministicEmbedder::new();
    let encoded = encode_vector(&embed(&embedder, "abc")).expect("sample must encode");
    assert_eq!(encoded.len(), 1024);
    assert_eq!(
        encoded,
        hex_to_bytes(SAMPLE_ABC_HEX),
        "the deterministic-v1 byte representation must not change"
    );
}

#[test]
fn encode_and_decode_round_trip_little_endian_values() {
    let values = [-0.0_f32, 1.0, -2.5, f32::MIN_POSITIVE];
    let encoded = encode_vector(&values).expect("finite values must encode");
    assert_eq!(
        encoded,
        vec![
            0x00, 0x00, 0x00, 0x80, // -0.0
            0x00, 0x00, 0x80, 0x3f, // 1.0
            0x00, 0x00, 0x20, 0xc0, // -2.5
            0x00, 0x00, 0x80, 0x00, // f32::MIN_POSITIVE
        ],
        "encoding must be little-endian f32"
    );

    let decoded = decode_vector(&encoded, values.len()).expect("valid bytes must decode");
    assert_eq!(
        decoded
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        values
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    );
}

#[test]
fn invalid_vectors_are_rejected_with_structured_errors() {
    let invalid_length = decode_vector(&[0x00, 0x00, 0x00], 1)
        .expect_err("three bytes are not a whole number of f32 values");
    assert!(matches!(
        invalid_length,
        VectorError::InvalidByteLength { len: 3 }
    ));

    let dimension_mismatch = decode_vector(&[0; 8], 3).expect_err("dimensions must match exactly");
    assert!(matches!(
        dimension_mismatch,
        VectorError::DimensionMismatch {
            expected: 3,
            actual: 2
        }
    ));

    let nan_bytes = f32::NAN.to_le_bytes();
    let nan = decode_vector(&nan_bytes, 1).expect_err("NaN must not decode");
    assert!(matches!(nan, VectorError::NonFinite { index: 0 }));

    let infinity_bytes = f32::INFINITY.to_le_bytes();
    let infinity = decode_vector(&infinity_bytes, 1).expect_err("Infinity must not decode");
    assert!(matches!(infinity, VectorError::NonFinite { index: 0 }));

    let encode_nan = encode_vector(&[1.0, f32::NAN]).expect_err("NaN must not encode");
    assert!(matches!(encode_nan, VectorError::NonFinite { index: 1 }));
    let encode_infinity =
        encode_vector(&[f32::NEG_INFINITY]).expect_err("Infinity must not encode");
    assert!(matches!(
        encode_infinity,
        VectorError::NonFinite { index: 0 }
    ));
}

#[test]
fn cosine_ranking_orders_exact_over_partial_over_unrelated() {
    let log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace");
    let exact = append(&log, &scope, "exact");
    let same_direction = append(&log, &scope, "same direction");
    let partial = append(&log, &scope, "partial");
    let unrelated = append(&log, &scope, "unrelated");

    let index = BruteForceIndex::new(&log);
    let now = Timestamp::from_millis(1);
    index
        .add(exact, "model-a", &[1.0, 0.0, 0.0], now)
        .expect("exact vector should store");
    index
        .add(same_direction, "model-a", &[2.0, 0.0, 0.0], now)
        .expect("same-direction vector should store");
    index
        .add(partial, "model-a", &[1.0, 1.0, 0.0], now)
        .expect("partial vector should store");
    index
        .add(unrelated, "model-a", &[0.0, 1.0, 0.0], now)
        .expect("unrelated vector should store");

    let matches = index
        .search(&[1.0, 0.0, 0.0], "model-a", &scope, 10)
        .expect("search should succeed");
    assert_eq!(
        matches
            .iter()
            .map(|matched| matched.entry_id())
            .collect::<Vec<_>>(),
        vec![exact, same_direction, partial, unrelated],
        "scores must descend and row-id ties must ascend"
    );
    assert_close(matches[0].score(), 1.0);
    assert_close(matches[1].score(), 1.0);
    assert_close(matches[2].score(), std::f32::consts::FRAC_1_SQRT_2);
    assert_close(matches[3].score(), 0.0);

    let limited = index
        .search(&[1.0, 0.0, 0.0], "model-a", &scope, 2)
        .expect("limited search should succeed");
    assert_eq!(
        limited
            .iter()
            .map(|matched| matched.entry_id())
            .collect::<Vec<_>>(),
        vec![exact, same_direction]
    );
}

#[test]
fn vector_search_filters_workspace_model_dims_and_soft_deleted_entries() {
    let mut log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let workspace_one = workspace("workspace-one");
    let workspace_two = workspace("workspace-two");
    let first = append(&log, &workspace_one, "first");
    let other_model = append(&log, &workspace_one, "other model");
    let other_dims = append(&log, &workspace_one, "other dims");
    let other_workspace = append(&log, &workspace_two, "other workspace");
    let now = Timestamp::from_millis(1);

    let index = BruteForceIndex::new(&log);
    index
        .add(first, "model-a", &[1.0, 0.0, 0.0], now)
        .expect("first vector should store");
    index
        .add(other_model, "model-b", &[1.0, 0.0, 0.0], now)
        .expect("other-model vector should store");
    index
        .add(other_dims, "model-a", &[1.0, 0.0, 0.0, 0.0], now)
        .expect("other-dims vector should store");
    index
        .add(other_workspace, "model-a", &[1.0, 0.0, 0.0], now)
        .expect("other-workspace vector should store");

    let workspace_one_matches = index
        .search(&[1.0, 0.0, 0.0], "model-a", &workspace_one, 10)
        .expect("workspace-one search should succeed");
    assert_eq!(workspace_one_matches.len(), 1);
    assert_eq!(workspace_one_matches[0].entry_id(), first);

    let workspace_two_matches = index
        .search(&[1.0, 0.0, 0.0], "model-a", &workspace_two, 10)
        .expect("workspace-two search should succeed");
    assert_eq!(workspace_two_matches.len(), 1);
    assert_eq!(workspace_two_matches[0].entry_id(), other_workspace);

    assert!(
        index
            .search(&[1.0, 0.0], "model-a", &workspace_one, 10)
            .expect("dimension mismatch should be an empty result")
            .is_empty()
    );
    assert!(
        index
            .search(&[1.0, 0.0, 0.0], "missing-model", &workspace_one, 10)
            .expect("missing model should be an empty result")
            .is_empty()
    );
    assert!(
        index
            .search(&[], "model-a", &workspace_one, 10)
            .expect("empty query should be an empty result")
            .is_empty()
    );
    assert!(
        index
            .search(&[1.0, 0.0, 0.0], "model-a", &workspace_one, 0)
            .expect("zero limit should be an empty result")
            .is_empty()
    );

    log.soft_delete(first, Timestamp::from_millis(2))
        .expect("soft delete should succeed");
    let index = BruteForceIndex::new(&log);
    assert!(
        index
            .search(&[1.0, 0.0, 0.0], "model-a", &workspace_one, 10)
            .expect("search after soft delete should succeed")
            .is_empty(),
        "soft-deleted entries must not be returned by vector search"
    );
}

#[test]
fn adding_a_vector_replaces_the_previous_value_and_rejects_non_finite_input() {
    let log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace");
    let entry_id = append(&log, &scope, "replaceable");
    let index = BruteForceIndex::new(&log);
    let now = Timestamp::from_millis(1);

    index
        .add(entry_id, "model-a", &[1.0, 0.0], now)
        .expect("first vector should store");
    let initial = index
        .search(&[1.0, 0.0], "model-a", &scope, 10)
        .expect("initial search should succeed");
    assert_eq!(initial.len(), 1);

    index
        .add(entry_id, "model-a", &[0.0, 2.0], now)
        .expect("replacement vector should store");
    let replaced = index
        .search(&[0.0, 1.0], "model-a", &scope, 10)
        .expect("replacement search should succeed");
    assert_eq!(replaced.len(), 1);
    assert_close(replaced[0].score(), 1.0);

    let nan_error = index
        .add(entry_id, "model-a", &[f32::NAN, 1.0], now)
        .expect_err("NaN must be rejected");
    assert!(matches!(
        nan_error,
        VectorIndexError::Vector(VectorError::NonFinite { index: 0 })
    ));
    let infinity_error = index
        .add(entry_id, "model-a", &[1.0, f32::INFINITY], now)
        .expect_err("Infinity must be rejected");
    assert!(matches!(
        infinity_error,
        VectorIndexError::Vector(VectorError::NonFinite { index: 1 })
    ));

    let missing = MemoryEntryId::from_raw(entry_id.get() + 1_000);
    let missing_error = index
        .add(missing, "model-a", &[1.0, 0.0], now)
        .expect_err("an embedding for a missing entry must be rejected");
    assert!(matches!(missing_error, VectorIndexError::Storage(_)));
    let still_replaced = index
        .search(&[0.0, 1.0], "model-a", &scope, 10)
        .expect("failed writes must not disturb the stored vector");
    assert_eq!(still_replaced.len(), 1);
    assert_close(still_replaced[0].score(), 1.0);
}

#[test]
fn zero_norm_similarity_is_zero_for_stored_and_query_vectors() {
    let log = MemoryLog::open_in_memory().expect("in-memory log should open");
    let scope = workspace("workspace");
    let zero_entry = append(&log, &scope, "zero vector");
    let non_zero_entry = append(&log, &scope, "non-zero vector");
    let index = BruteForceIndex::new(&log);
    let now = Timestamp::from_millis(1);

    index
        .add(zero_entry, "model-a", &[0.0, 0.0], now)
        .expect("zero vector should store");
    index
        .add(non_zero_entry, "model-a", &[1.0, 0.0], now)
        .expect("non-zero vector should store");

    let non_zero_query = index
        .search(&[1.0, 0.0], "model-a", &scope, 10)
        .expect("non-zero query should succeed");
    assert_eq!(non_zero_query.len(), 2);
    assert_close(non_zero_query[0].score(), 1.0);
    assert_close(non_zero_query[1].score(), 0.0);

    let zero_query = index
        .search(&[0.0, 0.0], "model-a", &scope, 10)
        .expect("zero query should succeed");
    assert_eq!(zero_query.len(), 2);
    assert!(
        zero_query
            .iter()
            .all(|matched| matched.score().abs() < f32::EPSILON)
    );
}
