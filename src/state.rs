//! Wasm-compatible state-proof verification against a GRANDPA-finalized
//! state root.
//!
//! Deliberately avoids `sp-state-machine`: its `std` feature pulls in
//! `substrate-prometheus-endpoint → hyper → tokio → mio`, none of which
//! compile to `wasm32-unknown-unknown`. `CompactProof` + `TrieDBBuilder`
//! + `trie-db` give the same trust property without the dependency
//! weight, and work in both native and browser builds.
//!
//! The trust chain — GRANDPA → finalized block header → `state_root` →
//! merkle proof → key/value — is the user's responsibility to assemble:
//! pass a state root that *was* observed at a justified finalized
//! header, supply the `/state/2` response bytes, and you get back the
//! verified value (or an error if the proof doesn't witness the key).
//!
//! ## Top vs. child tries
//!
//! Substrate stores per-parachain state in *child* tries indexed under
//! `:child_storage:default:` + the parachain's `trie_id`. Use
//! [`verify_top_proof`] for relay-chain reads and
//! [`verify_child_proof`] when the key lives inside a parachain's child
//! trie. The latter walks the parent trie first to recover the child
//! trie root, then walks the child trie for the actual value.

use parity_scale_codec::Decode;
use sp_core::Blake2Hasher;
use sp_trie::CompactProof;
use sp_trie::trie_types::TrieDBBuilder;
use trie_db::Trie;

/// Verify a Substrate `/state/2` response against `expected_state_root`
/// and return the value stored at `key` in the top trie.
///
/// Returns `Err` if the proof is malformed, fails verification against
/// the root, or doesn't witness `key`.
pub fn verify_top_proof(
    response_bytes: &[u8],
    expected_state_root: &[u8; 32],
    key: &[u8],
) -> Result<Vec<u8>, String> {
    let proof_bytes = extract_state_response_proof(response_bytes)?;
    let encoded_nodes = <Vec<Vec<u8>>>::decode(&mut &proof_bytes[..])
        .map_err(|e| format!("SCALE decoding compact proof: {e}"))?;
    let compact = CompactProof { encoded_nodes };
    let expected_root = sp_core::H256(*expected_state_root);
    let (storage_proof, _) = compact
        .to_storage_proof::<Blake2Hasher>(Some(&expected_root))
        .map_err(|e| format!("compact->storage failed: {e:?}"))?;
    let db = storage_proof.into_memory_db::<Blake2Hasher>();
    let trie = TrieDBBuilder::<Blake2Hasher>::new(&db, &expected_root).build();
    trie.get(key)
        .map_err(|e| format!("trie lookup failed: {e:?}"))?
        .ok_or_else(|| "key absent from verified proof".to_string())
}

/// Verify a Substrate `/state/2` response that witnesses a key inside
/// a parachain's child trie (indexed under `:child_storage:default:` +
/// `trie_id`). Walks the parent trie to recover the child root, then
/// walks the child trie for `child_key`.
pub fn verify_child_proof(
    response_bytes: &[u8],
    expected_state_root: &[u8; 32],
    trie_id: &[u8],
    child_key: &[u8; 32],
) -> Result<Vec<u8>, String> {
    let proof_bytes = extract_state_response_proof(response_bytes)?;
    let encoded_nodes = <Vec<Vec<u8>>>::decode(&mut &proof_bytes[..])
        .map_err(|e| format!("SCALE decoding compact proof: {e}"))?;
    let compact = CompactProof { encoded_nodes };
    let expected_root = sp_core::H256(*expected_state_root);
    let (storage_proof, _) = compact
        .to_storage_proof::<Blake2Hasher>(Some(&expected_root))
        .map_err(|e| format!("compact->storage failed: {e:?}"))?;
    let db = storage_proof.into_memory_db::<Blake2Hasher>();

    let parent_trie = TrieDBBuilder::<Blake2Hasher>::new(&db, &expected_root).build();
    let top_key = child_storage_default_prefix(trie_id);
    let child_root_bytes = parent_trie
        .get(&top_key)
        .map_err(|e| format!("parent trie lookup failed: {e:?}"))?
        .ok_or_else(|| {
            format!(
                "child root not in parent proof at b\":child_storage:default:\" ++ 0x{}",
                hex::encode(trie_id),
            )
        })?;
    if child_root_bytes.len() != 32 {
        return Err(format!(
            "child root has unexpected length {} (expected 32)",
            child_root_bytes.len()
        ));
    }
    let mut child_root_arr = [0u8; 32];
    child_root_arr.copy_from_slice(&child_root_bytes);
    let child_root = sp_core::H256(child_root_arr);

    let child_trie = TrieDBBuilder::<Blake2Hasher>::new(&db, &child_root).build();
    child_trie
        .get(child_key.as_slice())
        .map_err(|e| format!("child trie lookup failed: {e:?}"))?
        .ok_or_else(|| "child key absent from verified proof".to_string())
}

/// Build the top-trie key under which a parachain's child trie root is
/// stored. Substrate convention: `b":child_storage:default:" ++ trie_id`.
pub fn child_storage_default_prefix(trie_id: &[u8]) -> Vec<u8> {
    const PREFIX: &[u8] = b":child_storage:default:";
    let mut k = Vec::with_capacity(PREFIX.len() + trie_id.len());
    k.extend_from_slice(PREFIX);
    k.extend_from_slice(trie_id);
    k
}

/// Encode a Substrate `/state/2` (StateRequest) protobuf message:
/// `block_hash` + one or more `start` keys. Returns the bytes to feed
/// into `Connection::request(state_id, …)`.
pub fn encode_state_request(block_hash: &[u8; 32], start: &[&[u8]]) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(8 + 32 + start.iter().map(|s| s.len() + 4).sum::<usize>());
    encode_len_delim(&mut out, 1, block_hash);
    for s in start {
        encode_len_delim(&mut out, 2, s);
    }
    out
}

// ============================================================================
// Internal helpers — protobuf wire-format for substrate's StateResponse.
// ============================================================================

fn extract_state_response_proof(mut input: &[u8]) -> Result<Vec<u8>, String> {
    let mut proof: Option<Vec<u8>> = None;
    while !input.is_empty() {
        let tag = decode_varint(&mut input)?;
        let field_id = tag >> 3;
        let wire_type = tag & 0x07;
        match (field_id, wire_type) {
            (2, 2) => {
                let len = decode_varint(&mut input)? as usize;
                proof = Some(take_bytes(&mut input, len)?.to_vec());
            }
            (_, 2) => {
                let len = decode_varint(&mut input)? as usize;
                take_bytes(&mut input, len)?;
            }
            (_, 0) => {
                decode_varint(&mut input)?;
            }
            (_, w) => {
                return Err(format!("unexpected wire type {w} for field {field_id}"));
            }
        }
    }
    proof.ok_or_else(|| "StateResponse contained no proof field".to_string())
}

fn encode_len_delim(out: &mut Vec<u8>, field_id: u32, data: &[u8]) {
    encode_varint(out, (u64::from(field_id) << 3) | 2);
    encode_varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_varint(input: &mut &[u8]) -> Result<u64, String> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = *input.first().ok_or_else(|| "EOF in varint".to_string())?;
        *input = &input[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint > 64 bits".to_string());
        }
    }
}

fn take_bytes<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8], String> {
    if input.len() < n {
        return Err(format!("EOF: need {n}, have {}", input.len()));
    }
    let (head, rest) = input.split_at(n);
    *input = rest;
    Ok(head)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_storage_prefix_matches_substrate_format() {
        let trie_id = b"some_para_id_bytes";
        let key = child_storage_default_prefix(trie_id);
        assert!(key.starts_with(b":child_storage:default:"));
        assert_eq!(&key[b":child_storage:default:".len()..], trie_id);
    }

    #[test]
    fn encode_state_request_layout() {
        // field 1 (block_hash), field 2 (start_key) — both length-delimited
        let block_hash = [0xABu8; 32];
        let start_key = [0xCDu8; 10];
        let bytes = encode_state_request(&block_hash, &[&start_key[..]]);
        // tag for field 1 = (1 << 3) | 2 = 0x0a
        assert_eq!(bytes[0], 0x0a);
        // length of block_hash = 32 = 0x20
        assert_eq!(bytes[1], 0x20);
        assert_eq!(&bytes[2..34], &block_hash[..]);
        // next field: tag = (2 << 3) | 2 = 0x12
        assert_eq!(bytes[34], 0x12);
        // length of start_key = 10 = 0x0a
        assert_eq!(bytes[35], 0x0a);
        assert_eq!(&bytes[36..46], &start_key[..]);
    }

    #[test]
    fn varint_round_trip() {
        for v in [0u64, 1, 127, 128, 16383, 16384, 1 << 20, u64::MAX / 2] {
            let mut buf = Vec::new();
            encode_varint(&mut buf, v);
            let mut slice = &buf[..];
            assert_eq!(decode_varint(&mut slice).unwrap(), v);
            assert!(slice.is_empty(), "decoder did not consume all bytes for {v}");
        }
    }

    #[test]
    fn extract_state_response_proof_reads_field_2() {
        // synthetic StateResponse with: field 1 (ignored varint = 0x07),
        // field 2 (proof bytes = [0xde, 0xad, 0xbe, 0xef]),
        // field 3 (ignored length-delimited blob).
        let mut bytes = Vec::new();
        // field 1, varint
        encode_varint(&mut bytes, (1 << 3) | 0);
        encode_varint(&mut bytes, 7);
        // field 2, length-delimited: our proof
        encode_varint(&mut bytes, (2 << 3) | 2);
        encode_varint(&mut bytes, 4);
        bytes.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        // field 3, length-delimited junk
        encode_varint(&mut bytes, (3 << 3) | 2);
        encode_varint(&mut bytes, 3);
        bytes.extend_from_slice(&[1, 2, 3]);

        let proof = extract_state_response_proof(&bytes).unwrap();
        assert_eq!(proof, vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn extract_proof_errors_when_proof_field_missing() {
        // StateResponse with only field 1 — no proof.
        let mut bytes = Vec::new();
        encode_varint(&mut bytes, (1 << 3) | 0);
        encode_varint(&mut bytes, 42);
        let err = extract_state_response_proof(&bytes).unwrap_err();
        assert!(err.contains("no proof field"), "got: {err}");
    }

    #[test]
    fn verify_top_proof_rejects_garbage_input() {
        let garbage = [0u8; 16];
        let root = [0u8; 32];
        let err = verify_top_proof(&garbage, &root, b"any_key").unwrap_err();
        // Either "no proof field" (if extract succeeds and finds none) or
        // SCALE / trie failure — but never panic.
        assert!(!err.is_empty());
    }
}
