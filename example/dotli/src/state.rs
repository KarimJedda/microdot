//! Persistent state via browser localStorage. Mirrors the schema used
//! by the original dotli example so trust chain, head cache, trie-id
//! cache, and resolved-CID cache all carry across page reloads.
//!
//! Microdot supplies the `GrandpaState`, `AuthorityId`, `BlockHeader`,
//! and `PeerPool` types — this file is just app-level persistence: a
//! versioned JSON schema and a localStorage adapter.

use anyhow::Context;
use microdot::{AuthorityId, BlockHeader, GrandpaState, PeerPool};
use parity_scale_codec::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const STORAGE_KEY: &str = "microdot-dotli-state";
const VERSION: u32 = 1;

/// Bundled relay-chain `lightSyncState` checkpoint. A 4-line JSON file
/// with `finalizedBlockHeader` + `grandpaAuthoritySet` (the dotli
/// example pulls the same shape from `paseo-balance/`).
const BUNDLED_LIGHTSYNC_JSON: &str = include_str!("../paseo-lightsync.json");

#[derive(Serialize, Deserialize, Clone)]
pub struct PersistedState {
    pub version: u32,
    pub saved_at_unix_ms: u64,
    pub relay: RelaySnapshot,
    #[serde(default)]
    pub assethub_head: Option<AssetHubHeadCache>,
    #[serde(default)]
    pub contracts: BTreeMap<String, ContractCacheEntry>,
    #[serde(default)]
    pub resolved: BTreeMap<String, ResolvedEntry>,
    #[serde(default)]
    pub relay_peers: PeerPool,
    #[serde(default)]
    pub assethub_peers: PeerPool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RelaySnapshot {
    pub finalized_number: u32,
    pub finalized_hash: String,
    pub finalized_state_root: String,
    pub set_id: u64,
    pub authorities_scale: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AssetHubHeadCache {
    pub number: u32,
    pub hash: String,
    pub state_root: String,
    pub saved_at_unix_ms: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ContractCacheEntry {
    pub trie_id: String,
    pub verified_at_block: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ResolvedEntry {
    pub cid: String,
    pub verified_at_block: u32,
}

pub enum Source {
    SavedLocalStorage,
    BundledCheckpoint,
}

#[derive(Deserialize)]
struct BundledLightSync {
    #[serde(rename = "finalizedBlockHeader")]
    finalized_block_header: String,
    #[serde(rename = "grandpaAuthoritySet")]
    grandpa_authority_set: String,
}

#[derive(Decode)]
struct AuthoritySetPrefix {
    current_authorities: Vec<(AuthorityId, u64)>,
    set_id: u64,
}

pub fn load_or_bundled() -> anyhow::Result<(PersistedState, Source)> {
    if let Some(stored) = read_storage(STORAGE_KEY)? {
        match serde_json::from_str::<PersistedState>(&stored) {
            Ok(state) if state.version == VERSION => {
                return Ok((state, Source::SavedLocalStorage));
            }
            Ok(_) => crate::log(
                "[state] schema version mismatch; falling back to bundled checkpoint",
            ),
            Err(e) => crate::log(&format!(
                "[state] failed to parse saved state ({e}); falling back to bundled checkpoint"
            )),
        }
    }
    Ok((load_bundled()?, Source::BundledCheckpoint))
}

fn load_bundled() -> anyhow::Result<PersistedState> {
    let lss: BundledLightSync = serde_json::from_str(BUNDLED_LIGHTSYNC_JSON)?;
    let header_bytes = decode_0x(&lss.finalized_block_header)?;
    let header = BlockHeader::decode(&mut &header_bytes[..])
        .context("decoding bundled finalizedBlockHeader")?;
    let auth_bytes = decode_0x(&lss.grandpa_authority_set)?;
    let auth = AuthoritySetPrefix::decode(&mut &auth_bytes[..])
        .context("decoding bundled grandpaAuthoritySet")?;
    Ok(PersistedState {
        version: VERSION,
        saved_at_unix_ms: 0,
        relay: RelaySnapshot {
            finalized_number: header.number,
            finalized_hash: encode_0x(&header.hash()),
            finalized_state_root: encode_0x(&header.state_root),
            set_id: auth.set_id,
            authorities_scale: encode_0x(&auth.current_authorities.encode()),
        },
        assethub_head: None,
        contracts: BTreeMap::new(),
        resolved: BTreeMap::new(),
        relay_peers: PeerPool::new(),
        assethub_peers: PeerPool::new(),
    })
}

impl PersistedState {
    #[allow(dead_code)] // kept as part of the published surface; the
    // pool-fallback path in `lib.rs` calls `snapshot_to_grandpa_state`
    // instead so it can hold the snapshot independent of `&self`.
    pub fn to_grandpa_state(&self) -> anyhow::Result<GrandpaState> {
        Self::snapshot_to_grandpa_state(&self.relay)
    }

    /// Reify a [`RelaySnapshot`] into a [`GrandpaState`]. Free-standing
    /// counterpart to [`to_grandpa_state`] so callers can hold an owned
    /// snapshot independent of `&self` and reify per attempt (used by
    /// the pool-fallback retry path in `lib.rs`).
    pub fn snapshot_to_grandpa_state(relay: &RelaySnapshot) -> anyhow::Result<GrandpaState> {
        let authorities_bytes = decode_0x(&relay.authorities_scale)?;
        let authorities: Vec<(AuthorityId, u64)> =
            Decode::decode(&mut &authorities_bytes[..])
                .context("decoding authorities_scale")?;
        let finalized_hash: [u8; 32] = decode_0x_array(&relay.finalized_hash)?;
        let finalized_state_root: [u8; 32] =
            decode_0x_array(&relay.finalized_state_root)?;
        Ok(GrandpaState {
            authorities,
            set_id: relay.set_id,
            finalized_number: relay.finalized_number,
            finalized_hash,
            finalized_state_root,
        })
    }

    pub fn update_relay(&mut self, gs: &GrandpaState) {
        self.relay = RelaySnapshot {
            finalized_number: gs.finalized_number,
            finalized_hash: encode_0x(&gs.finalized_hash),
            finalized_state_root: encode_0x(&gs.finalized_state_root),
            set_id: gs.set_id,
            authorities_scale: encode_0x(&gs.authorities.encode()),
        };
    }

    pub fn save(&mut self) -> anyhow::Result<()> {
        self.saved_at_unix_ms = unix_now_ms();
        let json = serde_json::to_string(self)?;
        write_storage(STORAGE_KEY, &json)
    }
}

// ── localStorage I/O ────────────────────────────────────────────────────────

fn read_storage(key: &str) -> anyhow::Result<Option<String>> {
    let Some(window) = web_sys::window() else {
        return Ok(None);
    };
    let Ok(Some(storage)) = window.local_storage() else {
        return Ok(None);
    };
    storage
        .get_item(key)
        .map_err(|e| anyhow::anyhow!("localStorage.getItem failed: {e:?}"))
}

fn write_storage(key: &str, value: &str) -> anyhow::Result<()> {
    let window = web_sys::window().context("no window")?;
    let storage = window
        .local_storage()
        .map_err(|e| anyhow::anyhow!("localStorage access denied: {e:?}"))?
        .context("localStorage unavailable (private mode?)")?;
    storage
        .set_item(key, value)
        .map_err(|e| anyhow::anyhow!("localStorage.setItem failed: {e:?}"))
}

pub fn unix_now_ms() -> u64 {
    js_sys::Date::now() as u64
}

// ── hex helpers ─────────────────────────────────────────────────────────────

fn encode_0x(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn decode_0x(s: &str) -> anyhow::Result<Vec<u8>> {
    let stripped = s
        .strip_prefix("0x")
        .with_context(|| format!("expected 0x-prefixed hex, got {s:?}"))?;
    hex::decode(stripped).context("invalid hex")
}

fn decode_0x_array<const N: usize>(s: &str) -> anyhow::Result<[u8; N]> {
    let bytes = decode_0x(s)?;
    bytes
        .try_into()
        .map_err(|v: Vec<u8>| anyhow::anyhow!("expected {N} bytes, got {}", v.len()))
}
