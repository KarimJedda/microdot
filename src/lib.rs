//! # microdot
//!
//! A tiny, testable, wasm-friendly toolkit for trustless reads against
//! Substrate-based chains. Smoldot-shaped: import this one crate, get
//! peer discovery, GRANDPA finality, and merkle-proof verified state
//! reads. Six modules:
//!
//! * **kad** — one-shot Kademlia client. Single FIND_NODE round trip
//!   over a polkadot-p2p-connect [`Connection`]. Filters peers to those
//!   with a browser-reachable wss endpoint.
//! * **peer_pool** — persistable reputation pool. Track success/failure
//!   per peer, quarantine on repeated failure, evict to a cap.
//! * **discovery** — orchestrator. Fan probes out to known bootnodes,
//!   harvest peers, fold into the pool. Generic over [`Connect`] and
//!   [`Clock`] so it works against any transport and supports
//!   deterministic tests.
//! * **request** — pool-aware retry combinator. Pick the best pooled
//!   peer, run a user-supplied async closure against it, record
//!   success/failure, fall back to a bootnode on failure or empty pool.
//! * **state** — wasm-compatible state-proof verification.
//!   [`verify_top_proof`] / [`verify_child_proof`] take a `/state/2`
//!   response and a finalized state root and return the value at a
//!   given key, or an error. Avoids `sp-state-machine` (pulls in
//!   hyper/tokio/mio).
//! * **(re-exported from [`warp_sync`])** — [`GrandpaState`],
//!   [`BlockHeader`], [`load_checkpoint`], etc. The finality chain
//!   that anchors trust for `state`'s proof checks.
//!
//! The per-peer connection lifecycle (noise + yamux + multistream +
//! substrate request-response / subscription protocols) lives in
//! [`polkadot_p2p_connect`]; the per-chain finality + warp-sync
//! protocol lives in [`warp_sync`]. microdot sits on top of both and
//! adds the multi-peer concerns plus the state-proof primitive.
//!
//! ## Layering
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │ app code (downstream wasm / tokio adapters / examples)       │
//! │   - WebSocket/TCP bridge implementing Connect                │
//! │   - localStorage/file implementing Storage                   │
//! │   - Date.now()/SystemTime implementing Clock                 │
//! │   - chain-specific constants & storage-key derivation        │
//! └────────────────────────────────────┬─────────────────────────┘
//!                                      │
//! ┌────────────────────────────────────▼─────────────────────────┐
//! │ microdot                                                     │
//! │   - kad        peer_pool       discovery                     │
//! │   - request    state           (re-exported warp-sync)       │
//! │   - traits: Connect, Storage, Clock                          │
//! └────────────────────────────────────┬─────────────────────────┘
//!                                      │
//! ┌────────────────────────────────────▼─────────────────────────┐
//! │ warp-sync                                                    │
//! │   - GrandpaState, justification verification, checkpoint     │
//! │   - Substrate wire types (BlockHeader, etc.)                 │
//! └────────────────────────────────────┬─────────────────────────┘
//!                                      │
//! ┌────────────────────────────────────▼─────────────────────────┐
//! │ polkadot-p2p-connect                                         │
//! │   - single-peer Connection<R, W, P>                          │
//! │   - noise + yamux + multistream + sub/req protocols          │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Testability
//!
//! Every layer here accepts dependencies via traits, so each is
//! independently testable with mocks:
//!
//! * `kad` is pure — works on `&[u8]`, unit-testable with no I/O.
//! * `peer_pool` takes `now_ms: u64` so a `TestClock` drives time.
//! * `discovery` takes `&impl Connect` so a mock connector simulates
//!   any combination of slow / failed / successful bootnodes.
//! * `request` takes `&impl Clock` plus a user closure, so the
//!   retry/fallback logic is exercisable with an in-memory pool.
//! * `state` is pure — feeds bytes in, gets bytes out — no I/O, no
//!   network, just trie verification.
//!
//! See [`testing`] for in-memory `Clock` and `Storage` impls intended
//! for use in downstream test suites.

pub mod discovery;
pub mod kad;
pub mod peer_pool;
pub mod request;
pub mod state;
pub mod traits;

#[cfg(any(test, feature = "testing"))]
pub use traits::testing;

pub use peer_pool::{PeerEntry, PeerPool};
pub use request::run_with_pool_fallback;
pub use state::{
    child_storage_default_prefix, encode_state_request, verify_child_proof, verify_top_proof,
};
pub use traits::{Clock, Connect, Storage};

// Re-export warp-sync so consumers have a single smoldot-style import
// surface: `use microdot::{GrandpaState, BlockHeader, …}`. The
// underlying crate stays a separate sibling because the wire format
// types and warp-sync protocol logic belong on the polkadot-p2p-connect
// side of the layer cake.
pub use warp_sync::{
    AuthorityId, BlockDigest, BlockDigestItem, BlockHash, BlockHeader, ConsensusEngineId,
    GrandpaState, Hash, checkpoint, load_checkpoint,
};
