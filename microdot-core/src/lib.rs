//! # microdot-core
//!
//! A tiny, testable substrate-light-client toolkit. Three pieces:
//!
//! * **kad** — a one-shot Kademlia client. Single FIND_NODE round trip
//!   over a polkadot-p2p-connect [`Connection`]. Filters peers to those
//!   with a browser-reachable wss endpoint.
//! * **peer_pool** — persistable reputation pool. Track success/failure
//!   per peer, quarantine on repeated failure, evict to a cap.
//! * **discovery** — orchestrator. Fan probes out to known bootnodes,
//!   harvest peers, fold into the pool. Generic over [`Connect`] and
//!   [`Clock`] so it works against any transport and supports
//!   deterministic tests.
//!
//! The single-peer connection lifecycle (noise + yamux + multistream +
//! substrate request-response / notification protocols) lives in
//! [`polkadot_p2p_connect`]; this crate composes on top of that, adding
//! the multi-peer concerns the upstream crate intentionally leaves out.
//!
//! ## Layering
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │ app code (microdot-browser / microdot-tokio / examples)      │
//! │   - WebSocket/TCP bridge implementing Connect                │
//! │   - localStorage/file implementing Storage                   │
//! │   - Date.now()/SystemTime implementing Clock                 │
//! └────────────────────────────────────┬─────────────────────────┘
//!                                      │
//! ┌────────────────────────────────────▼─────────────────────────┐
//! │ microdot-core                                                │
//! │   - kad (one-shot FIND_NODE, protobuf, multiaddr parsing)    │
//! │   - peer_pool (reputation, serde, eviction)                  │
//! │   - discovery (orchestrator, retries, fold-into-pool)        │
//! │   - traits: Connect, Storage, Clock                          │
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
//!
//! See [`testing`] for in-memory `Clock` and `Storage` impls intended
//! for use in downstream test suites.

pub mod discovery;
pub mod kad;
pub mod peer_pool;
pub mod traits;

#[cfg(any(test, feature = "testing"))]
pub use traits::testing;

pub use peer_pool::{PeerEntry, PeerPool};
pub use traits::{Clock, Connect, Storage};
