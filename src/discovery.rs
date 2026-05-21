//! Discovery driver: fan probes out to bootnodes in parallel, run a
//! one-shot Kademlia FIND_NODE on each, fold the returned peers into a
//! [`PeerPool`].
//!
//! ### Invariants
//!
//! * **No-bootnodes-in-the-pool.** Bootnodes are never observed into
//!   the pool, even after a successful probe. They stay in the
//!   hardcoded list and serve only as the discovery seed and last-
//!   resort fallback. Putting them in the pool would defeat the
//!   privacy property: hot-path queries should never flow back to a
//!   node that already saw the kad-discovery half.
//! * **Per-probe timeout.** Each bootnode probe owns its own deadline
//!   (`PER_PROBE_TIMEOUT`). The burst returns when every probe has
//!   either finished or timed out — there's no outer timeout that
//!   would discard partials.
//! * **No connection reuse.** Each probe opens, queries, closes. v1
//!   design choice — keeps lifecycle trivial and makes per-probe
//!   timeouts straightforward.
//!
//! ### Generic parameters
//!
//! * `P: PlatformT` — randomness + sleep, from polkadot-p2p-connect.
//!   The platform's `sleep` is what powers the per-probe deadline.
//! * `C: Connect` — opens the byte stream. Browser adapter:
//!   WebSocket. Native: TCP. Test: a mock that returns programmed
//!   `AsyncRead`/`AsyncWrite` halves.
//! * `K: Clock` — wall clock for pool timestamps. In tests, a
//!   counter you can advance manually.

use core::time::Duration;
use std::time::Duration as StdDuration;

use futures::future::join_all;
use polkadot_p2p_connect::{Configuration, PlatformT, RequestProtocol};

use crate::kad::{self, DiscoveredPeer};
use crate::peer_pool::PeerPool;
use crate::traits::{Clock, Connect};

/// Max acceptable kad response size. Substrate kad responses are dense
/// (~14–20 KiB observed in the wasm spike); 64 KiB is comfortable
/// headroom without inviting abuse from a misbehaving peer.
pub const KAD_MAX_RESPONSE_SIZE: usize = 64 * 1024;

/// Kad-request timeout enforced inside `RequestProtocol` itself.
/// Bounds the FIND_NODE round-trip once the substream is open.
pub const KAD_REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(8);

/// Per-probe hard cap covering the full lifecycle: open, noise, yamux,
/// multistream-select, kad request, kad response. Each probe owns this
/// deadline independently. Leaves headroom over [`KAD_REQUEST_TIMEOUT`]
/// for the handshake chain (~2–4 s on cold cache).
pub const PER_PROBE_TIMEOUT: Duration = Duration::from_secs(12);

/// Outcome of one bootnode probe. Held as a value (not a `Result`) so
/// the caller's bookkeeping is uniform: every input bootnode produces
/// exactly one outcome, whether it succeeded, timed out, or never even
/// opened.
#[derive(Debug, Clone)]
pub struct ProbeOutcome {
    /// Bootnode peer-id as parsed from the multiaddr. Used for
    /// logging and to deduplicate with whatever `Connection::their_id`
    /// reports (which can differ for DNS-load-balanced endpoints).
    pub bootnode_peer_id: String,
    /// Bootnode wss URL the probe targeted.
    pub bootnode_wss_url: String,
    /// Either the harvested peers or a failure reason string.
    /// String-typed because the failure source is a mix of i/o,
    /// timeout, protobuf-decode, and remote-error cases — none of
    /// which the caller can act on differently in v1.
    pub result: Result<Vec<DiscoveredPeer>, String>,
}

/// Summary of one discovery burst. Returned to the caller so they can
/// log / surface metrics without microdot taking a dependency on
/// any particular logging facade.
#[derive(Debug, Clone, Default)]
pub struct DiscoveryReport {
    /// One outcome per input bootnode, in input order.
    pub outcomes: Vec<ProbeOutcome>,
    /// Count of probes that returned `Ok(_)`. Convenience field —
    /// always equal to `outcomes.iter().filter(|o| o.result.is_ok()).count()`.
    pub bootnodes_succeeded: usize,
    /// Count of fresh peers observed into the pool across all probes.
    /// May be larger than `pool.len()` after the burst if some
    /// observations updated existing entries instead of inserting.
    pub peers_observed: usize,
}

// ---------------------------------------------------------------------------
// One probe
// ---------------------------------------------------------------------------

async fn probe_one_bootnode<P, C>(
    bootnode_multiaddr: String,
    protocol_prefix_hex: String,
    connect: &C,
) -> ProbeOutcome
where
    P: PlatformT + 'static,
    C: Connect,
{
    let (peer_id, wss_url) = match kad::parse_bootnode_multiaddr(&bootnode_multiaddr) {
        Some(parts) => parts,
        None => {
            return ProbeOutcome {
                bootnode_peer_id: bootnode_multiaddr.clone(),
                bootnode_wss_url: bootnode_multiaddr,
                result: Err("could not parse bootnode multiaddr".to_string()),
            };
        }
    };

    let kad_protocol_name = format!("/{protocol_prefix_hex}/kad");

    let inner = async {
        let (reader, writer) = connect
            .connect(&wss_url)
            .await
            .map_err(|e| format!("connect: {e}"))?;
        let mut config: Configuration<P> = Configuration::new();
        let kad_id = config.add_protocol(
            RequestProtocol::new(kad_protocol_name)
                .with_max_response_size(KAD_MAX_RESPONSE_SIZE)
                .with_timeout(KAD_REQUEST_TIMEOUT),
        );
        let mut conn = config
            .connect(reader, writer)
            .await
            .map_err(|e| format!("handshake: {e}"))?;
        let target = kad::random_target::<P>();
        kad::one_shot_find_node(&mut conn, kad_id, &target)
            .await
            .map_err(|e| format!("find_node: {e}"))
        // conn drops here — no reuse.
    };

    // Per-probe deadline. Loser drops the in-flight future, which
    // closes the byte stream and unblocks `join_all` to record this
    // outcome alongside the rest.
    let timeout = P::sleep(PER_PROBE_TIMEOUT);
    let result = match futures::future::select(Box::pin(inner), Box::pin(timeout)).await {
        futures::future::Either::Left((res, _)) => res,
        futures::future::Either::Right((_, _)) => {
            Err(format!("probe timed out after {}s", PER_PROBE_TIMEOUT.as_secs()))
        }
    };
    ProbeOutcome {
        bootnode_peer_id: peer_id,
        bootnode_wss_url: wss_url,
        result,
    }
}

// ---------------------------------------------------------------------------
// The burst
// ---------------------------------------------------------------------------

/// Fan probes out to every bootnode in `bootnodes` in parallel. Folds
/// the discovered peers into `pool` (bootnodes themselves are NOT
/// added — see module docs). Returns the updated pool together with a
/// `DiscoveryReport` summary; the caller decides what to log and when
/// to persist.
pub async fn discovery_burst<P, C, K>(
    mut pool: PeerPool,
    bootnodes: &[&str],
    protocol_prefix_hex: String,
    connect: &C,
    clock: &K,
) -> (PeerPool, DiscoveryReport)
where
    P: PlatformT + 'static,
    C: Connect,
    K: Clock,
{
    if bootnodes.is_empty() {
        return (pool, DiscoveryReport::default());
    }

    let probes = bootnodes.iter().map(|b| {
        let s = (*b).to_string();
        let prefix = protocol_prefix_hex.clone();
        probe_one_bootnode::<P, C>(s, prefix, connect)
    });
    let outcomes = join_all(probes).await;

    let now = clock.now_ms();
    let mut bootnodes_succeeded = 0usize;
    let mut peers_observed = 0usize;

    for outcome in &outcomes {
        match &outcome.result {
            Ok(peers) => {
                bootnodes_succeeded += 1;
                for p in peers {
                    pool.observe(p.peer_id_base58.clone(), p.wss_url.clone(), now);
                    peers_observed += 1;
                }
            }
            Err(_) => {
                // Bootnode failures are intentionally NOT recorded
                // against the pool. Bootnodes live in the hardcoded
                // list, not in reputation tracking — see module docs.
            }
        }
    }

    pool.evict_to_capacity();

    let report = DiscoveryReport {
        outcomes,
        bootnodes_succeeded,
        peers_observed,
    };
    (pool, report)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    //! End-to-end tests for `discovery_burst` require a mock `Connect`
    //! whose Reader/Writer satisfy polkadot-p2p-connect's
    //! `AsyncRead`/`AsyncWrite` traits. That mock is non-trivial and
    //! lands in a follow-up alongside the orchestrator tests. For now
    //! we cover the parts that don't need network:
    //!
    //! * The `DiscoveryReport` default shape (so callers can build
    //!   summaries without thinking about field initialisation).
    //! * The clock contract: pool observations stamp `last_seen` to
    //!   `clock.now_ms()` at the moment of the fold.

    use super::*;
    use crate::testing::TestClock;

    #[test]
    fn discovery_report_default_is_zeroed() {
        let r = DiscoveryReport::default();
        assert_eq!(r.bootnodes_succeeded, 0);
        assert_eq!(r.peers_observed, 0);
        assert!(r.outcomes.is_empty());
    }

    #[test]
    fn clock_drives_pool_observation_timestamps() {
        // Verifies the contract: a probe's observed peers get
        // last_seen=clock.now_ms() at the time the burst was folded.
        // The actual fold lives inside `discovery_burst`, but the
        // expectation is exercisable against `PeerPool::observe`
        // directly with a controlled clock.
        let clock = TestClock::new(1_000);
        let mut pool = PeerPool::new();
        pool.observe("P".into(), "wss://p/".into(), clock.now_ms());
        clock.advance(500);
        pool.observe("Q".into(), "wss://q/".into(), clock.now_ms());
        assert_eq!(pool.peers["P"].last_seen_unix_ms, 1_000);
        assert_eq!(pool.peers["Q"].last_seen_unix_ms, 1_500);
    }
}
