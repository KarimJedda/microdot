//! Peer pool with simple, persistable reputation scoring.
//!
//! Held alongside the existing `PersistedState` so the schema rides on the
//! same localStorage flow. **Storage and time are injected**: this module
//! does not touch the browser. That makes every behavior unit-testable
//! against `BTreeMap` + a constant `now_ms`.
//!
//! ### Scoring model
//!
//! `score = successes - 2 * failures`. Ties broken by `last_seen_unix_ms`
//! (more-recent wins). A peer with 3 **consecutive** failures (success
//! resets the counter) is quarantined for `QUARANTINE_DURATION_MS`. The
//! quarantine timestamp lives on the entry so it survives reloads.
//!
//! ### Eviction
//!
//! Pool is bounded at `MAX_POOL_SIZE` entries. When the bound is
//! exceeded, lowest-scoring entries (then least-recently-seen) are
//! dropped. Eviction is explicit (`evict_to_capacity`) rather than
//! automatic on insert, so callers can decide when to pay the cost.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Hard cap on entries we'll keep in a single pool. 50 × ~200 bytes is
/// well under any localStorage quota and well above what discovery from
/// 7 bootnodes will produce in a 10-second burst (~30-60 useful peers).
pub const MAX_POOL_SIZE: usize = 50;

/// Number of **consecutive** failures (success resets) before a peer is
/// quarantined.
pub const QUARANTINE_FAIL_THRESHOLD: u32 = 3;

/// How long a quarantined peer stays unavailable.
pub const QUARANTINE_DURATION_MS: u64 = 5 * 60 * 1000;

/// One entry in the pool. Keyed by the peer's base58-encoded peer-id.
///
/// All fields are `#[serde(default)]` so adding fields later doesn't
/// invalidate persisted state.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PeerEntry {
    /// The wss URL we use to open a WebSocket. Stored rather than the
    /// raw multiaddr because that's what the existing `open_ws()` flow
    /// in `lib.rs` needs.
    pub wss_url: String,
    #[serde(default)]
    pub successes: u32,
    /// **Consecutive** failures since the last success. Reset on success.
    /// We intentionally don't track lifetime failure count — the
    /// quarantine + score system is enough for v1.
    #[serde(default)]
    pub failures: u32,
    #[serde(default)]
    pub last_seen_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantined_until_unix_ms: Option<u64>,
}

impl PeerEntry {
    pub fn score(&self) -> i64 {
        self.successes as i64 - 2 * (self.failures as i64)
    }

    pub fn is_available(&self, now_ms: u64) -> bool {
        match self.quarantined_until_unix_ms {
            None => true,
            Some(until) => now_ms >= until,
        }
    }
}

/// The pool itself. `BTreeMap` chosen over `HashMap` for deterministic
/// serialization order in localStorage (helps with diffing saved state
/// during debugging).
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct PeerPool {
    pub peers: BTreeMap<String, PeerEntry>,
}

impl PeerPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// Insert (or refresh) an observation of a peer. Preserves reputation
    /// across reloads — only `wss_url` and `last_seen_unix_ms` are
    /// updated.
    pub fn observe(&mut self, peer_id_base58: String, wss_url: String, now_ms: u64) {
        let entry = self.peers.entry(peer_id_base58).or_insert(PeerEntry {
            wss_url: wss_url.clone(),
            successes: 0,
            failures: 0,
            last_seen_unix_ms: now_ms,
            quarantined_until_unix_ms: None,
        });
        entry.wss_url = wss_url;
        entry.last_seen_unix_ms = now_ms;
    }

    /// Pick the highest-scoring available peer. Ties broken by recency.
    /// Returns `None` if every peer is quarantined or the pool is empty.
    pub fn pick_best(&self, now_ms: u64) -> Option<(String, PeerEntry)> {
        self.ranked_available(now_ms).into_iter().next()
    }

    /// Return all available peers sorted from best to worst. Score is
    /// primary; recency breaks ties. The returned entries are cloned so
    /// callers can attempt async work without holding a borrow on the pool.
    pub fn ranked_available(&self, now_ms: u64) -> Vec<(String, PeerEntry)> {
        let mut available: Vec<_> = self
            .peers
            .iter()
            .filter(|(_id, e)| e.is_available(now_ms))
            .map(|(id, e)| (id.clone(), e.clone()))
            .collect();
        available.sort_by(|a, b| {
            b.1.score()
                .cmp(&a.1.score())
                .then_with(|| b.1.last_seen_unix_ms.cmp(&a.1.last_seen_unix_ms))
        });
        available
    }

    pub fn record_success(&mut self, peer_id_base58: &str, now_ms: u64) {
        if let Some(e) = self.peers.get_mut(peer_id_base58) {
            e.successes = e.successes.saturating_add(1);
            // A success resets consecutive-failure counter so a previously
            // good-then-quarantined peer rejoins the pool cleanly.
            e.failures = 0;
            e.last_seen_unix_ms = now_ms;
            e.quarantined_until_unix_ms = None;
        }
    }

    pub fn record_failure(&mut self, peer_id_base58: &str, now_ms: u64) {
        if let Some(e) = self.peers.get_mut(peer_id_base58) {
            e.failures = e.failures.saturating_add(1);
            if e.failures >= QUARANTINE_FAIL_THRESHOLD {
                e.quarantined_until_unix_ms = Some(now_ms + QUARANTINE_DURATION_MS);
            }
        }
    }

    /// Remove every entry whose peer-id appears in `peer_ids`. Used to
    /// purge bootnode entries that older builds inadvertently seeded
    /// into the pool — bootnodes belong in the hardcoded fallback list,
    /// never in the pool. Idempotent: dropping unknown ids is a no-op.
    pub fn purge_peers(&mut self, peer_ids: &[String]) {
        for id in peer_ids {
            self.peers.remove(id);
        }
    }

    /// Drop the lowest-scoring entries until size ≤ `MAX_POOL_SIZE`.
    pub fn evict_to_capacity(&mut self) {
        if self.peers.len() <= MAX_POOL_SIZE {
            return;
        }
        let mut all: Vec<_> = self
            .peers
            .iter()
            .map(|(id, e)| (id.clone(), e.score(), e.last_seen_unix_ms))
            .collect();
        // Lowest score first, then oldest last_seen first.
        all.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.2.cmp(&b.2)));
        let to_drop = self.peers.len() - MAX_POOL_SIZE;
        for (id, _, _) in all.into_iter().take(to_drop) {
            self.peers.remove(&id);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn t(ms: u64) -> u64 {
        ms
    }

    #[test]
    fn observe_inserts_and_refreshes() {
        let mut p = PeerPool::new();
        p.observe("A".into(), "wss://a:443/".into(), t(1));
        assert_eq!(p.len(), 1);
        // Refresh: wss_url changes, last_seen updates, reputation
        // (none yet) is preserved.
        p.record_success("A", t(2));
        p.observe("A".into(), "wss://a-new:443/".into(), t(3));
        let e = &p.peers["A"];
        assert_eq!(e.wss_url, "wss://a-new:443/");
        assert_eq!(e.successes, 1);
        assert_eq!(e.last_seen_unix_ms, 3);
    }

    #[test]
    fn pick_best_prefers_higher_score() {
        let mut p = PeerPool::new();
        p.observe("A".into(), "wss://a/".into(), t(1));
        p.observe("B".into(), "wss://b/".into(), t(2));
        p.record_success("A", t(3));
        p.record_success("A", t(4));
        p.record_failure("B", t(5));
        let (id, _) = p.pick_best(t(10)).unwrap();
        assert_eq!(id, "A");
    }

    #[test]
    fn pick_best_ties_break_by_recency() {
        let mut p = PeerPool::new();
        p.observe("OLD".into(), "wss://old/".into(), t(1));
        p.observe("NEW".into(), "wss://new/".into(), t(100));
        // Both score 0; the newer one should win.
        let (id, _) = p.pick_best(t(200)).unwrap();
        assert_eq!(id, "NEW");
    }

    #[test]
    fn ranked_available_returns_best_to_worst_and_skips_quarantine() {
        let mut p = PeerPool::new();
        p.observe("OLD".into(), "wss://old/".into(), t(1));
        p.observe("BEST".into(), "wss://best/".into(), t(2));
        p.observe("NEW".into(), "wss://new/".into(), t(100));
        p.observe("BAD".into(), "wss://bad/".into(), t(200));

        p.record_success("BEST", t(201));
        p.record_failure("BAD", t(202));
        p.record_failure("BAD", t(203));
        p.record_failure("BAD", t(204));

        let ranked: Vec<_> = p
            .ranked_available(t(205))
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ranked, vec!["BEST", "NEW", "OLD"]);
    }

    #[test]
    fn quarantine_after_three_consecutive_failures() {
        let mut p = PeerPool::new();
        p.observe("A".into(), "wss://a/".into(), t(0));
        p.record_failure("A", t(1));
        p.record_failure("A", t(2));
        p.record_failure("A", t(3));
        assert!(!p.peers["A"].is_available(t(3)));
        assert!(p.pick_best(t(3)).is_none());
        // Quarantine ends after QUARANTINE_DURATION_MS.
        let later = 3 + QUARANTINE_DURATION_MS;
        assert!(p.peers["A"].is_available(later));
    }

    #[test]
    fn success_resets_consecutive_failure_counter() {
        let mut p = PeerPool::new();
        p.observe("A".into(), "wss://a/".into(), t(0));
        p.record_failure("A", t(1));
        p.record_failure("A", t(2));
        p.record_success("A", t(3));
        assert_eq!(p.peers["A"].failures, 0);
        // Two more failures shouldn't trip quarantine because the
        // counter was reset.
        p.record_failure("A", t(4));
        p.record_failure("A", t(5));
        assert!(p.peers["A"].is_available(t(10)));
    }

    #[test]
    fn evict_drops_lowest_scoring_first() {
        let mut p = PeerPool::new();
        // Fill the pool past capacity.
        for i in 0..(MAX_POOL_SIZE + 5) {
            let id = format!("p{i:03}");
            p.observe(id.clone(), format!("wss://h{i}/"), t(i as u64));
            // Give the first 5 entries failures to make them obvious
            // eviction targets.
            if i < 5 {
                p.record_failure(&id, t(i as u64));
            } else {
                p.record_success(&id, t(i as u64));
            }
        }
        assert_eq!(p.len(), MAX_POOL_SIZE + 5);
        p.evict_to_capacity();
        assert_eq!(p.len(), MAX_POOL_SIZE);
        // The "p000".."p004" entries (with failures) should be gone.
        for i in 0..5 {
            assert!(!p.peers.contains_key(&format!("p{i:03}")));
        }
    }

    #[test]
    fn serde_round_trip_preserves_fields() {
        let mut p = PeerPool::new();
        p.observe("A".into(), "wss://a/".into(), t(100));
        p.record_success("A", t(101));
        p.record_failure("A", t(102));
        let json = serde_json::to_string(&p).unwrap();
        let back: PeerPool = serde_json::from_str(&json).unwrap();
        assert_eq!(back.peers.len(), 1);
        assert_eq!(back.peers["A"].wss_url, "wss://a/");
        assert_eq!(back.peers["A"].successes, 1);
        assert_eq!(back.peers["A"].failures, 1);
        // last_seen_unix_ms tracks the last *success* (or initial observe),
        // not the last attempt — failures don't promote a peer's recency
        // tiebreaker. So t(101) is what we expect, not t(102).
        assert_eq!(back.peers["A"].last_seen_unix_ms, 101);
    }
}
