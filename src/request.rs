//! Pool-aware request retry. The smoldot-style hot path: try available
//! peers from the pool in reputation order, run the user-supplied async
//! closure against each peer's URL, record success/failure on the pool,
//! and fall back to a hardcoded bootnode URL only if the pool is empty
//! or every available pooled peer failed.
//!
//! This is intentionally generic: the closure receives a `&str` URL
//! and returns any `Result<T, String>`. It can do a one-off state
//! read, an entire warp-sync session, or anything in between — the
//! combinator only cares about the success/failure signal so it can
//! update the pool's reputation tracking.
//!
//! ## Privacy property
//!
//! Bootnodes are intentionally *not* added to the pool here. They
//! serve as the last-resort fallback only; the pool exists to hold
//! peers learned via kad `FIND_NODE` so the hot path can avoid
//! disclosing both halves of a query (discovery + actual read) to the
//! same party. Callers should warm the pool via [`crate::discovery`]
//! before invoking this combinator, otherwise every request will fall
//! through to the bootnode.

use core::future::Future;

use crate::peer_pool::PeerPool;
use crate::traits::Clock;

/// Run `request` against available pooled peers from best to worst. On
/// failure of every available pooled peer (or if the pool is empty),
/// retry once against `bootnode_url` and return that result regardless
/// of outcome.
///
/// `request` is invoked at most `available_pool_peers + 1` times.
/// Success against any pooled peer short-circuits the fallback. Pool
/// reputation is updated only for pooled-peer attempts; the bootnode
/// attempt is not tracked because bootnodes do not live in the pool.
pub async fn run_with_pool_fallback<C, F, Fut, T>(
    pool: &mut PeerPool,
    bootnode_url: &str,
    clock: &C,
    mut request: F,
) -> Result<T, String>
where
    C: Clock,
    F: FnMut(&str) -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let now = clock.now_ms();
    for (picked_id, picked_entry) in pool.ranked_available(now) {
        let result = request(&picked_entry.wss_url).await;
        let now2 = clock.now_ms();
        match result {
            Ok(value) => {
                pool.record_success(&picked_id, now2);
                return Ok(value);
            }
            Err(_) => {
                pool.record_failure(&picked_id, now2);
            }
        }
    }
    request(bootnode_url).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::testing::TestClock;
    use core::cell::RefCell;
    use futures::executor::block_on;

    fn fresh_pool_with_one_peer(clock: &TestClock) -> PeerPool {
        let mut pool = PeerPool::new();
        pool.observe("P".into(), "wss://pool-peer/".into(), clock.now_ms());
        pool
    }

    #[test]
    fn picks_pool_peer_and_records_success_when_request_succeeds() {
        let clock = TestClock::new(1_000);
        let mut pool = fresh_pool_with_one_peer(&clock);
        let urls_seen = RefCell::new(Vec::<String>::new());

        let result = block_on(run_with_pool_fallback(
            &mut pool,
            "wss://bootnode/",
            &clock,
            |url| {
                let url = url.to_string();
                let urls_seen = &urls_seen;
                async move {
                    urls_seen.borrow_mut().push(url);
                    Ok::<_, String>(42)
                }
            },
        ))
        .unwrap();

        assert_eq!(result, 42);
        assert_eq!(
            urls_seen.borrow().as_slice(),
            &["wss://pool-peer/".to_string()]
        );
        // Reputation bumped, last_seen updated to clock.now_ms() at record time.
        let entry = &pool.peers["P"];
        assert!(entry.failures == 0);
        assert!(entry.successes >= 1);
    }

    #[test]
    fn records_failure_and_retries_bootnode_when_pool_peer_errors() {
        let clock = TestClock::new(2_000);
        let mut pool = fresh_pool_with_one_peer(&clock);
        let urls_seen = RefCell::new(Vec::<String>::new());

        let result = block_on(run_with_pool_fallback(
            &mut pool,
            "wss://bootnode/",
            &clock,
            |url| {
                let url = url.to_string();
                let urls_seen = &urls_seen;
                async move {
                    urls_seen.borrow_mut().push(url.clone());
                    if url == "wss://pool-peer/" {
                        Err::<u32, _>("simulated peer failure".to_string())
                    } else {
                        Ok(7)
                    }
                }
            },
        ))
        .unwrap();

        assert_eq!(result, 7);
        assert_eq!(
            urls_seen.borrow().as_slice(),
            &[
                "wss://pool-peer/".to_string(),
                "wss://bootnode/".to_string(),
            ]
        );
        // Pool peer recorded one failure.
        assert_eq!(pool.peers["P"].failures, 1);
    }

    #[test]
    fn exhausts_available_pool_peers_before_bootnode_fallback() {
        let clock = TestClock::new(2_500);
        let mut pool = PeerPool::new();
        pool.observe("A".into(), "wss://a/".into(), clock.now_ms());
        pool.observe("B".into(), "wss://b/".into(), clock.now_ms() + 1);
        pool.observe("C".into(), "wss://c/".into(), clock.now_ms() + 2);
        pool.record_success("A", clock.now_ms() + 3);
        pool.record_success("A", clock.now_ms() + 4);
        pool.record_success("B", clock.now_ms() + 5);

        let urls_seen = RefCell::new(Vec::<String>::new());
        let result = block_on(run_with_pool_fallback(
            &mut pool,
            "wss://bootnode/",
            &clock,
            |url| {
                let url = url.to_string();
                let urls_seen = &urls_seen;
                async move {
                    urls_seen.borrow_mut().push(url.clone());
                    if url == "wss://b/" {
                        Ok::<_, String>(17)
                    } else {
                        Err::<u32, _>(format!("{url} failed"))
                    }
                }
            },
        ))
        .unwrap();

        assert_eq!(result, 17);
        assert_eq!(
            urls_seen.borrow().as_slice(),
            &["wss://a/".to_string(), "wss://b/".to_string()]
        );
        assert_eq!(pool.peers["A"].failures, 1);
        assert_eq!(pool.peers["B"].failures, 0);
        assert_eq!(pool.peers["B"].successes, 2);
        assert_eq!(pool.peers["C"].failures, 0);
    }

    #[test]
    fn falls_back_to_bootnode_when_pool_is_empty() {
        let clock = TestClock::new(3_000);
        let mut pool = PeerPool::new();
        let urls_seen = RefCell::new(Vec::<String>::new());

        let result = block_on(run_with_pool_fallback(
            &mut pool,
            "wss://bootnode/",
            &clock,
            |url| {
                let url = url.to_string();
                let urls_seen = &urls_seen;
                async move {
                    urls_seen.borrow_mut().push(url);
                    Ok::<_, String>(99)
                }
            },
        ))
        .unwrap();

        assert_eq!(result, 99);
        assert_eq!(
            urls_seen.borrow().as_slice(),
            &["wss://bootnode/".to_string()]
        );
    }

    #[test]
    fn surfaces_bootnode_error_when_both_paths_fail() {
        let clock = TestClock::new(4_000);
        let mut pool = fresh_pool_with_one_peer(&clock);

        let err = block_on(run_with_pool_fallback(
            &mut pool,
            "wss://bootnode/",
            &clock,
            |_url| async { Err::<u32, _>("everything is on fire".to_string()) },
        ))
        .unwrap_err();

        assert!(err.contains("on fire"));
        // Pool peer still got its failure recorded.
        assert_eq!(pool.peers["P"].failures, 1);
    }
}
