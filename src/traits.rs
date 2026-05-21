//! Injection points that make the orchestrator platform-agnostic.
//!
//! Three traits, each capturing one slice of "stuff the host environment
//! provides":
//!
//! * [`Connect`] — opens an authenticated byte stream to a peer. The
//!   browser adapter wraps WebSockets; a native adapter wraps TCP.
//! * [`Storage`] — durable key-value bytes-bag. Browser: localStorage.
//!   Native: a file on disk. Tests: a `BTreeMap`.
//! * [`Clock`] — `now_ms()` since unix epoch. Tests: an in-memory
//!   counter so quarantine / recency tie-breaking can be asserted
//!   deterministically.
//!
//! Randomness is **not** a trait here — it's already provided by
//! [`polkadot_p2p_connect::PlatformT::fill_with_random_bytes`], which
//! every consumer of this crate already needs to implement for the
//! underlying noise handshake. Re-exposing it as a separate trait
//! would double the wiring with no benefit.

use core::fmt::Display;
use core::future::Future;

use polkadot_p2p_connect::{AsyncRead, AsyncWrite};

/// Opens an authenticated byte stream to a remote peer addressed by URL.
///
/// The `url` argument is a flat `wss://host:port/` (browser) or
/// `host:port` (native TCP) string — whichever shape the adapter
/// expects. microdot's discovery layer feeds it the result of
/// [`crate::kad::multiaddr_to_wss_url`] or
/// [`crate::kad::parse_bootnode_multiaddr`], which always produce
/// `wss://…` strings. Native adapters can parse those back to a
/// host:port if they want.
pub trait Connect {
    /// Reader half of the opened stream.
    type Reader: AsyncRead + 'static;
    /// Writer half of the opened stream.
    type Writer: AsyncWrite + 'static;
    /// Failure reason. Held by-value rather than as `&dyn Error` so
    /// no_std consumers (future) don't need the std error trait.
    type Error: Display;
    /// Future returned by [`connect`](Self::connect). Spelled out as
    /// an associated type so the trait works in async-trait-free,
    /// dyn-compatible-ish contexts.
    type Future<'a>: Future<Output = Result<(Self::Reader, Self::Writer), Self::Error>>
    where
        Self: 'a;

    fn connect<'a>(&'a self, url: &'a str) -> Self::Future<'a>;
}

/// Durable bytes-bag. Used by the consumer-supplied state layer to
/// persist things across page reloads / process restarts. microdot
/// itself does not call `Storage` directly — it's exposed here so
/// downstream pool-persistence helpers can build on a single trait.
pub trait Storage {
    type Error: Display;
    fn load(&self) -> Result<Option<Vec<u8>>, Self::Error>;
    fn save(&self, data: &[u8]) -> Result<(), Self::Error>;
}

/// Wall-clock-ish time source. We only need milliseconds since unix
/// epoch — enough to drive the pool's recency tiebreaker and quarantine
/// timestamps. Adapters expose `Date.now()` (browser) or
/// `SystemTime::now()` (native). Tests inject a counter.
pub trait Clock {
    fn now_ms(&self) -> u64;
}

#[cfg(test)]
mod tests {
    //! Tests for the trait surface live next to the trait so adapter
    //! crates can mirror this structure. Most of the actual behavioural
    //! tests are in modules that consume the traits — there's not much
    //! to assert about the traits themselves besides that they're
    //! object-safe-shaped and that simple in-memory impls compile.

    use super::*;
    use core::cell::Cell;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// Tick-counter Clock for tests. Mutating advances time; reading
    /// observes the current tick.
    pub struct TestClock(Cell<u64>);

    impl TestClock {
        pub fn new(initial_ms: u64) -> Self {
            Self(Cell::new(initial_ms))
        }
        pub fn advance(&self, by_ms: u64) {
            self.0.set(self.0.get() + by_ms);
        }
    }

    impl Clock for TestClock {
        fn now_ms(&self) -> u64 {
            self.0.get()
        }
    }

    /// In-memory Storage that any test can use to drive the
    /// persistence path without touching disk or browser APIs.
    pub struct MemoryStorage(Mutex<Option<Vec<u8>>>);

    impl Default for MemoryStorage {
        fn default() -> Self {
            Self(Mutex::new(None))
        }
    }

    impl Storage for MemoryStorage {
        type Error = std::convert::Infallible;
        fn load(&self) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn save(&self, data: &[u8]) -> Result<(), Self::Error> {
            *self.0.lock().unwrap() = Some(data.to_vec());
            Ok(())
        }
    }

    #[test]
    fn test_clock_advances_monotonically() {
        let c = TestClock::new(100);
        assert_eq!(c.now_ms(), 100);
        c.advance(50);
        assert_eq!(c.now_ms(), 150);
        c.advance(0);
        assert_eq!(c.now_ms(), 150);
    }

    #[test]
    fn memory_storage_round_trip() {
        let s = MemoryStorage::default();
        assert_eq!(s.load().unwrap(), None);
        s.save(b"hello").unwrap();
        assert_eq!(s.load().unwrap(), Some(b"hello".to_vec()));
        // Idempotent overwrite.
        s.save(b"world").unwrap();
        assert_eq!(s.load().unwrap(), Some(b"world".to_vec()));
    }

    /// Sanity: a `Vec<Box<dyn Clock>>` compiles. We expect Clock to
    /// be dyn-safe so test harnesses can mix-and-match implementations.
    #[test]
    fn clock_is_dyn_safe() {
        let _clocks: Vec<Box<dyn Clock>> = vec![Box::new(TestClock::new(0))];
    }

    #[test]
    fn key_value_map_can_back_storage_with_a_thin_wrapper() {
        // Demonstrates the intended pattern: an adapter can wrap any
        // KV store by impl'ing Storage for it. Not strictly a test of
        // the trait, but useful as living documentation.
        struct MapStorage(Mutex<BTreeMap<String, Vec<u8>>>);
        impl Storage for MapStorage {
            type Error = std::convert::Infallible;
            fn load(&self) -> Result<Option<Vec<u8>>, Self::Error> {
                Ok(self.0.lock().unwrap().get("state").cloned())
            }
            fn save(&self, data: &[u8]) -> Result<(), Self::Error> {
                self.0.lock().unwrap().insert("state".into(), data.to_vec());
                Ok(())
            }
        }
        let s = MapStorage(Mutex::new(BTreeMap::new()));
        s.save(b"abc").unwrap();
        assert_eq!(s.load().unwrap(), Some(b"abc".to_vec()));
    }
}

// Re-export the test helpers under a `pub mod testing` so downstream
// adapter crates (wasm bindings, tokio bridge, examples) can reuse
// them in their own tests.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use super::*;
    use core::cell::Cell;
    use std::sync::Mutex;

    /// See [`crate::traits::tests::TestClock`].
    pub struct TestClock(Cell<u64>);
    impl TestClock {
        pub fn new(initial_ms: u64) -> Self {
            Self(Cell::new(initial_ms))
        }
        pub fn advance(&self, by_ms: u64) {
            self.0.set(self.0.get() + by_ms);
        }
    }
    impl Clock for TestClock {
        fn now_ms(&self) -> u64 {
            self.0.get()
        }
    }

    /// See [`crate::traits::tests::MemoryStorage`].
    #[derive(Default)]
    pub struct MemoryStorage(Mutex<Option<Vec<u8>>>);
    impl Storage for MemoryStorage {
        type Error = core::convert::Infallible;
        fn load(&self) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn save(&self, data: &[u8]) -> Result<(), Self::Error> {
            *self.0.lock().unwrap() = Some(data.to_vec());
            Ok(())
        }
    }
}
