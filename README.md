# microdot

A tiny, testable substrate light-client toolkit. The immediate goal is
read-only chain-state access from a browser tab (or any host environment)
without trusting a centralised RPC provider, and without shipping smoldot.

> Status: very early. The platform-agnostic core (kad client, peer pool,
> discovery orchestrator) is extracted from a working browser prototype,
> generic over host traits, and covered by 24 unit tests. Browser / native
> adapters (WebSocket / localStorage / Crypto, or TCP / file / SystemTime)
> are intended to live in downstream crates, à la smoldot's `light-js`.


**Disclaimer**: This project is the result of Claude, Codex and Kimi working together very hard over the weekend. It is intended as a teaching project to understand how a read-only client could potentially work and not intended as a smoldot replacement. 


## What's in the crate

`microdot` is a single Rust crate providing a one-shot Kademlia client, a
persistable peer pool, and a discovery orchestrator. It's generic over
`Connect`, `Storage`, and `Clock` traits, so it composes against any
transport / persistence / time source. It depends on James' (@jsdw)
[`polkadot-p2p-connect`](https://github.com/paritytech/polkadot-p2p-connect)
for the per-peer connection lifecycle (noise + yamux + multistream-select +
substrate request-response).

Browser and native adapters live downstream. A future `microdot-light-js`
will satisfy the core traits using `web-sys` (WebSocket → `Connect`,
localStorage → `Storage`, `Date.now()` → `Clock`, `window.crypto` →
`PlatformT`) and ship as an npm package, mirroring smoldot's `light-js`
wrapper. A `microdot-tokio` will do the same for native TCP + file-backed
storage, and a `microdot-queries` crate will hold higher-level helpers
(warp-sync, state-read with proof verification, pool-aware query wrappers)
that currently live inline in prototype/example apps.

## Why "microdot"?

* Built for substrate-based chains (the "dot" in microdot is Polkadot).
* Aims to be **smaller**, **simpler**, and **easier to test** than alternatives
  like smoldot. Trade-off: less feature surface (no transaction submission, no
  finality streaming yet, no parachain runtime execution).
* Designed in layers, each independently testable with mocks. The architectural
  invariant is: anything platform-dependent goes through a trait.

## How it relates to `polkadot-p2p-connect`

`polkadot-p2p-connect` does the single-peer part: noise, yamux, multistream,
substrate's request-response and notification protocols, all in `no_std`. It
intentionally leaves out everything multi-peer.

`microdot` is the layer on top:

* **Discover** more peers via Kademlia (one-shot `FIND_NODE` per bootnode).
* **Pool** peers with reputation tracking — successes vs. failures, recency
  tiebreak, quarantine on repeated failures.
* **Orchestrate** discovery: fan out Kademlia probes, fold browser-reachable
  peers into the pool, and return a report the host can log or persist.
* **Support** the hot path: the pool can pick the best available peer and
  record success/failure, while query wrappers are still app-level code.

The two libraries compose cleanly because `microdot` consumes the
`polkadot-p2p-connect` `Connection<R, W, P>` type via traits — no wrapper, no
fork, no duplicated code paths.

## Privacy property

By design, **the hot path never queries a bootnode once the pool is warm**.
Bootnodes are used only for the (short, content-free) Kademlia probe that
discovers other peers. Any actual chain query (`/sync/warp`, `/state/2`, etc.)
flows through a peer the bootnode doesn't know we're talking to. This means
no single party sees both halves of "this client did kad discovery for chain
X" + "this client read storage key Y at block N".

This is implemented by separating bootnodes from discovered peers:

1. `microdot::discovery` does not directly observe the probed bootnode
   into the pool, even after a successful probe. Bootnodes live in a separate
   host-supplied list and serve as the discovery seed and last-resort fallback.
2. The pool is intended for peers learned via `FIND_NODE` responses. Hosts
   should purge/filter any known bootnode peer IDs when loading or merging
   persisted pools.

Caveat: the **first ever** cold load has no pool yet and necessarily falls
back to hardcoded bootnodes for both the kad probe AND the first hot-path
query. Subsequent loads can use the warmed pool once the host persists it.

## Testability

The core philosophy: **maximise pure functions, trait-bound the rest, make
the platform layer thin enough that almost everything can be unit-tested
without a browser, a network, or a clock**.

Current state:

* `microdot::kad` — 11 unit tests covering protobuf encoding, multiaddr
  decoding (positive + negative), bootnode-string parsing.
* `microdot::peer_pool` — 7 unit tests covering reputation, quarantine,
  eviction, serde round-trip.
* `microdot::traits` — 4 unit tests of the in-memory `TestClock` and
  `MemoryStorage` helpers + dyn-safety.
* `microdot::discovery` — 2 unit tests covering the report shape and
  the clock-driven observation timestamp.

Total: **24/24 passing**. The roadmap is to expand to several hundred tests
using property-based testing (`proptest`), snapshot testing (`insta`),
mocked transport (a programmable `Connect` impl that simulates slow / failed
/ successful handshakes), and a feature-gated live-network suite that
exercises real Paseo bootnodes.

## Building

```
cargo test
```

That's it for now. A future `microdot-light-js` crate will provide the
browser bindings (cdylib + wasm-pack) and ship as an npm package; today
this crate is host-only (`rlib`) and remains target-agnostic.

## Architecture diagram

```
┌──────────────────────────────────────────────────────────────┐
│ app code (examples now / microdot-queries future)            │
│   - bring chain spec, run pool-aware queries                  │
└────────────────────────────────────┬─────────────────────────┘
                                     │
┌────────────────────────────────────▼─────────────────────────┐
│ microdot                                                     │
│   ┌──────────────────────────────────────────────────────┐   │
│   │ kad        peer_pool       discovery                 │   │
│   │   ▲          ▲                ▲                      │   │
│   │   └─ pure ───┴── pure ────────┘                      │   │
│   │                                                      │   │
│   │ traits: Connect, Storage, Clock                      │   │
│   └──────────────────────────────────────────────────────┘   │
└────────────────────────────────────┬─────────────────────────┘
                                     │ Connection<R,W,P>
┌────────────────────────────────────▼─────────────────────────┐
│ polkadot-p2p-connect                                         │
│   noise + yamux + multistream + sub/req protocols            │
└──────────────────────────────────────────────────────────────┘
                                     │  AsyncRead/AsyncWrite traits
┌────────────────────────────────────▼─────────────────────────┐
│ microdot-light-js  (WebSocket / localStorage / web_sys)      │
│ microdot-tokio     (TCP / file / SystemTime)    ← future     │
└──────────────────────────────────────────────────────────────┘
```

Reading the layers bottom-up: the host environment provides `AsyncRead` +
`AsyncWrite` (just bytes). `polkadot-p2p-connect` layers noise + yamux +
multistream + substrate protocols on top to give you a typed `Connection`.
`microdot` layers peer discovery, reputation, and discovery orchestration
on top of that. Today, application code owns the pool-aware query wrappers;
those are planned to move into `microdot-queries`.

## License

GPL-3.0-or-later.
