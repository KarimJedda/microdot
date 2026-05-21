# microdot

Please read this first: [microdot rationale](https://blog.jedda.eu/bafybeiftqywairpupbszwhefmcwr4l2okt4yts3ngu4hrrxhp5du75zyli/)

A tiny, testable, wasm-friendly toolkit for trustless reads against
Substrate-based chains. Smoldot-shaped: import one crate, get peer
discovery, GRANDPA finality, and merkle-proof verified state reads. The
goal is read-only chain access from a browser tab (or any host) without
trusting a centralised RPC provider, and without shipping smoldot.

> Status: still early, but functional end-to-end. The platform-agnostic
> core (kad client, peer pool, discovery orchestrator, pool-aware
> request combinator, wasm-compatible state-proof verification, plus
> re-exported GRANDPA warp-sync from a sibling crate) is covered by
> 36 unit tests and builds for both native and `wasm32-unknown-unknown`.
> A working browser example (`example/dotli`) does trustless
> `<label>.dot` → IPFS CID resolution against live Paseo Asset Hub
> Next in ~1300 lines of app code — down from ~2800 in the inline
> prototype it was extracted from.

**Disclaimer**: This project is the result of Claude, Codex and Kimi
working together very hard over a weekend. It is intended as a
teaching project to understand how a read-only client could potentially
work and not intended as a smoldot replacement.

## What's in the crate

`microdot` is a single Rust crate exposing six modules + a re-exported
finality layer:

| Module                  | What it does                                                                                                                                                                              |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `microdot::kad`         | One-shot Kademlia `FIND_NODE` over a `polkadot-p2p-connect` `Connection`. Decodes the response, filters peers to those with a browser-reachable `wss://` endpoint.                        |
| `microdot::peer_pool`   | Persistable reputation pool. Tracks success/failure per peer, quarantines on consecutive failures, evicts to a configurable cap. `serde` round-trippable.                                  |
| `microdot::discovery`   | Orchestrator. Fans probes out to known bootnodes, harvests peers from each, folds them into the pool. Generic over `Connect` and `Clock` so it works against any transport.                |
| `microdot::request`     | Pool-aware retry combinator. `run_with_pool_fallback(pool, bootnode, clock, request)` tries pooled peers by reputation, records success/failure, then falls back to the bootnode.          |
| `microdot::state`       | Wasm-compatible state-proof verification. `verify_top_proof` / `verify_child_proof` decode a `/state/2` response, verify the compact proof against a finalized state root, return the value. |
| `microdot::traits`      | The three platform-shaped traits the layers above are written against: `Connect` (byte streams to peer URLs), `Storage` (durable key/value), `Clock` (millis).                              |
| **(re-exported)**       | `GrandpaState`, `BlockHeader`, `BlockHash`, `Hash`, `AuthorityId`, `BlockDigest`, `checkpoint`, `load_checkpoint` — from the sibling `warp-sync` crate.                                     |

It depends on the [KarimJedda fork of
`polkadot-p2p-connect`](https://github.com/KarimJedda/polkadot-p2p-connect)
for the per-peer connection lifecycle (noise + yamux +
multistream-select + substrate request-response) and the sibling
`warp-sync` library crate in the same repo for GRANDPA authority-set
tracking and `lightSyncState` checkpoint loading.

Microdot is what app code should depend on. Chain-specific bits
(genesis hashes, bootnode lists, storage-key derivation, contract ABIs,
DOM/wasm-bindgen glue) live in the consumer — see `example/dotli` for
a concrete shape.

## Why "microdot"?

* Built for substrate-based chains (the "dot" in microdot is Polkadot).
* Aims to be **smaller**, **simpler**, and **easier to test** than
  alternatives like smoldot. Trade-off: less feature surface (no
  transaction submission, no finality streaming yet, no parachain
  runtime execution).
* Designed in layers, each independently testable with mocks. The
  architectural invariant: anything platform-dependent goes through a
  trait.

## How it relates to `polkadot-p2p-connect` and `warp-sync`

`polkadot-p2p-connect` does the single-peer part: noise, yamux,
multistream, substrate's request-response and notification protocols,
all in `no_std`. It intentionally leaves out everything multi-peer.

`warp-sync` (sibling crate in the same fork) does the per-chain
finality part: `GrandpaState`, justification verification, set-id
rotation, and `lightSyncState` checkpoint decoding. It does **not**
know about peers or transports.

`microdot` composes both:

* **Discover** more peers via Kademlia (one-shot `FIND_NODE` per bootnode).
* **Pool** peers with reputation tracking — successes vs. failures,
  recency tiebreak, quarantine on repeated failures.
* **Orchestrate** discovery: fan out Kademlia probes, fold
  browser-reachable peers into the pool, and return a report the host
  can log or persist.
* **Route** requests through the pool: `run_with_pool_fallback` tries
  available peers from best to worst, attempts the user-supplied async
  closure, records outcomes on the pool, and falls back to a bootnode
  only after the pool is empty or exhausted.
* **Verify** state reads: take a `/state/2` response from any peer,
  verify the merkle proof against a finalized state root from
  `warp-sync`, return the trusted value.

The dep graph stays clean because microdot consumes
`polkadot-p2p-connect`'s `Connection<R, W, P>` and `warp-sync`'s
`GrandpaState` via owned types — no wrappers, no forks, no duplicated
code paths.

## Privacy property

By design, **the hot path tries every available pooled peer before
querying a bootnode**. Bootnodes are used primarily for the (short,
content-free) Kademlia probe that discovers other peers. Actual chain
queries (`/sync/warp`, `/state/2`, etc.) flow through discovered peers
while at least one pooled peer succeeds, so no single party sees both
halves of "this client did kad discovery for chain X" + "this client
read storage key Y at block N".

This is implemented by separating bootnodes from discovered peers:

1. `microdot::discovery` does not directly observe the probed bootnode
   into the pool, even after a successful probe. Bootnodes live in a
   separate host-supplied list and serve as the discovery seed and
   last-resort fallback.
2. The pool is intended for peers learned via `FIND_NODE` responses.
   Hosts should purge/filter any known bootnode peer IDs when loading
   or merging persisted pools.
3. `microdot::run_with_pool_fallback` retries against a bootnode only
   when the pool is empty/exhausted or every available pooled peer
   failed. The bootnode is **not** added to the pool by the combinator.

Caveat: the **first ever** cold load has no pool yet and necessarily
falls back to hardcoded bootnodes for both the kad probe AND the first
hot-path query. Subsequent loads can use the warmed pool once the host
persists it (e.g. to localStorage in the browser).

## Testability

The core philosophy: **maximise pure functions, trait-bound the rest,
make the platform layer thin enough that almost everything can be
unit-tested without a browser, a network, or a clock**.

Current state — **36/36 core tests passing** on native:

* `microdot::kad` — 11 unit tests covering protobuf encoding, multiaddr
  decoding (positive + negative), bootnode-string parsing.
* `microdot::peer_pool` — 8 unit tests covering reputation, quarantine,
  ranked peer selection, eviction, serde round-trip.
* `microdot::traits` — 4 unit tests of the in-memory `TestClock` and
  `MemoryStorage` helpers + dyn-safety.
* `microdot::discovery` — 2 unit tests covering the report shape and
  the clock-driven observation timestamp.
* `microdot::state` — 6 unit tests covering child-storage prefix
  layout, `StateRequest` encoding, varint round-trip, proof extraction,
  missing-field handling, garbage-input safety.
* `microdot::request` — 5 unit tests of the pool-aware combinator:
  records success on the pool peer, records failure + retries through
  the ranked pool before bootnode fallback, falls back when pool is
  empty, surfaces bootnode error when all paths fail.

The roadmap is to expand to several hundred tests using property-based
testing (`proptest`), snapshot testing (`insta`), mocked transport (a
programmable `Connect` impl that simulates slow / failed / successful
handshakes), and a feature-gated live-network suite that exercises
real Paseo bootnodes.

## Building

Native (tests + lib):

```sh
cargo test
```

Wasm (browsers — no `sp-state-machine`, so no hyper/tokio/mio bleeds
into the dep graph):

```sh
cargo build --target wasm32-unknown-unknown
```

The browser example:

```sh
cd example/dotli
wasm-pack build --target web --dev
python3 -m http.server 8000
# open http://localhost:8000/
```

## The browser example

`example/dotli/` is a concrete consumer that emulates the original
`paseo-revive-dotli-web-discovery` prototype using microdot's public
API. It does trustless `<label>.dot` → IPFS CID resolution against
live Paseo Asset Hub Next:

1. Loads a bundled `lightSyncState` checkpoint (or a saved snapshot
   from `localStorage`).
2. Kicks off background `discovery_burst` against both relay and
   AssetHub kad networks.
3. Warp-syncs GRANDPA finality on the relay chain.
4. Fetches the latest AssetHub head via a `paras::heads` storage proof
   (verified against the relay chain's finalized state root).
5. Reads the dotNS contract's `contenthash` storage slot from
   AssetHub's child trie, then decodes the bytes as an IPFS CID.
6. Caches everything back to `localStorage` for the next page load.

The chain-specific bits (Paseo genesis hashes, bootnode lists, dotNS
contract address, EVM mapping-slot math, contenthash CID decoding,
WebSocket adapter, DOM glue) are all app-domain. Everything else —
peer discovery, pool management, pool-aware retries, GRANDPA finality,
state-proof verification — is `microdot::*`.

`example/dotli` is part of the Cargo workspace, so root-level
`cargo test`, `cargo fmt --all --check`, and `cargo clippy --all-targets`
cover both the core crate and the browser example.

## Architecture diagram

```
┌──────────────────────────────────────────────────────────────┐
│ app code (e.g. example/dotli, future microdot-tokio bridge)  │
│   - WebSocket/TCP impl of Connect                            │
│   - localStorage/file impl of Storage                        │
│   - Date.now()/SystemTime impl of Clock                      │
│   - chain-specific constants + storage-key derivation        │
└────────────────────────────────────┬─────────────────────────┘
                                     │
┌────────────────────────────────────▼─────────────────────────┐
│ microdot                                                     │
│   ┌──────────────────────────────────────────────────────┐   │
│   │ kad     peer_pool   discovery                        │   │
│   │ request  state                                       │   │
│   │   ▲       ▲           ▲                              │   │
│   │   └─ pure ┴─ pure ────┘                              │   │
│   │                                                      │   │
│   │ traits: Connect, Storage, Clock                      │   │
│   │ re-exports: GrandpaState, BlockHeader, …             │   │
│   └──────────────────────────────────────────────────────┘   │
└────────────────────────────────────┬─────────────────────────┘
                                     │ GrandpaState, BlockHeader
┌────────────────────────────────────▼─────────────────────────┐
│ warp-sync                                                    │
│   GRANDPA authority-set tracking, justification verification │
│   lightSyncState checkpoint loading, Substrate wire types    │
└────────────────────────────────────┬─────────────────────────┘
                                     │ Connection<R,W,P>
┌────────────────────────────────────▼─────────────────────────┐
│ polkadot-p2p-connect                                         │
│   noise + yamux + multistream + sub/req protocols            │
└──────────────────────────────────────────────────────────────┘
                                     │  AsyncRead/AsyncWrite
                                     ▼
                            (host environment: TCP / WebSocket)
```

Reading bottom-up: the host environment provides `AsyncRead` +
`AsyncWrite` (just bytes). `polkadot-p2p-connect` layers noise + yamux
+ multistream + substrate protocols on top to give a typed
`Connection`. `warp-sync` consumes that to track finality.
`microdot` consumes both to add peer discovery, reputation, the
pool-aware request combinator, and state-proof verification.

## License

GPL-3.0-or-later.
