# paseo-revive-dotli-web

Browser version of `paseo-revive-dotli-resumable`: trustless resolution of
`<label>.dot` to an IPFS CID, fully verified against Paseo GRANDPA finality,
running in WebAssembly with state persisted in `localStorage`.

## Build

Install [wasm-pack](https://rustwasm.github.io/wasm-pack/installer/) once:

```
cargo install wasm-pack
```

From this directory:

```
wasm-pack build --target web
```

Output lands in `pkg/`, which `index.html` imports.

## Run

Serve this directory with any static HTTP server, but expose it on a
wildcard local hostname so subdomains route to the same page:

```
python3 -m http.server 8000
```

Then open one of:

- http://host-playground.localhost:8000  → resolves `host-playground.dot`
- http://testwebsite.localhost:8000      → resolves `testwebsite.dot`

The leftmost label of the hostname is treated as the dotns name to resolve.

### URL parameters

- `?fresh=1`         force a fresh AssetHub head (skip cache)
- `?max-head-age=60` accept a cached head up to 60 seconds old (default 30)

### State

The light-client state (GRANDPA authorities, AssetHub head cache, resolved
CID cache) is persisted in `localStorage` under the key
`paseo-revive-dotli-state`. Clear it from devtools or
`localStorage.removeItem("paseo-revive-dotli-state")` to force cold start.
