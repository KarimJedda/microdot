//! WASM/browser version of paseo-revive-dotli-resumable, ported on top
//! of microdot's smoldot-shaped public API. The trust chain and proof
//! verification are unchanged — kad / peer_pool / discovery / grandpa /
//! state-proof now come from `microdot` instead of being inlined.

mod polkadot;
mod state;

use core::cell::{Cell, RefCell};
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::collections::VecDeque;
use std::rc::Rc;

use anyhow::Context as _;
use parity_scale_codec::{Decode, Encode};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BinaryType, MessageEvent, WebSocket};

use polkadot_p2p_connect::{
    AsyncRead, AsyncReadError, AsyncWrite, AsyncWriteError, Configuration, Connection, Message,
    PlatformT, RequestProtocol, RequestProtocolId, RequestResponse, SubscriptionProtocol,
    SubscriptionResponse,
};
use sp_core::hashing::{keccak_256, twox_128};

use microdot::{BlockHeader, Clock as MicrodotClock, GrandpaState};

use polkadot::{
    ASSETHUB_GENESIS_HASH, ASSETHUB_PARA_ID, ASSETHUB_WSS, CONTENTHASH_SLOT,
    DOTNS_CONTENT_RESOLVER, RELAY_GENESIS_HASH, RELAY_WSS,
};

// ============================================================================
// DOM helpers
// ============================================================================

thread_local! {
    /// When the wasm is driven from a host JS callback (dotli protocol iframe),
    /// each `log()` line is delivered through this function instead of being
    /// written into the `#log` DOM element. The standalone demo leaves it
    /// `None` and falls back to the DOM path.
    static STATUS_CB: RefCell<Option<js_sys::Function>> = const { RefCell::new(None) };
}

pub(crate) fn log(msg: &str) {
    web_sys::console::log_1(&msg.into());
    let delivered = STATUS_CB.with(|cb| {
        if let Some(f) = cb.borrow().as_ref() {
            let _ = f.call1(&JsValue::NULL, &JsValue::from_str(msg));
            true
        } else {
            false
        }
    });
    if delivered {
        return;
    }
    if let Some(el) = web_sys::window()
        .and_then(|w| w.document())
        .and_then(|d| d.get_element_by_id("log"))
    {
        let prev = el.text_content().unwrap_or_default();
        el.set_text_content(Some(&format!("{prev}{msg}\n")));
    }
}

fn show_result(html: &str, ok: bool) {
    if let Some(el) = web_sys::window()
        .and_then(|w| w.document())
        .and_then(|d| d.get_element_by_id("result"))
    {
        el.set_inner_html(html);
        el.set_class_name(if ok { "ok" } else { "err" });
        if let Some(html_el) = el.dyn_ref::<web_sys::HtmlElement>() {
            let _ = html_el.style().set_property("display", "block");
        }
    }
}

fn now_ms() -> f64 {
    web_sys::window()
        .and_then(|w| w.performance())
        .map(|p| p.now())
        .unwrap_or(0.0)
}

/// Per-peer WebSocket open deadline. If the WSS upgrade hasn't completed in
/// this window we abort and let the peer-pool retry path pick the next node.
/// Substrate's noise/multistream timers only run once bytes are flowing, so
/// the open itself needs its own bound.
pub(crate) const WS_OPEN_TIMEOUT: Duration = Duration::from_secs(10);

// ============================================================================
// WebSocket → AsyncRead / AsyncWrite bridge
// ============================================================================

struct WsState {
    buffer: VecDeque<u8>,
    waker: Option<Waker>,
    opened: bool,
    closed: bool,
    error: Option<String>,
}

pub(crate) struct WsReader(Rc<RefCell<WsState>>);

impl AsyncRead for WsReader {
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), AsyncReadError> {
        core::future::poll_fn(|cx| {
            let mut st = self.0.borrow_mut();
            if let Some(err) = st.error.take() {
                return Poll::Ready(Err(AsyncReadError::from_string(err)));
            }
            if st.buffer.len() >= buf.len() {
                for b in buf.iter_mut() {
                    *b = st.buffer.pop_front().unwrap();
                }
                Poll::Ready(Ok(()))
            } else if st.closed {
                Poll::Ready(Err(AsyncReadError::from_string("connection closed")))
            } else {
                st.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

pub(crate) struct WsWriter(WebSocket);

impl AsyncWrite for WsWriter {
    async fn write_all(&mut self, data: &[u8]) -> Result<(), AsyncWriteError> {
        self.0
            .send_with_u8_array(data)
            .map_err(|e| AsyncWriteError::from_string(format!("{e:?}")))
    }
}

pub(crate) async fn open_ws(url: &str, timeout: Duration) -> Result<(WsReader, WsWriter), String> {
    log(&format!("[ws] opening {url}"));
    let ws = WebSocket::new(url).map_err(|e| format!("{e:?}"))?;
    ws.set_binary_type(BinaryType::Arraybuffer);

    let state = Rc::new(RefCell::new(WsState {
        buffer: VecDeque::new(),
        waker: None,
        opened: false,
        closed: false,
        error: None,
    }));

    {
        let s = state.clone();
        let cb = Closure::once(move || {
            let mut st = s.borrow_mut();
            st.opened = true;
            if let Some(w) = st.waker.take() {
                w.wake();
            }
        });
        ws.set_onopen(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }
    {
        let s = state.clone();
        let cb = Closure::wrap(Box::new(move |e: MessageEvent| {
            if let Ok(buf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                let arr = js_sys::Uint8Array::new(&buf);
                let mut st = s.borrow_mut();
                st.buffer.extend(arr.to_vec());
                if let Some(w) = st.waker.take() {
                    w.wake();
                }
            }
        }) as Box<dyn FnMut(MessageEvent)>);
        ws.set_onmessage(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }
    {
        let s = state.clone();
        let cb = Closure::wrap(Box::new(move |_: JsValue| {
            let mut st = s.borrow_mut();
            st.closed = true;
            if let Some(w) = st.waker.take() {
                w.wake();
            }
        }) as Box<dyn FnMut(JsValue)>);
        ws.set_onclose(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }
    {
        let s = state.clone();
        let cb = Closure::wrap(Box::new(move |_: JsValue| {
            let mut st = s.borrow_mut();
            st.error = Some("WebSocket error".into());
            if let Some(w) = st.waker.take() {
                w.wake();
            }
        }) as Box<dyn FnMut(JsValue)>);
        ws.set_onerror(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }

    // Wait for open, racing a timer so an unresponsive peer doesn't stall the
    // whole bootstrap. The libp2p noise/multistream timeouts only kick in after
    // bytes start flowing, so the WS open itself needs its own deadline.
    let open_fut = core::future::poll_fn(|cx| {
        let mut st = state.borrow_mut();
        if st.opened {
            Poll::Ready(Ok::<(), String>(()))
        } else if st.error.is_some() || st.closed {
            Poll::Ready(Err(st.error.take().unwrap_or("failed to connect".into())))
        } else {
            st.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    });
    let timer = BrowserPlatform::sleep(timeout);
    futures::pin_mut!(open_fut, timer);
    match futures::future::select(open_fut, timer).await {
        futures::future::Either::Left((res, _)) => res?,
        futures::future::Either::Right(_) => {
            ws.close().ok();
            return Err(format!("ws open timed out after {}ms", timeout.as_millis()));
        }
    }
    log(&format!("[ws] open: {url}"));
    Ok((WsReader(state), WsWriter(ws)))
}

// ============================================================================
// Browser PlatformT
// ============================================================================

pub(crate) struct WasmSleep {
    done: Rc<Cell<bool>>,
    waker: Rc<Cell<Option<Waker>>>,
}

unsafe impl Send for WasmSleep {}
impl Unpin for WasmSleep {}

impl core::future::Future for WasmSleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        if self.done.get() {
            Poll::Ready(())
        } else {
            self.waker.set(Some(cx.waker().clone()));
            Poll::Pending
        }
    }
}

pub(crate) struct BrowserPlatform;

impl PlatformT for BrowserPlatform {
    type Sleep = WasmSleep;

    fn fill_with_random_bytes(bytes: &mut [u8]) {
        let array = js_sys::Uint8Array::new_with_length(bytes.len() as u32);
        web_sys::window()
            .unwrap()
            .crypto()
            .unwrap()
            .get_random_values_with_array_buffer_view(&array)
            .unwrap();
        array.copy_to(bytes);
    }

    fn sleep(duration: Duration) -> WasmSleep {
        let done = Rc::new(Cell::new(false));
        let waker: Rc<Cell<Option<Waker>>> = Rc::new(Cell::new(None));
        let (d, w) = (done.clone(), waker.clone());
        let cb = Closure::once(move || {
            d.set(true);
            if let Some(wk) = w.take() {
                wk.wake();
            }
        });
        web_sys::window()
            .unwrap()
            .set_timeout_with_callback_and_timeout_and_arguments_0(
                cb.as_ref().unchecked_ref(),
                duration.as_millis() as i32,
            )
            .unwrap();
        cb.forget();
        WasmSleep { done, waker }
    }
}

type BrowserConnection = Connection<WsReader, WsWriter, BrowserPlatform>;

// ============================================================================
// microdot adapters: Clock + Connect
// ============================================================================

/// `Date.now()`-backed Clock. Used wherever the relay/assethub phase
/// wrappers (and microdot's `run_with_pool_fallback` / `discovery_burst`)
/// need wall-clock-ish time for the peer pool's reputation timestamps.
pub(crate) struct BrowserClock;

impl MicrodotClock for BrowserClock {
    fn now_ms(&self) -> u64 {
        state::unix_now_ms()
    }
}

/// Browser-side [`microdot::Connect`] adapter. Hands microdot's discovery
/// burst the same WebSocket transport the hot path uses.
pub(crate) struct BrowserConnect;

impl microdot::Connect for BrowserConnect {
    type Reader = WsReader;
    type Writer = WsWriter;
    type Error = String;
    type Future<'a>
        = core::pin::Pin<Box<dyn core::future::Future<Output = Result<(WsReader, WsWriter), String>> + 'a>>
    where
        Self: 'a;

    fn connect<'a>(&'a self, url: &'a str) -> Self::Future<'a> {
        Box::pin(async move { open_ws(url, WS_OPEN_TIMEOUT).await })
    }
}

// ============================================================================
// Entry points
// ============================================================================

/// Standalone-demo entry: reads the label from `?domain=` / subdomain, runs
/// the resolution, and renders the result into `#title` / `#log` / `#result`.
/// Invoked explicitly from `index.html`.
#[wasm_bindgen]
pub fn start_demo() {
    wasm_bindgen_futures::spawn_local(async {
        let label = match read_subdomain_label() {
            Ok(l) => l,
            Err(e) => {
                let msg = format!("[fatal] {e}");
                log(&msg);
                show_result(&format!("<strong>error</strong><br/>{e}"), false);
                return;
            }
        };
        let full_name = format!("{label}.dot");
        if let Some(t) = web_sys::window()
            .and_then(|w| w.document())
            .and_then(|d| d.get_element_by_id("title"))
        {
            t.set_text_content(Some(&format!("resolving: {full_name}")));
        }

        match run_inner(label).await {
            Ok(r) => {
                let result_html = format!(
                    r#"<div>{full_name}</div>
<div class="cid">→ {cid}</div>
<div class="muted">verified-at-block: {block} (Paseo AssetHub){cache_note}<br/>
total: {elapsed:.1}s<br/>
contenthash: 0x{ch}</div>"#,
                    full_name = html_escape(&r.full_name),
                    cid = html_escape(&r.cid),
                    block = r.final_head_number,
                    cache_note = html_escape(&r.cache_note),
                    elapsed = r.elapsed_ms / 1000.0,
                    ch = hex::encode(&r.contenthash_bytes),
                );
                show_result(&result_html, true);
            }
            Err(e) => {
                let msg = format!("[fatal] {e}");
                log(&msg);
                show_result(&format!("<strong>error</strong><br/>{e}"), false);
            }
        }
    });
}

/// Host-driven entry: dotli's protocol iframe calls this with the bare label
/// and a status callback. Every `log()` line during the call is delivered to
/// `on_status` instead of the DOM. Returns `{ cid, verifiedAtBlock }` on
/// success; rejects with the error message string on failure.
#[wasm_bindgen]
pub async fn resolve(label: String, on_status: JsValue) -> Result<JsValue, JsValue> {
    let cb = on_status.dyn_into::<js_sys::Function>().ok();
    STATUS_CB.with(|c| *c.borrow_mut() = cb);
    let result = run_inner(label).await;
    STATUS_CB.with(|c| *c.borrow_mut() = None);
    match result {
        Ok(r) => {
            let obj = js_sys::Object::new();
            js_sys::Reflect::set(&obj, &"cid".into(), &JsValue::from_str(&r.cid))?;
            js_sys::Reflect::set(
                &obj,
                &"verifiedAtBlock".into(),
                &JsValue::from_f64(r.final_head_number as f64),
            )?;
            Ok(obj.into())
        }
        Err(e) => Err(JsValue::from_str(&format!("{e}"))),
    }
}

/// Output of the resolution pipeline. Carries everything either entry point
/// needs to render — `start_demo` builds HTML from it, `resolve` projects a
/// subset onto a JS object.
struct RunResult {
    full_name: String,
    cid: String,
    contenthash_bytes: Vec<u8>,
    final_head_number: u32,
    cache_note: String,
    elapsed_ms: f64,
}

#[derive(Clone, Copy)]
struct AssetHubHead {
    number: u32,
    hash: [u8; 32],
    state_root: [u8; 32],
}

async fn run_inner(label: String) -> anyhow::Result<RunResult> {
    let full_name = format!("{label}.dot");

    log(&format!("{full_name}\n"));

    let params = parse_url_params();
    log(&format!(
        "[params] max-head-age={}s{}",
        params.max_head_age_secs,
        if params.max_head_age_secs == 0 {
            " (--fresh)"
        } else {
            ""
        },
    ));

    let namehash = namehash_eip137(&full_name);
    let base_slot_key = compute_mapping_slot(&namehash, CONTENTHASH_SLOT);
    log(&format!("[dotns] namehash      = 0x{}", hex::encode(namehash)));
    log(&format!("[dotns] base slot key = 0x{}", hex::encode(base_slot_key)));

    let total_start = now_ms();

    // ── State ──────────────────────────────────────────────────────────────
    let (mut persisted, source) = state::load_or_bundled()?;
    match source {
        state::Source::SavedLocalStorage => log(&format!(
            "[state] resumed from localStorage — relay #{} set_id={} ({} contracts, {} resolutions cached)",
            persisted.relay.finalized_number,
            persisted.relay.set_id,
            persisted.contracts.len(),
            persisted.resolved.len(),
        )),
        state::Source::BundledCheckpoint => log(&format!(
            "[state] cold start from bundled checkpoint at relay #{}",
            persisted.relay.finalized_number
        )),
    }

    // Self-heal: earlier builds seeded bootnode peer-ids into the pools
    // with success credit, which made `pick_best` always return a
    // bootnode. Bootnodes belong only in the hardcoded fallback list,
    // never in the pool — strip them on load so old saves migrate
    // forward automatically.
    let relay_bootnode_ids: Vec<String> = polkadot::RELAY_WSS_BOOTNODES
        .iter()
        .filter_map(|s| microdot::kad::parse_bootnode_multiaddr(s).map(|(id, _)| id))
        .collect();
    persisted.relay_peers.purge_peers(&relay_bootnode_ids);
    let assethub_bootnode_ids: Vec<String> = polkadot::ASSETHUB_WSS_BOOTNODES
        .iter()
        .filter_map(|s| microdot::kad::parse_bootnode_multiaddr(s).map(|(id, _)| id))
        .collect();
    persisted.assethub_peers.purge_peers(&assethub_bootnode_ids);

    // ── Decide AssetHub head: cached vs refresh ───────────────────────────
    let head_decision = HeadDecision::decide(
        persisted.assethub_head.as_ref(),
        params.max_head_age_secs,
        state::unix_now_ms(),
    );
    let (ah_head, head_was_cached) = match head_decision {
        HeadDecision::UseCached(c, age_secs) => {
            log(&format!(
                "[head] using cached AssetHub head #{} ({}s old, max-head-age={}s)",
                c.number, age_secs, params.max_head_age_secs
            ));
            (cached_head_to_struct(c)?, true)
        }
        HeadDecision::Refresh(reason) => {
            log(&format!("[head] refreshing AssetHub head ({reason})"));
            let (new_gs, head) = relay_phase_with_pool(&mut persisted).await?;
            persisted.update_relay(&new_gs);
            persisted.assethub_head = Some(state::AssetHubHeadCache {
                number: head.number,
                hash: format!("0x{}", hex::encode(head.hash)),
                state_root: format!("0x{}", hex::encode(head.state_root)),
                saved_at_unix_ms: state::unix_now_ms(),
            });
            (head, false)
        }
    };
    log(&format!(
        "[parachain] AssetHub head: #{} state_root=0x{}",
        ah_head.number,
        hex::encode(ah_head.state_root)
    ));

    // ── Query: cached trie_id if known, otherwise fetch ContractInfoOf ─────
    let contract_key = format!("0x{}", hex::encode(DOTNS_CONTENT_RESOLVER));
    let cached_trie_id: Option<Vec<u8>> = persisted
        .contracts
        .get(&contract_key)
        .map(|c| decode_0x_to_vec(&c.trie_id))
        .transpose()?;

    let attempt = assethub_phase_with_pool(
        &mut persisted,
        &ah_head,
        &DOTNS_CONTENT_RESOLVER,
        &base_slot_key,
        cached_trie_id.as_deref(),
    )
    .await;

    // Retry once with a fresh head if the cached one looked stale.
    let (contenthash_bytes, final_head, fetched_trie_id) = match attempt {
        Ok(r) => (r.contenthash, ah_head, r.fetched_trie_id),
        Err(e) if head_was_cached => {
            log(&format!("[head] cached head returned an error: {e}"));
            log("[head] refreshing AssetHub head and retrying once...");
            let (new_gs, fresh_head) = relay_phase_with_pool(&mut persisted).await?;
            persisted.update_relay(&new_gs);
            persisted.assethub_head = Some(state::AssetHubHeadCache {
                number: fresh_head.number,
                hash: format!("0x{}", hex::encode(fresh_head.hash)),
                state_root: format!("0x{}", hex::encode(fresh_head.state_root)),
                saved_at_unix_ms: state::unix_now_ms(),
            });
            let r = assethub_phase_with_pool(
                &mut persisted,
                &fresh_head,
                &DOTNS_CONTENT_RESOLVER,
                &base_slot_key,
                None,
            )
            .await?;
            (r.contenthash, fresh_head, r.fetched_trie_id)
        }
        Err(e) => return Err(e),
    };

    if let Some(trie_id) = fetched_trie_id {
        persisted.contracts.insert(
            contract_key,
            state::ContractCacheEntry {
                trie_id: format!("0x{}", hex::encode(&trie_id)),
                verified_at_block: final_head.number,
            },
        );
    }

    let cid = decode_contenthash_cid(&contenthash_bytes).map_err(|e| {
        anyhow::anyhow!(
            "could not decode as IPFS contenthash: {e}; raw=0x{}",
            hex::encode(&contenthash_bytes)
        )
    })?;

    persisted.resolved.insert(
        full_name.clone(),
        state::ResolvedEntry {
            cid: cid.clone(),
            verified_at_block: final_head.number,
        },
    );

    if let Err(e) = persisted.save() {
        log(&format!("[state] save failed (continuing): {e}"));
    } else {
        log(&format!(
            "[state] saved to localStorage ({} contracts, {} resolutions)",
            persisted.contracts.len(),
            persisted.resolved.len()
        ));
    }

    // Fire-and-forget background discovery. Captures clones of both pools
    // as they stand AFTER the hot-path's reputation updates, runs the
    // burst (10s hard cap per probe), then reloads state and persists the
    // merged pools. Outlives `run()` because spawn_local detaches.
    spawn_background_discovery(persisted.relay_peers.clone(), persisted.assethub_peers.clone());

    let elapsed = now_ms() - total_start;
    let cache_note = if head_was_cached {
        let age = (state::unix_now_ms()
            .saturating_sub(persisted.assethub_head.as_ref().map(|c| c.saved_at_unix_ms).unwrap_or(0)))
            / 1000;
        format!(" — head cached, {age}s old")
    } else {
        String::new()
    };

    log(&format!("\n→ {cid}"));
    log(&format!(
        "  verified-at-block: {} (Paseo AssetHub){}",
        final_head.number, cache_note
    ));
    log(&format!("  total: {:.1}s", elapsed / 1000.0));

    Ok(RunResult {
        full_name,
        cid,
        contenthash_bytes,
        final_head_number: final_head.number,
        cache_note,
        elapsed_ms: elapsed,
    })
}

// ── URL parameter parsing ───────────────────────────────────────────────────

struct UrlParams {
    max_head_age_secs: u64,
}

fn parse_url_params() -> UrlParams {
    let mut max_head_age_secs: u64 = 30;
    let search = web_sys::window()
        .and_then(|w| w.location().search().ok())
        .unwrap_or_default();
    for kv in search.trim_start_matches('?').split('&') {
        if kv.is_empty() {
            continue;
        }
        let mut parts = kv.splitn(2, '=');
        let key = parts.next().unwrap_or("");
        let val = parts.next().unwrap_or("");
        match key {
            "fresh" => max_head_age_secs = 0,
            "max-head-age" => {
                if let Ok(n) = val.parse::<u64>() {
                    max_head_age_secs = n;
                }
            }
            _ => {}
        }
    }
    UrlParams { max_head_age_secs }
}

// ── Head-cache decision (same shape as native resumable) ────────────────────

enum HeadDecision<'a> {
    UseCached(&'a state::AssetHubHeadCache, u64),
    Refresh(&'static str),
}

impl<'a> HeadDecision<'a> {
    fn decide(
        cached: Option<&'a state::AssetHubHeadCache>,
        max_age_secs: u64,
        now_ms_unix: u64,
    ) -> Self {
        match cached {
            None => HeadDecision::Refresh("no cached head"),
            Some(_) if max_age_secs == 0 => HeadDecision::Refresh("?fresh / max-head-age=0"),
            Some(c) => {
                let age_secs = now_ms_unix.saturating_sub(c.saved_at_unix_ms) / 1000;
                if age_secs <= max_age_secs {
                    HeadDecision::UseCached(c, age_secs)
                } else {
                    HeadDecision::Refresh("cached head older than max-head-age")
                }
            }
        }
    }
}

fn cached_head_to_struct(c: &state::AssetHubHeadCache) -> anyhow::Result<AssetHubHead> {
    Ok(AssetHubHead {
        number: c.number,
        hash: parse_0x_array(&c.hash)?,
        state_root: parse_0x_array(&c.state_root)?,
    })
}

fn parse_0x_array<const N: usize>(s: &str) -> anyhow::Result<[u8; N]> {
    let v = decode_0x_to_vec(s)?;
    v.try_into()
        .map_err(|v: Vec<u8>| anyhow::anyhow!("expected {N} bytes, got {}", v.len()))
}

fn decode_0x_to_vec(s: &str) -> anyhow::Result<Vec<u8>> {
    let stripped = s
        .strip_prefix("0x")
        .with_context(|| format!("expected 0x-prefixed hex, got {s:?}"))?;
    hex::decode(stripped).context("invalid hex")
}

fn read_subdomain_label() -> anyhow::Result<String> {
    // ?domain=<label> override — handy when not served from a *.dotli-style host
    // (e.g. github.io pages). Takes precedence over the hostname-derived label.
    let search = web_sys::window()
        .and_then(|w| w.location().search().ok())
        .unwrap_or_default();
    for kv in search.trim_start_matches('?').split('&') {
        let mut parts = kv.splitn(2, '=');
        if parts.next() == Some("domain") {
            let val = parts.next().unwrap_or("").trim();
            if !val.is_empty() {
                log(&format!("[hint] using ?domain={val} override"));
                return Ok(val.to_string());
            }
        }
    }

    let host = web_sys::window()
        .and_then(|w| w.location().hostname().ok())
        .ok_or_else(|| anyhow::anyhow!("could not read window.location.hostname"))?;
    // For host like "host-playground.localhost", we want "host-playground".
    // For "localhost" (no subdomain), fall back to a default for the demo.
    if !host.contains('.') {
        log("[hint] no subdomain in hostname; defaulting to label `host-playground`");
        return Ok("host-playground".to_string());
    }
    let label = host
        .split('.')
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("empty hostname"))?;
    Ok(label.to_string())
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// ============================================================================
// Pool-aware wrappers — thin shims over `microdot::run_with_pool_fallback`.
// ============================================================================
//
// Each wrapper picks the highest-scoring available peer from the relevant
// pool (falling back to the hardcoded bootnode constant when the pool is
// empty), invokes the underlying phase, and records success/failure on
// the pool entry.

/// Pick a pool peer (if any), invoke `relay_phase`, and record the
/// outcome. **On pool-peer failure, retry once against the hardcoded
/// `RELAY_WSS` bootnode** before propagating the error. The
/// `RelaySnapshot` is fed into `to_grandpa_state` on each attempt by
/// reading from a snapshotted clone of the input — that way a partial
/// mutation during the first attempt cannot leak into the retry, while
/// still letting `run_with_pool_fallback` borrow `relay_peers` mutably
/// alongside it.
async fn relay_phase_with_pool(
    persisted: &mut state::PersistedState,
) -> anyhow::Result<(GrandpaState, AssetHubHead)> {
    let clock = BrowserClock;
    // Snapshot the relay-state portion once. `relay_phase` takes the
    // `GrandpaState` by value, so each attempt's mutations stay local
    // to that invocation — the snapshot itself is read-only here.
    let relay_snapshot = persisted.relay.clone();
    let result = microdot::run_with_pool_fallback(
        &mut persisted.relay_peers,
        RELAY_WSS,
        &clock,
        |url| {
            let snapshot = relay_snapshot.clone();
            let url = url.to_string();
            async move {
                let gs = state::PersistedState::snapshot_to_grandpa_state(&snapshot)
                    .map_err(|e| format!("relay state snapshot: {e}"))?;
                relay_phase(gs, &url).await.map_err(|e| format!("{e}"))
            }
        },
    )
    .await;
    result.map_err(|e| anyhow::anyhow!(e))
}

/// AssetHub counterpart to `relay_phase_with_pool` — same retry-once
/// semantics: if the pool peer fails, record the failure and retry
/// against `ASSETHUB_WSS` before propagating the error.
async fn assethub_phase_with_pool(
    persisted: &mut state::PersistedState,
    head: &AssetHubHead,
    contract_addr: &[u8; 20],
    base_slot_key: &[u8; 32],
    cached_trie_id: Option<&[u8]>,
) -> anyhow::Result<AssetHubResult> {
    let clock = BrowserClock;
    let result = microdot::run_with_pool_fallback(
        &mut persisted.assethub_peers,
        ASSETHUB_WSS,
        &clock,
        |url| {
            let url = url.to_string();
            async move {
                assethub_phase(head, contract_addr, base_slot_key, cached_trie_id, &url)
                    .await
                    .map_err(|e| format!("{e}"))
            }
        },
    )
    .await;
    result.map_err(|e| anyhow::anyhow!(e))
}

/// Detach a background task that runs the discovery burst for both
/// pools concurrently, then reloads the latest persisted state, replaces
/// the two pool fields, and saves. Outlives `run()`.
fn spawn_background_discovery(
    relay_pool: microdot::PeerPool,
    assethub_pool: microdot::PeerPool,
) {
    wasm_bindgen_futures::spawn_local(async move {
        log("[discovery] background burst starting (relay + assethub in parallel)");
        let connect = BrowserConnect;
        let clock = BrowserClock;
        let (relay_result, assethub_result) = futures::future::join(
            microdot::discovery::discovery_burst::<BrowserPlatform, _, _>(
                relay_pool,
                polkadot::RELAY_WSS_BOOTNODES,
                polkadot::relay_protocol_prefix_hex(),
                &connect,
                &clock,
            ),
            microdot::discovery::discovery_burst::<BrowserPlatform, _, _>(
                assethub_pool,
                polkadot::ASSETHUB_WSS_BOOTNODES,
                polkadot::assethub_protocol_prefix_hex(),
                &connect,
                &clock,
            ),
        )
        .await;
        let (relay_updated, relay_report) = relay_result;
        let (assethub_updated, assethub_report) = assethub_result;
        log(&format!(
            "[discovery] relay burst: {}/{} bootnodes ok, {} peers observed",
            relay_report.bootnodes_succeeded,
            relay_report.outcomes.len(),
            relay_report.peers_observed,
        ));
        log(&format!(
            "[discovery] assethub burst: {}/{} bootnodes ok, {} peers observed",
            assethub_report.bootnodes_succeeded,
            assethub_report.outcomes.len(),
            assethub_report.peers_observed,
        ));
        match state::load_or_bundled() {
            Ok((mut latest, _)) => {
                latest.relay_peers = relay_updated;
                latest.assethub_peers = assethub_updated;
                if let Err(e) = latest.save() {
                    log(&format!("[discovery] merge save failed: {e}"));
                } else {
                    log(&format!(
                        "[discovery] merged pools persisted (relay={}, assethub={})",
                        latest.relay_peers.len(),
                        latest.assethub_peers.len()
                    ));
                }
            }
            Err(e) => log(&format!("[discovery] could not reload state for merge: {e}")),
        }
    });
}

// ============================================================================
// Phase 1: relay warp sync + Paras::Heads[<para_id>]
// ============================================================================

async fn relay_phase(
    mut grandpa_state: GrandpaState,
    wss_url: &str,
) -> anyhow::Result<(GrandpaState, AssetHubHead)> {
    let genesis_hex = hex::encode(RELAY_GENESIS_HASH);

    let mut config: Configuration<BrowserPlatform> = Configuration::new();
    let ba_id = config.add_protocol(SubscriptionProtocol::new(
        format!("/{genesis_hex}/block-announces/1"),
        (2u8, 0u32, RELAY_GENESIS_HASH, RELAY_GENESIS_HASH).encode(),
        move |hs| hs.len() >= 69 && hs[37..69] == RELAY_GENESIS_HASH,
    ));
    let _grandpa_id = config.add_protocol(SubscriptionProtocol::new(
        format!("/{genesis_hex}/grandpa/1"),
        vec![2u8],
        |hs| hs.len() == 1,
    ));
    let warp_id = config.add_protocol(
        RequestProtocol::new(format!("/{genesis_hex}/sync/warp"))
            .with_max_response_size(32 * 1024 * 1024)
            .with_timeout(Duration::from_secs(30)),
    );
    let state_id = config.add_protocol(
        RequestProtocol::new(format!("/{genesis_hex}/state/2"))
            .with_max_response_size(16 * 1024 * 1024)
            .with_timeout(Duration::from_secs(30)),
    );

    let (reader, writer) = open_ws(wss_url, WS_OPEN_TIMEOUT).await.map_err(|e| anyhow::anyhow!(e))?;
    let mut conn = config
        .connect(reader, writer)
        .await
        .map_err(|e| anyhow::anyhow!("relay handshake: {e}"))?;
    log(&format!("[relay] connected as {} → {}", conn.our_id(), conn.their_id()));

    conn.subscribe(ba_id)?;

    let paras_key = paras_heads_key(ASSETHUB_PARA_ID);

    while let Some(result) = conn.next().await {
        match result? {
            Message::Notification {
                protocol_id,
                res: SubscriptionResponse::Opened,
            } if protocol_id == ba_id => {
                log(&format!(
                    "[relay] warp sync from #{}",
                    grandpa_state.finalized_number
                ));
                conn.request(warp_id, grandpa_state.finalized_hash.to_vec())?;
            }
            Message::Response {
                protocol_id,
                res: RequestResponse::Value(bytes),
                ..
            } if protocol_id == warp_id => {
                let done = grandpa_state
                    .update_with_warp_sync_response(&bytes)
                    .map_err(|e| anyhow::anyhow!(e))?;
                log(&format!(
                    "[relay] warp progress: #{}, set_id={}, {} authorities",
                    grandpa_state.finalized_number,
                    grandpa_state.set_id,
                    grandpa_state.authorities.len()
                ));
                if !done {
                    conn.request(warp_id, grandpa_state.finalized_hash.to_vec())?;
                    continue;
                }
                log(&format!(
                    "[relay] warp complete; requesting Paras::Heads[{}]",
                    ASSETHUB_PARA_ID
                ));
                let req = microdot::encode_state_request(
                    &grandpa_state.finalized_hash,
                    &[paras_key.as_slice()],
                );
                conn.request(state_id, req)?;
            }
            Message::Response {
                protocol_id,
                res: RequestResponse::Value(bytes),
                ..
            } if protocol_id == state_id => {
                let value = microdot::verify_top_proof(
                    &bytes,
                    &grandpa_state.finalized_state_root,
                    &paras_key,
                )
                .map_err(|e| anyhow::anyhow!(e))?;
                let head_data: Vec<u8> =
                    Decode::decode(&mut &value[..]).context("decoding HeadData")?;
                let header: BlockHeader =
                    Decode::decode(&mut &head_data[..]).context("decoding AssetHub BlockHeader")?;
                let head = AssetHubHead {
                    number: header.number,
                    hash: header.hash(),
                    state_root: header.state_root,
                };
                return Ok((grandpa_state, head));
            }
            Message::Response {
                protocol_id,
                res: RequestResponse::Error(e),
                ..
            } if protocol_id == warp_id || protocol_id == state_id => {
                anyhow::bail!("relay request error: {e}");
            }
            _ => {}
        }
    }
    anyhow::bail!("relay connection closed before we got our answer")
}

// ============================================================================
// Phase 2: AssetHub — ContractInfo + child slot reads
// ============================================================================

struct AssetHubResult {
    contenthash: Vec<u8>,
    fetched_trie_id: Option<Vec<u8>>,
}

async fn assethub_phase(
    head: &AssetHubHead,
    contract_addr: &[u8; 20],
    base_slot_key: &[u8; 32],
    cached_trie_id: Option<&[u8]>,
    wss_url: &str,
) -> anyhow::Result<AssetHubResult> {
    let genesis_hex = hex::encode(ASSETHUB_GENESIS_HASH);

    let mut config: Configuration<BrowserPlatform> = Configuration::new();
    let ba_id = config.add_protocol(SubscriptionProtocol::new(
        format!("/{genesis_hex}/block-announces/1"),
        (2u8, 0u32, ASSETHUB_GENESIS_HASH, ASSETHUB_GENESIS_HASH).encode(),
        move |hs| hs.len() >= 69 && hs[37..69] == ASSETHUB_GENESIS_HASH,
    ));
    let _grandpa_id = config.add_protocol(SubscriptionProtocol::new(
        format!("/{genesis_hex}/grandpa/1"),
        vec![2u8],
        |hs| hs.len() == 1,
    ));
    let state_id = config.add_protocol(
        RequestProtocol::new(format!("/{genesis_hex}/state/2"))
            .with_max_response_size(16 * 1024 * 1024)
            .with_timeout(Duration::from_secs(30)),
    );

    let (reader, writer) = open_ws(wss_url, WS_OPEN_TIMEOUT).await.map_err(|e| anyhow::anyhow!(e))?;
    let mut conn = config
        .connect(reader, writer)
        .await
        .map_err(|e| anyhow::anyhow!("assethub handshake: {e}"))?;
    log(&format!(
        "[assethub] connected as {} → {}",
        conn.our_id(),
        conn.their_id()
    ));

    conn.subscribe(ba_id)?;
    wait_for_subscription_open(&mut conn, ba_id).await?;

    // ContractInfoOf — use the cached trie_id if we have one.
    let contract_key_hex = hex::encode(contract_addr);
    let (trie_id, fetched_trie_id) = match cached_trie_id {
        Some(t) => {
            log(&format!("[assethub] using cached trie_id: 0x{}", hex::encode(t)));
            (t.to_vec(), None)
        }
        None => {
            log(&format!(
                "[assethub] reading Revive::AccountInfoOf[0x{contract_key_hex}]"
            ));
            let info_key = revive_account_info_key(contract_addr);
            let value =
                request_top_storage(&mut conn, state_id, &head.hash, &head.state_root, &info_key)
                    .await?;
            let mut cursor = &value[..];
            let acc = AccountInfo::decode(&mut cursor).context("decoding AccountInfo")?;
            if !cursor.is_empty() {
                anyhow::bail!("AccountInfo schema drift: {} leftover bytes", cursor.len());
            }
            let trie_id = match acc.account_type {
                AccountType::Contract(info) => info.trie_id,
                AccountType::EOA => anyhow::bail!("no contract at 0x{contract_key_hex}"),
            };
            log(&format!("[assethub] fetched trie_id = 0x{}", hex::encode(&trie_id)));
            (trie_id.clone(), Some(trie_id))
        }
    };

    // Base slot
    let base_slot = request_child_slot(
        &mut conn,
        state_id,
        &head.hash,
        &head.state_root,
        &trie_id,
        base_slot_key,
    )
    .await?;
    log(&format!(
        "[assethub] base slot value: 0x{}",
        hex::encode(base_slot)
    ));

    // Decode inline-vs-long and read additional slots if needed
    let contenthash: Vec<u8> = if base_slot[31] & 1 == 0 {
        let len = (base_slot[31] >> 1) as usize;
        log(&format!("[assethub] short inline value, length={len}"));
        base_slot[..len].to_vec()
    } else {
        let total_len = decode_long_bytes_length(&base_slot)?;
        log(&format!("[assethub] long value, length={total_len}"));
        let data_slot_base = keccak_256(base_slot_key);
        let n_slots = (total_len + 31) / 32;
        let mut out = Vec::with_capacity(total_len);
        for i in 0..n_slots {
            let slot_key = u256_add(&data_slot_base, i as u64);
            log(&format!(
                "[assethub] reading data slot {}/{}",
                i + 1,
                n_slots
            ));
            let chunk = request_child_slot(
                &mut conn,
                state_id,
                &head.hash,
                &head.state_root,
                &trie_id,
                &slot_key,
            )
            .await?;
            let take = (total_len - i * 32).min(32);
            out.extend_from_slice(&chunk[..take]);
        }
        out
    };

    Ok(AssetHubResult {
        contenthash,
        fetched_trie_id,
    })
}

async fn wait_for_subscription_open(
    conn: &mut BrowserConnection,
    sub_id: polkadot_p2p_connect::SubscriptionProtocolId,
) -> anyhow::Result<()> {
    while let Some(result) = conn.next().await {
        if let Message::Notification {
            protocol_id,
            res: SubscriptionResponse::Opened,
        } = result?
        {
            if protocol_id == sub_id {
                return Ok(());
            }
        }
    }
    anyhow::bail!("connection closed before subscription opened")
}

async fn request_top_storage(
    conn: &mut BrowserConnection,
    state_id: RequestProtocolId,
    block_hash: &[u8; 32],
    expected_state_root: &[u8; 32],
    key: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let req = microdot::encode_state_request(block_hash, &[key]);
    conn.request(state_id, req)?;
    let bytes = wait_for_state_response(conn, state_id).await?;
    microdot::verify_top_proof(&bytes, expected_state_root, key).map_err(|e| anyhow::anyhow!(e))
}

async fn request_child_slot(
    conn: &mut BrowserConnection,
    state_id: RequestProtocolId,
    block_hash: &[u8; 32],
    expected_state_root: &[u8; 32],
    trie_id: &[u8],
    raw_slot_key: &[u8; 32],
) -> anyhow::Result<[u8; 32]> {
    let child_key = sp_core::hashing::blake2_256(raw_slot_key);
    let top_key = microdot::child_storage_default_prefix(trie_id);
    let req = microdot::encode_state_request(block_hash, &[top_key.as_slice(), child_key.as_slice()]);
    conn.request(state_id, req)?;
    let bytes = wait_for_state_response(conn, state_id).await?;
    let value = microdot::verify_child_proof(&bytes, expected_state_root, trie_id, &child_key)
        .map_err(|e| anyhow::anyhow!(e))?;
    if value.len() != 32 {
        anyhow::bail!("child slot value is {} bytes, expected 32", value.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&value);
    Ok(out)
}

async fn wait_for_state_response(
    conn: &mut BrowserConnection,
    state_id: RequestProtocolId,
) -> anyhow::Result<Vec<u8>> {
    while let Some(result) = conn.next().await {
        match result? {
            Message::Response {
                protocol_id,
                res: RequestResponse::Value(bytes),
                ..
            } if protocol_id == state_id => return Ok(bytes),
            Message::Response {
                protocol_id,
                res: RequestResponse::Error(e),
                ..
            } if protocol_id == state_id => anyhow::bail!("/state/2 error: {e}"),
            _ => {}
        }
    }
    anyhow::bail!("connection closed before /state/2 response")
}

// ============================================================================
// pallet-revive types
// ============================================================================

#[derive(Decode, Debug)]
struct AccountInfo {
    account_type: AccountType,
    #[allow(dead_code)]
    dust: u32,
}

#[derive(Decode, Debug)]
enum AccountType {
    Contract(ContractInfo),
    EOA,
}

#[derive(Decode, Debug)]
#[allow(dead_code)]
struct ContractInfo {
    trie_id: Vec<u8>,
    code_hash: [u8; 32],
    storage_bytes: u32,
    storage_items: u32,
    storage_byte_deposit: u128,
    storage_item_deposit: u128,
    storage_base_deposit: u128,
    immutable_data_len: u32,
}

// ============================================================================
// storage keys, EVM/Solidity, contenthash — platform-adapter business logic
// that microdot intentionally doesn't carry.
// ============================================================================

fn revive_account_info_key(addr: &[u8; 20]) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + 16 + 20);
    key.extend_from_slice(&twox_128(b"Revive"));
    key.extend_from_slice(&twox_128(b"AccountInfoOf"));
    key.extend_from_slice(addr);
    key
}

fn paras_heads_key(para_id: u32) -> Vec<u8> {
    use sp_core::hashing::twox_64;
    let id_encoded = para_id.encode();
    let mut key = Vec::with_capacity(16 + 16 + 8 + 4);
    key.extend_from_slice(&twox_128(b"Paras"));
    key.extend_from_slice(&twox_128(b"Heads"));
    key.extend_from_slice(&twox_64(&id_encoded));
    key.extend_from_slice(&id_encoded);
    key
}

fn namehash_eip137(name: &str) -> [u8; 32] {
    let mut node = [0u8; 32];
    if name.is_empty() {
        return node;
    }
    for label in name.split('.').rev() {
        let label_hash = keccak_256(label.as_bytes());
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        node = keccak_256(&combined);
    }
    node
}

fn compute_mapping_slot(key: &[u8; 32], slot_number: u8) -> [u8; 32] {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(key);
    buf[63] = slot_number;
    keccak_256(&buf)
}

fn decode_long_bytes_length(slot: &[u8; 32]) -> anyhow::Result<usize> {
    if slot[31] & 1 == 0 {
        anyhow::bail!("slot is not long-bytes (LSB even)");
    }
    if slot[..16].iter().any(|&b| b != 0) {
        anyhow::bail!("long-bytes length doesn't fit in u128");
    }
    let mut acc: u128 = 0;
    for &b in &slot[16..32] {
        acc = (acc << 8) | (b as u128);
    }
    Ok(((acc - 1) / 2) as usize)
}

fn u256_add(base: &[u8; 32], offset: u64) -> [u8; 32] {
    let mut out = *base;
    let mut carry: u128 = offset as u128;
    for i in (0..32).rev() {
        let sum = (out[i] as u128) + (carry & 0xff);
        out[i] = (sum & 0xff) as u8;
        carry = (carry >> 8) + (sum >> 8);
        if carry == 0 {
            break;
        }
    }
    out
}

fn decode_contenthash_cid(bytes: &[u8]) -> anyhow::Result<String> {
    if bytes.len() < 2 || bytes[0] != 0xe3 || bytes[1] != 0x01 {
        anyhow::bail!("not an ipfs contenthash (expected leading 0xe3 0x01)");
    }
    let cid_bytes = &bytes[2..];
    if !cid_bytes.is_empty() && cid_bytes[0] == 0x01 {
        return Ok(format!("b{}", base32_lower_no_pad(cid_bytes)));
    }
    if cid_bytes.len() == 34 && cid_bytes[0] == 0x12 && cid_bytes[1] == 0x20 {
        return Ok(bs58::encode(cid_bytes).into_string());
    }
    anyhow::bail!("unrecognised CID format after 0xe301 prefix")
}

fn base32_lower_no_pad(input: &[u8]) -> String {
    const ALPHA: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(input.len() * 8 / 5 + 1);
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    for &byte in input {
        buf = (buf << 8) | (byte as u32);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHA[((buf >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHA[((buf << (5 - bits)) & 0x1f) as usize] as char);
    }
    out
}
