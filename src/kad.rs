//! Substrate Kademlia client. **One-shot FIND_NODE** over a single
//! polkadot-p2p-connect [`Connection`]. We act as a kad client only —
//! never as a server (we don't accept inbound kad streams).
//!
//! ### Wire format
//!
//! Substrate's kad uses the libp2p kad-dht protobuf `Message`:
//!
//! ```proto
//! enum MessageType { ... FIND_NODE = 4 ... }
//! enum ConnectionType { ... }
//! message Peer {
//!     bytes id = 1;
//!     repeated bytes addrs = 2;
//!     ConnectionType connection = 3;
//! }
//! message Message {
//!     MessageType type = 1;
//!     bytes key = 2;
//!     repeated Peer closerPeers = 8;
//!     ...
//! }
//! ```
//!
//! Each substream message is framed by the substrate request-response
//! layer (unsigned-varint length prefix) — which polkadot-p2p-connect's
//! [`RequestProtocol`] handles for us. So we just emit the protobuf body
//! and the connection delivers `Vec<u8>` back.
//!
//! ### Why this is tiny
//!
//! Encoding: type=4 + key bytes. Two fields, hand-encoded → <10 lines.
//! Decoding: walk top-level fields, descend into each `closerPeers`,
//! extract peer id + multiaddrs. We **discard** any peer whose multiaddrs
//! don't yield a browser-reachable wss URL — that filter happens here
//! rather than in the pool because it never makes sense to remember a
//! peer we can't dial from a browser.

use anyhow::Context as _;
use polkadot_p2p_connect::{
    AsyncRead, AsyncWrite, Connection, Message as ConnMessage, PlatformT, RequestProtocolId,
    RequestResponse,
};

// Multicodec proto codes for the multiaddr components we recognise. Full
// list lives at https://github.com/multiformats/multicodec/blob/master/table.csv
const MA_IP4: u64 = 4;
const MA_TCP: u64 = 6;
const MA_IP6: u64 = 41;
const MA_DNS: u64 = 53;
const MA_DNS4: u64 = 54;
const MA_DNS6: u64 = 55;
const MA_UDP: u64 = 273;
const MA_P2P: u64 = 421;
const MA_TLS: u64 = 448;
const MA_SNI: u64 = 449;
const MA_QUIC_V1: u64 = 460;
const MA_WS: u64 = 477;
const MA_WSS: u64 = 478;
const MA_P2P_CIRCUIT: u64 = 290;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A peer surfaced by a kad FIND_NODE response **and** filtered to those
/// with a wss endpoint we can actually open from a browser tab.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredPeer {
    pub peer_id_base58: String,
    pub wss_url: String,
}

// ---------------------------------------------------------------------------
// Target generation
// ---------------------------------------------------------------------------

/// Build a 38-byte PeerId-shaped multihash from 32 random bytes. The
/// inner bytes don't have to correspond to any real key — kad treats them
/// as opaque content over which XOR distance is computed.
pub fn random_target<P: PlatformT>() -> [u8; 38] {
    let mut key = [0u8; 32];
    P::fill_with_random_bytes(&mut key);
    target_multihash(&key)
}

/// Wrap 32 raw bytes in the canonical libp2p ed25519 PeerId multihash
/// layout. Matches [`polkadot_p2p_connect::utils::peer_id::PeerId::
/// from_ed25519_public_key`] but kept inline here so kad.rs can build
/// targets without reaching into the host crate's internals.
pub fn target_multihash(ed25519_pubkey: &[u8; 32]) -> [u8; 38] {
    let mut out = [0u8; 38];
    out[0] = 0x00; // multihash code: identity
    out[1] = 0x24; // length: 36 (single-byte varint)
    out[2] = 0x08; // protobuf field 1 (key_type), varint
    out[3] = 0x01; // KeyType::Ed25519 = 1
    out[4] = 0x12; // field 2 (key bytes), length-delim
    out[5] = 0x20; // length: 32
    out[6..38].copy_from_slice(ed25519_pubkey);
    out
}

// ---------------------------------------------------------------------------
// Encode FIND_NODE
// ---------------------------------------------------------------------------

/// Encode a kad `Message { type: FIND_NODE, key: target }` to bytes.
pub fn encode_find_node(target: &[u8; 38]) -> Vec<u8> {
    // field 1 (type, varint): tag=0x08, value=4
    // field 2 (key, length-delim): tag=0x12, len=38, bytes
    let mut out = Vec::with_capacity(2 + 2 + 38);
    out.push(0x08);
    out.push(0x04);
    out.push(0x12);
    encode_varint(&mut out, 38);
    out.extend_from_slice(target);
    out
}

// ---------------------------------------------------------------------------
// Decode FIND_NODE response
// ---------------------------------------------------------------------------

/// Decode a kad `Message` response and return only the peers from
/// `closerPeers` that have at least one usable wss multiaddr.
pub fn decode_find_node_response(bytes: &[u8]) -> anyhow::Result<Vec<DiscoveredPeer>> {
    let mut out = Vec::new();
    let mut input = bytes;
    while !input.is_empty() {
        let tag = decode_varint(&mut input).context("kad response: top-level tag")?;
        let field_id = tag >> 3;
        let wire_type = tag & 0x07;
        match (field_id, wire_type) {
            // closerPeers — field 8, length-delim (each entry is one Peer)
            (8, 2) => {
                let len = decode_varint(&mut input)? as usize;
                let peer_bytes = take_bytes(&mut input, len)?;
                if let Some(peer) = decode_peer(peer_bytes) {
                    out.push(peer);
                }
            }
            // Skip every other field type. We don't care about
            // providerPeers, record, etc. for FIND_NODE.
            (_, 0) => {
                decode_varint(&mut input)?;
            }
            (_, 2) => {
                let len = decode_varint(&mut input)? as usize;
                take_bytes(&mut input, len)?;
            }
            (_, 5) => {
                take_bytes(&mut input, 4)?;
            }
            (_, 1) => {
                take_bytes(&mut input, 8)?;
            }
            (_, w) => anyhow::bail!(
                "kad response: unsupported wire type {w} on field {field_id}"
            ),
        }
    }
    Ok(out)
}

/// Decode one Peer submessage. Returns `None` if it lacks both a peer-id
/// and a wss-reachable multiaddr.
fn decode_peer(mut input: &[u8]) -> Option<DiscoveredPeer> {
    let mut id_bytes: Option<&[u8]> = None;
    let mut wss_url: Option<String> = None;
    while !input.is_empty() {
        let tag = decode_varint(&mut input).ok()?;
        let field_id = tag >> 3;
        let wire_type = tag & 0x07;
        match (field_id, wire_type) {
            // Peer.id — field 1, length-delim
            (1, 2) => {
                let len = decode_varint(&mut input).ok()? as usize;
                id_bytes = Some(take_bytes(&mut input, len).ok()?);
            }
            // Peer.addrs — field 2, length-delim (repeated). Stop at the
            // first usable wss url, but keep walking so we don't poison
            // subsequent fields.
            (2, 2) => {
                let len = decode_varint(&mut input).ok()? as usize;
                let addr_bytes = take_bytes(&mut input, len).ok()?;
                if wss_url.is_none() {
                    wss_url = multiaddr_to_wss_url(addr_bytes);
                }
            }
            // Skip all other fields (connection enum, etc.).
            (_, 0) => {
                decode_varint(&mut input).ok()?;
            }
            (_, 2) => {
                let len = decode_varint(&mut input).ok()? as usize;
                take_bytes(&mut input, len).ok()?;
            }
            (_, _) => return None,
        }
    }
    let id_bytes = id_bytes?;
    let wss_url = wss_url?;
    let peer_id_base58 = bs58::encode(id_bytes).into_string();
    Some(DiscoveredPeer {
        peer_id_base58,
        wss_url,
    })
}

// ---------------------------------------------------------------------------
// Binary multiaddr → `wss://host:port/` string
// ---------------------------------------------------------------------------

/// Walk a binary multiaddr looking for a browser-reachable wss endpoint.
/// Accepts both `/wss` (canonical) and `/tls/ws` (functionally
/// equivalent) forms.
///
/// Returns `wss://{host}:{port}/` if the address has a usable host
/// (`/dns`, `/dns4`, or `/ip4`) plus a `/tcp/<port>` plus the wss/tls-ws
/// marker. Returns `None` for anything else, including ipv6 hosts
/// (browsers handle them but we play it safe), plaintext `/ws`, and
/// quic/p2p-circuit/etc.
pub fn multiaddr_to_wss_url(mut input: &[u8]) -> Option<String> {
    let mut host: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut has_wss = false;
    let mut has_tls = false;
    let mut has_ws = false;
    while !input.is_empty() {
        let code = decode_varint(&mut input).ok()?;
        match code {
            MA_IP4 => {
                let b = take_bytes(&mut input, 4).ok()?;
                host = Some(format!("{}.{}.{}.{}", b[0], b[1], b[2], b[3]));
            }
            MA_TCP => {
                let b = take_bytes(&mut input, 2).ok()?;
                port = Some(u16::from_be_bytes([b[0], b[1]]));
            }
            MA_DNS | MA_DNS4 | MA_DNS6 => {
                let len = decode_varint(&mut input).ok()? as usize;
                let b = take_bytes(&mut input, len).ok()?;
                host = Some(core::str::from_utf8(b).ok()?.to_string());
            }
            MA_WSS => has_wss = true,
            MA_WS => has_ws = true,
            MA_TLS => has_tls = true,
            // Length-delimited components we skip cleanly.
            MA_P2P | MA_SNI => {
                let len = decode_varint(&mut input).ok()? as usize;
                take_bytes(&mut input, len).ok()?;
            }
            // Fixed-size components we don't use.
            MA_IP6 => {
                take_bytes(&mut input, 16).ok()?;
                // ipv6 hosts are reachable from browsers but our pool
                // keys on `host:port` strings; skip to avoid string
                // ambiguity with brackets/no-brackets.
                host = None;
            }
            MA_UDP => {
                take_bytes(&mut input, 2).ok()?;
            }
            // Marker-only components (no payload).
            MA_QUIC_V1 | MA_P2P_CIRCUIT => {}
            _ => return None,
        }
    }
    let wss_like = has_wss || (has_tls && has_ws);
    if !wss_like {
        return None;
    }
    Some(format!("wss://{host}:{port}/", host = host?, port = port?))
}

/// Parse a textual multiaddr like
/// `/dns/foo.example/tcp/443/wss/p2p/12D3...` into `(peer_id, wss_url)`.
/// Used by the discovery driver to seed kad from the hardcoded bootnode
/// list.
pub fn parse_bootnode_multiaddr(s: &str) -> Option<(String, String)> {
    let mut parts = s.trim_start_matches('/').split('/');
    let mut host: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut has_wss = false;
    let mut has_tls = false;
    let mut has_ws = false;
    let mut peer_id: Option<String> = None;
    while let Some(proto) = parts.next() {
        match proto {
            "ip4" => host = parts.next().map(|s| s.to_string()),
            "dns" | "dns4" | "dns6" => host = parts.next().map(|s| s.to_string()),
            "tcp" => port = parts.next().and_then(|s| s.parse().ok()),
            "wss" => has_wss = true,
            "ws" => has_ws = true,
            "tls" => has_tls = true,
            "p2p" | "ipfs" => peer_id = parts.next().map(|s| s.to_string()),
            "ip6" => {
                let _ = parts.next();
                // We don't surface ipv6 hosts; see the binary parser.
            }
            // Anything we don't understand: skip its value if there
            // looks to be one. This is lossy but only the bootnodes
            // const feeds us, so we control the input format.
            other => {
                let looks_keyed = matches!(other, "udp" | "sni" | "memory" | "unix");
                if looks_keyed {
                    let _ = parts.next();
                }
            }
        }
    }
    let wss_like = has_wss || (has_tls && has_ws);
    if !wss_like {
        return None;
    }
    Some((peer_id?, format!("wss://{}:{}/", host?, port?)))
}

// ---------------------------------------------------------------------------
// The kad request itself
// ---------------------------------------------------------------------------

/// Send one FIND_NODE on the given Connection and return the peers we
/// learned about. The Connection must already have the kad
/// `RequestProtocol` registered with the given `kad_id`.
pub async fn one_shot_find_node<R, W, P>(
    conn: &mut Connection<R, W, P>,
    kad_id: RequestProtocolId,
    target: &[u8; 38],
) -> anyhow::Result<Vec<DiscoveredPeer>>
where
    R: AsyncRead + 'static,
    W: AsyncWrite + 'static,
    P: PlatformT + 'static,
{
    conn.request(kad_id, encode_find_node(target))?;
    while let Some(result) = conn.next().await {
        let message = result?;
        match message {
            ConnMessage::Response {
                protocol_id,
                res: RequestResponse::Value(bytes),
                ..
            } if protocol_id == kad_id => {
                return decode_find_node_response(&bytes);
            }
            ConnMessage::Response {
                protocol_id,
                res: RequestResponse::Error(e),
                ..
            } if protocol_id == kad_id => {
                anyhow::bail!("kad request error: {e:?}");
            }
            _ => {}
        }
    }
    anyhow::bail!("connection closed before kad response")
}

// ---------------------------------------------------------------------------
// varint / take_bytes — same shape as the helpers in lib.rs
// ---------------------------------------------------------------------------

fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_varint(input: &mut &[u8]) -> anyhow::Result<u64> {
    let mut value = 0u64;
    let mut shift: u32 = 0;
    loop {
        let byte = *input.first().context("EOF in varint")?;
        *input = &input[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            anyhow::bail!("varint > 64 bits");
        }
    }
}

fn take_bytes<'a>(input: &mut &'a [u8], n: usize) -> anyhow::Result<&'a [u8]> {
    if input.len() < n {
        anyhow::bail!("EOF: need {n}, have {}", input.len());
    }
    let (head, rest) = input.split_at(n);
    *input = rest;
    Ok(head)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_target() -> [u8; 38] {
        let mut k = [0u8; 32];
        for i in 0..32 {
            k[i] = i as u8;
        }
        target_multihash(&k)
    }

    #[test]
    fn target_multihash_is_well_formed() {
        let t = make_target();
        assert_eq!(t[0], 0x00); // identity
        assert_eq!(t[1], 0x24); // length 36
        assert_eq!(t[2], 0x08);
        assert_eq!(t[3], 0x01);
        assert_eq!(t[4], 0x12);
        assert_eq!(t[5], 0x20);
        assert_eq!(t[6..38], [
            0, 1, 2, 3, 4, 5, 6, 7,
            8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ]);
    }

    #[test]
    fn encode_find_node_layout_matches_spec() {
        let t = make_target();
        let bytes = encode_find_node(&t);
        assert_eq!(bytes[0], 0x08); // field 1 tag
        assert_eq!(bytes[1], 0x04); // FIND_NODE
        assert_eq!(bytes[2], 0x12); // field 2 tag
        assert_eq!(bytes[3], 0x26); // varint(38) — single byte
        assert_eq!(&bytes[4..], &t[..]);
    }

    #[test]
    fn multiaddr_dns_tcp_wss_parses() {
        // /dns/example.com/tcp/443/wss
        let mut input = Vec::new();
        encode_varint(&mut input, MA_DNS);
        let host = b"example.com";
        encode_varint(&mut input, host.len() as u64);
        input.extend_from_slice(host);
        encode_varint(&mut input, MA_TCP);
        input.extend_from_slice(&443u16.to_be_bytes());
        encode_varint(&mut input, MA_WSS);
        assert_eq!(
            multiaddr_to_wss_url(&input),
            Some("wss://example.com:443/".to_string())
        );
    }

    #[test]
    fn multiaddr_dns4_tcp_tls_ws_parses_as_wss() {
        // /dns4/example.com/tcp/443/tls/ws — equivalent to /wss
        let mut input = Vec::new();
        encode_varint(&mut input, MA_DNS4);
        let host = b"example.com";
        encode_varint(&mut input, host.len() as u64);
        input.extend_from_slice(host);
        encode_varint(&mut input, MA_TCP);
        input.extend_from_slice(&443u16.to_be_bytes());
        encode_varint(&mut input, MA_TLS);
        encode_varint(&mut input, MA_WS);
        assert_eq!(
            multiaddr_to_wss_url(&input),
            Some("wss://example.com:443/".to_string())
        );
    }

    #[test]
    fn multiaddr_plain_tcp_rejected() {
        // /ip4/1.2.3.4/tcp/30333 — no wss/tls-ws → reject
        let mut input = Vec::new();
        encode_varint(&mut input, MA_IP4);
        input.extend_from_slice(&[1, 2, 3, 4]);
        encode_varint(&mut input, MA_TCP);
        input.extend_from_slice(&30333u16.to_be_bytes());
        assert_eq!(multiaddr_to_wss_url(&input), None);
    }

    #[test]
    fn multiaddr_ws_without_tls_rejected() {
        // /dns/host/tcp/30333/ws — plaintext, browser would block over https
        let mut input = Vec::new();
        encode_varint(&mut input, MA_DNS);
        let host = b"host";
        encode_varint(&mut input, host.len() as u64);
        input.extend_from_slice(host);
        encode_varint(&mut input, MA_TCP);
        input.extend_from_slice(&30333u16.to_be_bytes());
        encode_varint(&mut input, MA_WS);
        assert_eq!(multiaddr_to_wss_url(&input), None);
    }

    #[test]
    fn parse_bootnode_multiaddr_dns_wss_p2p() {
        let s = "/dns/host.example/tcp/443/wss/p2p/12D3KooWAB";
        assert_eq!(
            parse_bootnode_multiaddr(s),
            Some(("12D3KooWAB".to_string(), "wss://host.example:443/".to_string()))
        );
    }

    #[test]
    fn parse_bootnode_multiaddr_dns_tcp_tls_ws_p2p() {
        let s = "/dns/host.example/tcp/443/tls/ws/p2p/12D3KooWAB";
        assert_eq!(
            parse_bootnode_multiaddr(s),
            Some(("12D3KooWAB".to_string(), "wss://host.example:443/".to_string()))
        );
    }

    #[test]
    fn parse_bootnode_multiaddr_plain_ws_rejected() {
        let s = "/dns/host.example/tcp/30333/ws/p2p/12D3KooWAB";
        assert_eq!(parse_bootnode_multiaddr(s), None);
    }

    #[test]
    fn decode_find_node_response_minimal() {
        // closerPeers (field 8) containing one Peer with id + one addr.
        let id = b"\x00\x24\x08\x01\x12\x20"; // shortened "identity"-style id (just for the test bytes)
        let mut addr = Vec::new();
        encode_varint(&mut addr, MA_DNS);
        let host = b"node.test";
        encode_varint(&mut addr, host.len() as u64);
        addr.extend_from_slice(host);
        encode_varint(&mut addr, MA_TCP);
        addr.extend_from_slice(&443u16.to_be_bytes());
        encode_varint(&mut addr, MA_WSS);

        // Build Peer { id: <id>, addrs: [<addr>] }
        let mut peer = Vec::new();
        peer.push(0x0a); // field 1 tag (id)
        encode_varint(&mut peer, id.len() as u64);
        peer.extend_from_slice(id);
        peer.push(0x12); // field 2 tag (addrs)
        encode_varint(&mut peer, addr.len() as u64);
        peer.extend_from_slice(&addr);

        // Build Message { closerPeers: [<peer>] }
        let mut msg = Vec::new();
        msg.push(0x42); // field 8 tag = (8<<3)|2
        encode_varint(&mut msg, peer.len() as u64);
        msg.extend_from_slice(&peer);

        let peers = decode_find_node_response(&msg).expect("decode ok");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].wss_url, "wss://node.test:443/");
        // peer_id_base58 should be the base58 encoding of `id`.
        assert_eq!(peers[0].peer_id_base58, bs58::encode(id).into_string());
    }

    #[test]
    fn decode_find_node_response_skips_useless_peer() {
        // Peer with id but no wss addr → filtered out.
        let id = b"\x00\x01\x02\x03";
        let mut addr = Vec::new();
        encode_varint(&mut addr, MA_IP4);
        addr.extend_from_slice(&[1, 2, 3, 4]);
        encode_varint(&mut addr, MA_TCP);
        addr.extend_from_slice(&30333u16.to_be_bytes());

        let mut peer = Vec::new();
        peer.push(0x0a);
        encode_varint(&mut peer, id.len() as u64);
        peer.extend_from_slice(id);
        peer.push(0x12);
        encode_varint(&mut peer, addr.len() as u64);
        peer.extend_from_slice(&addr);

        let mut msg = Vec::new();
        msg.push(0x42);
        encode_varint(&mut msg, peer.len() as u64);
        msg.extend_from_slice(&peer);

        let peers = decode_find_node_response(&msg).expect("decode ok");
        assert!(peers.is_empty());
    }
}
