//! Chain-specific constants for the dotli example.
//!
//! Targets paseo-next-v2 — the chain set dotli defaults to. v2 lives on a
//! distinct AssetHub para (id 1500) with its own dotNS contract addresses
//! and storage layout. Sourced from
//! `dotli/packages/resolver/src/chain-specs/paseo-asset-hub-next.smol.json`
//! and `dotli/packages/config/src/network.ts`.
//!
//! The Substrate-generic types (`BlockHeader`, etc.) are re-exported from
//! `microdot` rather than duplicated here.

/// Paseo relay-chain genesis hash. Used as the protocol-name prefix on
/// substrate's `/<genesis>/sync/warp`, `/<genesis>/state/2`, etc. The
/// value `77afd61…` is inherited from the chain Paseo was forked from
/// and is intentionally different from Paseo's current genesis-block
/// hash.
pub const RELAY_GENESIS_HASH: [u8; 32] =
    hex_literal::hex!("77afd6190f1554ad45fd0d31aee62aacc33c6db0ea801129acb813f913e0764f");

/// Paseo Asset Hub Next (v2) genesis hash.
pub const ASSETHUB_GENESIS_HASH: [u8; 32] =
    hex_literal::hex!("173cea9df45656cf612c8b8ece56e04e9a693c69cfaac47d3628dae735067af8");

/// AssetHub-Next's `para_id` on Paseo.
pub const ASSETHUB_PARA_ID: u32 = 1500;

/// dotNS ContentResolver contract address on Paseo AH-Next.
pub const DOTNS_CONTENT_RESOLVER: [u8; 20] =
    hex_literal::hex!("2c9FF5D9136DBE5814C7B4FDbeDC15273a776663");

/// Solidity storage slot (within the contract) holding the contenthash
/// mapping.
pub const CONTENTHASH_SLOT: u8 = 0;

/// `wss://` endpoint for the Paseo relay chain. Single-URL fallback when
/// the discovery pool is cold or quarantined.
pub const RELAY_WSS: &str = "wss://paseo-boot-ng.dwellir.com:443";

/// `wss://` endpoint for Paseo Asset Hub Next (v2). Single-URL fallback.
pub const ASSETHUB_WSS: &str =
    "wss://paseo-asset-hub-next-collator-node-0.parity-testnet.parity.io:443";

/// Full multiaddr list of Paseo **relay** wss bootnodes. Each entry
/// carries `/p2p/<peer-id>` so we can populate the peer pool with a
/// known identity. Only `/wss/` entries are kept — plaintext `/tcp/`
/// and `/ws/` are unreachable from a browser tab over https.
pub const RELAY_WSS_BOOTNODES: &[&str] = &[
    "/dns/paseo.bootnode.amforc.com/tcp/29999/wss/p2p/12D3KooWSdf63rZjtGdeWXpQwQwPh8K8c22upcB3B1VmqW8rxrjw",
    "/dns/paseo-boot-ng.dwellir.com/tcp/443/wss/p2p/12D3KooWBLLFKDGBxCwq3QmU3YwWKXUx953WwprRshJQicYu4Cfr",
    "/dns/boot.gatotech.network/tcp/35400/wss/p2p/12D3KooWEvz5Ygv3MhCUNTVQbUTVhzhvf4KKcNoe5M5YbVLPBeeW",
    "/dns/paseo.boot.rotko.net/tcp/30335/wss/p2p/12D3KooWRH8eBMhw8c7bucy6pJfy94q4dKpLkF3pmeGohHmemdRu",
    "/dns/boot.stake.plus/tcp/43334/wss/p2p/12D3KooWNhgAC3hjZHxaT52EpPFZohkCL1AHFAijqcN8xB9Rwud2",
    "/dns/paseo.bootnode.stkd.io/tcp/30633/wss/p2p/12D3KooWMdND5nwfCs5M2rfp5kyRo41BGDgD8V67rVRaB3acgZ53",
    "/dns/paseo-bootnode.turboflakes.io/tcp/30730/wss/p2p/12D3KooWMjCN2CrnN71hAdehn6M2iYKeGdGbZ1A3SKhf4hxrgG9e",
];

/// Full multiaddr list of Paseo Asset Hub Next (v2) wss bootnodes.
pub const ASSETHUB_WSS_BOOTNODES: &[&str] = &[
    "/dns/paseo-asset-hub-next-collator-node-0.parity-testnet.parity.io/tcp/443/wss/p2p/12D3KooWKT5DcVLoBbVDAM6N5ujDVknPfQWHk8SGJqJPyAM8Z4Y4",
    "/dns/paseo-asset-hub-next-collator-node-1.parity-testnet.parity.io/tcp/443/wss/p2p/12D3KooWFjpVdBnfJFntogWhinn15WW2n8Fd2DiAcqU6i9rg47Yg",
];

/// Hex form of [`RELAY_GENESIS_HASH`] — what substrate uses as the
/// protocol-name prefix.
pub fn relay_protocol_prefix_hex() -> String {
    hex::encode(RELAY_GENESIS_HASH)
}

/// Hex form of [`ASSETHUB_GENESIS_HASH`].
pub fn assethub_protocol_prefix_hex() -> String {
    hex::encode(ASSETHUB_GENESIS_HASH)
}
