

use xxhash_rust::xxh3::Xxh3;
use std::{
    hash::Hash, 
    sync::atomic::AtomicU64
};

pub const HASH_SEED_SELECTION: [u64; 6] = [
    0x8badf00d, 0xdeadbabe, 0xabad1dea, 0xdeadbeef, 0xcafebabe, 0xfeedface,
];

static SEED_SELECTION: AtomicU64 = AtomicU64::new(HASH_SEED_SELECTION[0]);

pub struct Hashes {
    pub shard_idx: u16,          // 1024 is the max we need, top 10 bits are for the radix shard index
    pub directory_key: u32,      // Extendible hashing trie, used via global depth
    pub fingerprint_alpha: u8,   // 8-bit fingerprint SIMD tag, layer 1 filter
    pub fingerprint_bravo: u16,  // 16-bit probably cause, layer 2 filter 
}

#[inline(always)]
pub fn pod_hasher<K: Hash>(key: &K) -> Hashes {
    let mut s = Xxh3::with_seed(HASH_SEED_SELECTION[0]);
    key.hash(&mut s);
    let h = s.digest();

    Hashes { 
        // Top 54-63 10 Bits
        shard_idx: (h >> 54) as u16, 
        // Mid 24-53 32 bits
        directory_key: (h >> 24) as u32, 
        //fingerprint_a 16-23
        fingerprint_alpha: (h >> 16) as u8, 
        // fingerprint_b 0-15
        fingerprint_bravo: h as u16 
    }
}


