use std::hash::Hash;

use xxhash_rust::xxh3::{self, Xxh3};
pub const HASH_SEED_SELECTION: [u64; 6] = [
    0x8badf00d, 0xdeadbabe, 0xabad1dea, 0xdeadbeef, 0xcafebabe, 0xfeedface,
];


pub struct Hashes {
    pub directory_key: u64,
    pub fingerprint: u8,
}

#[inline(always)]
pub fn pod_hasher<K: Hash>(key: &K) -> Hashes {
    let mut s = Xxh3::with_seed(HASH_SEED_SELECTION[0]);
    key.hash(&mut s);
    let h = s.digest();

    let fingerprint = (h >> 56) as u8; 

    Hashes { directory_key: h, fingerprint }
}


