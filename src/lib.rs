use std::{ 
    hash::Hash, 
    marker::PhantomData, 
    mem::MaybeUninit,
    arch::x86_64::{
        __m128i, 
        _MM_HINT_T0, 
        _mm_cmpeq_epi8,
         _mm_loadu_si128, 
         _mm_movemask_epi8, 
         _mm_prefetch, 
         _mm_set1_epi8 
    }, 
};
use libc::{
    MADV_HUGEPAGE, 
    MADV_SEQUENTIAL, 
    MADV_WILLNEED, 
    c_void, 
    madvise
};
use memmap2::MmapMut;
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use xxhash_rust::xxh3::Xxh3;



#[derive(Debug)]
pub struct HashMap<K, V> {
    maps: Box<[RwLock<ShardHashMap<K, V>>; 256]>
}


impl<K: Hash + PartialEq, V> HashMap<K, V> {

    pub fn new() -> Self {
        Self { maps: Box::new(std::array::from_fn(|_| RwLock::new(ShardHashMap::<K, V>::new()))) }
    }

    pub fn upsert(&self, key: K, value: V) { // rwlock allows for &self nonmut
        let h = pod_hasher(&key, HASH_SEED_SELECTION[0]);
        self.maps[h.shard as usize].write().upsert(key, value, h);
    }

    pub fn get(&self, key: &K) -> Option<MappedRwLockReadGuard<'_, V>> {
        let h = pod_hasher(&key, HASH_SEED_SELECTION[0]);
        let guard = self.maps[h.shard as usize].read();

        RwLockReadGuard::try_map(guard, |shard| {
            shard.get(key, h)
        }).ok()
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let h = pod_hasher(&key, HASH_SEED_SELECTION[0]);
        self.maps[h.shard as usize].write().remove(key, h)
    }

    pub fn stats(&self) {
        self.maps.iter().for_each(|shard| {
            shard.read().stats();
        });
    }
}

unsafe impl<K: Send, V: Send> Send for ShardHashMap<K, V> {}
unsafe impl<K: Send, V: Send> Sync for ShardHashMap<K, V> {}

#[derive(Debug)]
pub struct ShardHashMap<K, V> {
    inlined_directory: [u16; 1024],
    directory_cap: usize,
    directory_len: usize,
    global_depth: u32, 
    _mmap_dir: MmapMut,
    buckets_count: usize,
    buckets_capacity: usize,
    _mmap_buck: MmapMut,
    _marker: PhantomData<(K, V)>
}

impl<K, V> ShardHashMap<K, V> {
    #[inline(always)]
    fn get_bucket_ptr(&self) -> *mut Bucket<K, V> {
        self._mmap_buck.as_ptr() as *mut Bucket<K, V>
    }

    #[inline(always)]
    fn get_bucket_mut(&mut self, idx: u16) -> &mut Bucket<K, V> {
        unsafe { &mut *self.get_bucket_ptr().add(idx as usize) }
    }

    #[inline(always)]
    fn get_bucket(&self, idx: u16) -> &Bucket<K, V> {
        unsafe { &*self.get_bucket_ptr().add(idx as usize) }
    }

    #[inline(always)]
    fn get_bucket_handle_fast(&self, h: &Hashes) -> u16 {
        let depth = self.global_depth;
        let shift = if depth == 0 { 63 } else { 64 - depth };
        let idx = ((h.directory_key >> shift) as usize) & (self.directory_len - 1);

        let base_ptr = if depth <= 10 {
            self.inlined_directory.as_ptr()
        } else {
            self._mmap_dir.as_ptr() as *const u16
        };

        unsafe {
            *base_ptr.add(idx)
        }
    }
}


impl<K: Hash + PartialEq, V> ShardHashMap<K, V> {

    pub fn new() -> Self {
        // 1. Mmap the Bucket Arena (32MB)
        let arena_size = 32 * 1024 * 1024;
        //let arena_size = arena.to_size();
        let bucket_mmap = MmapMut::map_anon(arena_size).expect("Failed to mmap buckets");
        //let bucket_ptr = NonNull::new(bucket_mmap.as_ptr() as *mut Bucket<K, V>).unwrap();
        let buckets_capacity = arena_size / std::mem::size_of::<Bucket<K, V>>();

        // 2. Mmap the Directory (2MB) - Supports up to 1M directory entries
        let dir_cap_bytes = 2 * 1024 * 1024; 
        //let dir_cap_bytes = directory.to_size();
        let dir_mmap = MmapMut::map_anon(dir_cap_bytes).expect("Failed to mmap directory");


        unsafe {
            // Apply to Bucket Arena
            madvise(bucket_mmap.as_ptr() as *mut c_void, arena_size, MADV_HUGEPAGE);
            madvise(bucket_mmap.as_ptr() as *mut c_void, arena_size, MADV_WILLNEED);

            // Warmup the pages (best for low latency/consistency, bad for initial boot times) - manual pre page faulting
            let ptr = bucket_mmap.as_ptr() as *mut u8;
            for i in (0..arena_size).step_by(4096) {
                std::ptr::write_volatile(ptr.add(i), 0);
            }
        
            // Apply to Directory (Sequental access during expansion)
            madvise(dir_mmap.as_ptr() as *mut c_void, dir_cap_bytes, MADV_SEQUENTIAL);
        }

        let mut shard = Self {
            inlined_directory: [0; 1024],
            directory_cap: dir_cap_bytes / 2, // capacity in u16s
            directory_len: 1,
            global_depth: 0,
            _mmap_dir: dir_mmap,
            buckets_count: 1,
            buckets_capacity,
            _mmap_buck: bucket_mmap,
            _marker: PhantomData
        };

        // Initialize the first bucket inline
        let b0 = shard.get_bucket_mut(0);
        b0.local_depth = 0;
        b0.control = [0; 64];
        shard.inlined_directory[0] = 0;

        shard
    }


    #[inline(always)]
    fn get_active_dir_ptr_mut(&mut self) -> *mut u16 {
        if self.global_depth <= 10 {
            self.inlined_directory.as_mut_ptr()
        } else {
            self._mmap_dir.as_mut_ptr() as *mut u16
        }
    }

    fn global_expansion(&mut self) {
        let cur_size = self.directory_len;
    
        // Check for mmap capacity overflow
        if cur_size * 2 > self.directory_cap {
            panic!("Dragon Map: Directory mmap capacity exceeded!");
        }

        unsafe {
            if self.global_depth == 10 {
                // transfer case
                let mmap_ptr = self._mmap_dir.as_mut_ptr() as *mut u16;
                let inline_ptr = self.inlined_directory.as_ptr();
                for i in (0..cur_size).rev() {
                    let val = *inline_ptr.add(i);
                    *mmap_ptr.add(i * 2) = val;
                    *mmap_ptr.add(i * 2 + 1) = val;
                }
                self.inlined_directory = [0; 1024];
            } else if self.global_depth < 10 {
                // CASE B: Standard expansion inline
                let ptr = self.get_active_dir_ptr_mut();
                for i in (0..cur_size).rev() {
                    let val = *ptr.add(i);
                    *ptr.add(i * 2) = val;
                    *ptr.add(i * 2 + 1) = val;
                }
            } else {
                // Expansion within the Mmap arena
                let ptr = self._mmap_dir.as_mut_ptr() as *mut u16;
                for i in (0..cur_size).rev() {
                    let val = *ptr.add(i);
                    *ptr.add(i * 2) = val;
                    *ptr.add(i * 2 + 1) = val;
                }
            }
        }

        self.directory_len *= 2;
        self.global_depth += 1;

    }

    pub fn upsert(&mut self, key: K, value: V, h: Hashes) {
        loop {
            let handle = self.get_bucket_handle_fast(&h);
            
            // CASE 1: Update an existing value
            if let Some((_, i)) = self.simd_lookup(&key, handle, h.fingerprint) {
                let b = self.get_bucket_mut(handle);
                unsafe {
                    b.data.keys[i].assume_init_drop();
                    b.data.values[i].assume_init_drop();
                    std::ptr::write(b.data.keys[i].as_mut_ptr(), key);
                    std::ptr::write(b.data.values[i].as_mut_ptr(), value);
                }
                return;
            }

            // CASE 2: Insert payload
            let b: &mut Bucket<K, V> = self.get_bucket_mut(handle);
            let free_mask = b.free_mask();

            if free_mask != 0 {
                let i = free_mask.trailing_zeros() as usize;// looks for the first 1 bit
                unsafe {
                    std::ptr::write(b.data.keys[i].as_mut_ptr(), key);
                    std::ptr::write(b.data.values[i].as_mut_ptr(), value);
                }
                b.control[i] = h.fingerprint; 
                return;
            } else {
                // CASE 3: Bucket is full, call split
                self.split(handle, h.directory_key, !free_mask);
            }
        }
    }


   
    
    fn split(&mut self, bucket_idx: u16, trigger_hash: u64, mut occupied_mask: u64) {
      //  let idx = self.buckets[]
        if self.buckets_count >= self.buckets_capacity {
            panic!("Dragon Map Shard Overflow! Increase mmap reservation.");
        }

        // check if global expansion is needed
        //let old_depth = self.get_bucket(bucket_idx).header.local_depth;
        let old_depth = self.get_bucket(bucket_idx).local_depth;
        if old_depth == self.global_depth {
            self.global_expansion();
        }

        let new_local_depth = old_depth + 1;
        let new_bucket_idx = self.buckets_count as u16;
        self.buckets_count += 1;

        unsafe {
            let bucket_base = self.get_bucket_ptr();
            let old_bucket = &mut *bucket_base.add(bucket_idx as usize);
            let new_bucket = &mut *bucket_base.add(new_bucket_idx as usize);

            new_bucket.control = [0x00; 64];
            new_bucket.local_depth = new_local_depth;
            old_bucket.local_depth = new_local_depth;

            // 2. Process ONLY occupied slots
       //     let mut temp_mask = occupied_mask;
            let mut new_idx = 0;

            while occupied_mask != 0 {
                let i = occupied_mask.trailing_zeros() as usize;
                
                // Safety: We only access initialized keys/values based on the occupied mask
                let key_ref = old_bucket.data.keys[i].assume_init_ref();
                let h_move = pod_hasher(key_ref, HASH_SEED_SELECTION[0]);

                // Check if the item moves to the new bucket based on the new depth bit
                if (h_move.directory_key >> (64 - new_local_depth)) & 1 == 1 {
                    // Move to new bucket
                    std::ptr::copy_nonoverlapping(old_bucket.data.keys[i].as_ptr(), new_bucket.data.keys[new_idx].as_mut_ptr(), 1);
                    std::ptr::copy_nonoverlapping(old_bucket.data.values[i].as_ptr(), new_bucket.data.values[new_idx].as_mut_ptr(), 1);
                    new_bucket.control[new_idx] = old_bucket.control[i];
                    
                    // Mark old as EMPTY
                    old_bucket.control[i] = 0x00; 
                    new_idx += 1;
                }
                
                occupied_mask &= !(1 << i);// advanced the loop via tarailign zero returning the next result 1
            }
        }

        let stride = 1 << (self.global_depth - old_depth);
        let half_stride = stride >> 1;
        let block_start = (trigger_hash >> (64 - self.global_depth)) as usize & !(stride - 1);

        unsafe {
            // Use the same logic as the lookup to find the right memory region
            let base_ptr = if self.global_depth <= 10 {
                self.inlined_directory.as_mut_ptr()
            } else {
                self._mmap_dir.as_mut_ptr() as *mut u16
            };

            for j in (block_start + half_stride)..(block_start + stride) {
                *base_ptr.add(j) = new_bucket_idx; 
            }
        }
    }

    pub fn get(&self, key: &K, h: Hashes) -> Option<&V> {
        let handle = self.get_bucket_handle_fast(&h);
        
        match self.simd_lookup(key, handle, h.fingerprint) {
            Some((v,_)) => Some(v),
            None => None,
        }
    }

    pub fn remove(&mut self, key: &K, h: Hashes) -> Option<V> {
        let handle = self.get_bucket_handle_fast(&h);
    
        if let Some((_, idx)) = self.simd_lookup(key, handle, h.fingerprint) {
            unsafe {
                let bucket = self.get_bucket_mut(handle);

                let _k = bucket.data.keys[idx].assume_init_read();
                let v = bucket.data.values[idx].assume_init_read(); // returens the real vlaue nto a ref to it
                bucket.control[idx] = 0x7F; // tombstone

                return Some(v);
            }
        }

        None
    }

    #[inline(always)]
    fn simd_lookup(&self, key: &K, bucket_idx: u16, fingerprint: u8) -> Option<(&V, usize)> {
        let bucket = self.get_bucket(bucket_idx); // Using our helper


        unsafe {
            // Prefetch
            _mm_prefetch(bucket.data.keys.as_ptr() as *const i8, _MM_HINT_T0);

            let target = _mm_set1_epi8(fingerprint as i8);
            let ptr = bucket.control.as_ptr() as *const __m128i;

            let m0 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(ptr), target)) as u32;
            let m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(ptr.add(1)), target)) as u32;
            let m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(ptr.add(2)), target)) as u32;
            let m3 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(ptr.add(3)), target)) as u32;

            let mut final_mask = (m0 as u64) | ((m1 as u64) << 16) | ((m2 as u64) << 32) | ((m3 as u64) << 48);

            while final_mask != 0 {
                let idx = final_mask.trailing_zeros() as usize;
                if bucket.data.keys.get_unchecked(idx).assume_init_ref() == key {
                    return Some((bucket.data.values.get_unchecked(idx).assume_init_ref(), idx));
                }
                final_mask &= !(1 << idx);
            }
            None
        }


    }


    pub fn stats(&self) {
        let base = if self.global_depth <= 10 {
            self.inlined_directory.as_ptr()
        } else {
            self._mmap_dir.as_ptr() as *const u16
        };
        
        let directory_slice = unsafe {
            std::slice::from_raw_parts(base, self.directory_len)
        };

        // Count unique buckets in the directory
        let unique_buckets = directory_slice.iter()
            .collect::<std::collections::HashSet<_>>().len();
        
        let total_slots = unique_buckets * 64;
        let mut occupied = 0;

        // Iterate through the bump-allocated arena
        for i in 0..self.buckets_count {
            let b = self.get_bucket(i as u16);
            for slot in 0..64 {
                // Check MSB: 1000 0000
                if (b.control[slot] & 0x80) != 0 {
                    occupied += 1;
                }
            }
        }

        let load_factor = if total_slots > 0 {
            (occupied as f64 / total_slots as f64) * 100.0
        } else {
            0.0
        };

        println!("--- Dragon Map Shard Stats ---");
        println!("Global Depth:   {}", self.global_depth);
        println!("Directory Len:  {}", self.directory_len);
        println!("Unique Buckets: {}", unique_buckets);
        println!("Total Buckets:  {}", self.buckets_count); 
        println!("Occupied Slots: {}", occupied);
        println!("Load Factor:    {:.2}%\n", load_factor);
    }

}

impl<K, V> Drop for ShardHashMap<K, V> {
    fn drop(&mut self) {
        let base: *mut Bucket<K, V> = self.get_bucket_ptr();
        for i in 0..self.buckets_count {
            unsafe {
                std::ptr::drop_in_place(base.add(i));
            }
        }
    }
}

#[repr(align(64))]
struct Bucket<K, V> {
    // Cache line 1
    control: [u8; 64],

    // Cache line 2
    local_depth: u32,
    _pad: [u32; 15],

    // Cache line 3+
    data: Payload<K, V>
}

struct Payload<K, V> {
    keys: [MaybeUninit<K>; 64],
    values: [MaybeUninit<V>; 64],
}

// Occupied 0x80 - 1000 0000
// Tombstone 0x7F - 0111 1111
// Empty 0x00 - 0000 0000

impl<K, V> Bucket<K, V> {
    fn free_mask(&self) -> u64 {
        !unsafe {
            let ptr = self.control.as_ptr() as *const __m128i;
            let m0 = _mm_movemask_epi8(_mm_loadu_si128(ptr)) as u32;
            let m1 = _mm_movemask_epi8(_mm_loadu_si128(ptr.add(1))) as u32;
            let m2 = _mm_movemask_epi8(_mm_loadu_si128(ptr.add(2))) as u32;
            let m3 = _mm_movemask_epi8(_mm_loadu_si128(ptr.add(3))) as u32;
            (m0 as u64) | ((m1 as u64) << 16) | ((m2 as u64) << 32) | ((m3 as u64) << 48)
        }
    }
}


impl<K, V> Drop for Bucket<K, V> {
    fn drop(&mut self) {
        for i in 0..64 {
            // ONLY drop if MSB is 1. Tombstones (0x7F) and Empty (0x00) have MSB 0.
            if (self.control[i] & 0x80) != 0 {
                unsafe {
                    self.data.keys[i].assume_init_drop();
                    self.data.values[i].assume_init_drop();
                }
            }
        }
    }
}


pub const HASH_SEED_SELECTION: [u64; 6] = [
    0x8badf00d, 0xdeadbabe, 0xabad1dea, 0xdeadbeef, 0xcafebabe, 0xfeedface,
];

#[derive(Debug)]
pub struct Hashes {
    directory_key: u64,
    fingerprint: u8,
    shard: u8,
}

#[inline(always)]
fn pod_hasher<K: std::hash::Hash>(key: &K, seed: u64) -> Hashes {
    let mut s = Xxh3::with_seed(seed);
    key.hash(&mut s);
    let h = s.digest128();
    
    let directory_key = (h >> 64) as u64; 
    let shard = ((h >> 56) & 0xFF) as u8; 
    // Force bit 7 to 1 (0x80) so it's always marked "Occupied"
    let fingerprint = ((h & 0x7F) as u8) | 0x80;

    Hashes { directory_key, fingerprint, shard }
}

#[derive(Debug, Clone)]
pub enum Memory {
    Mb2,
    Mb4,
    Mb32,
    Mb64,
    Mb128
}


impl Memory {
    fn to_size(&self) -> usize {
        match self {
            Memory::Mb2 => 1024 * 1024 * 2,
            Memory::Mb4 => 1024 * 1024 * 4,
            Memory::Mb32 => 1024 * 1024 * 32,
            Memory::Mb64 => 1024 * 1024 * 64,
            Memory::Mb128 => 1024 * 1024 * 128,
        }
    }
}
