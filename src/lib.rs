

pub mod directory;
pub mod map;
pub mod payload;
pub mod hash;

// pub struct HashMap<K, V> {
//     maps: Box<[RwLock<ShardHashMap<K, V>>; 256]>
// }


// impl<K: Hash + PartialEq, V> HashMap<K, V> {

//     pub fn new() -> Self {
//         Self { maps: Box::new(std::array::from_fn(|_| RwLock::new(ShardHashMap::<K, V>::new()))) }
//     }

//     pub fn upsert(&self, key: K, value: V) { // rwlock allows for &self nonmut
//         let h = pod_hasher(&key, HASH_SEED_SELECTION[0]);
//         self.maps[h.shard as usize].write().upsert(key, value, h);
//     }

//     pub fn get(&self, key: &K) -> Option<MappedRwLockReadGuard<'_, V>> {
//         let h = pod_hasher(&key, HASH_SEED_SELECTION[0]);
//         let guard = self.maps[h.shard as usize].read();

//         RwLockReadGuard::try_map(guard, |shard| {
//             shard.get(key, h)
//         }).ok()
//     }

//     pub fn remove(&self, key: &K) -> Option<V> {
//         let h = pod_hasher(&key, HASH_SEED_SELECTION[0]);
//         self.maps[h.shard as usize].write().remove(key, h)
//     }

//     pub fn stats(&self) {
//         self.maps.iter().for_each(|shard| {
//             shard.read().stats();
//         });
//     }
// }

// unsafe impl<K: Send, V: Send> Send for ShardHashMap<K, V> {}
// unsafe impl<K: Send, V: Send> Sync for ShardHashMap<K, V> {}


// pub struct ShardHashMap<K, V> {
//     //inlined_directory: [u16; 1024], // First 0-9 expansions stay on the stack for inlined handle retreival for smaller maps
    
//     // -- HOT DATA -- 
//     dir_ptr: *const u16, // 8 Bytes 
//     buck_ptr: *mut Bucket<K, V>, // 8 Bytes  // 16
//     mask: usize, // 8 Bytes  // 24
//     shift: u32, // 4 Bytes // 28

//     // -- WARM DATA --
//     state: AtomicU32, // 4 Bytes // 32
//     buckets_count: usize, // 8 Bytes // 40
//     buckets_capacity: usize, // 8 Bytes // 48
//     directory_cap: usize, // 8 Bytes 56
//     directory_len: usize, // 8 Bytes 

//     // ---------------- CACHE LINE ----------------

//     // -- COLD DATA (CONST OWNERS) -- 
//     _mmap_dir: MmapMut,
//     _mmap_buck: MmapMut,
//     _marker: PhantomData<(K, V)>
// }

// impl<K, V> ShardHashMap<K, V> {
//     #[inline(always)]
//     fn get_bucket_ptr(&self) -> *mut Bucket<K, V> {
//         self.buck_ptr as *mut Bucket<K, V>
//     }

//     #[inline(always)]
//     fn get_bucket_mut(&mut self, idx: u16) -> &mut Bucket<K, V> {
//         unsafe { &mut *self.get_bucket_ptr().add(idx as usize) }
//     }

//     #[inline(always)]
//     fn get_bucket(&self, idx: u16) -> &Bucket<K, V> {
//         unsafe { &*self.get_bucket_ptr().add(idx as usize) }
//     }

//     #[inline(always)]
//     fn get_bucket_handle_fast(&self, h: &Hashes) -> u16 {
//         unsafe {
//             // If shift is 64, index is always 0. Otherwise, shift down.
//             let idx = if self.shift >= 64 { 
//                 0 
//             } else { 
//                 (h.directory_key >> self.shift) as usize 
//             };
//             *self.dir_ptr.add(idx)
//         }
//     }
// }


// impl<K: Hash + PartialEq, V> ShardHashMap<K, V> {

//     pub fn new() -> Self {
//         // 1. Mmap the Bucket Arena (32MB)
//         let arena_size = 32 * 1024 * 1024;
//         let bucket_mmap = MmapMut::map_anon(arena_size).expect("Failed to mmap buckets");
//         let buckets_capacity = arena_size / std::mem::size_of::<Bucket<K, V>>();

//         // 2. Mmap the Directory (2MB) - Supports up to 1M directory entries
//         let dir_cap_bytes = 2 * 1024 * 1024; 
//         let dir_mmap = MmapMut::map_anon(dir_cap_bytes).expect("Failed to mmap directory");

//         let dir_ptr = dir_mmap.as_ptr() as *mut u16;
//         unsafe { *dir_ptr = 0; }

//         unsafe {
//             // Apply to Bucket Arena
//             madvise(bucket_mmap.as_ptr() as *mut c_void, arena_size, MADV_HUGEPAGE);
//             madvise(bucket_mmap.as_ptr() as *mut c_void, arena_size, MADV_WILLNEED);

//             // Warmup the pages (best for low latency/consistency, bad for initial boot times) - manual pre page faulting
//             let ptr = bucket_mmap.as_ptr() as *mut u8;
//             for i in (0..arena_size).step_by(4096) {
//                 std::ptr::write_volatile(ptr.add(i), 0);
//             }
        
//             // Apply to Directory (Sequental access during expansion)
//             madvise(dir_mmap.as_ptr() as *mut c_void, dir_cap_bytes, MADV_SEQUENTIAL);
//         }

//         let mut shard = Self {
//             dir_ptr,
//             buck_ptr: bucket_mmap.as_ptr() as *mut Bucket<K, V>,
//             mask: 0,       // (1 << 0) - 1
//             shift: 64,     // 64 - global_depth(0)
//             state: AtomicU32::new(0),
//             buckets_count: 1,
//             buckets_capacity,
//             directory_len: 1,
//             directory_cap: dir_cap_bytes / 2,
//             _mmap_dir: dir_mmap,
//             _mmap_buck: bucket_mmap,
//             _marker: PhantomData
//         };

//         // Initialize the first bucket inline
//         let b0 = shard.get_bucket_mut(0);
//         b0.local_depth = 0;
//         b0.control = [0; 64];

//         shard
//     }

//     fn global_expansion(&mut self) {
//         let old_len = self.directory_len;
//         let new_len = old_len * 2;
    
//         // Check for mmap capacity overflow
//         if new_len > self.directory_cap {
//             panic!("Dragon Map: Directory mmap capacity exceeded!");
//         }

//         unsafe {
//             let ptr = self.dir_ptr as *mut u16;
//             // Expand in place within the Mmap
//             for i in (0..old_len).rev() {
//                 let val = *ptr.add(i);
//                 *ptr.add(i * 2) = val;
//                 *ptr.add(i * 2 + 1) = val;
//             }
//         }

//         self.directory_len = new_len;
//         self.mask = new_len - 1;
//         self.shift -= 1;
//     }

//     pub fn upsert(&mut self, key: K, value: V, h: Hashes) {
//         loop {
//             let handle = self.get_bucket_handle_fast(&h);
//             let b: &mut Bucket<K, V> = self.get_bucket_mut(handle);
            
//             // CASE 1: Update an existing value
//             if let Some((_, i)) = b.simd_lookup_bucket(&key, h.fingerprint) {
                
//                 unsafe {
//                     b.data.keys[i].assume_init_drop();
//                     b.data.values[i].assume_init_drop();
//                     std::ptr::write(b.data.keys[i].as_mut_ptr(), key);
//                     std::ptr::write(b.data.values[i].as_mut_ptr(), value);
//                 }
//                 return;
//             }

//             // CASE 2: Insert payload
//             let free_mask = b.reusable_mask();

//             if free_mask != 0 {
//                 let i = free_mask.trailing_zeros() as usize;// looks for the first 1 bit
//                 unsafe {
//                     std::ptr::write(b.data.keys[i].as_mut_ptr(), key);
//                     std::ptr::write(b.data.values[i].as_mut_ptr(), value);
//                 }
//                 b.control[i] = h.fingerprint; 
//                 return;
//             } else {
//                 // CASE 3: Bucket is full, call split
//                 let occupied_mask = b.occupied_mask();
//                 self.split(handle, h.directory_key, occupied_mask);
//             }
//         }
//     }


   
    
//     fn split(&mut self, bucket_idx: u16, trigger_hash: u64, mut occupied_mask: u64) {
//       //  let idx = self.buckets[]
//         if self.buckets_count >= self.buckets_capacity {
//             panic!("Dragon Map Shard Overflow! Increase mmap reservation.");
//         }

//         // check if global expansion is needed
//         //let old_depth = self.get_bucket(bucket_idx).header.local_depth;
//         let global_depth = 64 - self.shift;
//         let old_depth = self.get_bucket(bucket_idx).local_depth;
//         if old_depth == global_depth {
//             self.global_expansion();
//         }


//         let current_global_depth = 64 - self.shift;
//         let new_local_depth = old_depth + 1;
//         let new_bucket_idx = self.buckets_count as u16;
//         self.buckets_count += 1;

//         unsafe {
//             let bucket_base = self.get_bucket_ptr();
//             let old_bucket = &mut *bucket_base.add(bucket_idx as usize);
//             let new_bucket = &mut *bucket_base.add(new_bucket_idx as usize);

//             new_bucket.control = [0x00; 64];
//             new_bucket.local_depth = new_local_depth;
//             old_bucket.local_depth = new_local_depth;

//             // 2. Process ONLY occupied slots
//        //     let mut temp_mask = occupied_mask;
//             let mut new_idx = 0;

//             while occupied_mask != 0 {
//                 let i = occupied_mask.trailing_zeros() as usize;
                
//                 // Safety: We only access initialized keys/values based on the occupied mask
//                 let key_ref = old_bucket.data.keys[i].assume_init_ref();
//                 let h_move = pod_hasher(key_ref, HASH_SEED_SELECTION[0]);

//                 // Check if the item moves to the new bucket based on the new depth bit
//                 if (h_move.directory_key >> (64 - new_local_depth)) & 1 == 1 {
//                     // Move to new bucket
//                     std::ptr::copy_nonoverlapping(old_bucket.data.keys[i].as_ptr(), new_bucket.data.keys[new_idx].as_mut_ptr(), 1);
//                     std::ptr::copy_nonoverlapping(old_bucket.data.values[i].as_ptr(), new_bucket.data.values[new_idx].as_mut_ptr(), 1);
//                     new_bucket.control[new_idx] = old_bucket.control[i];
                    
//                     // Mark old as EMPTY
//                     old_bucket.control[i] = 0x00; 
//                     new_idx += 1;
//                 }
                
//                 occupied_mask &= !(1 << i);// advanced the loop via tarailign zero returning the next result 1
//             }
//         }

//         // Update the directory pointers
//         let stride = 1 << (current_global_depth - new_local_depth);
//         let block_size = 1 << (current_global_depth - old_depth);
        
//         // Calculate which part of the directory needs to point to the new bucket
//         let base_idx = (trigger_hash >> self.shift) as usize & !(block_size - 1);
//         let split_idx = base_idx + stride;

//         unsafe {
//             let base_ptr = self.dir_ptr as *mut u16;
//             for j in split_idx..(base_idx + block_size) {
//                 *base_ptr.add(j) = new_bucket_idx;
//             }
//         }
//     }

//     pub fn get(&self, key: &K, h: Hashes) -> Option<&V> {
//         let handle = self.get_bucket_handle_fast(&h);
//         let b = self.get_bucket(handle);

//         unsafe { _mm_prefetch(b.data.keys.as_ptr() as *const i8, _MM_HINT_T0); }
        
//         match b.simd_lookup_bucket(key, h.fingerprint) {
//             Some((v,_)) => Some(v),
//             None => None,
//         }
//     }

//     pub fn remove(&mut self, key: &K, h: Hashes) -> Option<V> {
//         let handle = self.get_bucket_handle_fast(&h);
//         let b = self.get_bucket_mut(handle);
    
//         if let Some((_, idx)) = b.simd_lookup_bucket(key, h.fingerprint) {
//             unsafe {
//                 let bucket = self.get_bucket_mut(handle);
//                 std::ptr::drop_in_place(bucket.data.keys[idx].as_mut_ptr());
//                 let v = bucket.data.values[idx].assume_init_read(); // returens the real vlaue nto a ref to it
//                 bucket.control[idx] = 0x7F; // tombstone

//                 return Some(v);
//             }
//         }

//         None
//     }

    


//     pub fn stats(&self) {
//         let global_depth = 64 - self.shift;
//         let base = self.dir_ptr as *const u16;

//         let directory_slice = unsafe {
//             std::slice::from_raw_parts(base, self.directory_len)
//         };

//         // Count unique buckets in the directory
//         let unique_buckets = directory_slice.iter()
//             .collect::<std::collections::HashSet<_>>().len();
        
//         let total_slots = unique_buckets * 64;
//         let mut occupied = 0;

//         // Iterate through the bump-allocated arena
//         for i in 0..self.buckets_count {
//             let b = self.get_bucket(i as u16);
//             for slot in 0..64 {
//                 // Check MSB: 1000 0000
//                 if (b.control[slot] & 0x80) != 0 {
//                     occupied += 1;
//                 }
//             }
//         }

//         let load_factor = if total_slots > 0 {
//             (occupied as f64 / total_slots as f64) * 100.0
//         } else {
//             0.0
//         };

//         println!("--- Dragon Map Shard Stats ---");
//         println!("Global Depth:   {}", global_depth);
//         println!("Directory Len:  {}", self.directory_len);
//         println!("Unique Buckets: {}", unique_buckets);
//         println!("Total Buckets:  {}", self.buckets_count); 
//         println!("Occupied Slots: {}", occupied);
//         println!("Load Factor:    {:.2}%\n", load_factor);
//     }

// }

// impl<K, V> Drop for ShardHashMap<K, V> {
//     fn drop(&mut self) {
//         let base: *mut Bucket<K, V> = self.get_bucket_ptr();
//         for i in 0..self.buckets_count {
//             unsafe {
//                 std::ptr::drop_in_place(base.add(i));
//             }
//         }
//     }
// }

// #[repr(align(64))]
// struct Bucket<K, V> {
//     // Swiss Table fingerprints + Control Bit for SIMD
//     control: [u8; 64], // 64 Bytes 

//     // ---------------- CACHE LINE ----------------

//     // Cache line 2
//    // state: AtomicU32, // 4 Bytes 
//     local_depth: u32,  // 4 Bytes // 4

//     //version: AtomicU32, // FOR
//     //_pad: [u32; 15],

//     // ---------------- CACHE LINE ----------------

//     // Cache line 3+
//     data: Payload<K, V>
// }

// struct Payload<K, V> {
//     keys: [MaybeUninit<K>; 64],
//     values: [MaybeUninit<V>; 64],
// }

// // Occupied 0x80 - 1000 0000
// // Tombstone 0x7F - 0111 1111
// // Empty 0x00 - 0000 0000

// impl<K, V> Bucket<K, V> {
//     #[inline(always)]
//     fn free_masks(&self) -> u64 {
//         unsafe {
//             let ptr = self.control.as_ptr() as *const __m128i;
//             // A slot is "Free" ONLY if it is 0x00.
//             let m0 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_load_si128(ptr), _mm_setzero_si128())) as u32;
//             let m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_load_si128(ptr.add(1)), _mm_setzero_si128())) as u32;
//             let m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_load_si128(ptr.add(2)), _mm_setzero_si128())) as u32;
//             let m3 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_load_si128(ptr.add(3)), _mm_setzero_si128())) as u32;
//             (m0 as u64) | ((m1 as u64) << 16) | ((m2 as u64) << 32) | ((m3 as u64) << 48)
//         }
//     }

//     #[inline(always)]
//     fn occupied_mask(&self) -> u64 {
//         unsafe {
//             let ptr = self.control.as_ptr() as *const __m128i;
//             // A slot is "Occupied" ONLY if the MSB is 1 (0x80..0xFF)
//             let m0 = _mm_movemask_epi8(_mm_load_si128(ptr)) as u32;
//             let m1 = _mm_movemask_epi8(_mm_load_si128(ptr.add(1))) as u32;
//             let m2 = _mm_movemask_epi8(_mm_load_si128(ptr.add(2))) as u32;
//             let m3 = _mm_movemask_epi8(_mm_load_si128(ptr.add(3))) as u32;
//             (m0 as u64) | ((m1 as u64) << 16) | ((m2 as u64) << 32) | ((m3 as u64) << 48)
//         }
//     }

//     #[inline(always)]
//     fn reusable_mask(&self) -> u64 {
//         unsafe {
//             let ptr = self.control.as_ptr() as *const __m128i;
//             // Movemask extracts the MSB (bit 7). 
//             // We want slots where bit 7 is 0. So we get the occupied mask and bitwise NOT it.
//             let m0 = _mm_movemask_epi8(_mm_load_si128(ptr)) as u32;
//             let m1 = _mm_movemask_epi8(_mm_load_si128(ptr.add(1))) as u32;
//             let m2 = _mm_movemask_epi8(_mm_load_si128(ptr.add(2))) as u32;
//             let m3 = _mm_movemask_epi8(_mm_load_si128(ptr.add(3))) as u32;
//             let occupied = (m0 as u64) | ((m1 as u64) << 16) | ((m2 as u64) << 32) | ((m3 as u64) << 48);
//             !occupied // Any slot not occupied is reusable
//         }
//     }
// }



// impl<K: PartialEq, V> Bucket<K, V> {
//     #[inline(always)]
//     fn simd_lookup_bucket(&self, key: &K,fingerprint: u8) -> Option<(&V, usize)> {
//        // let bucket = self.get_bucket(bucket_idx); // Using our helper


//         unsafe {
//             // Prefetch
//             _mm_prefetch(self.data.keys.as_ptr() as *const i8, _MM_HINT_T0);

//             let target = _mm_set1_epi8(fingerprint as i8);
//             let ptr = self.control.as_ptr() as *const __m128i;

//             let m0 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_load_si128(ptr), target)) as u32;
//             let m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_load_si128(ptr.add(1)), target)) as u32;
//             let m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_load_si128(ptr.add(2)), target)) as u32;
//             let m3 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_load_si128(ptr.add(3)), target)) as u32;

//             let mut final_mask = (m0 as u64) | ((m1 as u64) << 16) | ((m2 as u64) << 32) | ((m3 as u64) << 48);

//             while final_mask != 0 {
//                 let idx = final_mask.trailing_zeros() as usize;
//                 if self.data.keys.get_unchecked(idx).assume_init_ref() == key {
//                     return Some((self.data.values.get_unchecked(idx).assume_init_ref(), idx));
//                 }
//                 final_mask &= !(1 << idx);
//             }
//             None
//         }
//     }
// }


// impl<K, V> Drop for Bucket<K, V> {
//     fn drop(&mut self) {
//         for i in 0..64 {
//             // ONLY drop if MSB is 1. Tombstones (0x7F) and Empty (0x00) have MSB 0.
//             if (self.control[i] & 0x80) != 0 {
//                 unsafe {
//                     self.data.keys[i].assume_init_drop();
//                     self.data.values[i].assume_init_drop();
//                 }
//             }
//         }
//     }
// }


// pub const HASH_SEED_SELECTION: [u64; 6] = [
//     0x8badf00d, 0xdeadbabe, 0xabad1dea, 0xdeadbeef, 0xcafebabe, 0xfeedface,
// ];


// pub struct Hashes {
//     directory_key: u64,
//     fingerprint: u8,
//     shard: u8,
// }

// #[inline(always)]
// fn pod_hasher<K: Hash>(key: &K, seed: u64) -> Hashes {
//     //let mut s = Xxh3::with_seed(seed);
//     //key.hash(&mut s);
//     //let h = s.digest128();
//     let mut s = Xxh3::with_seed(seed);
//     key.hash(&mut s);
//     let h = s.digest();


//     let shard = (h &0xFF) as u8; 
//     // Force bit 7 to 1 (0x80) so it's always marked "Occupied"
//     let fingerprint = (((h >> 8) & 0x7F) as u8) | 0x80;

//     Hashes { directory_key: h, fingerprint, shard }
// }

// pub enum Memory {
//     Mb2,
//     Mb4,
//     Mb32,
//     Mb64,
//     Mb128
// }


// impl Memory {
//     fn to_size(&self) -> usize {
//         match self {
//             Memory::Mb2 => 1024 * 1024 * 2,
//             Memory::Mb4 => 1024 * 1024 * 4,
//             Memory::Mb32 => 1024 * 1024 * 32,
//             Memory::Mb64 => 1024 * 1024 * 64,
//             Memory::Mb128 => 1024 * 1024 * 128,
//         }
//     }
// }
