
//! # Associative Array
//! 
//! A low-latency, SIMD-accelerated extendible hash map.
//! 
//! ## Architecture 
//! Associative Array utlizes extensible hashing with a global directory to index into a bucket  and commit SIMD fingerprint cmpeq operations.
//! This allows for $O(1)$ lookups at scale and amortized bucket rehashing/splits minimize p99 latency.
//! 
//! ## Examples
//! 
//! 
//! 
//! 
//! 



// So having the ac



pub mod hash;
pub mod bucket;
pub mod directory;
pub mod map;







































//     pub struct HashMap<K, V> {
//         // --- Inlined radix sharding SOA, handles point to shard which points to a mmapped arena  --- 
//         directory_routing: [RouterShard;  1 << 10], //  shard_size (16 Bytes) * 1024
//         directory_allocation: [AllocatorShard; 1 << 10],  //  shard_size (16 Bytes) * 1024

//         // ----- BUCKETS ----- 
//         bucket_ptr: *mut Bucket<K, V>, 

//         // memory allocation and data type handles
//         _mmap_directory: MmapMut,
//         _mmap_bucket: MmapMut, 

//         _marker: PhantomData<(K, V)>
//     }

//     #[repr(C, align(16))]
//     struct RouterShard {
//         // mmapped arena of the shards directory handles 
//         pub data_ptr: AtomicPtr<u32>, // 8 bytes 

//         // [u8: global_depth | u24: mask(len - 1)]
//         pub shard_depth: AtomicU32, // 4 bytes, 12
//         pub shard_mask: AtomicU32 // 4 bytes, 16
//     }

//     #[repr(C, align(16))]
//     struct AllocatorShard {
//         pub bucket_base_idx: u32, // 4 bytes
//         pub next_offset: AtomicU32, // 4 bytes, 8
//         pub max_offset: u32, // 4 bytes, 12
//         _pad: u32
//     }


//     impl<K, V> HashMap<K, V> 
//     where 
//         K: PartialEq + Hash 
//     {
//         pub fn new(system_memory_gb: usize, directory_gb: usize) -> Result<Self> {
//             // Need to pass in the mb the user needs since this can fail due to lack of memory on system and linux kernel not liking mass Mmapping of non-existent memory
//             let directory_arena_size = directory_gb * 1024 * 1024 * 1024;
//             let bucket_arena_size = (system_memory_gb - directory_gb) * 1024 * 1024 * 1024;


//             let directory_mmap = MmapMut::map_anon(directory_arena_size)?;
//             let mut bucket_mmap = MmapMut::map_anon(bucket_arena_size)?;


//             let directory_ptr = directory_mmap.as_ptr() as *mut u32;
//             let bucket_ptr = bucket_mmap.as_mut_ptr() as *mut Bucket<K, V>;


//             let total_handles = directory_arena_size / std::mem::size_of::<u32>();
//             let handles_per_shard = total_handles / 1024;


//             let total_buckets = bucket_arena_size / std::mem::size_of::<Bucket<K, V>>();
//             let buckets_per_shard = total_buckets / 1024;

//             // let mut routing: [RouterShard; 1024] = unsafe { std::mem::zeroed() };
//             // let mut allocation: [AllocatorShard; 1024] = unsafe { std::mem::zeroed() };


//             let mut routing: [std::mem::MaybeUninit<RouterShard>; 1024] = unsafe {
//                 std::mem::MaybeUninit::uninit().assume_init()
//             };
//             let mut allocation: [std::mem::MaybeUninit<AllocatorShard>; 1024] = unsafe {
//                 std::mem::MaybeUninit::uninit().assume_init()
//             };


//             for i in 0..1024 {
//                 let shard_dir_start = unsafe { directory_ptr.add(i * handles_per_shard) };

//                 let first_bucket_idx = (i * buckets_per_shard) as u32;

                


//                 routing[i].write( RouterShard {
//                     data_ptr: AtomicPtr::new(shard_dir_start),
//                     shard_depth: AtomicU32::new(0),
//                     shard_mask: AtomicU32::new(0),                    
//                 });

//                 allocation[i].write(AllocatorShard {
//                     bucket_base_idx: first_bucket_idx,
//                     next_offset: AtomicU32::new(1), // already used 0
//                     max_offset: buckets_per_shard as u32,
//                     _pad: 0,
//                 });


//                 unsafe {
//                     std::ptr::write_volatile(shard_dir_start, first_bucket_idx);
//                     // Initialize the actual bucket in the bucket arena
//                     Bucket::init_at(bucket_ptr.add(first_bucket_idx as usize), 0);
//                 }
//             }


//             // MMAP Optimizations 
//             unsafe {
//                 madvise(bucket_ptr as *mut c_void, bucket_arena_size, MADV_HUGEPAGE);
//                 //madvise(payload_ptr as *mut c_void, payload_arena_size, MADV_WILLNEED);

//                 // Sequential for directory because expansion doubles it linearly
//                 madvise(directory_ptr as *mut c_void, directory_arena_size, MADV_SEQUENTIAL);

//             }


//             let directory_routing: [RouterShard; 1024] = unsafe { std::mem::transmute(routing) };
//             let directory_allocation: [AllocatorShard; 1024] = unsafe { std::mem::transmute(allocation) };

            
//             Ok(Self { 
//                 directory_routing, 
//                 directory_allocation, 
//                 bucket_ptr, 
//                 _mmap_directory: directory_mmap, 
//                 _mmap_bucket: bucket_mmap, 
//                 _marker: PhantomData, 
//             })

//         }







//         // Upsert
//         pub fn upsert(&self, key: K, value: V) {
//             let hashes: Hashes = pod_hasher(&key);

//             let target = unsafe { _mm_set1_epi8(hashes.fingerprint as i8) };

//             loop {
                
//                 let bucket_handle = self.get_bucket_handle(&hashes);
//                 let bucket = self.get_bucket_mut(&bucket_handle);

//                 // CASE 1: Key exists, uppdate the value
//                 unsafe {
//                     // Prefetch Keys for comaprasion operation after simd
//                     _mm_prefetch(bucket.keys.as_ptr() as *const i8, _MM_HINT_T0);

//                     // // Load the 16 fingerprints into the SSE 128 bit register 
//                     let fingerprints = _mm_load_si128(bucket.fingerprints.as_ptr() as *const __m128i);

//                     // // Broadcasts the fingerprint across all the u8 in the SSE
//                     // let target = _mm_set1_epi8(hashes.fingerprint as i8);

//                     // Compare the target to thefingerprints
//                     let m = _mm_cmpeq_epi8(fingerprints, target);

//                     // Bridge the results, for a u8 hit flips the bit mask to 1
//                     let bitmask = _mm_movemask_epi8(m) as u16; 

//                     // filter out trash results, ensure fingerpritn is active not removed or invalid
//                     let mut filtered_bitmask = bitmask & bucket.occupancy_bitmask;

//                     // Iterate oiver hits
//                     while filtered_bitmask != 0 {
//                         let i = filtered_bitmask.trailing_zeros() as usize; // returns when abit == 1

//                         if *bucket.keys[i].assume_init_ref() == key { // Hit on the key
//                             // Update Value
//                             bucket.values[i].assume_init_drop();
//                             std::ptr::write(bucket.values[i].as_mut_ptr(), value);
//                             return;
//                         } 

//                         filtered_bitmask &= filtered_bitmask - 1;
//                     }



//                     // CASE 2: No Key, But bucket has space, InsertPayload
//                     if bucket.occupancy_bitmask != 0xFFFF { // if ocupancy is not full, this is a common path for a insert 
//                         // insert elements
//                         let i = (!bucket.occupancy_bitmask).trailing_zeros() as usize; // flip occupancy mask to free mask
                        
//                         std::ptr::write(bucket.keys[i].as_mut_ptr(), key);
//                         std::ptr::write(bucket.values[i].as_mut_ptr(), value);

//                         bucket.fingerprints[i] = hashes.fingerprint;
//                         bucket.occupancy_bitmask |= 1 << i; // flip the bit to occupied
                    
//                         return;
//                     } else {

//                         // CASE 3: No Key, But bucket hno as space, Split (and globally expand if needed) then InsertPayload
//                         let current_free = self.next_free_bucket.get();
                        

//                         if current_free >= self.bucket_capacity {
//                             panic!("Hashmap Ran out of memory in the 32 MB arena")
//                         }
//                         self.next_free_bucket.set(current_free + 1);

//                     //   let old_bucket = self.get_bucket_mut(&bucket_handle);

//                         // Expand GLobally
//                         if bucket.local_depth == self.global_depth.get() {
//                             // Expand Directory
//                             let old_len = self.directory_len_mask.get() + 1;

//                             std::ptr::copy_nonoverlapping(self.directory_ptr, self.directory_ptr.add(old_len), old_len);
                        
//                             let current_depth = self.global_depth.get();
//                             self.global_depth.set(current_depth + 1);

//                             self.directory_len_mask.set((old_len * 2) - 1);
                        
//                         }


//                         let new_bucket_ptr = self.payload_ptr.add(current_free);
//                         Payload::init_at(new_bucket_ptr, bucket.local_depth + 1);

//                         let new_bucket = &mut *new_bucket_ptr;

//                         bucket.local_depth += 1;
//                         let split_bit = 1 << (bucket.local_depth - 1);
//                         let mut occupied = bucket.occupancy_bitmask;
//                         let mut new_bucketcount = 0;

//                         while occupied != 0 {
//                             let i = occupied.trailing_zeros() as usize;

//                             let key_ref = bucket.keys[i].assume_init_ref();
//                             let h = pod_hasher(key_ref);

//                             if (h.directory_key & split_bit) != 0 {
//                                 let dest_idx = new_bucketcount;


//                                 std::ptr::copy_nonoverlapping(bucket.keys[i].as_ptr(), new_bucket.keys[dest_idx].as_mut_ptr(), 1);
//                                 std::ptr::copy_nonoverlapping(bucket.values[i].as_ptr(), new_bucket.values[dest_idx].as_mut_ptr(), 1);
                                
//                                 //let target_idx = new_bucket.occupancy_bitmask.count_ones() as usize;
//                                 new_bucket.fingerprints[dest_idx] = bucket.fingerprints[i];
//                                 new_bucket.occupancy_bitmask |= 1 << dest_idx; // Set new occupancy bit
//                                 new_bucketcount += 1;

//                                 // Clear from old bucket
//                                 bucket.occupancy_bitmask &= !(1 << i);

//                             }

//                             occupied &= !(1 << i); // lets loop continue
//                         }

//                         let step = 1 << bucket.local_depth;
//                         let mask = self.directory_len_mask.get();
//                         let mut dir_idx = (hashes.directory_key as usize & (step - 1)) | split_bit as usize;
                        
//                         while dir_idx <= mask {
//                             *self.directory_ptr.add(dir_idx) = current_free as u32;
//                             dir_idx += step;
//                         }
//                     }
//                 }
//             }
//         }

//         // Insert, unsafe no simd or key check, does not ovewrite data, therefore will duplicate, only use for bulk loads or guearenteed unique keys inserts, otherwise use upsert

//         pub unsafe fn insert(&self, key: K, value: V) {
//             let hashes = pod_hasher(&key);

//             loop {
//                 // let new_slot = self.next_free_bucket.get_mut();

//                 let bucket_handle = self.get_bucket_handle(&hashes);
//                 let bucket = self.get_bucket_mut(&bucket_handle);
                
                
//                 // check if we need to expand the bucket or directiory
//                 unsafe {
//                     if bucket.occupancy_bitmask != 0xFFFF {
//                         // insert elements
                        
//                         let i = (!bucket.occupancy_bitmask).trailing_zeros() as usize; // flip occupancy mask to free mask
                        
//                         std::ptr::write(bucket.keys[i].as_mut_ptr(), key);
//                         std::ptr::write(bucket.values[i].as_mut_ptr(), value);

//                         bucket.fingerprints[i] = hashes.fingerprint;
//                         bucket.occupancy_bitmask |= 1 << i; // flip the bit to occupied
                    
//                         return;
                        
//                     } else {
//                         // CASE 3: No Key, But bucket hno as space, Split (and globally expand if needed) then InsertPayload
//                         let current_free = self.next_free_bucket.get();
                        

//                         if current_free >= self.bucket_capacity {
//                             panic!("Hashmap Ran out of memory in the 32 MB arena")
//                         }
//                         self.next_free_bucket.set(current_free + 1);

//                         //   let old_bucket = self.get_bucket_mut(&bucket_handle);

//                         // Expand GLobally
//                         if bucket.local_depth == self.global_depth.get() {
//                             // Expand Directory
//                             let old_len = self.directory_len_mask.get() + 1;

//                             // Double the directory, replicating the hanldes
//                             std::ptr::copy_nonoverlapping(self.directory_ptr, self.directory_ptr.add(old_len), old_len);
                        
//                             let current_depth = self.global_depth.get();
//                             self.global_depth.set(current_depth + 1);

//                             self.directory_len_mask.set((old_len * 2) - 1);
                        
//                         }


//                         let new_bucket_ptr = self.payload_ptr.add(current_free);
//                         Payload::init_at(new_bucket_ptr, bucket.local_depth + 1);

//                         let new_bucket = &mut *new_bucket_ptr;

//                         bucket.local_depth += 1;
//                         let split_bit = 1 << (bucket.local_depth - 1);
//                         let mut occupied = bucket.occupancy_bitmask;
//                         let mut new_bucketcount = 0;

//                         while occupied != 0 {
//                             let i = occupied.trailing_zeros() as usize;

//                             let key_ref = bucket.keys[i].assume_init_ref();
//                             let h = pod_hasher(key_ref);

//                             if (h.directory_key & split_bit) != 0 {
//                                 let dest_idx = new_bucketcount;


//                                 std::ptr::copy_nonoverlapping(bucket.keys[i].as_ptr(), new_bucket.keys[dest_idx].as_mut_ptr(), 1);
//                                 std::ptr::copy_nonoverlapping(bucket.values[i].as_ptr(), new_bucket.values[dest_idx].as_mut_ptr(), 1);
                                
//                                 //let target_idx = new_bucket.occupancy_bitmask.count_ones() as usize;
//                                 new_bucket.fingerprints[dest_idx] = bucket.fingerprints[i];
//                                 new_bucket.occupancy_bitmask |= 1 << dest_idx; // Set new occupancy bit
//                                 new_bucketcount += 1;

//                                 // Clear from old bucket
//                                 bucket.occupancy_bitmask &= !(1 << i);

//                             }

//                             occupied &= !(1 << i); // lets loop continue
//                         }

//                         let step = 1 << bucket.local_depth;
//                         let mask = self.directory_len_mask.get();
//                         let mut dir_idx = (hashes.directory_key as usize & (step - 1)) | split_bit as usize;
                        
//                         while dir_idx <= mask {
//                             *self.directory_ptr.add(dir_idx) = current_free as u32;
//                             dir_idx += step;
//                         }
//                         continue;
//                     }
//                 }
//             }
//         }


//         // Get
//         pub fn get(&self, key: &K) -> Option<&V> {
//             let hashes = pod_hasher(key);
//             let bucket_handle = self.get_bucket_handle(&hashes);
//             let bucket = self.get_bucket(&bucket_handle);

//             unsafe {
//                 _mm_prefetch(bucket.keys.as_ptr() as *const i8, _MM_HINT_T0);

//                 let fingerprints = _mm_load_si128(bucket.fingerprints.as_ptr() as *const __m128i);
//                 let target = _mm_set1_epi8(hashes.fingerprint as i8);
//                 let m = _mm_cmpeq_epi8(fingerprints, target);
//                 let bitmask = _mm_movemask_epi8(m) as u16;

//                 let mut filtered_bitmask = bitmask & bucket.occupancy_bitmask;

//                 while filtered_bitmask != 0 {
//                     let i = filtered_bitmask.trailing_zeros() as usize;

//                     if bucket.keys[i].assume_init_ref() == key {
//                         return Some(bucket.values[i].assume_init_ref());
//                     }
//                     filtered_bitmask &= filtered_bitmask - 1;
//                 }
//             }
//             None
//         }


//         // Remove
//         pub fn remove(&self, key: &K) -> Option<V> {
//             let hashes = pod_hasher(key);
//             let bucket_handle = self.get_bucket_handle(&hashes);
//             let bucket = self.get_bucket_mut(&bucket_handle);

//             unsafe {
//                 _mm_prefetch(bucket.keys.as_ptr() as *const i8, _MM_HINT_T0);

//                 let fingerprints = _mm_load_si128(bucket.fingerprints.as_ptr() as *const __m128i);
//                 let target = _mm_set1_epi8(hashes.fingerprint as i8);
//                 let m = _mm_cmpeq_epi8(fingerprints, target);
//                 let bitmask = _mm_movemask_epi8(m) as u16;

//                 let mut filtered_bitmask = bitmask & bucket.occupancy_bitmask;

//                 while filtered_bitmask != 0 {
//                     let i = filtered_bitmask.trailing_zeros() as usize;

//                     if bucket.keys[i].assume_init_ref() == key {
//                         std::ptr::drop_in_place(bucket.keys[i].as_mut_ptr());
//                         let v = bucket.values[i].assume_init_read(); 
//                         bucket.occupancy_bitmask &= !(1 << i);
//                         return Some(v);
//                     }
//                     filtered_bitmask &= filtered_bitmask - 1;
//                 }
//             }
//             None
//         }

//         // Helpers 
//         #[inline(always)]
//         fn get_bucket_handle(&self, h: &Hashes) -> u32 {
//             unsafe {
//                 let idx = (h.directory_key as usize) & self.directory_len_mask.get(); // get the global depth bits
//                 *self.directory_ptr.add(idx)
//             }
//         }

//         #[inline(always)]
//         fn get_bucket_ptr(&self) -> *mut Payload<K, V> {
//             self.payload_ptr as *mut Payload<K, V>
//         }

//         #[inline(always)]
//         fn get_bucket_mut(&self, idx: &u32) -> &mut Payload<K, V> { // returning a mut ref from a non mut self
//             unsafe { &mut *self.get_bucket_ptr().add(*idx as usize) }
//         }

//         #[inline(always)]
//         fn get_bucket(&self, idx: &u32) -> &Payload<K, V> {
//             unsafe { &*self.get_bucket_ptr().add(*idx as usize) }
//         }

//         pub fn stats(&self) {
//             let global_depth = self.global_depth.get();
//             let directory_size = self.directory_len_mask.get() + 1;
//             let allocated_buckets = self.next_free_bucket.get();
            
//             let mut total_items = 0;
//             let mut max_local_depth = 0;
//             let mut min_local_depth = u32::MAX;
            
//             // We only iterate through the unique buckets in the payload arena
//             for i in 0..allocated_buckets {
//                 let bucket = self.get_bucket(&(i as u32));
//                 total_items += bucket.occupancy_bitmask.count_ones();
//                 max_local_depth = max_local_depth.max(bucket.local_depth);
//                 min_local_depth = min_local_depth.min(bucket.local_depth);
//             }

//             let fill_factor = (total_items as f64 / (allocated_buckets as f64 * 16.0)) * 100.0;
//             let bytes_per_item = (self._mmap_payload.len() + self._mmap_directory.len()) as f64 / total_items as f64;

//             println!("--- HashMap Stats ---");
//             println!("Items:            {}", total_items);
//             println!("Buckets Used:     {}/{}", allocated_buckets, self.bucket_capacity);
//             println!("Directory Size:   {} (Depth: {})", directory_size, global_depth);
//             println!("Local Depth:      Min: {}, Max: {}", min_local_depth, max_local_depth);
//             println!("Fill Factor:      {:.2}%", fill_factor);
//             println!("Memory Efficiency: {:.2} bytes/item", bytes_per_item);
//             println!("---------------------");
//         }
//     }

// }


