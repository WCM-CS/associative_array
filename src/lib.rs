

pub use crate::hash_map::AssociativeArray;

mod hash_map {
    use std::{
        arch::x86_64::{
            __m128i, 
            _MM_HINT_T0, 
            _mm_cmpeq_epi8, 
            _mm_load_si128, 
            _mm_movemask_epi8, 
            _mm_prefetch, 
            _mm_set1_epi8
        },
        hash::Hash,
        cell::Cell, 
        marker::PhantomData
    };
    use libc::{
        c_void, 
        MADV_HUGEPAGE, 
        MADV_SEQUENTIAL, 
        MADV_WILLNEED, 
        madvise
    };
    use anyhow::Result;
    use memmap2::MmapMut;

    use crate::{
        hash::{
            Hashes, pod_hasher
        }, 
        payload::Payload
    };

    pub struct AssociativeArray<K, V> {
        // ------ HOT DATA --
        directory_ptr: *mut u32, // 8 Bytes, ROUTER, 

        payload_ptr: *mut Payload<K, V>, // 8 Bytes, 16 Bytes, DESTINATION

        directory_len_mask: Cell<usize>, // 8 Bytes, 24 Bytes
        global_depth: Cell<u32>, // 4 Bytes, 28 Bytes
        _pad: u32, // 4 bytes, 32 bytes

            // -------- HALF CACHE LINE --------

        // ---- WARM DATA ----
        next_free_bucket: Cell<usize>, // 8 Bytes, 40 Bytes 
        bucket_capacity: usize, // 8 Bytes, 48 Bytes

        _padding: [u8; 16],

            // -------- CACHE LINE --------

        // -- COLD DATA ----
        _mmap_directory: MmapMut,
        _mmap_payload: MmapMut, // MMAPPED ARENA FOR BUCKETS 32MB

        _marker: PhantomData<(K, V)>

    }


    impl<K, V> AssociativeArray<K, V> 
    where 
        K: PartialEq + Hash 
    {

        pub fn new() -> Result<Self> {
            // Need to pass in the mb the user needs since this can fail due to lack of memory on system and linux kernel not liking mass Mmapping of non-existent memory
            let directory_arena_size = 1024 * 1024 * 1024;
            let payload_arena_size = 32 * 1024 * 1024 * 1024;
            

            let payload_capacity = payload_arena_size / std::mem::size_of::<Payload<K, V>>();


            let directory_mmap = MmapMut::map_anon(directory_arena_size)?;
            let mut payload_mmap = MmapMut::map_anon(payload_arena_size)?;

            let directory_ptr = directory_mmap.as_ptr() as *mut u32;
            let payload_ptr = payload_mmap.as_mut_ptr() as *mut Payload<K, V>;

            // MMAP Optimizations 
            unsafe {
                madvise(payload_ptr as *mut c_void, payload_arena_size, MADV_HUGEPAGE);
                madvise(payload_ptr as *mut c_void, payload_arena_size, MADV_WILLNEED);

                // Sequential for directory because expansion doubles it linearly
                madvise(directory_ptr as *mut c_void, directory_arena_size, MADV_SEQUENTIAL);

                // // Physical page pre-faulting - this is really fast but actually eats the memory ofo the full mmapping, aka touches all virtual allcoation making it physcial 

                // let p_ptr = payload_mmap.as_mut_ptr();
                // for i in (0..payload_arena_size).step_by(4096) {
                //     std::ptr::write_volatile(p_ptr.add(i), 0);
                // }
            }


            // Initialize MetaData
            unsafe {
                
                // Directory Index 0 points to Bucket 0
                std::ptr::write_volatile(directory_ptr, 0);
                Payload::init_at(payload_ptr, 0);
            }


            

            Ok(Self {
                directory_ptr,
                payload_ptr,
                directory_len_mask: Cell::new(0), 
                global_depth: Cell::new(0),
                _pad: 0,
                next_free_bucket: Cell::new(1), // Bucket 0 is taken
                bucket_capacity: payload_capacity,
                _padding: [0; 16],
                _mmap_directory: directory_mmap,
                _mmap_payload: payload_mmap,
                _marker: PhantomData,
            })

        }


        // Upsert
        pub fn upsert(&self, key: K, value: V) {
            let hashes: Hashes = pod_hasher(&key);

            let target = unsafe { _mm_set1_epi8(hashes.fingerprint as i8) };

            loop {
                
                let bucket_handle = self.get_bucket_handle(&hashes);
                let bucket = self.get_bucket_mut(&bucket_handle);

                // CASE 1: Key exists, uppdate the value
                unsafe {
                    // Prefetch Keys for comaprasion operation after simd
                    _mm_prefetch(bucket.keys.as_ptr() as *const i8, _MM_HINT_T0);

                    // // Load the 16 fingerprints into the SSE 128 bit register 
                    let fingerprints = _mm_load_si128(bucket.fingerprints.as_ptr() as *const __m128i);

                    // // Broadcasts the fingerprint across all the u8 in the SSE
                    // let target = _mm_set1_epi8(hashes.fingerprint as i8);

                    // Compare the target to thefingerprints
                    let m = _mm_cmpeq_epi8(fingerprints, target);

                    // Bridge the results, for a u8 hit flips the bit mask to 1
                    let bitmask = _mm_movemask_epi8(m) as u16; 

                    // filter out trash results, ensure fingerpritn is active not removed or invalid
                    let mut filtered_bitmask = bitmask & bucket.occupancy_bitmask;

                    // Iterate oiver hits
                    while filtered_bitmask != 0 {
                        let i = filtered_bitmask.trailing_zeros() as usize; // returns when abit == 1

                        if *bucket.keys[i].assume_init_ref() == key { // Hit on the key
                            // Update Value
                            bucket.values[i].assume_init_drop();
                            std::ptr::write(bucket.values[i].as_mut_ptr(), value);
                            return;
                        } 

                        filtered_bitmask &= filtered_bitmask - 1;
                    }



                    // CASE 2: No Key, But bucket has space, InsertPayload
                    if bucket.occupancy_bitmask != 0xFFFF { // if ocupancy is not full, this is a common path for a insert 
                        // insert elements
                        let i = (!bucket.occupancy_bitmask).trailing_zeros() as usize; // flip occupancy mask to free mask
                        
                        std::ptr::write(bucket.keys[i].as_mut_ptr(), key);
                        std::ptr::write(bucket.values[i].as_mut_ptr(), value);

                        bucket.fingerprints[i] = hashes.fingerprint;
                        bucket.occupancy_bitmask |= 1 << i; // flip the bit to occupied
                    
                        return;
                    } else {

                        // CASE 3: No Key, But bucket hno as space, Split (and globally expand if needed) then InsertPayload
                        let current_free = self.next_free_bucket.get();
                        

                        if current_free >= self.bucket_capacity {
                            panic!("Hashmap Ran out of memory in the 32 MB arena")
                        }
                        self.next_free_bucket.set(current_free + 1);

                    //   let old_bucket = self.get_bucket_mut(&bucket_handle);

                        // Expand GLobally
                        if bucket.local_depth == self.global_depth.get() {
                            // Expand Directory
                            let old_len = self.directory_len_mask.get() + 1;

                            std::ptr::copy_nonoverlapping(self.directory_ptr, self.directory_ptr.add(old_len), old_len);
                        
                            let current_depth = self.global_depth.get();
                            self.global_depth.set(current_depth + 1);

                            self.directory_len_mask.set((old_len * 2) - 1);
                        
                        }


                        let new_bucket_ptr = self.payload_ptr.add(current_free);
                        Payload::init_at(new_bucket_ptr, bucket.local_depth + 1);

                        let new_bucket = &mut *new_bucket_ptr;

                        bucket.local_depth += 1;
                        let split_bit = 1 << (bucket.local_depth - 1);
                        let mut occupied = bucket.occupancy_bitmask;
                        let mut new_bucketcount = 0;

                        while occupied != 0 {
                            let i = occupied.trailing_zeros() as usize;

                            let key_ref = bucket.keys[i].assume_init_ref();
                            let h = pod_hasher(key_ref);

                            if (h.directory_key & split_bit) != 0 {
                                let dest_idx = new_bucketcount;


                                std::ptr::copy_nonoverlapping(bucket.keys[i].as_ptr(), new_bucket.keys[dest_idx].as_mut_ptr(), 1);
                                std::ptr::copy_nonoverlapping(bucket.values[i].as_ptr(), new_bucket.values[dest_idx].as_mut_ptr(), 1);
                                
                                //let target_idx = new_bucket.occupancy_bitmask.count_ones() as usize;
                                new_bucket.fingerprints[dest_idx] = bucket.fingerprints[i];
                                new_bucket.occupancy_bitmask |= 1 << dest_idx; // Set new occupancy bit
                                new_bucketcount += 1;

                                // Clear from old bucket
                                bucket.occupancy_bitmask &= !(1 << i);

                            }

                            occupied &= !(1 << i); // lets loop continue
                        }

                        let step = 1 << bucket.local_depth;
                        let mask = self.directory_len_mask.get();
                        let mut dir_idx = (hashes.directory_key as usize & (step - 1)) | split_bit as usize;
                        
                        while dir_idx <= mask {
                            *self.directory_ptr.add(dir_idx) = current_free as u32;
                            dir_idx += step;
                        }
                    }
                }
            }
        }

        // Insert, unsafe no simd or key check, does not ovewrite data, therefore will duplicate, only use for bulk loads or guearenteed unique keys inserts, otherwise use upsert

        pub unsafe fn insert(&self, key: K, value: V) {
            let hashes = pod_hasher(&key);

            loop {
                // let new_slot = self.next_free_bucket.get_mut();

                let bucket_handle = self.get_bucket_handle(&hashes);
                let bucket = self.get_bucket_mut(&bucket_handle);
                
                
                // check if we need to expand the bucket or directiory
                unsafe {
                    if bucket.occupancy_bitmask != 0xFFFF {
                        // insert elements
                        
                        let i = (!bucket.occupancy_bitmask).trailing_zeros() as usize; // flip occupancy mask to free mask
                        
                        std::ptr::write(bucket.keys[i].as_mut_ptr(), key);
                        std::ptr::write(bucket.values[i].as_mut_ptr(), value);

                        bucket.fingerprints[i] = hashes.fingerprint;
                        bucket.occupancy_bitmask |= 1 << i; // flip the bit to occupied
                    
                        return;
                        
                    } else {
                        // CASE 3: No Key, But bucket hno as space, Split (and globally expand if needed) then InsertPayload
                        let current_free = self.next_free_bucket.get();
                        

                        if current_free >= self.bucket_capacity {
                            panic!("Hashmap Ran out of memory in the 32 MB arena")
                        }
                        self.next_free_bucket.set(current_free + 1);

                        //   let old_bucket = self.get_bucket_mut(&bucket_handle);

                        // Expand GLobally
                        if bucket.local_depth == self.global_depth.get() {
                            // Expand Directory
                            let old_len = self.directory_len_mask.get() + 1;

                            std::ptr::copy_nonoverlapping(self.directory_ptr, self.directory_ptr.add(old_len), old_len);
                        
                            let current_depth = self.global_depth.get();
                            self.global_depth.set(current_depth + 1);

                            self.directory_len_mask.set((old_len * 2) - 1);
                        
                        }


                        let new_bucket_ptr = self.payload_ptr.add(current_free);
                        Payload::init_at(new_bucket_ptr, bucket.local_depth + 1);

                        let new_bucket = &mut *new_bucket_ptr;

                        bucket.local_depth += 1;
                        let split_bit = 1 << (bucket.local_depth - 1);
                        let mut occupied = bucket.occupancy_bitmask;
                        let mut new_bucketcount = 0;

                        while occupied != 0 {
                            let i = occupied.trailing_zeros() as usize;

                            let key_ref = bucket.keys[i].assume_init_ref();
                            let h = pod_hasher(key_ref);

                            if (h.directory_key & split_bit) != 0 {
                                let dest_idx = new_bucketcount;


                                std::ptr::copy_nonoverlapping(bucket.keys[i].as_ptr(), new_bucket.keys[dest_idx].as_mut_ptr(), 1);
                                std::ptr::copy_nonoverlapping(bucket.values[i].as_ptr(), new_bucket.values[dest_idx].as_mut_ptr(), 1);
                                
                                //let target_idx = new_bucket.occupancy_bitmask.count_ones() as usize;
                                new_bucket.fingerprints[dest_idx] = bucket.fingerprints[i];
                                new_bucket.occupancy_bitmask |= 1 << dest_idx; // Set new occupancy bit
                                new_bucketcount += 1;

                                // Clear from old bucket
                                bucket.occupancy_bitmask &= !(1 << i);

                            }

                            occupied &= !(1 << i); // lets loop continue
                        }

                        let step = 1 << bucket.local_depth;
                        let mask = self.directory_len_mask.get();
                        let mut dir_idx = (hashes.directory_key as usize & (step - 1)) | split_bit as usize;
                        
                        while dir_idx <= mask {
                            *self.directory_ptr.add(dir_idx) = current_free as u32;
                            dir_idx += step;
                        }
                        continue;
                    }
                }
            }
        }


        // Get
        pub fn get(&self, key: &K) -> Option<&V> {
            let hashes = pod_hasher(key);
            let bucket_handle = self.get_bucket_handle(&hashes);
            let bucket = self.get_bucket(&bucket_handle);

            unsafe {
                _mm_prefetch(bucket.keys.as_ptr() as *const i8, _MM_HINT_T0);

                let fingerprints = _mm_load_si128(bucket.fingerprints.as_ptr() as *const __m128i);
                let target = _mm_set1_epi8(hashes.fingerprint as i8);
                let m = _mm_cmpeq_epi8(fingerprints, target);
                let bitmask = _mm_movemask_epi8(m) as u16;

                let mut filtered_bitmask = bitmask & bucket.occupancy_bitmask;

                while filtered_bitmask != 0 {
                    let i = filtered_bitmask.trailing_zeros() as usize;

                    if bucket.keys[i].assume_init_ref() == key {
                        return Some(bucket.values[i].assume_init_ref());
                    }
                    filtered_bitmask &= filtered_bitmask - 1;
                }
            }
            None
        }


        // Remove
        pub fn remove(&self, key: &K) -> Option<V> {
            let hashes = pod_hasher(key);
            let bucket_handle = self.get_bucket_handle(&hashes);
            let bucket = self.get_bucket_mut(&bucket_handle);

            unsafe {
                _mm_prefetch(bucket.keys.as_ptr() as *const i8, _MM_HINT_T0);

                let fingerprints = _mm_load_si128(bucket.fingerprints.as_ptr() as *const __m128i);
                let target = _mm_set1_epi8(hashes.fingerprint as i8);
                let m = _mm_cmpeq_epi8(fingerprints, target);
                let bitmask = _mm_movemask_epi8(m) as u16;

                let mut filtered_bitmask = bitmask & bucket.occupancy_bitmask;

                while filtered_bitmask != 0 {
                    let i = filtered_bitmask.trailing_zeros() as usize;

                    if bucket.keys[i].assume_init_ref() == key {
                        std::ptr::drop_in_place(bucket.keys[i].as_mut_ptr());
                        let v = bucket.values[i].assume_init_read(); 
                        bucket.occupancy_bitmask &= !(1 << i);
                        return Some(v);
                    }
                    filtered_bitmask &= filtered_bitmask - 1;
                }
            }
            None
        }

        // Helpers 
        #[inline(always)]
        fn get_bucket_handle(&self, h: &Hashes) -> u32 {
            unsafe {
                let idx = (h.directory_key as usize) & self.directory_len_mask.get(); // get the global depth bits
                *self.directory_ptr.add(idx)
            }
        }

        #[inline(always)]
        fn get_bucket_ptr(&self) -> *mut Payload<K, V> {
            self.payload_ptr as *mut Payload<K, V>
        }

        #[inline(always)]
        fn get_bucket_mut(&self, idx: &u32) -> &mut Payload<K, V> { // returning a mut ref from a non mut self
            unsafe { &mut *self.get_bucket_ptr().add(*idx as usize) }
        }

        #[inline(always)]
        fn get_bucket(&self, idx: &u32) -> &Payload<K, V> {
            unsafe { &*self.get_bucket_ptr().add(*idx as usize) }
        }

        pub fn stats(&self) {
            let global_depth = self.global_depth.get();
            let directory_size = self.directory_len_mask.get() + 1;
            let allocated_buckets = self.next_free_bucket.get();
            
            let mut total_items = 0;
            let mut max_local_depth = 0;
            let mut min_local_depth = u32::MAX;
            
            // We only iterate through the unique buckets in the payload arena
            for i in 0..allocated_buckets {
                let bucket = self.get_bucket(&(i as u32));
                total_items += bucket.occupancy_bitmask.count_ones();
                max_local_depth = max_local_depth.max(bucket.local_depth);
                min_local_depth = min_local_depth.min(bucket.local_depth);
            }

            let fill_factor = (total_items as f64 / (allocated_buckets as f64 * 16.0)) * 100.0;
            let bytes_per_item = (self._mmap_payload.len() + self._mmap_directory.len()) as f64 / total_items as f64;

            println!("--- HashMap Stats ---");
            println!("Items:            {}", total_items);
            println!("Buckets Used:     {}/{}", allocated_buckets, self.bucket_capacity);
            println!("Directory Size:   {} (Depth: {})", directory_size, global_depth);
            println!("Local Depth:      Min: {}, Max: {}", min_local_depth, max_local_depth);
            println!("Fill Factor:      {:.2}%", fill_factor);
            println!("Memory Efficiency: {:.2} bytes/item", bytes_per_item);
            println!("---------------------");
        }
    }

}



mod payload {
    use std::{
        mem::MaybeUninit, 
        sync::atomic::{
            AtomicU32, Ordering
        }
    };

    #[repr(C, align(64))]
    pub struct Payload<K, V> {
        pub fingerprints: [u8; 16], // 16 Bytes 

        //Bitmask repalced control bit in fingerprint, allows 2x entrophy
        pub occupancy_bitmask: u16, // 2 Bytes, 18 Bytes 


        pub control_state: AtomicU32, // 22
        // The MSB determined if the bucket is being rehashed, the other 31 bits determine the ref count

        pub local_depth: u32, // 4 Bytes, 26 Bytes 
        _padding: [u8; 6], // 10 Bytes, 32 Bytes

        // -------- HALF CACHE LINE --------

        pub keys: [MaybeUninit<K>; 16],
        pub values: [MaybeUninit<V>; 16],
    }



    impl<K, V> Payload<K, V> {
        pub unsafe fn init_at(ptr: *mut Self, local_depth: u32) {
            let b = unsafe { &mut *ptr };
            
            b.occupancy_bitmask = 0;
            b.fingerprints = [0u8; 16];
            b.control_state.store(0, Ordering::Relaxed);
            b.local_depth = local_depth;
        }
    }

}



mod hash {
    use xxhash_rust::xxh3::Xxh3;
    use std::hash::Hash;

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
}


