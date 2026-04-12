use std::{arch::x86_64::{__m128i, _MM_HINT_T0, _mm_cmpeq_epi8, _mm_load_si128, _mm_movemask_epi8, _mm_prefetch, _mm_set1_epi8}, marker::PhantomData, sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, Ordering}};
use std::hash::Hash;
use crossbeam_epoch::{Atomic, Shared};
use libc::{
    MADV_HUGEPAGE, MADV_SEQUENTIAL, c_void, madvise
};
use memmap2::MmapMut;

use crate::{
    bucket::{Bucket, BucketRef}, 
    directory::{
        AllocatorShard, RouterShard
    }, hash::pod_hasher
};

 
 
 use anyhow::Result;
 
 



 
 
 
pub struct HashMap<K, V> {
    // --- Inlined radix sharding SOA, handles point to shard which points to a mmapped arena  --- 
    directory_routing: [RouterShard;  1 << 10], //  shard_size (16 Bytes) * 1024
    directory_allocation: [AllocatorShard; 1 << 10],  //  shard_size (16 Bytes) * 1024

    // ----- BUCKETS ----- 
    bucket_ptr: *mut Bucket<K, V>, 

    // memory allocation and data type handles
    _mmap_directory: MmapMut,
    _mmap_bucket: MmapMut, 

    _marker: PhantomData<(K, V)>
}





impl<K, V> HashMap<K, V> 
where
    K: Hash + PartialEq
{
        


        pub fn new(system_memory_gb: usize, directory_gb: usize) -> Result<Self> {
            // Need to pass in the mb the user needs since this can fail due to lack of memory on system and linux kernel not liking mass Mmapping of non-existent memory
            let directory_arena_size = directory_gb * 1024 * 1024 * 1024;
            let bucket_arena_size = (system_memory_gb - directory_gb) * 1024 * 1024 * 1024;
            // NOTE ENSURE THAT EACH RADIX DIRECTORY SHARD GETS AT LEAST 64MB VIRTUAL SPACE ALLOCATED TO IT to account for alllocator leapfrog EBR implementation

            let directory_mmap = MmapMut::map_anon(directory_arena_size)?;

            let mut bucket_mmap = MmapMut::map_anon(bucket_arena_size)?;


            let directory_ptr = directory_mmap.as_ptr() as *mut u32;
            let bucket_ptr = bucket_mmap.as_mut_ptr() as *mut Bucket<K, V>;


            let total_dir_slots = directory_arena_size / std::mem::size_of::<u32>();
            let slots_per_shard = total_dir_slots / 1024;



            let total_buckets = bucket_arena_size / std::mem::size_of::<Bucket<K, V>>();
            let buckets_per_shard = total_buckets / 1024;


            let mut routing: [std::mem::MaybeUninit<RouterShard>; 1024] = unsafe {
                std::mem::MaybeUninit::uninit().assume_init()
            };
            let mut allocation: [std::mem::MaybeUninit<AllocatorShard>; 1024] = unsafe {
                std::mem::MaybeUninit::uninit().assume_init()
            };


            for i in 0..1024 {
                let shard_dir_start = unsafe { directory_ptr.add(i * slots_per_shard) };
                let first_bucket_idx = (i * buckets_per_shard) as u32;

                unsafe {
                    Bucket::init_at(bucket_ptr.add(first_bucket_idx as usize), 0);
                    // Write the handle for index 0 of the directory to point to this first bucket
                    for j in  0..slots_per_shard {
                        *shard_dir_start.add(j) = first_bucket_idx;
                    }
                   // *shard_dir_start = first_bucket_idx;
                }

                let data_ptr = Atomic::null();
                data_ptr.store(Shared::from(shard_dir_start as *const u32), Ordering::Relaxed);
                
                


                routing[i].write( RouterShard {
                    data_ptr,
                    shard_depth: AtomicU32::new(0),
                    shard_mask: AtomicU32::new(0),                    
                });

                allocation[i].write(AllocatorShard {
                    bucket_base_idx: first_bucket_idx,
                    next_bucket_offset: AtomicU32::new(1), // Index 0 is now used
                    max_buckets: buckets_per_shard as u32,

                    // Directory metadata
                    runway_base_ptr: shard_dir_start,
                    next_dir_offsets: AtomicU32::new(1), 
                    max_dir_slots: slots_per_shard as u32,

                    is_active_expanding: AtomicBool::new(false)
                });

            }


            // MMAP Optimizations 
            unsafe {
                madvise(bucket_ptr as *mut c_void, bucket_arena_size, MADV_HUGEPAGE);
                //madvise(payload_ptr as *mut c_void, payload_arena_size, MADV_WILLNEED);

                // Sequential for directory because expansion doubles it linearly
                madvise(directory_ptr as *mut c_void, directory_arena_size, MADV_SEQUENTIAL);

            }


            Ok(Self { 
                directory_routing: unsafe { std::mem::transmute(routing) }, 
                directory_allocation: unsafe { std::mem::transmute(allocation) }, 
                bucket_ptr, 
                _mmap_directory: directory_mmap, 
                _mmap_bucket: bucket_mmap, 
                _marker: PhantomData, 
            })
        }

    pub fn get(&self, key: &K) -> Option<BucketRef<'_, K, V>> {
        // Hash the key, index into radix directory shard, aquire the proper directory for the key
        let guard = &crossbeam_epoch::pin();
        // pin for the EBR over the atomic pointer to the dir handles

        let h = pod_hasher(key);
        let directory = &self.directory_routing[h.shard_idx as usize & 1023];

        loop {
            // Using the directorys router shard ( the HOT PATH )

            let (dir_ptr, mask, _depth)  = directory.get_routing_snapshot(guard);
            let bucket_idx = unsafe { directory.get_bucket_idx(dir_ptr, h.directory_key, mask)};

            unsafe {
                // use handle to increment raw bucket poionter the cast bucket to a reference
                let bucket_ptr = self.bucket_ptr.add(bucket_idx as usize);
                let bucket_reference = &*bucket_ptr;

                // SPIN LOCK IF WRITER IS BLOCKING
                // this could fail if you have liek 2 billion concurrent readers per shard cause the ref counter can trigger the lock bit clause
                let state = bucket_reference.control_state.load(std::sync::atomic::Ordering::Acquire);
                if state >= Bucket::<K, V>::LOCK_BIT {
                    std::hint::spin_loop();
                    continue; // writer clocked, you get fucked, retry loop
                }



                // Incrememtn the atomic reference counter for the bucket
                if bucket_reference.control_state.fetch_add(1, Ordering::Acquire) >= Bucket::<K, V>::LOCK_BIT {
                    bucket_reference.control_state.fetch_sub(1, Ordering::Release);
                    std::hint::spin_loop();
                    continue;
                }



  
                // prefetch bucket
                _mm_prefetch(bucket_ptr as *const i8, _MM_HINT_T0);

                // load fingerprints
                let fingerprints = _mm_load_si128(bucket_reference.fingerprints_a.as_ptr() as *const __m128i);


                let target = _mm_set1_epi8(h.fingerprint_alpha as i8);

                let m = _mm_cmpeq_epi8(fingerprints, target);
                let bitmask = _mm_movemask_epi8(m) as u16;



                let mut filtered_mask = bitmask & bucket_reference.occupancy_bitmask;

                while filtered_mask != 0 {
                    let curr_idx = filtered_mask.trailing_zeros() as usize;

                    // first check the secondary hash aka the inlined key prefix before hopping to mmap for full key comparasin to avoid cache misses
                    if bucket_reference.fingerprints_b[curr_idx] == h.fingerprint_bravo {
                        if bucket_reference.keys[curr_idx].assume_init_ref() == key {
                            // return the value as a ref or fn mut ref or some shit
                            return Some(BucketRef {
                                bucket: bucket_reference,
                                slot_idx: curr_idx,
                            });
                        }
                    }

                    filtered_mask &= filtered_mask - 1;
                }

                bucket_reference.control_state.fetch_sub(1, Ordering::Release);

                return None;
            };
        }
    }



    pub fn update<F, R>(&self, key: &K, f: F) -> Option<R> 
    where
        F: FnOnce(&mut V) -> R
    {
        // Hash the key, index into radix directory shard, aquire the proper directory for the key
        let guard = &crossbeam_epoch::pin();
        // pin for the EBR over the atomic pointer to the dir handles

        let h = pod_hasher(key);
        let directory = &self.directory_routing[h.shard_idx as usize & 1023];



        loop {


            let (dir_ptr, mask, _depth)  = directory.get_routing_snapshot(guard);
            let bucket_idx = unsafe { directory.get_bucket_idx(dir_ptr, h.directory_key, mask)};



            unsafe {

                let bucket_ptr = self.bucket_ptr.add(bucket_idx as usize);
                let bucket_reference = &mut *bucket_ptr;

                // fetch or to set the bit, if its already then spin
                if bucket_reference.control_state.fetch_or(Bucket::<K, V>::LOCK_BIT, Ordering::Acquire) & Bucket::<K, V>::LOCK_BIT != 0 {
                    std::hint::spin_loop();
                    continue; 
                }

                // Wait for readers to drain
                while bucket_reference.control_state.load(Ordering::Acquire) != Bucket::<K, V>::LOCK_BIT {
                    std::hint::spin_loop();
                }


                 // prefetch bucket
                _mm_prefetch(bucket_ptr as *const i8, _MM_HINT_T0);

                // load fingerprints
                let fingerprints = _mm_load_si128(bucket_reference.fingerprints_a.as_ptr() as *const __m128i);


                let target = _mm_set1_epi8(h.fingerprint_alpha as i8);

                let m = _mm_cmpeq_epi8(fingerprints, target);
                let bitmask = _mm_movemask_epi8(m) as u16;



                let mut filtered_mask = bitmask & bucket_reference.occupancy_bitmask;


                while filtered_mask != 0 {
                    let curr_idx = filtered_mask.trailing_zeros() as usize;

                    // first check the secondary hash aka the inlined key prefix before hopping to mmap for full key comparasin to avoid cache misses
                    if bucket_reference.fingerprints_b[curr_idx] == h.fingerprint_bravo {
                        if bucket_reference.keys[curr_idx].assume_init_ref() == key {
                            // return the value as a ref or fn mut ref or some shit
                            let val_mut = bucket_reference.values[curr_idx].assume_init_mut();
                            let result = f(val_mut);
                            
                            // UNLOCK
                            bucket_reference.control_state.store(0, Ordering::Release);
                            return Some(result);
                        }
                    }

                    filtered_mask &= filtered_mask - 1;
                }

                bucket_reference.control_state.store(0, Ordering::Release);

                return None;
            }
        }
    }


    pub fn upsert(&self, key: K, value: V) -> UpsertStatus<V> {

        // Hash the key, index into radix directory shard, aquire the proper directory for the key
        let guard = &crossbeam_epoch::pin();
        // pin for the EBR over the atomic pointer to the dir handles

        let h = pod_hasher(&key);
        let shard_idx = h.shard_idx as usize & 1023;
        let directory = &self.directory_routing[shard_idx];





        loop {

            let (dir_ptr, mask, _depth)  = directory.get_routing_snapshot(guard);
            let bucket_idx = unsafe { directory.get_bucket_idx(dir_ptr, h.directory_key, mask)};



            unsafe {
                let bucket_ptr = self.bucket_ptr.add(bucket_idx as usize);
                let bucket_reference = &mut *bucket_ptr;



                // fetch or to set the bit, if its already then spin
                if bucket_reference.control_state.fetch_or(Bucket::<K, V>::LOCK_BIT, Ordering::Acquire) & Bucket::<K, V>::LOCK_BIT != 0 {
                    std::hint::spin_loop();
                    continue; 
                }

                // Wait for readers to drain
                while bucket_reference.control_state.load(Ordering::Acquire) != Bucket::<K, V>::LOCK_BIT {
                    std::hint::spin_loop();
                }


                // load fingerprints
                let fingerprints = _mm_load_si128(bucket_reference.fingerprints_a.as_ptr() as *const __m128i);


                let target = _mm_set1_epi8(h.fingerprint_alpha as i8);

                let m = _mm_cmpeq_epi8(fingerprints, target);
                let bitmask = _mm_movemask_epi8(m) as u16;



                let mut filtered_mask = bitmask & bucket_reference.occupancy_bitmask;



                while filtered_mask != 0 {
                    let curr_idx = filtered_mask.trailing_zeros() as usize;

                    // first check the secondary hash aka the inlined key prefix before hopping to mmap for full key comparasin to avoid cache misses
                    if bucket_reference.fingerprints_b[curr_idx] == h.fingerprint_bravo {
                        if bucket_reference.keys[curr_idx].assume_init_ref() == &key {
                            // replace the value

                            // CASE A: REPLACE THE VALUE, THE KEY EXISTS
                            let v_ptr = bucket_reference.values[curr_idx].as_mut_ptr();
                            let old_value = std::mem::replace(&mut *v_ptr, value);

                        
                            bucket_reference.control_state.store(0, Ordering::Release);
                            return UpsertStatus::Updated(old_value)
                        }
                    }

                    filtered_mask &= filtered_mask - 1;
                }

            

                

                // Key does not exist so insert it

                if bucket_reference.occupancy_bitmask != 0xFFFF {
                    // CASE B: THE KEY DOES NOT EXISTS BUT THERE IS SPACE IN THE CURRENT BUCKET SO USE IT AND ADD THE K-V PAIR
                    // There is a open addres handle so grab it
                    let i = (!bucket_reference.occupancy_bitmask).trailing_zeros() as usize;

                    std::ptr::write(bucket_reference.keys[i].as_mut_ptr(), key);
                    std::ptr::write(bucket_reference.values[i].as_mut_ptr(), value);

                    bucket_reference.fingerprints_a[i] = h.fingerprint_alpha;
                    bucket_reference.fingerprints_b[i] = h.fingerprint_bravo;
                    bucket_reference.occupancy_bitmask |= 1 << i; // flip the bit to occupied

                    // free the lock bit
                    bucket_reference.control_state.store(0, Ordering::Release);

                    return UpsertStatus::Inserted
                } else {
                    // CASE C: THE KEY DOES NOT EXISTS ADN THERE IS NO SPACE IN THE BUCKEY SO WE MUST SPLIT
                    // THE MAS ALSO TRIGGER A DIRECTORY SHARD EXPANSIOJN TO SUPPOPRT THE BUCKET SPLIT IF LOCAL DEPTH IS ALREADY EQUAL TO GLOBAL DEPTH
                    let allocation_shard = &self.directory_allocation[shard_idx];

                    // CHECK IF WE NEE TO COMMIT A GLOBAL SHARD EXPANSION TO SUPPORT THE BUCKET SPLIT 



                    // For any update either  abucket split or and expansiopn we need to lock the allocator shard for the process
                    if allocation_shard.is_active_expanding.compare_exchange(
                        false, true, Ordering::Acquire, Ordering::Relaxed
                    ).is_err() {
                        // someone else is splitting the bucket so backoff
                        bucket_reference.control_state.store(0, Ordering::Release);
                        std::hint::spin_loop();
                        continue;
                    }


                    let (old_dir_ptr, old_mask, global_depth) = directory.get_routing_snapshot(guard);

                    // GLOBAL EXPANSTION OF DIRECTORY
                    if bucket_reference.local_depth == global_depth {
                        let old_len = old_mask + 1;
                        let new_len = old_len * 2;


                        let runway_offset = allocation_shard.next_dir_offsets.fetch_add(new_len, Ordering::Relaxed);
                        if runway_offset + new_len > allocation_shard.max_dir_slots {
                            allocation_shard.is_active_expanding.store(false, Ordering::Release);
                            panic!("Directory runway exhausted for shard {}", shard_idx);
                        }



                        let new_dir_raw = allocation_shard.runway_base_ptr.add(runway_offset as usize);
                        std::ptr::copy_nonoverlapping(old_dir_ptr.as_raw(), new_dir_raw, old_len as usize);
                        std::ptr::copy_nonoverlapping(old_dir_ptr.as_raw(), new_dir_raw.add(old_len as usize), old_len as usize);
                    
                        let new_mask = new_len - 1;
                        let new_global_depth = global_depth + 1;

                        directory.data_ptr.store(Shared::from(new_dir_raw as *const u32), Ordering::Release);
                        directory.shard_mask.store((new_global_depth << 24) | new_mask, Ordering::Release);


                    }




                    let new_bucket_idx = allocation_shard.next_bucket_offset.fetch_add(1, Ordering::Relaxed);
                    if new_bucket_idx >= allocation_shard.max_buckets {
                        allocation_shard.is_active_expanding.store(false, Ordering::Release);
                        panic!("Shard allocator exhuasted per the mmap allocation limit")
                    }

                    let real_new_index = allocation_shard.bucket_base_idx + new_bucket_idx;
                    let new_bucket_ptr = self.bucket_ptr.add(real_new_index as usize);

                    Bucket::<K, V>::init_at(new_bucket_ptr, bucket_reference.local_depth + 1);
                    let new_bucket = &mut *new_bucket_ptr;

                    bucket_reference.local_depth += 1;


                    let split_bit = 1 << (bucket_reference.local_depth - 1);

                    let mut occupied = bucket_reference.occupancy_bitmask;
                    let mut new_bucketcount = 0;

                    while occupied != 0 {
                        let src_idx = occupied.trailing_zeros() as usize;

                        let key_ref = bucket_reference.keys[src_idx].assume_init_ref();
                        let key_hash = pod_hasher(key_ref);

                        if (key_hash.directory_key & split_bit) != 0 {
                            let dst_idx = new_bucketcount;

                            std::ptr::copy_nonoverlapping(bucket_reference.keys[src_idx].as_ptr(), new_bucket.keys[dst_idx].as_mut_ptr(), 1);
                            std::ptr::copy_nonoverlapping(bucket_reference.values[src_idx].as_ptr(), new_bucket.values[dst_idx].as_mut_ptr(), 1);
                            
                            new_bucket.fingerprints_a[dst_idx] = bucket_reference.fingerprints_a[src_idx];
                            new_bucket.fingerprints_b[dst_idx] = bucket_reference.fingerprints_b[src_idx];
                            
                            new_bucket.occupancy_bitmask |= 1 << dst_idx;
                            new_bucketcount += 1;

                            // REMOVE from old bucket bitmask
                            bucket_reference.occupancy_bitmask &= !(1 << src_idx);
                        }

                        occupied &= occupied - 1;
                    }


                    // reroute the directry
                    let (current_dir_ptr, current_mask, _) = directory.get_routing_snapshot(guard);
                    let raw_dir_mut = current_dir_ptr.as_raw() as *mut u32;


                    let stride = 1 << (bucket_reference.local_depth - 1);


                    for d_idx in 0..=current_mask {
                        let entry_ptr = raw_dir_mut.add(d_idx as usize);
                        // If this slot points to our old bucket AND the split bit is set...
                        if *entry_ptr == bucket_idx && (d_idx & split_bit) != 0 {
                            // ...reroute it to the new bucket.
                            *entry_ptr = real_new_index;
                        }
                    }
              
                
                    // --- 3. CLEANUP ---
                    // Release allocator lock
                    std::sync::atomic::fence(Ordering::Release);
                    allocation_shard.is_active_expanding.store(false, Ordering::Release);
                    
                    // Release bucket lock
                    bucket_reference.control_state.store(0, Ordering::Release);
                    
                    // RETRY: The loop starts over, h.directory_key & mask will now 
                    // resolve to the correct bucket (old or new) which has space.
                    continue;


                }
            }
        }
    }




    pub fn stats(&self) {
        let mut total_items = 0;
        let mut total_buckets_allocated = 0;
        let mut max_local_depth = 0;
        let mut min_local_depth = u32::MAX;
        let mut total_directory_slots = 0;

        // pin for safe reading of shard metadata if needed
        let _guard = &crossbeam_epoch::pin();

        println!("--- Sharded HashMap Stats ---");
        
        for i in 0..1024 {
            let allocation = &self.directory_allocation[i];
            let routing = &self.directory_routing[i];
            
            let buckets_in_shard = allocation.next_bucket_offset.load(Ordering::Relaxed);
            total_buckets_allocated += buckets_in_shard;
            
            // Current directory capacity for this shard
            let mask = routing.shard_mask.load(Ordering::Relaxed) & 0x00FF_FFFF;
            total_directory_slots += mask + 1;

            // Iterate through buckets allocated to THIS shard
            for b_offset in 0..buckets_in_shard {
                unsafe {
                    let bucket_idx = allocation.bucket_base_idx + b_offset;
                    let bucket = &*self.bucket_ptr.add(bucket_idx as usize);
                    
                    let items = bucket.occupancy_bitmask.count_ones();
                    total_items += items;
                    
                    max_local_depth = max_local_depth.max(bucket.local_depth);
                    min_local_depth = min_local_depth.min(bucket.local_depth);
                }
            }
        }

        let total_capacity = total_buckets_allocated as f64 * 16.0;
        let fill_factor = if total_capacity > 0.0 {
            (total_items as f64 / total_capacity) * 100.0
        } else {
            0.0
        };

        let mem_payload = self._mmap_bucket.len();
        let mem_dir = self._mmap_directory.len();
        let bytes_per_item = if total_items > 0 {
            (mem_payload + mem_dir) as f64 / total_items as f64
        } else {
            0.0
        };

        println!("Total Items:         {}", total_items);
        println!("Buckets Allocated:   {}", total_buckets_allocated);
        println!("Avg Items/Bucket:    {:.2}", total_items as f64 / total_buckets_allocated as f64);
        println!("Directory Slots:     {}", total_directory_slots);
        println!("Local Depth:         Min: {}, Max: {}", min_local_depth, max_local_depth);
        println!("Fill Factor:         {:.2}%", fill_factor);
        println!("Memory Efficiency:    {:.2} bytes/item", bytes_per_item);
        println!("-----------------------------");
    }





}


#[derive(PartialEq, Debug)]
pub enum UpsertStatus<V> {
    Inserted,
    Updated(V)
}