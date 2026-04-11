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


        let directory_mmap = MmapMut::map_anon(directory_arena_size)?;
        let mut bucket_mmap = MmapMut::map_anon(bucket_arena_size)?;


        let directory_ptr = directory_mmap.as_ptr() as *mut u32;
        let bucket_ptr = bucket_mmap.as_mut_ptr() as *mut Bucket<K, V>;


        let total_handles = directory_arena_size / std::mem::size_of::<u32>();
        let handles_per_shard = total_handles / 1024;


        let total_buckets = bucket_arena_size / std::mem::size_of::<Bucket<K, V>>();
        let buckets_per_shard = total_buckets / 1024;

        // let mut routing: [RouterShard; 1024] = unsafe { std::mem::zeroed() };
        // let mut allocation: [AllocatorShard; 1024] = unsafe { std::mem::zeroed() };


        let mut routing: [std::mem::MaybeUninit<RouterShard>; 1024] = unsafe {
            std::mem::MaybeUninit::uninit().assume_init()
        };
        let mut allocation: [std::mem::MaybeUninit<AllocatorShard>; 1024] = unsafe {
            std::mem::MaybeUninit::uninit().assume_init()
        };


        for i in 0..1024 {
            let shard_dir_start = unsafe { directory_ptr.add(i * handles_per_shard) };

            let first_bucket_idx = (i * buckets_per_shard) as u32;

            unsafe {
                Bucket::init_at(bucket_ptr.add(first_bucket_idx as usize), 0);
                // Write the handle for index 0 of the directory to point to this first bucket
                *shard_dir_start = first_bucket_idx;
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
                next_offset: AtomicU32::new(1), // already used 0
                max_offset: buckets_per_shard as u32,
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


        let directory_routing: [RouterShard; 1024] = unsafe { std::mem::transmute(routing) };
        let directory_allocation: [AllocatorShard; 1024] = unsafe { std::mem::transmute(allocation) };

        
        Ok(Self { 
            directory_routing, 
            directory_allocation, 
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

                bucket_reference.control_state.fetch_sub(1, Ordering::Release);

                return None;
            }
        }
    }


    pub fn upsert(&self, key: K, value: V) -> UpsertStatus<V> {

        // Hash the key, index into radix directory shard, aquire the proper directory for the key
        let guard = &crossbeam_epoch::pin();
        // pin for the EBR over the atomic pointer to the dir handles

        let h = pod_hasher(&key);
        let directory = &self.directory_routing[h.shard_idx as usize & 1023];





        loop {

            let (dir_ptr, mask, _depth)  = directory.get_routing_snapshot(guard);
            let bucket_idx = unsafe { directory.get_bucket_idx(dir_ptr, h.directory_key, mask)};












        }




        UpsertStatus::Inserted
    }










}

enum UpsertStatus<V> {
    Inserted,
    Updated(V)
}