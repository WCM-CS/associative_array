use std::sync::atomic::{
    AtomicBool, 
    AtomicU32, 
    Ordering
};

use crossbeam_epoch::{
    Atomic, 
    Guard, 
    Shared
};




#[repr(C, align(16))]
pub struct RouterShard {
    // mmapped arena of the shards directory handles 
    pub data_ptr: Atomic<u32>, // 8 bytes  , ebr over the directoiry handles to account for expansioins and splits

    pub shard_depth: AtomicU32, // 4 bytes, 12
    pub shard_mask: AtomicU32 // 4 bytes, 16
}

impl RouterShard {


    #[inline(always)]
    pub fn get_routing_snapshot<'a>(&self, guard: &'a Guard) -> (Shared<'a, u32>, u32, u32) {
        // Load depth and mask in a single atomic operation

        // Load the pointer to the directiory handles using atomci epoch based reclamation and a guard
        let dir_ptr = self.data_ptr.load(Ordering::Acquire, guard);


        let raw_depth_mask = self.shard_mask.load(Ordering::Acquire);

        let depth = raw_depth_mask >> 24;
        let mask = raw_depth_mask & 0x00FF_FFFF;

        
        (dir_ptr, mask, depth)
    }

    /// Pure function: uses provided snapshot to find bucket index
    #[inline(always)]
    pub unsafe fn get_bucket_idx(&self, dir_shared: Shared<'_, u32>, dir_key: u32, mask: u32) -> u32 {
        let dir_idx = dir_key & mask;
        let dir_ptr = dir_shared.as_raw();
        unsafe { *dir_ptr.add(dir_idx as usize)}
    }


}

#[repr(C, align(16))]
pub struct AllocatorShard {
    pub bucket_base_idx: u32, // 4 bytes
    pub next_offset: AtomicU32, // 4 bytes, 8
    pub max_offset: u32, // 4 bytes, 12
    pub is_active_expanding: AtomicBool
}


