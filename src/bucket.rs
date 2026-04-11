



use std::{
    mem::MaybeUninit, 
    sync::atomic::{
        AtomicU32, Ordering
    }
};

#[repr(C, align(64))]
pub struct Bucket<K, V> {
    pub fingerprints_a: [u8; 16], // 16 Bytes 

    //Bitmask repalced control bit in fingerprint, allows 2x entrophy
    pub occupancy_bitmask: u16, // 2 Bytes, 18 Bytes 


    pub control_state: AtomicU32, // 22
    // The MSB determined if the bucket is being rehashed, the other 31 bits determine the ref count

    pub local_depth: u32, // 4 Bytes, 26 Bytes 
    _padding: [u8; 6], // 10 Bytes, 32 Bytes


    // -------- HALF CACHE LINE --------
    pub fingerprints_b: [u16; 16], // 32 Bytes, 64 Bytes


    // -------- FULL CACHE LINE --------
    // swap to raw pointers for miri memory leak debug testing purposes 
    pub keys: [MaybeUninit<K>; 16],
    pub values: [MaybeUninit<V>; 16],
}



impl<K, V> Bucket<K, V> {

    pub const LOCK_BIT: u32 = 1 << 31;



    pub unsafe fn init_at(ptr: *mut Self, local_depth: u32) {
        let b = unsafe { &mut *ptr };
        
        b.occupancy_bitmask = 0;
        b.fingerprints_a = [0u8; 16];
        b.control_state.store(0, Ordering::Relaxed);
        b.local_depth = local_depth;
        b.fingerprints_b = [0u16; 16];
    }

    #[inline]
    pub fn lock(&self) {
        // Spin until LOCK_BIT is 0, then try to set it
        while self.control_state.fetch_or(Self::LOCK_BIT, Ordering::Acquire) & Self::LOCK_BIT != 0 {
            std::hint::spin_loop();
        }
    }

    #[inline]
    pub fn unlock(&self) {
        self.control_state.fetch_and(!Self::LOCK_BIT, Ordering::Release);
    }

    // Readers check if locked without acquiring
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.control_state.load(Ordering::Acquire) & Self::LOCK_BIT != 0
    }
    
}


pub struct BucketRef<'a, K, V> {
    pub bucket: &'a Bucket<K, V>,
    pub slot_idx: usize,
}

impl<'a, K, V> std::ops::Deref for BucketRef<'a, K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        unsafe { self.bucket.values[self.slot_idx].assume_init_ref() }
    }
}

impl<'a, K, V> Drop for BucketRef<'a, K, V> {
    fn drop(&mut self) {
        // Automatically release the pin so writers can eventually proceed
        self.bucket.control_state.fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
}
