use std::{mem::MaybeUninit, sync::atomic::AtomicU32};
use std::sync::atomic::Ordering;


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