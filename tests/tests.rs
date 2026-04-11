


#[cfg(test)]
mod tests {

    //use super::*;
    use std::sync::Arc;
    use std::thread;


    #[test]
    fn sync_test_perfect_path() {
        let s = std::time::Instant::now();
        let m = associative_array::HashMap::new(32, 1).unwrap();

        //let mut m = std::collections::HashMap::new();

        //let n: i32 = 50_000_000;
        let n: i32 = 50_000_000;

        for i in 0..n {

           unsafe { m.insert(i, i * i); }
           //m.insert(i, i*i);
        }

        for i in 0..n {
            //let r = m.get(&i).unwrap().eq(&(i * i));
            assert!(m.get(&i).unwrap().eq(&(i * i)));
        }

        // for i in 0..n {
        //     m.remove(&i);
        // }


        // for i in 0..n {
        //     assert_eq!(m.get(&i), None);
        // }

        let end = s.elapsed();
        
        //m.stats();
        println!("Line: {:?}", end);
       m.stats();

    }

    //  #[test]
    // fn sync_test_collider_path() {
    //     let s = std::time::Instant::now();
    //     let m = associative_array::AssociativeArray::new().unwrap();


    //     //let n: i32 = 50_000_000;
    //     let n: i32 = 50_000_000;

    //     for i in 0..n {
    //             unsafe { m.insert(i, i); }
    //         assert!(m.get(&i).unwrap().eq(&(i)));
    //     }

    //     let end = s.elapsed();
        
    //     //m.stats();
    //     println!("Line: {:?}", end);

    //     for i in 0..n {
    //         m.upsert(i, i+2);
    //         assert!(m.get(&i).unwrap().eq(&(i+2)));
    //     }
    //     let end = s.elapsed();
        
    //     //m.stats();
    //     println!("Line: {:?}", end);



    //     // for i in 0..n {
    //     //     //let r = m.get(&i).unwrap().eq(&(i * i));
    //     //     assert!(m.get(&i).unwrap().eq(&(i * i)));
    //     // }

    //     // for i in 0..n {
    //     //     m.remove(&i);
    //     // }


    //     // for i in 0..n {
    //     //     assert_eq!(m.get(&i), None);
    //     // }

    //     // let end = s.elapsed();
        
    //     // //m.stats();
    //     // println!("Line: {:?}", end);
    //     m.stats();

    // }


    // #[test]
    // fn parallel_test() {
    //     let s = std::time::Instant::now();
    //     let m = Arc::new(associative_array::HashMap::new());

    //     let mut handles = vec![];
    //     let num_threads = 10;
    //     let ops_per_thread = 1_000_000;

    //     for i in 0..num_threads {
    //         let map = Arc::clone(&m);

    //         handles.push(thread::spawn(move || {
    //             for j in 0..ops_per_thread {
    //                 let key = format!("thread_{}_key_{}", i, j);
    //                 map.upsert(key, j);
    //             }
    //         }));
    //     }

    //     for h in handles {
    //         h.join().unwrap();
    //     }

    //     for i in 0..num_threads {
    //         for j in 0..ops_per_thread {
    //             let key = format!("thread_{}_key_{}", i, j);
    //             match m.get(&key) {
    //                 Some(val) => {
    //                     //assert_eq!(val, j as i32, "Value mismatch for key {}", key);
    //                     assert_eq!(*val, j as i32, "Value mismatch for key {}", key);
    //                 }
    //                 None => panic!("Key {} not found in map!", key),
    //             }
    //         }
    //     }


    //     let end = s.elapsed();
        
    //    // m.stats();
    //     println!("Line: {:?}", end);
    // }

    // #[test]
    // fn high_parallel_test() {
    //     let s = std::time::Instant::now();
    //     let m = Arc::new(associative_array::HashMap::new());

    //     let mut handles = vec![];
    //     let num_threads = 20;
    //     let ops_per_thread = 1_000_000;

    //     for i in 0..num_threads {
    //         let map = Arc::clone(&m);

    //         handles.push(thread::spawn(move || {
    //             for j in 0..ops_per_thread {
    //                 let key = format!("thread_{}_key_{}", i, j);
    //                 map.upsert(key, j);
    //             }
    //         }));
    //     }

    //     for h in handles {
    //         h.join().unwrap();
    //     }

    //     for i in 0..num_threads {
    //         for j in 0..ops_per_thread {
    //             let key = format!("thread_{}_key_{}", i, j);
    //             match m.get(&key) {
    //                 Some(val) => {
    //                     assert_eq!(*val, j as i32, "Value mismatch for key {}", key);
    //                 }
    //                 None => panic!("Key {} not found in map!", key),
    //             }
    //         }
    //     }


    //     let end = s.elapsed();
        
    //     //m.stats();
    //     println!("Line: {:?}", end);
    // }


    // #[test]
    // fn triple_threat_benchmark() {
    //     use std::collections::HashMap as StdMap;
    //     use papaya::HashMap as PapMap;
    //     use std::sync::RwLock;

    //     let num_threads = 20;
    //     let ops_per_thread = 10_000_000; // 10M total ops per map

    //     // --- ROUND 1: Sharded SIMD Map ---
    //     {
    //         let m = Arc::new(associative_array::HashMap::new());
    //         let s = std::time::Instant::now();
    //         let mut handles = vec![];
    //         for _ in 0..num_threads {
    //             let map = Arc::clone(&m);
    //             handles.push(thread::spawn(move || {
    //                 for j in 0..ops_per_thread {
    //                     map.upsert(j, j);
    //                 }
    //             }));
    //         }
    //         for h in handles { h.join().unwrap(); }
    //         println!("Your Map:   {:?}", s.elapsed());
    //     }

    //     // --- ROUND 2: Papaya HashMap ---
    //     {
    //         let m = Arc::new(PapMap::with_capacity(10_000_000));
    //         let s = std::time::Instant::now();
    //         let mut handles = vec![];
    //         for _ in 0..num_threads {
    //             let map = Arc::clone(&m);
    //             handles.push(thread::spawn(move || {
    //                 let m = map.pin();
    //                 for j in 0..ops_per_thread {
                        
    //                     m.insert(j, j);
    //                 }
    //             }));
    //         }
    //         for h in handles { h.join().unwrap(); }
    //         println!("Papaya:    {:?}", s.elapsed());
    //     }

    //     // --- ROUND 3: Custom DashMap ---
    //     {
    //         // This IS DashMap's architecture. 
    //         let shards: Arc<Vec<RwLock<StdMap<i32, i32>>>> = Arc::new(
    //             (0..256).map(|_| RwLock::new(StdMap::with_capacity(40_000))).collect()
    //         );
    //         let s = std::time::Instant::now();
    //         let mut handles = vec![];
    //         for _ in 0..num_threads {
    //             let shards = Arc::clone(&shards);
    //             handles.push(std::thread::spawn(move || {
    //                 for j in 0..ops_per_thread {
    //                     let shard_idx = (j as usize) % 256;
    //                     shards[shard_idx].write().unwrap().insert(j, j);
    //                 }
    //             }));
    //         }
    //         for h in handles { h.join().unwrap(); }
    //         println!("DashMap Clone:   {:?}", s.elapsed());

    //     }

    //     //--- ROUND 4: Std Map + Single RwLock (The "Naive" approach) ---
    //     {
    //         let m = Arc::new(RwLock::new(StdMap::with_capacity(10_000_000)));
    //         let s = std::time::Instant::now();
    //         let mut handles = vec![];
    //         for _ in 0..num_threads {
    //             let map = Arc::clone(&m);
    //             handles.push(thread::spawn(move || {
    //                 for j in 0..ops_per_thread {
    //                     map.write().unwrap().insert(j, j); 
    //                 }
    //             }));
    //         }
    //         for h in handles { h.join().unwrap(); }
    //         println!("Std+RwLock: {:?}", s.elapsed());
    //     }
    // }


    // #[test]
    // fn steady_state_showdown() {
    //     use std::collections::HashMap as StdMap;
    //     use parking_lot::RwLock;
    //     use std::sync::Arc;

    //     let num_threads = 20;
    //     let total_keys = 10_000_000;
    //     let ops_per_thread = 20_000_000; // 40M total requests

    //     // --- SETUP: Pre-loading ---
    //     let my_map = Arc::new(associative_array::map::HashMap::new().unwrap());
    //     let dash_clone = Arc::new((0..256).map(|_| RwLock::new(StdMap::with_capacity(total_keys / 256))).collect::<Vec<_>>());

    //     for i in 0..total_keys {
    //         my_map.upsert(i, i);
    //         let shard = (i as usize) % 256;
    //         dash_clone[shard].write().insert(i, i);
    //     }

    //     println!("Warm-up complete. Starting Steady State Race...");

    //     // --- ROUND 1: Map (SIMD Lookups) ---
    //     let s1 = std::time::Instant::now();
    //     let mut handles = vec![];
    //     for t in 0..num_threads {
    //         let m = Arc::clone(&my_map);
    //         handles.push(std::thread::spawn(move || {
    //             for j in 0..ops_per_thread {
    //                 // Mix of reads and updates to existing keys
    //                 m.upsert(j as usize, j as usize); 
    //             }
    //         }));
    //     }
    //     for h in handles { h.join().unwrap(); }
    //     let your_time = s1.elapsed();

    //     // --- ROUND 2: DashMap Clone (Std Lookups) ---
    //     let s2 = std::time::Instant::now();
    //     let mut handles = vec![];
    //     for t in 0..num_threads {
    //         let dc = Arc::clone(&dash_clone);
    //         handles.push(std::thread::spawn(move || {
    //             for j in 0..ops_per_thread {
    //                 let shard = (j as usize) % 256;
    //                 dc[shard].write().insert(j as usize, j as usize);
    //             }
    //         }));
    //     }
    //     for h in handles { h.join().unwrap(); }
    //     let dash_time = s2.elapsed();

    //     println!("Map (Steady): {:?}", your_time);
    //     println!("DashMap  (Steady): {:?}", dash_time);
    // }

}