


#[cfg(test)]
mod tests {

    //use super::*;
    use std::sync::Arc;
    use std::thread;


    #[test]
    fn sync_test() {
        let s = std::time::Instant::now();
        let m = associative_array::HashMap::new();


        let n: i32 = 50_000_000;

        for i in 0..n {

        m.upsert(i, i * i);
        }

        for i in 0..n {
            assert_eq!(m.get(&i).unwrap(), i * i);
        }

        for i in 0..n {
        m.remove(&i);
        }


        for i in 0..n {
            assert_eq!(m.get(&i), None);
        }

        let end = s.elapsed();
        
        m.stats();
        println!("Line: {:?}", end);

    }


    #[test]
    fn parallel_test() {
        let s = std::time::Instant::now();
        let m = Arc::new(associative_array::HashMap::new());

        let mut handles = vec![];
        let num_threads = 10;
        let ops_per_thread = 1_000_000;

        for i in 0..num_threads {
            let map = Arc::clone(&m);

            handles.push(thread::spawn(move || {
                for j in 0..ops_per_thread {
                    let key = format!("thread_{}_key_{}", i, j);
                    map.upsert(key, j);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        for i in 0..num_threads {
            for j in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", i, j);
                match m.get(&key) {
                    Some(val) => {
                        assert_eq!(val, j as i32, "Value mismatch for key {}", key);
                    }
                    None => panic!("Key {} not found in map!", key),
                }
            }
        }


        let end = s.elapsed();
        
        m.stats();
        println!("Line: {:?}", end);
    }

    #[test]
    fn high_parallel_test() {
        let s = std::time::Instant::now();
        let m = Arc::new(associative_array::HashMap::new());

        let mut handles = vec![];
        let num_threads = 20;
        let ops_per_thread = 1_000_000;

        for i in 0..num_threads {
            let map = Arc::clone(&m);

            handles.push(thread::spawn(move || {
                for j in 0..ops_per_thread {
                    let key = format!("thread_{}_key_{}", i, j);
                    map.upsert(key, j);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        for i in 0..num_threads {
            for j in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", i, j);
                match m.get(&key) {
                    Some(val) => {
                        assert_eq!(val, j as i32, "Value mismatch for key {}", key);
                    }
                    None => panic!("Key {} not found in map!", key),
                }
            }
        }


        let end = s.elapsed();
        
        m.stats();
        println!("Line: {:?}", end);
    }

}