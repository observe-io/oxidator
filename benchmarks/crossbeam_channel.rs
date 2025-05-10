use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::{sync::mpsc, thread, time::Duration};
use crossbeam_channel;

const NUM_PRODUCERS: usize = 4;
const NUM_CONSUMERS: usize = 4;
const NUM_MESSAGES_PER_PRODUCER: usize = 1000;
const TOTAL_MESSAGES: usize = NUM_PRODUCERS * NUM_MESSAGES_PER_PRODUCER;

// --- std::sync::mpsc benchmark ---
// Note: std::sync::mpsc is not ideal for MPMC. This implementation uses Arc<Mutex<Receiver>>.
fn bench_std_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_mpmc");
    group.throughput(Throughput::Elements(TOTAL_MESSAGES as u64));

    group.bench_function(BenchmarkId::new("std_sync_mpsc", TOTAL_MESSAGES), |b| {
        b.iter(|| {
            let (tx, rx) = mpsc::channel();
            let mut producer_handles = Vec::with_capacity(NUM_PRODUCERS);
            let mut consumer_handles = Vec::with_capacity(NUM_CONSUMERS);

            // Spawn producers
            for _ in 0..NUM_PRODUCERS {
                let tx_clone = tx.clone();
                producer_handles.push(thread::spawn(move || {
                    for i in 0..NUM_MESSAGES_PER_PRODUCER {
                        tx_clone.send(black_box(i)).unwrap();
                    }
                }));
            }
            // Drop the original sender to signal no more messages once producers are done
            drop(tx);

            // Spawn consumers (using Arc<Mutex<Receiver>>)
            let rx_shared = std::sync::Arc::new(std::sync::Mutex::new(rx));
            for _ in 0..NUM_CONSUMERS {
                 let rx_clone = std::sync::Arc::clone(&rx_shared);
                consumer_handles.push(thread::spawn(move || {
                    let mut count = 0;
                    loop {
                        // Lock the mutex to access the receiver
                        let lock = rx_clone.lock().unwrap();
                        match lock.recv() {
                            Ok(msg) => {
                                black_box(msg);
                                count += 1;
                            }
                            Err(mpsc::RecvError) => break, // Channel closed and empty
                        }
                        // Release lock quickly
                        drop(lock);
                        // Yield to potentially allow other consumers to acquire the lock
                        thread::yield_now();
                    }
                    count
                }));
            }

            // Wait for producers to finish
            for handle in producer_handles {
                handle.join().unwrap();
            }

            // Wait for consumers to finish and collect counts
            let mut total_received = 0;
            for handle in consumer_handles {
                total_received += handle.join().unwrap();
            }
            assert_eq!(total_received, TOTAL_MESSAGES);
        });
    });
    group.finish();
}

// --- crossbeam-channel benchmark ---
fn bench_crossbeam(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_mpmc"); // Use the same group name
    group.throughput(Throughput::Elements(TOTAL_MESSAGES as u64));

    group.bench_function(BenchmarkId::new("crossbeam_channel", TOTAL_MESSAGES), |b| {
         b.iter(|| {
            let (s, r) = crossbeam_channel::unbounded(); // Or bounded(capacity)
            let mut producer_handles = Vec::with_capacity(NUM_PRODUCERS);
            let mut consumer_handles = Vec::with_capacity(NUM_CONSUMERS);

            // Spawn producers
            for _ in 0..NUM_PRODUCERS {
                let sender = s.clone();
                producer_handles.push(thread::spawn(move || {
                    for i in 0..NUM_MESSAGES_PER_PRODUCER {
                        sender.send(black_box(i)).unwrap();
                    }
                    // Sender is dropped when thread exits
                }));
            }
            // Drop the original sender so receivers know when to stop
            drop(s);

            // Spawn consumers
            for _ in 0..NUM_CONSUMERS {
                let receiver = r.clone();
                consumer_handles.push(thread::spawn(move || {
                    let mut count = 0;
                    // recv() blocks until a message or error
                    while let Ok(msg) = receiver.recv() {
                         black_box(msg);
                         count += 1;
                    }
                    // Channel is empty and all senders are dropped
                    count
                }));
            }
            // Drop the original receiver (optional, as consumers hold clones)
            drop(r);


            // Wait for producers to finish (optional but good practice)
            for handle in producer_handles {
                handle.join().unwrap();
            }

            // Wait for consumers to finish and collect counts
            let mut total_received = 0;
            for handle in consumer_handles {
                total_received += handle.join().unwrap();
            }
             assert_eq!(total_received, TOTAL_MESSAGES);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_std_mpsc, bench_crossbeam);
criterion_main!(benches);
