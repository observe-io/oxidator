use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::{
    sync::{
        mpsc,
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};
use crossbeam_channel;

// Oxidator imports
use oxidator::client::DisruptorClient;
use oxidator::traits::{EventProducer, Orchestrator, Sequence, Task, WorkerHandle};
use oxidator::RingBuffer;

const NUM_PRODUCERS: usize = 4;
const NUM_CONSUMERS: usize = 4;
const NUM_MESSAGES_PER_PRODUCER: usize = 1000;
const TOTAL_MESSAGES: usize = NUM_PRODUCERS * NUM_MESSAGES_PER_PRODUCER;

// Define Event and Task for Oxidator benchmark
#[derive(Default, Debug, Clone, Copy)]
struct Event(usize);

#[derive(Clone)]
struct OxidatorBenchmarkTask {
    processed_count: Arc<AtomicUsize>,
}

impl Task<Event> for OxidatorBenchmarkTask {
    fn execute_task(&self, event: &Event, _sequence: Sequence, _end_of_batch: bool) {
        black_box(*event);
        self.processed_count.fetch_add(1, Ordering::Relaxed);
    }

    fn clone_box(&self) -> Box<dyn Task<Event> + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

#[derive(Default, Clone, Copy)]
struct OxidatorDummyTask;

impl Task<Event> for OxidatorDummyTask {
    fn execute_task(&self, _event: &Event, _sequence: Sequence, _end_of_batch: bool) {
        // Do nothing
    }

    fn clone_box(&self) -> Box<dyn Task<Event> + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

const OXIDATOR_BUFFER_SIZE: usize = 16384; // A common power-of-2 buffer size

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

// --- Oxidator Disruptor benchmark ---
fn bench_oxidator_disruptor(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_mpmc");
    group.throughput(Throughput::Elements(TOTAL_MESSAGES as u64));

    group.bench_function(BenchmarkId::new("oxidator_disruptor", TOTAL_MESSAGES), |b| {
        b.iter(|| {
            let client = DisruptorClient;
            let data_storage_layer =
                client.init_data_storage::<Event, RingBuffer<Event>>(OXIDATOR_BUFFER_SIZE);
            let wait_strategy_layer = data_storage_layer.with_yielding_wait_strategy();
            let sequencer_layer = wait_strategy_layer.with_multi_producer();

            let (producers, mut consumer_factory) =
                sequencer_layer.build::<OxidatorDummyTask>(NUM_PRODUCERS);

            let processed_count = Arc::new(AtomicUsize::new(0));

            for _ in 0..NUM_CONSUMERS {
                let task_instance = Box::new(OxidatorBenchmarkTask {
                    processed_count: processed_count.clone(),
                });
                consumer_factory.add_task(task_instance, vec![]);
            }

            let mut consumers_handle = consumer_factory.init_consumers().start();

            let mut producer_handles = Vec::with_capacity(NUM_PRODUCERS);
            for producer_id in 0..NUM_PRODUCERS {
                let producer = producers[producer_id].clone();
                producer_handles.push(thread::spawn(move || {
                    for i in 0..NUM_MESSAGES_PER_PRODUCER {
                        producer.write(vec![Event(black_box(i))], |slot, _seq, event_ref| {
                            *slot = *event_ref;
                        });
                    }
                    producer.drain();
                }));
            }

            for handle in producer_handles {
                handle.join().unwrap();
            }

            consumers_handle.join();

            assert_eq!(
                processed_count.load(Ordering::Relaxed),
                TOTAL_MESSAGES,
                "Mismatch in processed messages count"
            );
        });
    });
    group.finish();
}

criterion_group!(benches, bench_std_mpsc, bench_crossbeam, bench_oxidator_disruptor);
criterion_main!(benches);
