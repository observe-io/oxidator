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

const NUM_PRODUCERS: usize = 1;
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

const OXIDATOR_BUFFER_SIZE: usize = 16384;

// --- std::sync::mpsc benchmark (Single Producer, Multi Consumer) ---
fn bench_std_mpsc_spmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("spmc");
    group.throughput(Throughput::Elements(TOTAL_MESSAGES as u64));

    group.bench_function(BenchmarkId::new("std_sync_mpsc_spmc", TOTAL_MESSAGES), |b| {
        b.iter(|| {
            let (tx, rx) = mpsc::channel();
            let mut consumer_handles = Vec::with_capacity(NUM_CONSUMERS);

            let producer_handle = thread::spawn(move || {
                for i in 0..TOTAL_MESSAGES {
                    tx.send(black_box(i)).unwrap();
                }
            });

            let rx_shared = std::sync::Arc::new(std::sync::Mutex::new(rx));
            for _ in 0..NUM_CONSUMERS {
                 let rx_clone = std::sync::Arc::clone(&rx_shared);
                consumer_handles.push(thread::spawn(move || {
                    let mut count = 0;
                    loop {
                        let lock = rx_clone.lock().unwrap();
                        match lock.recv() {
                            Ok(msg) => {
                                black_box(msg);
                                count += 1;
                            }
                            Err(mpsc::RecvError) => break,
                        }
                        drop(lock);
                        thread::yield_now();
                    }
                    count
                }));
            }

            producer_handle.join().unwrap();

            let mut total_received = 0;
            for handle in consumer_handles {
                total_received += handle.join().unwrap();
            }
            assert_eq!(total_received, TOTAL_MESSAGES);
        });
    });
    group.finish();
}

// --- crossbeam-channel benchmark (Single Producer, Multi Consumer) ---
fn bench_crossbeam_spmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("spmc");
    group.throughput(Throughput::Elements(TOTAL_MESSAGES as u64));

    group.bench_function(BenchmarkId::new("crossbeam_channel_spmc", TOTAL_MESSAGES), |b| {
         b.iter(|| {
            let (s, r) = crossbeam_channel::unbounded();
            let mut consumer_handles = Vec::with_capacity(NUM_CONSUMERS);

            let producer_handle = thread::spawn(move || {
                for i in 0..TOTAL_MESSAGES {
                    s.send(black_box(i)).unwrap();
                }
            });

            for _ in 0..NUM_CONSUMERS {
                let receiver = r.clone();
                consumer_handles.push(thread::spawn(move || {
                    let mut count = 0;
                    while let Ok(msg) = receiver.recv() {
                         black_box(msg);
                         count += 1;
                    }
                    count
                }));
            }
            drop(r);
            producer_handle.join().unwrap();

            let mut total_received = 0;
            for handle in consumer_handles {
                total_received += handle.join().unwrap();
            }
             assert_eq!(total_received, TOTAL_MESSAGES);
        });
    });
    group.finish();
}

// --- Oxidator Disruptor benchmark (Single Producer, Multi Consumer) ---
fn bench_oxidator_disruptor_spmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("spmc");
    group.throughput(Throughput::Elements(TOTAL_MESSAGES as u64));

    group.bench_function(BenchmarkId::new("oxidator_disruptor_spmc", TOTAL_MESSAGES), |b| {
        b.iter(|| {
            let (producers, mut factory) = DisruptorClient
                .init_data_storage::<Event, RingBuffer<Event>>(OXIDATOR_BUFFER_SIZE)
                .with_yielding_wait_strategy()
                .with_single_producer()
                .build::<OxidatorDummyTask>(NUM_PRODUCERS);

            let processed_count = Arc::new(AtomicUsize::new(0));

            let dummy_task = Box::new(OxidatorDummyTask::default());
            let dummy_idx = factory.add_task(dummy_task, vec![]);

            let benchmark_task = Box::new(OxidatorBenchmarkTask {
                processed_count: processed_count.clone(),
            });
            factory.add_task(benchmark_task, vec![dummy_idx]);

            let mut consumer_handles = factory.init_consumers().start();

            let producer = producers[0].clone();
            let producer_join_handle = thread::spawn(move || {
                for i in 0..TOTAL_MESSAGES {
                    producer.write(vec![Event(black_box(i))], |slot, _seq, event_ref| {
                        *slot = *event_ref;
                    });
                }
                producer
            });

            let producer_after_join = producer_join_handle.join().unwrap();
            producer_after_join.drain();

            consumer_handles.join();

            assert_eq!(
                processed_count.load(Ordering::Relaxed),
                TOTAL_MESSAGES,
                "Mismatch in processed messages count"
            );
        });
    });
    group.finish();
}

criterion_group!(benches, bench_std_mpsc_spmc, bench_crossbeam_spmc, bench_oxidator_disruptor_spmc);
criterion_main!(benches);
