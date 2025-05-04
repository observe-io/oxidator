use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use crossbeam_channel::unbounded as crossbeam_unbounded;
use oxidator::DisruptorClient;
use oxidator::traits::{EventProducer, Orchestrator, Sequence, Task, WorkerHandle};
use oxidator::RingBuffer;
use std::sync::{mpsc, Arc, Mutex, atomic::{AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const BUFFER_SIZE: usize = 512;
const NUM_EVENTS: u64 = 10_000;
const NUM_PRODUCERS_MULTI: usize = 2;
const EVENTS_PER_PRODUCER_MULTI: u64 = NUM_EVENTS / NUM_PRODUCERS_MULTI as u64;

#[derive(Default, Debug, Clone)]
struct Event {
    value: u64,
    producer_id: usize,
}

#[derive(Clone)]
struct DummyTask;

impl Task<Event> for DummyTask {
    fn execute_task(&self, _event: &Event, _sequence: Sequence, _end_of_batch: bool) {
    }

    fn clone_box(&self) -> Box<dyn Task<Event> + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct BenchmarkTask {
    latencies: Arc<Mutex<Vec<u64>>>,
    processed_count: Arc<AtomicU64>,
}

impl BenchmarkTask {
    fn new() -> Self {
        Self {
            latencies: Arc::new(Mutex::new(Vec::with_capacity(NUM_EVENTS as usize))),
            processed_count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn get_latencies(&self) -> Vec<u64> {
        self.latencies.lock().unwrap().clone()
    }

     fn get_processed_count(&self) -> u64 {
        self.processed_count.load(Ordering::Relaxed)
    }
}

impl Task<Event> for BenchmarkTask {
    fn execute_task(&self, event: &Event, _sequence: Sequence, _end_of_batch: bool) {
        self.processed_count.fetch_add(1, Ordering::Relaxed);
    }

    fn clone_box(&self) -> Box<dyn Task<Event> + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

fn bench_std_channel_single(n: u64) {
    let (tx, rx) = mpsc::channel::<Event>();
    let tx_clone = tx.clone();

    let consumer_handle = thread::spawn(move || {
        let mut received = 0;
        let task = BenchmarkTask::new();
        while received < n {
            if let Ok(event) = rx.recv() {
                 task.execute_task(&event, 0, false);
                received += 1;
            } else {
                break;
            }
        }
        assert_eq!(task.get_processed_count(), n);
    });

    let producer_handle = thread::spawn(move || {
        for _ in 0..n {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let event = Event { value: now, producer_id: 0 };
            if tx_clone.send(event).is_err() {
                break;
            }
        }
    });

    drop(tx); 

    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
}

fn bench_std_channel_multi(n_producers: usize, events_per_producer: u64) {
    let (tx, rx) = mpsc::channel::<Event>();
    let total_events = n_producers as u64 * events_per_producer;

    let consumer_handle = thread::spawn({
        move || {
            let mut received = 0;
            let task = BenchmarkTask::new();
            while received < total_events {
                if let Ok(event) = rx.recv() {
                    task.execute_task(&event, 0, false);
                    received += 1;
                } else {
                    break;
                }
            }
             assert_eq!(task.get_processed_count(), total_events);
        }
    });

    let mut producer_handles = Vec::new();
    for id in 0..n_producers {
        let tx_clone = tx.clone();
        let handle = thread::spawn(move || {
            for _ in 0..events_per_producer {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let event = Event { value: now, producer_id: id };
                if tx_clone.send(event).is_err() {
                    break;
                }
            }
        });
        producer_handles.push(handle);
    }

    drop(tx);

    for handle in producer_handles {
        handle.join().unwrap();
    }
    consumer_handle.join().unwrap();
}

fn bench_crossbeam_channel_single(n: u64) {
    let (tx, rx) = crossbeam_unbounded::<Event>();
    let tx_clone = tx.clone();

    let consumer_handle = thread::spawn(move || {
        let mut received = 0;
        let task = BenchmarkTask::new();
        while received < n {
            if let Ok(event) = rx.recv() {
                 task.execute_task(&event, 0, false);
                received += 1;
            } else {
                break;
            }
        }
         assert_eq!(task.get_processed_count(), n);
    });

    let producer_handle = thread::spawn(move || {
        for _ in 0..n {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let event = Event { value: now, producer_id: 0 };
            if tx_clone.send(event).is_err() {
                break;
            }
        }
    });

    drop(tx); 

    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
}

fn bench_crossbeam_channel_multi(n_producers: usize, events_per_producer: u64) {
    let (tx, rx) = crossbeam_unbounded::<Event>();
    let total_events = n_producers as u64 * events_per_producer;

    let consumer_handle = thread::spawn({
        move || {
            let mut received = 0;
            let task = BenchmarkTask::new();
            while received < total_events {
                 if let Ok(event) = rx.recv() {
                    task.execute_task(&event, 0, false);
                    received += 1;
                } else {
                    break;
                }
            }
             assert_eq!(task.get_processed_count(), total_events);
        }
    });

    let mut producer_handles = Vec::new();
    for id in 0..n_producers {
        let tx_clone = tx.clone();
        let handle = thread::spawn(move || {
            for _ in 0..events_per_producer {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let event = Event { value: now, producer_id: id };
                 if tx_clone.send(event).is_err() {
                    break;
                }
            }
        });
        producer_handles.push(handle);
    }

    drop(tx);

    for handle in producer_handles {
        handle.join().unwrap();
    }
    consumer_handle.join().unwrap();
}

fn bench_oxidator_single_busy(n: u64) {
    let total_events = n;
    let client = DisruptorClient;
    let data_storage_layer = client.init_data_storage::<Event, RingBuffer<Event>>(BUFFER_SIZE);
    let wait_strategy_layer = data_storage_layer.with_yielding_wait_strategy();
    let sequencer_layer = wait_strategy_layer.with_multi_producer();
    let (producers, mut consumer_factory) = sequencer_layer.build::<DummyTask>(1);

    let dummy_task = Box::new(DummyTask);
    let benchmark_task = Box::new(BenchmarkTask::new());
    let task_clone = benchmark_task.clone();

    let dummy_idx = consumer_factory.add_task(dummy_task, vec![]);
    let benchmark_idx = consumer_factory.add_task(benchmark_task, vec![dummy_idx]);

    let consumers = consumer_factory.init_consumers();
    let mut consumer_handle = consumers.start();

    let mut producer_handles = Vec::new();
    for (id, producer) in producers.into_iter().enumerate() {
        let handle = thread::spawn(move || {
            for _ in 0..n {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let event = Event { value: now, producer_id: id };
                producer.write(vec![event], |slot, _, event_ref| {
                    *slot = event_ref.clone();
                });
            }
            producer
        });
        producer_handles.push(handle);
    }

    let mut completed_producers = Vec::new();
    for handle in producer_handles {
        completed_producers.push(handle.join().unwrap());
    }

    // Drain all producers to ensure all events are published
    for producer in completed_producers {
        producer.drain();
    }

    consumer_handle.join();

    let final_processed_count = task_clone.get_processed_count();
    assert_eq!(final_processed_count, total_events, "Assertion failed: Processed count mismatch");
}

fn bench_oxidator_multi_busy(n_producers: usize, events_per_producer: u64) {
    let total_events = (n_producers as u64) * events_per_producer;
    let client = DisruptorClient;
    let data_storage_layer = client.init_data_storage::<Event, RingBuffer<Event>>(BUFFER_SIZE);
    let wait_strategy_layer = data_storage_layer.with_yielding_wait_strategy();
    let sequencer_layer = wait_strategy_layer.with_multi_producer();
    let (producers, mut consumer_factory) = sequencer_layer.build::<DummyTask>(n_producers);

    let dummy_task = Box::new(DummyTask);
    let benchmark_task = Box::new(BenchmarkTask::new());
    let task_clone = benchmark_task.clone();

    let dummy_idx = consumer_factory.add_task(dummy_task, vec![]);
    let benchmark_idx = consumer_factory.add_task(benchmark_task, vec![dummy_idx]);

    let consumers = consumer_factory.init_consumers();
    let mut consumer_handle = consumers.start();

    let mut producer_handles = Vec::new();
    for (id, producer) in producers.into_iter().enumerate() {
        let handle = thread::spawn(move || {
            for _ in 0..events_per_producer {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let event = Event { value: now, producer_id: id };
                 producer.write(vec![event], |slot, _, event_ref| {
                    *slot = event_ref.clone();
                });
            }
            producer
        });
        producer_handles.push(handle);
    }

    let mut completed_producers = Vec::new();
    for handle in producer_handles {
        completed_producers.push(handle.join().unwrap());
    }

    // Drain all producers to ensure all events are published
    for producer in completed_producers {
        producer.drain();
    }

    consumer_handle.join();

    let final_processed_count = task_clone.get_processed_count();
    assert_eq!(final_processed_count, total_events, "Assertion failed: Processed count mismatch");
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_producer");
    group.throughput(Throughput::Elements(NUM_EVENTS));
    group.warm_up_time(Duration::from_secs(5));
    group.measurement_time(Duration::from_secs(10));
    
    group.bench_function(BenchmarkId::new("std_channel", NUM_EVENTS), |b| {
        b.iter(|| bench_std_channel_single(black_box(NUM_EVENTS)));
    });
    
    group.bench_function(BenchmarkId::new("crossbeam_channel", NUM_EVENTS), |b| {
        b.iter(|| bench_crossbeam_channel_single(black_box(NUM_EVENTS)));
    });
    
    group.bench_function(BenchmarkId::new("oxidator_yielding", NUM_EVENTS), |b| {
        b.iter(|| bench_oxidator_single_busy(black_box(NUM_EVENTS)));
    });
    
    group.finish();

    let mut multi_group = c.benchmark_group("multi_producer");
    let multi_producer_label = format!("{}p_{}e", NUM_PRODUCERS_MULTI, EVENTS_PER_PRODUCER_MULTI);
    multi_group.throughput(Throughput::Elements(NUM_EVENTS));
    multi_group.warm_up_time(Duration::from_secs(5));
    multi_group.measurement_time(Duration::from_secs(10));
    
    multi_group.bench_function(BenchmarkId::new("std_channel", &multi_producer_label), |b| {
        b.iter(|| bench_std_channel_multi(black_box(NUM_PRODUCERS_MULTI), black_box(EVENTS_PER_PRODUCER_MULTI)));
    });
    
    multi_group.bench_function(BenchmarkId::new("crossbeam_channel", &multi_producer_label), |b| {
        b.iter(|| bench_crossbeam_channel_multi(black_box(NUM_PRODUCERS_MULTI), black_box(EVENTS_PER_PRODUCER_MULTI)));
    });
    
    multi_group.bench_function(BenchmarkId::new("oxidator_yielding", &multi_producer_label), |b| {
        b.iter(|| bench_oxidator_multi_busy(black_box(NUM_PRODUCERS_MULTI), black_box(EVENTS_PER_PRODUCER_MULTI)));
    });

    multi_group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
