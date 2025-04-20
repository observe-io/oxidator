use oxidator::client::DisruptorClient;
use oxidator::traits::{Task, Sequence, WorkerHandle, EventProducer, Orchestrator};
use oxidator::RingBuffer;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Instant;
use std::env;
use std::io::{self, Write};
use crossbeam_channel::unbounded;

fn init_minimal_logging() {
    env::set_var("RUST_LOG", "error");
    if let Err(_) = env::var("RUST_LOG") {
        env::set_var("RUST_LOG", "error");
    }
}

#[derive(Default, Debug, Clone)]
struct Event {
    value: u64,
    producer_id: usize,
}

#[derive(Clone)]
struct BenchmarkTask {
    counter: Arc<Mutex<u64>>,
    latencies: Arc<Mutex<Vec<u64>>>,
}

impl BenchmarkTask {
    fn new() -> Self {
        Self {
            counter: Arc::new(Mutex::new(0)),
            latencies: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_count(&self) -> u64 {
        *self.counter.lock().unwrap()
    }

    fn get_latencies(&self) -> Vec<u64> {
        self.latencies.lock().unwrap().clone()
    }
}

impl Task<Event> for BenchmarkTask {
    fn execute_task(&self, event: &Event, _sequence: Sequence, _is_end_of_batch: bool) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let latency = now - event.value;
        let mut latencies = self.latencies.lock().unwrap();
        latencies.push(latency);
        let mut counter = self.counter.lock().unwrap();
        *counter += 1;
    }

    fn clone_box(&self) -> Box<dyn Task<Event> + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct BenchmarkConfig {
    buffer_size: usize,
    num_producers: usize,
    num_consumers: usize,
    events_per_producer: usize,
    warmup_iterations: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1024,
            num_producers: 1,
            num_consumers: 1,
            events_per_producer: 1_000_000,
            warmup_iterations: 10_000,
        }
    }
}

#[derive(Clone)]
struct BenchmarkResult {
    name: String,
    throughput: f64,
    avg_latency_ns: f64,
    p99_latency_ns: f64,
    execution_time_ms: u64,
}

fn print_results(results: &[BenchmarkResult]) {
    println!("\n\n{:=^100}", " BENCHMARK RESULTS ");
    println!("{:30} | {:20} | {:15} | {:15} | {:15}", 
        "Benchmark", "Throughput (msg/s)", "Avg Latency (ns)", "p99 Latency (ns)", "Exec Time (ms)");
    println!("{:-^100}", "");
    for result in results {
        println!("{:30} | {:20.2} | {:15.2} | {:15.2} | {:15}", 
            result.name, 
            result.throughput, 
            result.avg_latency_ns,
            result.p99_latency_ns,
            result.execution_time_ms);
    }
    println!("{:=^100}", "");
}

fn percentile(values: &mut Vec<u64>, percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_unstable();
    let idx = (percentile / 100.0 * values.len() as f64) as usize;
    values[idx.min(values.len() - 1)] as f64
}

fn benchmark_std_channel_single(config: BenchmarkConfig) -> BenchmarkResult {
    let (tx, rx) = mpsc::channel::<Event>();
    for _ in 0..config.warmup_iterations {
        let _ = tx.send(Event::default());
        let _ = rx.try_recv();
    }
    let start = Instant::now();
    let events_per_producer = config.events_per_producer;
    let consumer_handle = thread::spawn(move || {
        let mut received = 0;
        let mut consumer_latencies = Vec::with_capacity(events_per_producer);
        while received < events_per_producer {
            if let Ok(event) = rx.recv() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let latency = now - event.value;
                consumer_latencies.push(latency);
                received += 1;
            }
        }
        consumer_latencies
    });
    let events_per_producer = config.events_per_producer;
    let producer_handle = thread::spawn(move || {
        for _ in 0..events_per_producer {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let event = Event {
                value: now,
                producer_id: 0,
            };
            let _ = tx.send(event);
        }
    });
    producer_handle.join().unwrap();
    let latencies = consumer_handle.join().unwrap();
    let elapsed = start.elapsed();
    let execution_time_ms = elapsed.as_millis() as u64;
    let throughput = config.events_per_producer as f64 / elapsed.as_secs_f64();
    let mut latencies_clone = latencies.clone();
    let avg_latency = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
    let p99_latency = percentile(&mut latencies_clone, 99.0);
    BenchmarkResult {
        name: "Std Channel (Single Producer)".to_string(),
        throughput,
        avg_latency_ns: avg_latency,
        p99_latency_ns: p99_latency,
        execution_time_ms,
    }
}

fn benchmark_std_channel_multi(config: BenchmarkConfig) -> BenchmarkResult {
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();
    let (tx, rx) = mpsc::channel::<Event>();
    let barrier = Arc::new(std::sync::Barrier::new(config.num_producers + 1));
    for _ in 0..config.warmup_iterations {
        let _ = tx.send(Event::default());
        let _ = rx.try_recv();
    }
    let start = Instant::now();
    let consumer_barrier = barrier.clone();
    let total_events = config.num_producers * config.events_per_producer;
    let consumer_handle = thread::spawn(move || {
        let mut received = 0;
        let mut consumer_latencies = Vec::with_capacity(total_events);
        consumer_barrier.wait();
        while received < total_events {
            if let Ok(event) = rx.recv() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let latency = now - event.value;
                consumer_latencies.push(latency);
                received += 1;
            }
        }
        consumer_latencies
    });
    let events_per_producer = config.events_per_producer;
    let num_producers = config.num_producers;
    for id in 0..num_producers {
        let tx = tx.clone();
        let producer_barrier = barrier.clone();
        let handle = thread::spawn(move || {
            producer_barrier.wait();
            for _ in 0..events_per_producer {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let event = Event {
                    value: now,
                    producer_id: id,
                };
                let _ = tx.send(event);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let latencies = consumer_handle.join().unwrap();
    let elapsed = start.elapsed();
    let execution_time_ms = elapsed.as_millis() as u64;
    let total_events = config.num_producers * config.events_per_producer;
    let throughput = total_events as f64 / elapsed.as_secs_f64();
    let mut latencies_clone = latencies.clone();
    let avg_latency = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
    let p99_latency = percentile(&mut latencies_clone, 99.0);
    BenchmarkResult {
        name: format!("Std Channel (Multi Producer: {})", config.num_producers),
        throughput,
        avg_latency_ns: avg_latency,
        p99_latency_ns: p99_latency,
        execution_time_ms,
    }
}

fn benchmark_crossbeam_single(config: BenchmarkConfig) -> BenchmarkResult {
    let (tx, rx) = unbounded::<Event>();
    for _ in 0..config.warmup_iterations {
        let _ = tx.send(Event::default());
        let _ = rx.try_recv();
    }
    let start = Instant::now();
    let events_per_producer = config.events_per_producer;
    let consumer_handle = thread::spawn(move || {
        let mut received = 0;
        let mut consumer_latencies = Vec::with_capacity(events_per_producer);
        while received < events_per_producer {
            if let Ok(event) = rx.recv() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let latency = now - event.value;
                consumer_latencies.push(latency);
                received += 1;
            }
        }
        consumer_latencies
    });
    let events_per_producer = config.events_per_producer;
    let producer_handle = thread::spawn(move || {
        for _ in 0..events_per_producer {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let event = Event {
                value: now,
                producer_id: 0,
            };
            let _ = tx.send(event);
        }
    });
    producer_handle.join().unwrap();
    let latencies = consumer_handle.join().unwrap();
    let elapsed = start.elapsed();
    let execution_time_ms = elapsed.as_millis() as u64;
    let throughput = config.events_per_producer as f64 / elapsed.as_secs_f64();
    let mut latencies_clone = latencies.clone();
    let avg_latency = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
    let p99_latency = percentile(&mut latencies_clone, 99.0);
    BenchmarkResult {
        name: "Crossbeam Channel (Single Producer)".to_string(),
        throughput,
        avg_latency_ns: avg_latency,
        p99_latency_ns: p99_latency,
        execution_time_ms,
    }
}

fn benchmark_crossbeam_multi(config: BenchmarkConfig) -> BenchmarkResult {
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();
    let (tx, rx) = unbounded::<Event>();
    let barrier = Arc::new(std::sync::Barrier::new(config.num_producers + 1));
    for _ in 0..config.warmup_iterations {
        let _ = tx.send(Event::default());
        let _ = rx.try_recv();
    }
    let start = Instant::now();
    let consumer_barrier = barrier.clone();
    let total_events = config.num_producers * config.events_per_producer;
    let consumer_handle = thread::spawn(move || {
        let mut received = 0;
        let mut consumer_latencies = Vec::with_capacity(total_events);
        consumer_barrier.wait();
        while received < total_events {
            if let Ok(event) = rx.recv() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let latency = now - event.value;
                consumer_latencies.push(latency);
                received += 1;
            }
        }
        consumer_latencies
    });
    let events_per_producer = config.events_per_producer;
    let num_producers = config.num_producers;
    for id in 0..num_producers {
        let tx = tx.clone();
        let producer_barrier = barrier.clone();
        let handle = thread::spawn(move || {
            producer_barrier.wait();
            for _ in 0..events_per_producer {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let event = Event {
                    value: now,
                    producer_id: id,
                };
                let _ = tx.send(event);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let latencies = consumer_handle.join().unwrap();
    let elapsed = start.elapsed();
    let execution_time_ms = elapsed.as_millis() as u64;
    let total_events = config.num_producers * config.events_per_producer;
    let throughput = total_events as f64 / elapsed.as_secs_f64();
    let mut latencies_clone = latencies.clone();
    let avg_latency = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
    let p99_latency = percentile(&mut latencies_clone, 99.0);
    BenchmarkResult {
        name: format!("Crossbeam Channel (Multi Producer: {})", config.num_producers),
        throughput,
        avg_latency_ns: avg_latency,
        p99_latency_ns: p99_latency,
        execution_time_ms,
    }
}

fn benchmark_oxidator_single(config: BenchmarkConfig) -> BenchmarkResult {
    let client = DisruptorClient;
    let data_storage_layer = client.init_data_storage::<Event, RingBuffer<Event>>(config.buffer_size);
    let wait_strategy_layer = data_storage_layer.with_busy_spin_wait_strategy();
    let sequencer_layer = wait_strategy_layer.with_single_producer();
    let (mut producers, mut consumer_factory) = sequencer_layer.build::<BenchmarkTask>(1);
    let producer = producers.remove(0);
    let mut tasks = Vec::new();
    for _ in 0..config.num_consumers {
        let task = Box::new(BenchmarkTask::new());
        tasks.push(task.clone());
        let _ = consumer_factory.add_task(task, vec![]);
    }
    let consumers = consumer_factory.init_consumers();
    let mut consumer_handle = consumers.start();
    for _ in 0..config.warmup_iterations {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let event = Event { 
            value: now,
            producer_id: 0,
        };
        producer.write(vec![event], |slot, _, event| {
            *slot = event.clone();
        });
    }
    let start = Instant::now();
    for _ in 0..config.events_per_producer {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let event = Event { 
            value: now,
            producer_id: 0,
        };
        producer.write(vec![event], |slot, _, event| {
            *slot = event.clone();
        });
    }
    producer.drain();
    consumer_handle.join();
    let elapsed = start.elapsed();
    let execution_time_ms = elapsed.as_millis() as u64;
    let throughput = config.events_per_producer as f64 / elapsed.as_secs_f64();
    let mut all_latencies = Vec::new();
    for task in &tasks {
        all_latencies.extend(task.get_latencies());
    }
    let avg_latency = if !all_latencies.is_empty() {
        all_latencies.iter().sum::<u64>() as f64 / all_latencies.len() as f64
    } else {
        0.0
    };
    let p99_latency = percentile(&mut all_latencies, 99.0);
    BenchmarkResult {
        name: format!("Oxidator (Single Producer, {} Consumer{})", 
               config.num_consumers, 
               if config.num_consumers > 1 { "s" } else { "" }),
        throughput,
        avg_latency_ns: avg_latency,
        p99_latency_ns: p99_latency,
        execution_time_ms,
    }
}

fn benchmark_oxidator_multi(config: BenchmarkConfig) -> BenchmarkResult {
    let client = DisruptorClient;
    let data_storage_layer = client.init_data_storage::<Event, RingBuffer<Event>>(config.buffer_size);
    let wait_strategy_layer = data_storage_layer.with_busy_spin_wait_strategy();
    let sequencer_layer = wait_strategy_layer.with_multi_producer();
    let (producers, mut consumer_factory) = sequencer_layer.build::<BenchmarkTask>(config.num_producers);
    let mut tasks = Vec::new();
    for _ in 0..config.num_consumers {
        let task = Box::new(BenchmarkTask::new());
        tasks.push(task.clone());
        let _ = consumer_factory.add_task(task, vec![]);
    }
    let consumers = consumer_factory.init_consumers();
    let mut consumer_handle = consumers.start();
    let barrier = Arc::new(std::sync::Barrier::new(config.num_producers + 1));
    for _ in 0..config.warmup_iterations {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let event = Event { 
            value: now,
            producer_id: 0,
        };
        producers[0].write(vec![event], |slot, _, event| {
            *slot = event.clone();
        });
    }
    let mut handles = Vec::new();
    let events_per_producer = config.events_per_producer;
    for (id, producer) in producers.into_iter().enumerate() {
        let producer_barrier = barrier.clone();
        let handle = thread::spawn(move || {
            producer_barrier.wait();
            for _ in 0..events_per_producer {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let event = Event { 
                    value: now,
                    producer_id: id,
                };
                producer.write(vec![event], |slot, _, event| {
                    *slot = event.clone();
                });
            }
            producer
        });
        handles.push(handle);
    }
    let start = Instant::now();
    barrier.wait();
    let mut completed_producers = Vec::new();
    for handle in handles {
        completed_producers.push(handle.join().unwrap());
    }
    if let Some(producer) = completed_producers.first() {
        producer.drain();
    }
    consumer_handle.join();
    let elapsed = start.elapsed();
    let execution_time_ms = elapsed.as_millis() as u64;
    let total_events = config.num_producers * config.events_per_producer;
    let throughput = total_events as f64 / elapsed.as_secs_f64();
    let mut all_latencies = Vec::new();
    for task in &tasks {
        all_latencies.extend(task.get_latencies());
    }
    let avg_latency = if !all_latencies.is_empty() {
        all_latencies.iter().sum::<u64>() as f64 / all_latencies.len() as f64
    } else {
        0.0
    };
    let p99_latency = percentile(&mut all_latencies, 99.0);
    BenchmarkResult {
        name: format!("Oxidator (Multi Producer: {}, Consumer: {})", 
               config.num_producers,
               config.num_consumers),
        throughput,
        avg_latency_ns: avg_latency,
        p99_latency_ns: p99_latency,
        execution_time_ms,
    }
}

fn main() {
    init_minimal_logging();
    println!("\n{:=^80}", " STARTING BENCHMARK ");
    println!("Comparing Oxidator vs Standard Rust Channels vs Crossbeam Channels");
    println!("Note: Debug output is suppressed for clarity");
    println!("{:=^80}\n", "");
    let basic_config = BenchmarkConfig {
        buffer_size: 16384,
        num_producers: 1,
        num_consumers: 1,
        events_per_producer: 10_000,
        warmup_iterations: 500,
    };
    let multi_producer_config = BenchmarkConfig {
        buffer_size: 16384,
        num_producers: 2,
        num_consumers: 1,
        events_per_producer: 5_000,
        warmup_iterations: 500,
    };
    let multi_consumer_config = BenchmarkConfig {
        buffer_size: 16384,
        num_producers: 1,
        num_consumers: 2,
        events_per_producer: 10_000,
        warmup_iterations: 500,
    };
    let mut results = Vec::new();
    println!("Running standard channel (single producer) benchmark...");
    let std_single = benchmark_std_channel_single(basic_config.clone());
    results.push(std_single.clone());
    io::stdout().flush().unwrap();
    println!("\nRunning crossbeam channel (single producer) benchmark...");
    let crossbeam_single = benchmark_crossbeam_single(basic_config.clone());
    results.push(crossbeam_single.clone());
    io::stdout().flush().unwrap();
    println!("\nRunning oxidator (single producer) benchmark...");
    let oxidator_single = benchmark_oxidator_single(basic_config.clone());
    results.push(oxidator_single.clone());
    io::stdout().flush().unwrap();
    let std_single_throughput = std_single.throughput;
    let crossbeam_single_throughput = crossbeam_single.throughput;
    let oxidator_single_throughput = oxidator_single.throughput;
    println!("\nRunning standard channel (multi-producer) benchmark...");
    let std_multi = benchmark_std_channel_multi(multi_producer_config.clone());
    results.push(std_multi.clone());
    io::stdout().flush().unwrap();
    println!("\nRunning crossbeam channel (multi-producer) benchmark...");
    let crossbeam_multi = benchmark_crossbeam_multi(multi_producer_config.clone());
    results.push(crossbeam_multi.clone());
    io::stdout().flush().unwrap();
    println!("\nRunning oxidator (multi-producer) benchmark...");
    let oxidator_multi = benchmark_oxidator_multi(multi_producer_config.clone());
    results.push(oxidator_multi.clone());
    io::stdout().flush().unwrap();
    let std_multi_throughput = std_multi.throughput;
    let crossbeam_multi_throughput = crossbeam_multi.throughput;
    let oxidator_multi_throughput = oxidator_multi.throughput;
    println!("\nRunning oxidator (multi-consumer) benchmark...");
    let oxidator_multi_consumer = benchmark_oxidator_single(multi_consumer_config);
    results.push(oxidator_multi_consumer);
    io::stdout().flush().unwrap();
    println!("\n\n");
    println!("{:=^100}", "");
    println!("{:=^100}", " FINAL BENCHMARK RESULTS (Oxidator vs Standard vs Crossbeam) ");
    println!("{:=^100}", "");
    print_results(&results);
    println!("\n{:=^80}", " BENCHMARK SUMMARY ");
    println!("- Events per producer: {}", basic_config.events_per_producer);
    println!("- Buffer size: {}", basic_config.buffer_size);
    println!("- Warmup iterations: {}", basic_config.warmup_iterations);
    let oxidator_vs_std_single = oxidator_single_throughput / std_single_throughput;
    let crossbeam_vs_std_single = crossbeam_single_throughput / std_single_throughput;
    println!("- Single producer speedup:");
    println!("  * Oxidator vs Std Channel: {:.2}x", oxidator_vs_std_single);
    println!("  * Crossbeam vs Std Channel: {:.2}x", crossbeam_vs_std_single);
    println!("  * Oxidator vs Crossbeam: {:.2}x", oxidator_single_throughput / crossbeam_single_throughput);
    let oxidator_vs_std_multi = oxidator_multi_throughput / std_multi_throughput;
    let crossbeam_vs_std_multi = crossbeam_multi_throughput / std_multi_throughput;
    println!("- Multi producer speedup:");
    println!("  * Oxidator vs Std Channel: {:.2}x", oxidator_vs_std_multi);
    println!("  * Crossbeam vs Std Channel: {:.2}x", crossbeam_vs_std_multi);
    println!("  * Oxidator vs Crossbeam: {:.2}x", oxidator_multi_throughput / crossbeam_multi_throughput);
    println!("{:=^80}", "");
}
