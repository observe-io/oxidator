use oxidator::client::DisruptorClient;
use oxidator::traits::{Task, Sequence, WorkerHandle, Orchestrator, EventProducer};
use oxidator::RingBuffer;
use std::sync::{Arc, Mutex, atomic::{AtomicI32, Ordering}};
use std::thread;
use std::time::Duration;

#[derive(Default, Debug, Clone)]
struct Event {
    value: i32,
    producer_id: usize,
}

#[derive(Clone)]
struct PrinterTask;

impl Task<Event> for PrinterTask {
    fn execute_task(&self, event: &Event, sequence: Sequence, end_of_batch: bool) {
    }

    fn clone_box(&self) -> Box<dyn Task<Event> +Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

struct EventCounters {
    producer_events: [i32; 1],
    producer_count: usize,
}

#[derive(Clone)]
struct SumTask {
    sum: Arc<AtomicI32>,
    counters: Arc<Mutex<EventCounters>>,
}

impl SumTask {
    fn new() -> Self {
        Self {
            sum: Arc::new(AtomicI32::new(0)),
            counters: Arc::new(Mutex::new(
                EventCounters { producer_events: [0; 1], producer_count: 1 }
            )),
        }
    }

    fn get_sum(&self) -> i32 {
        self.sum.load(Ordering::Relaxed)
    }

    fn get_counters(&self) -> [i32; 1] {
        let counters = self.counters.lock().unwrap();
        counters.producer_events
    }
}

impl Task<Event> for SumTask {
    fn execute_task(&self, event: &Event, sequence: Sequence, end_of_batch: bool) {
        self.sum.fetch_add(event.value, Ordering::Relaxed);

        let mut counters = self.counters.lock().unwrap();
        let producer_id = event.producer_id;
        if producer_id < 10 {
            counters.producer_events[producer_id] += 1;
        }
        drop(counters);
    }

    fn clone_box(&self) -> Box<dyn Task<Event> + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

fn main() {
    // Increase buffer size to make sure we can handle more events
    let buffer_size = 256;
    let num_producers = 1;
    let num_events = 400;

    // Configure disruptor
    let (mut producers, mut consumer_factory) = DisruptorClient
    .init_data_storage::<Event, RingBuffer<Event>>(buffer_size)
    .with_yielding_wait_strategy()
    .with_single_producer()
    .build::<PrinterTask>(num_producers);

    let producer = producers.remove(0);

    let printer_task = Box::new(PrinterTask);
    let sum_task = Box::new(SumTask::new());
    let sum_handle = sum_task.clone();

    let printer_idx = consumer_factory.add_task(printer_task, vec![]);
    let sum_idx = consumer_factory.add_task(sum_task, vec![printer_idx]);

    let consumers = consumer_factory.init_consumers();
    let mut consumer_handle = consumers.start();

    let mut producer_handles = Vec::new();

    // Produce events
    let handle = thread::spawn(move || {
        for i in 0..num_events {
            let event = Event {
                value: i + 1,
                producer_id: 0,
            };

            producer.write(vec![event], |slot: &mut Event, seq: i64, event| {
                *slot = event.clone();
            });
        }
        producer
    });
    producer_handles.push(handle);

    let mut completed_producers = Vec::new();
    for handle in producer_handles {
        completed_producers.push(handle.join().unwrap());
    }
    // Clean up and wait for completion
    if let Some(producer) = completed_producers.first() {
        producer.drain();
    }
    consumer_handle.join();

    // Final verification - keeping only these prints
    let final_sum = sum_handle.get_sum();
    let expected_sum = (1..=num_events).sum::<i32>();
    let producer_counts = sum_handle.get_counters();
    
    println!("Expected sum {}, Final Sum: {}", expected_sum, final_sum);
    println!("Expected events {}, Received events {}", num_events, producer_counts[0]);
    
    if final_sum != expected_sum {
        println!("ERROR: Final sum doesn't match expected sum!");
    } else {
        println!("SUCCESS: Final sum matches expected sum!");
    }
    
    if producer_counts[0] != num_events {
        println!("ERROR: Received events count doesn't match expected count!");
    } else {
        println!("SUCCESS: All events were processed correctly!");
    }
}
