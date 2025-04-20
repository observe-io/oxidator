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
        if sequence % 50 == 0 || end_of_batch {
            println!(
                "Printer: Processing event with value {} from producer {} at sequence {}",
                event.value,
                event.producer_id,
                sequence
            );

            if end_of_batch {
                println!(
                    "Printer: End of Batch at sequence: {}",
                    sequence
                )
            }
        }
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

        if sequence % 50 == 0 || end_of_batch {
            print!(
                "SumTask: Current sum is {} at sequence {}",
                self.get_sum(),
                sequence
            );

            if end_of_batch {
                println!(
                    "SumTask: End of Batch at sequence {}",
                    sequence
                );
            }
        }
    }

    fn clone_box(&self) -> Box<dyn Task<Event> + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

fn main() {
    let buffer_size = 1024;
    let num_producers = 1;
    let num_events = 400;

    let (mut producers, mut consumer_factory) = DisruptorClient
    .init_data_storage::<Event, RingBuffer<Event>>(buffer_size)
    .with_blocking_wait_strategy()
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

    for i in 0..num_events {
        let event = Event {
            value: i + 1,
            producer_id: 0,
        };

        producer.write(vec![event], |slot: &mut Event, seq: i64, event| {
            *slot = event.clone();

            if seq % 50 == 0 {
                println!("Producer wrote value {} at sequence {}", event.value, seq);
            }
        });

        if i % 50 == 0 {
            thread::sleep(Duration::from_millis(10));
        }
    }

    thread::sleep(Duration::from_millis(100));

    producer.drain();

    consumer_handle.join();

    let final_sum = sum_handle.get_sum();
    let expected_sum = (1..=num_events).sum::<i32>();
    let producer_counts = sum_handle.get_counters();
    println!("All prodcuers and consumers have completed. Final Sum: {}", final_sum);
    println!("Expected sum: {}",  expected_sum);

    println!("Expected sum {}, Final Sum: {}", expected_sum, final_sum);
    println!("Expected events {}, Received events {}", num_events, producer_counts[0]);
}