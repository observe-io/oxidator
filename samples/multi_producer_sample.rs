use oxidator::client::DisruptorClient;
use oxidator::traits::{Task, Sequence, WorkerHandle, Orchestrator, EventProducer};
use oxidator::RingBuffer;
use std::sync::{Arc, Mutex, atomic::{AtomicI32, Ordering}};

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
    producer_events: [i32; 10],
    producer_count: usize,
}

#[derive(Clone)]
struct SumTask {
    sum: Arc<AtomicI32>,
    counters: Arc<Mutex<EventCounters>>,
}

impl SumTask {
    fn new(num_producers: usize) -> Self {
        Self {
            sum: Arc::new(AtomicI32::new(0)),
            counters: Arc::new(Mutex::new(
                EventCounters { producer_events: [0; 10], producer_count: num_producers }
            )),
        }
    }

    fn get_sum(&self) -> i32 {
        self.sum.load(Ordering::Relaxed)
    }

    fn get_counters(&self) -> [i32; 10] {
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
    let num_producers = 4;
    let events_per_producer =100;

    let (mut producers, mut consumer_factory) = DisruptorClient
    .init_data_storage::<Event, RingBuffer<Event>>(buffer_size)
    .with_yielding_wait_strategy()
    .with_multi_producer()
    .build::<PrinterTask>(num_producers);

    let printer_task = Box::new(PrinterTask);
    let printer_idx = consumer_factory.add_task(printer_task, vec![]);
    let sum_task = Box::new(SumTask::new(num_producers));
    let sum_idx = consumer_factory.add_task(sum_task.clone(), vec![printer_idx]);

    let consumers = consumer_factory.init_consumers();
    let mut consumer_handle = consumers.start();

    let primary_producer = producers[0].clone();

    for producer_id in 0..num_producers {
        for i in 0..events_per_producer {
            let event_val = i as i32 + 1;

            let events = vec![
                Event {
                    value: event_val,
                    producer_id,
                }
            ];

            primary_producer.write(events, |slot, seq, event| {
                *slot = event.clone();

            });
        }
    }

    primary_producer.drain();

    consumer_handle.join();

    let final_sum = match sum_task.as_ref() {
        SumTask { sum, .. } => sum.load(Ordering::Relaxed),
    };

    let expected_per_producer = (1..=events_per_producer as i32).sum::<i32>();

    
    println!("Sum excepted per producer: {}", expected_per_producer);
    println!("All prodcuers and consumers have completed. Final Sum: {}", final_sum);
    println!("Expected sum: {}",  expected_per_producer * num_producers as i32);

    let producer_counts = sum_task.get_counters();
    for i in 0..num_producers {
        println!(
            "Producer {} | actual events processed: {} | expected events produced: {}",
            i,
            producer_counts[i],
            events_per_producer
        );
    }
    
}