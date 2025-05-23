use oxidator::client::DisruptorClient;
use oxidator::traits::{Task, Sequence, WorkerHandle, Orchestrator, EventProducer};
use oxidator::RingBuffer;
use std::sync::{Arc, Mutex, atomic::{AtomicI32, Ordering}};
use std::thread;

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
    producer_events: [i32; 10],
    producer_count: usize,
    max_values: [i32; 10],
    producer_sums: [i64; 10],
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
                EventCounters { 
                    producer_events: [0; 10], 
                    producer_count: num_producers, 
                    max_values: [0; 10], 
                    producer_sums: [0; 10] 
                }
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
    
    fn get_max_values(&self) -> [i32; 10] {
        let counters = self.counters.lock().unwrap();
        counters.max_values
    }
    
    fn get_producer_sums(&self) -> [i64; 10] {
        let counters = self.counters.lock().unwrap();
        counters.producer_sums
    }
}

impl Task<Event> for SumTask {
    fn execute_task(&self, event: &Event, sequence: Sequence, end_of_batch: bool) {
        self.sum.fetch_add(event.value, Ordering::Relaxed);

        let mut counters = self.counters.lock().unwrap();
        let producer_id = event.producer_id;
        if producer_id < counters.producer_count {
            counters.producer_events[producer_id] += 1;
            if event.value > counters.max_values[producer_id] {
                counters.max_values[producer_id] = event.value;
            }
            counters.producer_sums[producer_id] += event.value as i64;
        }
        drop(counters);
    }

    fn clone_box(&self) -> Box<dyn Task<Event> + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}


fn main() {
    let buffer_size = 128;
    let num_producers = 2;
    let events_per_producer = 200;

    // println!("Starting multi-producer benchmark with {} producers, {} events each (total: {})",
    //     num_producers, events_per_producer, num_producers * events_per_producer);

    let (producers, mut consumer_factory) = DisruptorClient
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

    let mut producer_handles = vec![];

    for producer_id in 0..num_producers {
        let producer = producers[producer_id % producers.len()].clone();
        
        let handle = thread::spawn(move || {
            let mut sent_count = 0;
            
            for i in 0..events_per_producer {
                let event_val = (producer_id * events_per_producer + i + 1) as i32;
                let event = Event {
                    value: event_val,
                    producer_id,
                };

                producer.write(vec![event], |slot, seq, event| {
                    *slot = event.clone();
                });
                
                sent_count += 1;
            }
        });
        producer_handles.push(handle);
    }

    for handle in producer_handles {
        handle.join().unwrap();
    }

    // println!("All producers finished, draining sequencer...");
    for producer in producers {
        producer.drain();
    }

    // println!("Waiting for consumers to finish...");
    consumer_handle.join();

    let final_sum = sum_task.get_sum();

    let producer_counts = sum_task.get_counters();
    let producer_max_values = sum_task.get_max_values();
    let producer_sums = sum_task.get_producer_sums();
    
    let mut total_processed = 0;
    let mut total_expected_sum_calculated = 0i64;
    let mut total_actual_sum_from_components = 0i64;

    for producer_id in 0..num_producers {
        let actual_count = producer_counts[producer_id];
        total_processed += actual_count;

        let producer_actual_sum = producer_sums[producer_id];
        total_actual_sum_from_components += producer_actual_sum;

        let start_val = (producer_id * events_per_producer + 1) as i64;
        let end_val = (producer_id * events_per_producer + events_per_producer) as i64;

        let expected_producer_sum = if events_per_producer > 0 {
             (events_per_producer as i64 * (start_val + end_val)) / 2
        } else {
            0
        };
        total_expected_sum_calculated += expected_producer_sum;

        // println!(
        //     "Producer {} | actual events processed: {} | expected events produced: {}",
        //     producer_id,
        //     actual_count,
        //     events_per_producer
        // );
        //
        // println!(
        //     "    Expected sum for generated events: {}, Actual tracked sum by consumer: {}, Difference: {}",
        //     expected_producer_sum, producer_actual_sum,
        //     (expected_producer_sum - producer_actual_sum).abs()
        // );

        // assert_eq!(actual_count as usize, events_per_producer, "Producer {} event count mismatch", producer_id);
        // assert_eq!(producer_actual_sum, expected_producer_sum, "Producer {} sum mismatch", producer_id);
    }

    let total_expected = num_producers * events_per_producer;
    let completion_percentage = (total_processed as f64 / total_expected as f64) * 100.0;

    // println!("All producers and consumers have completed:");
    println!("  Final tracked sum (Atomic): {}", final_sum);
    // println!("  Expected sum (Calculated from producer ranges): {}", total_expected_sum_calculated);
    // println!("  Sum from individual producer tracking (Mutex): {}", total_actual_sum_from_components);
    // println!("  Total events processed by SumTask: {}", total_processed);
    // println!("  Total events expected: {}", total_expected);
    // println!("  Completion percentage: {:.2}%", completion_percentage);

    // assert!(completion_percentage >= 99.99,
    //     "Total processed events count too low: {}/{} ({:.2}%)",
    //     total_processed, total_expected, completion_percentage);

    let sum_difference = (final_sum as i64 - total_expected_sum_calculated).abs();
    let tolerance_percentage = if total_expected_sum_calculated > 0 {
        (sum_difference as f64 / total_expected_sum_calculated as f64) * 100.0
    } else {
        0.0
    };

    let component_difference = (final_sum as i64 - total_actual_sum_from_components).abs();
    let component_tolerance = if total_actual_sum_from_components > 0 {
        (component_difference as f64 / total_actual_sum_from_components as f64) * 100.0
    } else {
        0.0
    };

    let tolerance_threshold = 0.01;

    // println!("Verification metrics:");
    // println!("  Expected sum vs final atomic sum difference: {} ({:.2}%)", sum_difference, tolerance_percentage);
    // println!("  Final atomic sum vs component sum (Mutex) difference: {} ({:.2}%)", component_difference, component_tolerance);
    //
    // assert!(tolerance_percentage <= tolerance_threshold,
    //     "Sum difference (Atomic vs Expected) too large: {} ({:.2}%)", sum_difference, tolerance_percentage);
    //
    // assert!(component_tolerance <= tolerance_threshold,
    //     "Component tracking inconsistency detected (Atomic vs Mutex): {} ({:.2}%)",
    //     component_difference, component_tolerance);
    //
    // println!("\nVerification successful!");
}
