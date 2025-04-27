use std::marker::PhantomData;
use std::sync::Arc;
use crate::traits::{DataStorage, EventProducer, Sequence, Sequencer};

pub struct Producer<T, S: Sequencer, D: DataStorage<T>> {
    sequencer: Arc<S>,
    data_storage: Arc<D>,
    phantom_data: PhantomData<T>,
}

impl<T: Default, S: Sequencer, D: DataStorage<T>> Producer<T, S, D> {
    pub fn new(sequencer: Arc<S>, data_storage: Arc<D>) -> Self {
        Self {
            sequencer,
            data_storage,
            phantom_data: PhantomData::default()
        }
    }
}

impl<T, S: Sequencer, D: DataStorage<T>> EventProducer for Producer<T, S, D> {
    type Event = T;
    fn write<E, F, G>(&self, events: E, func: F)
    where
        E: IntoIterator<Item = T, IntoIter = G>,
        F: Fn(&mut T, Sequence, &T),
        G: ExactSizeIterator<Item = T>
    {
        // 1. get items iterator
        let items = events.into_iter();
        let items_len = items.len();
        
        // 2. get start and end slots for available for these events from sequencer
        let (start, end) = self.sequencer.next(items_len);
        
        // 3. loop through items
        for (i, item) in items.enumerate() {
            // 4. get data provider slot and write to it
            let sequence = start + i as Sequence;
            unsafe {
                let slot = &mut self.data_storage.get_data_mut(sequence);
                func(slot, sequence, &item);
            };
        }
        // 5. exit loop
        // 6. publish on sequencer
        self.sequencer.publish(start, end);
    }

    fn drain(&self) {
        self.sequencer.drain();
    }
}

impl<T, S: Sequencer, D: DataStorage<T>> Clone for Producer<T, S, D> {
    fn clone(&self) -> Self {
        Self {
            sequencer: Arc::clone(&self.sequencer),
            data_storage: Arc::clone(&self.data_storage),
            phantom_data: PhantomData,
        }
    }
}

#[cfg(test)]
mod consumer_test {
    use super::*;
    use std::sync::Arc;
    use crate::coordinator::*;
    use crate::producer::Producer;
    use crate::storage::RingBuffer;
    use crate::traits::{Sequencer, AtomicSequence, WaitStrategy, Sequence, EventProducer};

    fn new_gating_sequence(s: Sequence) -> Arc<AtomicSequence> {
        Arc::new(AtomicSequence::from(s))
    }

    #[test]
    fn test_producer_write() {
        let storage_capacity = 16;
        let data_storage = Arc::new(RingBuffer::<i32>::new(storage_capacity));

        let wait_strategy = BlockingWaitStrategy::new();
        let mut sequencer = Arc::new(SingleProducerSequencer::new(wait_strategy, storage_capacity));
        Arc::get_mut(&mut sequencer).unwrap().add_gating_sequence(new_gating_sequence(100));

        let producer = Producer::<i32, SingleProducerSequencer<BlockingWaitStrategy>, _>::new(sequencer, data_storage.clone());

        let events = vec![1, 2, 3, 4];

        producer.write(events, |slot: &mut i32, _sequence: Sequence, event: &i32| {
            *slot = event + 10;
        });

        for (i, expected) in [11, 12, 13].iter().enumerate() {
            let value = unsafe { *data_storage.get_data(((i + 1) as Sequence)) };
            assert_eq!(value, *expected);
        }
    }
}
