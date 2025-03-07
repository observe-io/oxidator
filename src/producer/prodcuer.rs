use std::marker::PhantomData;
use std::sync::Arc;
use crate::traits::{DataStorage, EventProducer, Sequence, Sequencer};

struct Producer<T, S: Sequencer, D: DataStorage<T>> {
    sequencer: S,
    data_storage: Arc<D>,
    phantom_data: PhantomData<T>,
}

impl<T: Default, S: Sequencer, D: DataStorage<T>> Producer<T, S, D> {
    pub fn new(sequencer: S, data_storage: Arc<D>) -> Self {
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
        // TODO:
        // 1. get items iterator
        let items = events.into_iter();
        // 2. get start and end slots for available for these events from sequencer
        let (start, end) = self.sequencer.next(items.len());
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
