use std::sync::Arc;
use crate::traits::{DataStorage, EventProducer, Sequence, Sequencer};

struct SingleProducerSequencer {
    // TODO:
}

struct MultiProducerSequencer {
    // TODO:
}

struct Producer<T, D: DataStorage<T>, S: Sequencer> {
    sequencer: S,
    data_storage: D<T>,
}

impl<T, S: Sequencer, D: DataStorage<T>> Producer<T, S, D> {
    pub fn new(sequencer: S, data_storage: Arc<D>) -> Self {
        Self {
            sequencer,
            data_storage,
        }
    }
}

impl<T, S: Sequencer, D: DataStorage<T>> EventProducer for Producer<T, S, D> {
    type Event = T;
    fn write<E, F>(&self, events: E, f: F)
    where
        E: IntoIterator,
        F: Fn(&mut E, Sequence, &E)
    {
        // TODO:
        // 1. get items iterator
        // 2. get start and end slots for available for these events from sequencer
        // 3. loop through items
        // 4. get data provider slot and write to it
        // 5. exit loop
        // 6. publish on sequencer
    }

    fn drain(&self) {
        // drain sequencer
    }
}

