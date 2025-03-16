use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use crate::consumer::{Consumer, ConsumerWorkerHandle};
use crate::coordinator::{BlockingWaitStrategy, BusySpinWaitStrategy, DefaultSequenceBarrier, MultiProducerSequencer, SingleProducerSequencer, YieldingWaitStrategy};
use crate::producer::Producer;
use crate::storage::RingBuffer;
use crate::traits::{AtomicSequence, DataStorage, EventProducer, Sequence, SequenceBarrier, Sequencer, Task, WaitStrategy, Worker, WorkerHandle};

/// Uses Decorator pattern to construct
pub struct DisruptorClient;

pub struct DataStorageLayer<T: Send + Sync, D: DataStorage<T>> {
    data_storage: Arc<D>,
    event_marker: PhantomData<T>,
}

pub struct WaitStrategyLayer<T: Send + Sync, D: DataStorage<T>, W: WaitStrategy, > {
    wait_strategy: W,
    data_storage_layer: DataStorageLayer<T, D>,
}

pub struct SequencerLayer<T: Send + Sync, D: DataStorage<T>, W: WaitStrategy, S: Sequencer> {
    sequencer: S,
    wait_strategy_layer: WaitStrategyLayer<T, D, W>,
}

impl DisruptorClient {
    fn new<T: Send + Sync + Default, D: DataStorage<T>>(self, data_storage: D) -> DataStorageLayer<T, D> {
        DataStorageLayer::new(data_storage)
    }

    pub fn init_data_storage<T: Send + Sync + Default, D: DataStorage<T>>(self, size: usize) -> DataStorageLayer<T, RingBuffer<T>> {
        let storage: RingBuffer<T> = RingBuffer::new(size);
        self.new(storage)
    }
}

impl<T: Send + Sync, D: DataStorage<T>> DataStorageLayer<T, D> {
    fn new(data_storage: D) -> Self {
        Self {
            data_storage: Arc::new(data_storage),
            event_marker: PhantomData::default(),
        }
    }

    fn set_wait_strategy<W: WaitStrategy>(self, wait_strategy: W) -> WaitStrategyLayer<T, D, W> {
        WaitStrategyLayer::new(self, wait_strategy)
    }

    pub fn with_blocking_wait_strategy(self) -> WaitStrategyLayer<T, D, BlockingWaitStrategy> {
        self.set_wait_strategy(BlockingWaitStrategy::new())
    }

    pub fn with_busy_spin_wait_strategy(self) -> WaitStrategyLayer<T, D, BusySpinWaitStrategy> {
        self.set_wait_strategy(BusySpinWaitStrategy::new())
    }

    pub fn with_yielding_wait_strategy(self) -> WaitStrategyLayer<T, D, YieldingWaitStrategy> {
        self.set_wait_strategy(YieldingWaitStrategy::new())
    }
}

impl<T: Send + Sync, D: DataStorage<T>, W: WaitStrategy> WaitStrategyLayer<T, D, W> {
    fn new(data_storage_layer: DataStorageLayer<T, D>, wait_strategy: W) -> WaitStrategyLayer<T, D, W> {
        Self {
            data_storage_layer,
            wait_strategy,
        }
    }

    fn set_sequencer<S: Sequencer>(self, sequencer: S) -> SequencerLayer<T, D, W, S> {
        SequencerLayer::new(self, sequencer)
    }

    pub fn with_single_producer(self) -> SequencerLayer<T, D, W, SingleProducerSequencer<W>> {
        let size = self.data_storage_layer.data_storage.capacity();
        self.set_sequencer(SingleProducerSequencer::new(W::new(), size))
    }
    
    pub fn with_multi_producer(self) -> SequencerLayer<T, D, W, MultiProducerSequencer<W>> {
        unimplemented!()
    }
}

impl<T: Send + Sync, D: DataStorage<T>, W: WaitStrategy, S: Sequencer> SequencerLayer<T, D, W, S> {
    fn new(wait_strategy_layer: WaitStrategyLayer<T, D, W>, sequencer: S) -> Self {
        Self {
            wait_strategy_layer,
            sequencer,
        }
    }
    
    fn build() {
        // TODO:
        // should return data structure that supports:
        // 1. create consumers and create dependency among them
        // 1.1. post that, it should give method to start consumers
        // 2. create producer and start it
        // 3. handle for producer and consumers
        unimplemented!()
    }
}