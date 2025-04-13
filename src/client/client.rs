use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use crate::consumer::{self, Consumer, ConsumerOrchestrator};
use crate::coordinator::{BlockingWaitStrategy, BusySpinWaitStrategy, MultiProducerSequencer, SingleProducerSequencer, YieldingWaitStrategy};
use crate::producer::{self, Producer};
use crate::storage::{self, RingBuffer};
use crate::traits::{AtomicSequence, DataStorage, EventProducer, Sequence, SequenceBarrier, Sequencer, Task, WaitStrategy, Worker, WorkerHandle};

/// Uses Decorator pattern to construct
pub struct DisruptorClient;

pub struct DataStorageLayer<T: Default + Send + Sync + 'static, D: DataStorage<T>> {
    data_storage: Arc<D>,
    event_marker: PhantomData<T>,
}

pub struct WaitStrategyLayer<T: Default + Send + Sync + 'static, D: DataStorage<T>, W: WaitStrategy, > {
    wait_strategy: W,
    data_storage_layer: DataStorageLayer<T, D>,
}

pub struct SequencerLayer<T: Default + Send + Sync + 'static, D: DataStorage<T>, W: WaitStrategy, S: Sequencer>
where
    S::Barrier: 'static
{
    sequencer: Arc<S>,
    wait_strategy_layer: WaitStrategyLayer<T, D, W>,
}

pub struct ConsumerFactory<T: Default + Send + Sync + 'static, D: DataStorage<T> + 'static, W: WaitStrategy, S: Sequencer>
where
    S::Barrier: 'static
{
    tasks: Vec<Box<dyn Task<T>>>,
    dependency_map: HashMap<usize, Vec<usize>>,
    sequencer_layer: SequencerLayer<T, D, W, S>,
}

impl DisruptorClient {
    fn new<T: Default + Send + Sync + 'static + Default, D: DataStorage<T>>(self, data_storage: D) -> DataStorageLayer<T, D> {
        DataStorageLayer::new(data_storage)
    }

    pub fn init_data_storage<T: Default + Send + Sync + 'static + Default, D: DataStorage<T>>(self, size: usize) -> DataStorageLayer<T, RingBuffer<T>> {
        let storage: RingBuffer<T> = RingBuffer::new(size);
        self.new(storage)
    }
}

impl<T: Default + Send + Sync + 'static, D: DataStorage<T>> DataStorageLayer<T, D> {
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

impl<T: Default + Send + Sync + 'static, D: DataStorage<T>, W: WaitStrategy> WaitStrategyLayer<T, D, W> {
    fn new(data_storage_layer: DataStorageLayer<T, D>, wait_strategy: W) -> WaitStrategyLayer<T, D, W> {
        Self {
            data_storage_layer,
            wait_strategy,
        }
    }

    fn set_sequencer<S: Sequencer + Clone>(self, sequencer: S) -> SequencerLayer<T, D, W, S> {
        SequencerLayer::new(self, sequencer)
    }

    pub fn with_single_producer(self) -> SequencerLayer<T, D, W, SingleProducerSequencer<W>> {
        let size = self.data_storage_layer.data_storage.capacity();
        self.set_sequencer(SingleProducerSequencer::new(W::new(), size))
    }
    
    pub fn with_multi_producer(self) -> SequencerLayer<T, D, W, MultiProducerSequencer<W>> {
        let size = self.data_storage_layer.data_storage.capacity();
        self.set_sequencer(MultiProducerSequencer::new(W::new(), size))
    }
}

impl<T: Default + Send + Sync + 'static, D: DataStorage<T>, W: WaitStrategy, S: Sequencer> SequencerLayer<T, D, W, S> {
    fn new(wait_strategy_layer: WaitStrategyLayer<T, D, W>, sequencer: S) -> Self {
        Self {
            wait_strategy_layer,
            sequencer: Arc::new(sequencer),
        }
    }
    
    fn build<F>(self, producer_count: usize) -> (Vec<Producer<T, S, D>>, ConsumerFactory<T, D, W, S>) 
    where
        F: Task<T> + Send + 'static
    {
        let sequencer = self.sequencer.clone();
        let data_storage = self.wait_strategy_layer.data_storage_layer.data_storage.clone();

        let producers = (0..producer_count)
            .map(|_| Producer::new(sequencer.clone(), data_storage.clone()))
            .collect();

        let consumer_factory = ConsumerFactory::new(self);
        (producers, consumer_factory)
        
    }
}

impl<T: Default + Send + Sync, D: DataStorage<T>, W: WaitStrategy, S: Sequencer> ConsumerFactory<T, D, W, S> {
    pub fn new(sequencer_layer: SequencerLayer<T, D, W, S>) -> Self {
        Self {
            sequencer_layer,
            tasks: Vec::new(),
            dependency_map: HashMap::new(),
        }
    }

    fn is_cyclic(
        &self,
        node: usize,
        dependency_map: &HashMap<usize, Vec<usize>>,
        visited: &mut Vec<bool>,
        stack: &mut Vec<bool>,
    ) -> bool {
        if !visited[node] {
            visited[node] = true;
            stack[node] = true;

            if let Some(dependencies) = dependency_map.get(&node) {
                for &dep in dependencies {
                    if !visited[dep] && self.is_cyclic(dep, dependency_map, visited, stack) {
                        return true;
                    } else if stack[dep] {
                        return true;
                    }
                }
            }
        }

        stack[node] = false;
        false
    }

    fn validate_dependencies(&self, depndencies: &Vec<usize>) -> bool {
        for &dep in depndencies {
            if dep >= self.tasks.len() {
                return false;
            }
        }

        let mut visited = vec![false; self.tasks.len()];
        let mut stack = vec![false; self.tasks.len()];

        for &dep in depndencies {
            if self.is_cyclic(dep, &self.dependency_map, &mut visited, &mut stack) {
                return false;
            }
        }

        true
    }

    /// Adds a task to the factory and sets up its dependencies.
    /// Returns the index of the added task.
    pub fn add_task(&mut self, task: Box<dyn Task<T>>, depndencies: Vec<usize>) -> usize {
        let index = self.tasks.len();
        self.tasks.push(task);
        self.validate_dependencies(&depndencies);
        self.dependency_map.insert(index, depndencies);
        index
    }

    
}