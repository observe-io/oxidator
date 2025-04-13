use std::sync::{Arc};
use std::sync::atomic::{AtomicI64, Ordering};

pub type Sequence = i64;

#[cfg(any(
    target_arch = "x86_64",
    target_arch = "x86",
    target_arch = "aarch64",
    target_arch = "arm",
    target_arch = "powerpc64",
))]
pub const CACHE_LINE_SIZE: usize = 64;

#[cfg(target_arch = "mips")]
pub const CACHE_LINE_SIZE: usize = 32;

// For other architectures, assuming the default to be 64 bytes
#[cfg(not(any(
    target_arch = "x86_64",
    target_arch = "x86",
    target_arch = "aarch64",
    target_arch = "arm",
    target_arch = "powerpc64",
    target_arch = "mips",
)))]
pub const CACHE_LINE_SIZE: usize = 64;

pub const CACHE_LINE_PADDING: usize = CACHE_LINE_SIZE - size_of::<AtomicI64>();

pub struct AtomicSequence {
    value: AtomicI64,
    _padding: [u8; CACHE_LINE_PADDING]
}

impl AtomicSequence {
    pub fn load(&self) -> Sequence {
        self.value.load(Ordering::Acquire)
    }

    pub fn store(&self, s: Sequence) {
        self.value.store(s, Ordering::Release)
    }

    pub fn compare_exchange(&self, old: Sequence, new: Sequence) -> bool {
        self.value.compare_exchange(old, new, Ordering::SeqCst, Ordering::Relaxed).is_ok()
    }
}

impl From<Sequence> for AtomicSequence {
    fn from(value: Sequence) -> Self {
        Self {
            value: AtomicI64::new(value),
            _padding: [0u8; CACHE_LINE_PADDING]
        }
    }
}

impl Default for AtomicSequence {
    fn default() -> Self {
        AtomicSequence::from(-1)
    }
}

impl AsRef<AtomicSequence> for AtomicSequence {
    fn as_ref(&self) -> &AtomicSequence {
        self
    }
}

pub trait SequenceBarrier: Send + Sync {
    fn wait_for(&self, s: Sequence) -> Option<Sequence>;
    fn signal(&self);
}

pub trait Sequencer {
    type Barrier: SequenceBarrier;
    fn next(&self, count: usize) -> (Sequence, Sequence);
    fn publish(&self, low: Sequence, high: Sequence);
    fn create_barrier(&self, gating_sequences: Vec<Arc<AtomicSequence>>) -> Self::Barrier;
    fn add_gating_sequence(&mut self, gating_sequence: Arc<AtomicSequence>);
    fn get_cursor(&self) -> Arc<AtomicSequence>;
    fn drain(&self);
}

pub trait EventProducer {
    type Event;
    fn write<E, F, G>(&self, events: E, f: F)
    where
        E: IntoIterator<Item = Self::Event, IntoIter = G>,
        F: Fn(&mut Self::Event, Sequence, &Self::Event),
        G: ExactSizeIterator<Item = Self::Event>;
    fn drain(&self);
}

pub trait WaitStrategy: Send + Sync {
    fn new() -> Self;
    fn wait_for<T: AsRef<AtomicSequence>, F: Fn() -> bool>(
        &self,
        sequence: Sequence,
        dependencies: &[T],
        check_alert: F
    ) -> Option<Sequence>;
    fn signal(&self);
}

pub trait DataStorage<T>: Send + Sync {
    unsafe fn get_data(&self, s: Sequence) -> &T;
    unsafe fn get_data_mut(&self, s: Sequence) -> &mut T;
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;
}


pub trait Worker: Send {
    fn run(&self);
}

pub trait EventConsumer<T> {
    type ConsumerWorker: Worker;
    type Task: Task<T>;
    type DataStorage: DataStorage<T>;
    type Barrier: SequenceBarrier;
    
    fn init_concurrent_task(
        task: Self::Task,
        barrier: Self::Barrier,
        data_storage: Arc<Self::DataStorage>,
    ) -> Self::ConsumerWorker;
    
    fn get_consumer_cursor(&self) -> Arc<AtomicSequence>;
}

pub trait EventConsumerMut<T> {
    type ConcurrentTask: Worker;
    fn new<S: SequenceBarrier, D: DataStorage<T>>(
        &self,
        barrier: S,
        data_storage: D,
    ) -> Self::ConcurrentTask;

    fn get_consumer_cursor(&self) -> Arc<AtomicSequence>;
}

pub trait Task<T>: Send + Sync {
    fn execute_task(&self, event: &T, sequence: Sequence, eob: bool);
    fn clone_box(&self) -> Box<dyn Task<T> + Send + Sync + 'static>;
}

impl<T> Clone for Box<dyn Task<T> + Send + Sync + 'static> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl<T> Task<T> for Box<dyn Task<T> + Send + Sync + 'static> {
    fn execute_task(&self, event: &T, sequence: Sequence, eob: bool) {
        (**self).execute_task(event, sequence, eob);
    }

    fn clone_box(&self) -> Box<dyn Task<T> + Send + Sync + 'static> {
        (**self).clone_box()
    }
}

pub trait TaskMut<T> {
    fn execute_task(&self, event: &mut T, sequence: Sequence, eob: bool);
}

pub trait Orchestrator {
    fn with_workers(workers: Vec<Box<dyn Worker>>) -> Self;
    fn start(self) -> impl WorkerHandle;
}

pub trait WorkerHandle {
    fn join(&mut self);
}
