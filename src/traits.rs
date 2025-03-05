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

pub const CACHE_LINE_PADDING: usize = CACHE_LINE_SIZE - std::mem::size_of::<AtomicI64>();

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
        self.value.compare_exchange(old, new, Ordering::SeqCst, Ordering::AcqRel).is_ok()
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

pub trait SequenceBarrier {
    fn wait_for(&self, s: Sequence) -> Option<Sequence>;
    fn signal(&self);
}

pub trait Sequencer {
    type Barrier: SequenceBarrier;
    fn next(&self, count: usize) -> (Sequence, Sequence);
    fn publish(&self, low: Sequence, high: Sequence);
    fn create_barrier(&mut self, gating_sequences: Vec<Arc<AtomicSequence>>) -> Self::Barrier;
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

pub trait WaitStrategy {
    fn new() -> Self;
    fn wait_for<T: AsRef<AtomicSequence>, F: Fn() -> bool>(
        &self,
        sequence: Sequence,
        gating_sequence: &[T],
        check_alert: F
    ) -> Option<Sequence>;
    fn signal(&self);
}

pub trait EventConsumer<T> {
    type Barrier: SequenceBarrier;

    fn handle_event(&self, event: &T, sequence: Sequence, eob: bool);
}

pub trait EventConsumerMut<T> {
    type Barrier: SequenceBarrier;

    fn handle_event(&self, event: &mut T, sequence: Sequence, eob: bool);
}


pub trait DataStorage<'a, T>: Send + Sync {
    unsafe fn get_data(&self, s: Sequence) -> &T;
    unsafe fn get_data_mut(&self, s: Sequence) -> &'a mut T;
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;
}








