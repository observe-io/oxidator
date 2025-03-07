use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use crate::traits::{AtomicSequence, Sequence, SequenceBarrier, WaitStrategy};

pub struct DefaultSequenceBarrier<W: WaitStrategy> {
    gating_sequences: Vec<Arc<AtomicSequence>>,
    wait_strategy: Arc<W>,
    is_done: Arc<AtomicBool>
}

impl<W: WaitStrategy> DefaultSequenceBarrier<W> {
    pub fn new(gating_sequences: Vec<Arc<AtomicSequence>>,
           wait_strategy: Arc<W>,
           is_done: Arc<AtomicBool>
    ) -> Self {
        Self {
            gating_sequences,
            wait_strategy,
            is_done
        }
    }
}

impl<W: WaitStrategy> SequenceBarrier for DefaultSequenceBarrier<W> {
    fn wait_for(&self, s: Sequence) -> Option<Sequence> {
        self.wait_strategy.wait_for(s, &*self.gating_sequences, || {
            self.is_done.load(Ordering::Relaxed)
        })
    }

    fn signal(&self) {
        self.wait_strategy.signal();
    }
}