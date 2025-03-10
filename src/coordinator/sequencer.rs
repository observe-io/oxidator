use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use crate::coordinator::DefaultSequenceBarrier;
use crate::traits::{AtomicSequence, Sequence, Sequencer, WaitStrategy};
use crate::utils::min_sequence;

pub struct SingleProducerSequencer<W: WaitStrategy> {
    cursor: Arc<AtomicSequence>,
    wait_strategy: Arc<W>,
    gating_sequences: Vec<Arc<AtomicSequence>>,
    is_done: Arc<AtomicBool>,
    buffer_size: usize,

    current_producer_sequence: Cell<Sequence>,
    slowest_consumer_sequence: Cell<Sequence>,
}

impl<W: WaitStrategy> SingleProducerSequencer<W> {
    pub fn new(wait_strategy: W, buffer_size: usize) -> Self {
        Self {
            cursor: Arc::new(AtomicSequence::from(-1)),
            wait_strategy: Arc::new(wait_strategy),
            gating_sequences: Vec::new(),
            is_done: Arc::new(AtomicBool::from(false)),
            buffer_size,
            current_producer_sequence: Cell::new(Sequence::from(0)),
            slowest_consumer_sequence: Cell::new(Sequence::default())
        }
    }
}

impl<W:WaitStrategy> Sequencer for SingleProducerSequencer<W> {
    type Barrier = DefaultSequenceBarrier<W>;
    fn next(&self, count: usize) -> (Sequence, Sequence) {
        let curr_producer_idx = self.current_producer_sequence.take();
        let mut consumer_idx = self.slowest_consumer_sequence.take();

        let (low, high) = (curr_producer_idx + 1, curr_producer_idx + (count - 1) as i64);

        while consumer_idx + (self.buffer_size as Sequence) < high {
            consumer_idx = min_sequence(&self.gating_sequences);
        }

        self.current_producer_sequence.set(high);
        self.slowest_consumer_sequence.set(consumer_idx);

        (low, high)
    }

    fn publish(&self, low: Sequence, high: Sequence) {
        self.cursor.store(high);
        self.wait_strategy.signal();
    }

    fn create_barrier(&mut self, gating_sequences: Vec<Arc<AtomicSequence>>) -> Self::Barrier {
        DefaultSequenceBarrier::new(
            self.gating_sequences.clone(),
            self.wait_strategy.clone(),
            self.is_done.clone()
        )
    }

    fn add_gating_sequence(&mut self, gating_sequence: Arc<AtomicSequence>) {
        self.gating_sequences.push(gating_sequence);
    }

    fn get_cursor(&self) -> Arc<AtomicSequence> {
        self.cursor.clone()
    }

    fn drain(&self) {
        while min_sequence(&self.gating_sequences) < self.current_producer_sequence.take() {
            self.wait_strategy.signal();
        }
        self.is_done.store(true, Ordering::SeqCst);
    }
}

struct MultiProducerSequencer {
    // TODO:
}
