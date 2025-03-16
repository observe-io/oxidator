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

pub struct MultiProducerSequencer<W: WaitStrategy> {
    wait_strategy: Arc<W>,
    // TODO:
}

impl<W: WaitStrategy> MultiProducerSequencer<W> {}
impl<W: WaitStrategy> Sequencer for MultiProducerSequencer<W> {
    type Barrier = DefaultSequenceBarrier<W>;

    fn next(&self, count: usize) -> (Sequence, Sequence) {
        todo!()
    }

    fn publish(&self, low: Sequence, high: Sequence) {
        todo!()
    }

    fn create_barrier(&mut self, gating_sequences: Vec<Arc<AtomicSequence>>) -> Self::Barrier {
        todo!()
    }

    fn add_gating_sequence(&mut self, gating_sequence: Arc<AtomicSequence>) {
        todo!()
    }

    fn get_cursor(&self) -> Arc<AtomicSequence> {
        todo!()
    }

    fn drain(&self) {
        todo!()
    }
}

#[cfg(test)]
mod tests_sequencer {
    use super::*;
    use crate::coordinator::{BlockingWaitStrategy, BusySpinWaitStrategy};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use crate::traits::{AtomicSequence, Sequencer, Sequence, WaitStrategy};
    
    fn create_new_dating_sequence(seq: Sequence) -> Arc<AtomicSequence> {
        Arc::new(AtomicSequence::from(seq))
    }
    
    #[test]
    fn test_next_range() {
        let wait_strategy = BlockingWaitStrategy::new();
        let sequencer = SingleProducerSequencer::new(wait_strategy, 8);
        
        let gating_seq = create_new_dating_sequence(100);
        let mut sequencer = {
            let mut s = sequencer;
            s.add_gating_sequence(gating_seq.clone());
            s
        };
        
        let (low, high) = sequencer.next(3);
        assert_eq!(low, Sequence::from(1));
        assert_eq!(high, Sequence::from(2));
        
        let (low, high) = sequencer.next(4);
        assert_eq!(low, Sequence::from(3));
        assert_eq!(high, Sequence::from(5));
    }
    
    #[test]
    fn test_publish() {
        let wait_strategy = BusySpinWaitStrategy::new();
        let sequencer = SingleProducerSequencer::new(wait_strategy, 8);
        
        let gating_seq = create_new_dating_sequence(100);
        let mut sequencer = {
            let mut s = sequencer;
            s.add_gating_sequence(gating_seq.clone());
            s
        };
        
        let (low, high) = sequencer.next(5);
        sequencer.publish(low, high);
        
        let cursor = sequencer.get_cursor();
        assert_eq!(cursor.load(), low);
    }
}