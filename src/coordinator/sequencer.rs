use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use crate::coordinator::DefaultSequenceBarrier;
use crate::traits::{AtomicSequence, Sequence, Sequencer, WaitStrategy};
use crate::utils::{min_sequence, BitMap};

pub struct SingleProducerSequencer<W: WaitStrategy> {
    cursor: Arc<AtomicSequence>,
    wait_strategy: Arc<W>,
    gating_sequences: Vec<Arc<AtomicSequence>>,
    is_done: Arc<AtomicBool>,
    buffer_size: usize,

    current_producer_sequence: Arc<AtomicSequence>,
    slowest_consumer_sequence: Arc<AtomicSequence>,
}

impl<W: WaitStrategy> SingleProducerSequencer<W> {
    pub fn new(wait_strategy: W, buffer_size: usize) -> Self {
        Self {
            cursor: Arc::new(AtomicSequence::default()),
            wait_strategy: Arc::new(wait_strategy),
            gating_sequences: Vec::new(),
            is_done: Arc::new(AtomicBool::from(false)),
            buffer_size,
            current_producer_sequence: Arc::new(AtomicSequence::default()),
            slowest_consumer_sequence: Arc::new(AtomicSequence::default()),
        }
    }
}

impl<W: WaitStrategy> Clone for SingleProducerSequencer<W> {
    fn clone(&self) -> Self {
        Self {
            cursor: self.cursor.clone(),
            wait_strategy: self.wait_strategy.clone(),
            gating_sequences: self.gating_sequences.clone(),
            is_done: self.is_done.clone(),
            buffer_size: self.buffer_size,
            current_producer_sequence: self.current_producer_sequence.clone(),
            slowest_consumer_sequence: self.slowest_consumer_sequence.clone(),
        }
    }
}

impl<W:WaitStrategy> Sequencer for SingleProducerSequencer<W> {
    type Barrier = DefaultSequenceBarrier<W>;
    fn next(&self, count: usize) -> (Sequence, Sequence) {
        assert!(count > 0, "Count must be > 0");
        let current_producer_claimed_high = self.current_producer_sequence.load();
        let desired_next_producer_high = current_producer_claimed_high + count as i64;

        if !self.gating_sequences.is_empty() {
            loop {
                let min_consumer_seq = min_sequence(&self.gating_sequences);
                if desired_next_producer_high - min_consumer_seq < self.buffer_size as i64 {
                    break;
                }
                std::thread::yield_now();
            }
        }

        self.current_producer_sequence.store(desired_next_producer_high);

        let low = current_producer_claimed_high + 1;
        (low, desired_next_producer_high)
    }

    fn publish(&self, _low: Sequence, high: Sequence) {
        self.cursor.store(high);

        if !self.gating_sequences.is_empty() {
             let min_seq = min_sequence(&self.gating_sequences);
             self.slowest_consumer_sequence.store(min_seq);
        }

        self.wait_strategy.signal();
    }

    fn create_barrier(&self, gating_sequences: Vec<Arc<AtomicSequence>>) -> Self::Barrier {
        DefaultSequenceBarrier::new(
            gating_sequences,
            self.wait_strategy.clone(),
            self.is_done.clone(),
        )
    }

    fn add_gating_sequence(&mut self, gating_sequence: Arc<AtomicSequence>) {
        self.gating_sequences.push(gating_sequence);

        if !self.gating_sequences.is_empty() {
            self.slowest_consumer_sequence.store(min_sequence(&self.gating_sequences));
        }
    }

    fn get_cursor(&self) -> Arc<AtomicSequence> {
        self.cursor.clone()
    }

    fn drain(&self) {
        self.is_done.store(true, Ordering::SeqCst);
        self.wait_strategy.signal();
    }
}

pub struct MultiProducerSequencer<W: WaitStrategy> {
    wait_strategy: Arc<W>,
    gating_sequences: Vec<Arc<AtomicSequence>>,
    buffer_size: usize,
    is_done: Arc<AtomicBool>,
    cursor: Arc<AtomicSequence>,

    highest_claimed_sequence: AtomicSequence,
    sequence_tracker: Arc<BitMap>,
}

impl<W: WaitStrategy> MultiProducerSequencer<W> {
    pub fn new(wait_strategy: W, buffer_size: usize) -> Self {
        Self {
            wait_strategy: Arc::new(wait_strategy),
            gating_sequences: Vec::new(),
            buffer_size,
            is_done: Arc::new(AtomicBool::from(false)),
            cursor: Arc::new(AtomicSequence::default()),
            highest_claimed_sequence: AtomicSequence::default(),
            sequence_tracker: Arc::new(BitMap::new(buffer_size)),
        }
    }

    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }

    fn buffer_has_capacity(&self, count: usize) -> bool {
        if count > self.buffer_size {
            return false;
        }

        if self.gating_sequences.is_empty() {
            return true;
        }

        let highest_claimed = self.highest_claimed_sequence.load();
        let min_seq = min_sequence(&self.gating_sequences);

        let wrap_point = min_seq + self.buffer_size as i64;
        let available_slots = if highest_claimed < min_seq {
            (self.buffer_size as i64) - 1
        } else {
            wrap_point - highest_claimed - 1
        };

        available_slots >= count as i64
    }
}

impl<W: WaitStrategy> Clone for MultiProducerSequencer<W> {
    fn clone(&self) -> Self {
        Self {
            wait_strategy: self.wait_strategy.clone(),
            gating_sequences: self.gating_sequences.clone(),
            buffer_size: self.buffer_size,
            is_done: self.is_done.clone(),
            cursor: self.cursor.clone(),
            highest_claimed_sequence: AtomicSequence::from(self.highest_claimed_sequence.load()),
            sequence_tracker: self.sequence_tracker.clone(),
        }
    }
}

impl<W: WaitStrategy> Sequencer for MultiProducerSequencer<W> {
    type Barrier = DefaultSequenceBarrier<W>;

    fn next(&self, count: usize) -> (Sequence, Sequence) {
        loop {
            let highest_claimed = self.highest_claimed_sequence.load();
            if self.buffer_has_capacity(count) {
                let low = highest_claimed + 1;
                let high = highest_claimed + count as i64;
                if self.highest_claimed_sequence.compare_exchange(highest_claimed, high) {
                    return (low, high);
                }
            }
        }
    }

    fn publish(&self, low: Sequence, high: Sequence) {
        for i in low..=high {
            self.sequence_tracker.set(i);
        }

        let mut current_cursor = self.cursor.load();

        loop {
            let highest_claimed = self.highest_claimed_sequence.load();
            let mut next_cursor = current_cursor + 1;
            let mut max_committable_sequence = current_cursor;

            while next_cursor <= highest_claimed {
                if self.sequence_tracker.is_set(next_cursor) {
                    max_committable_sequence = next_cursor;
                    next_cursor += 1;
                } else {
                    break;
                }
            }

            if max_committable_sequence > current_cursor {
                if self.cursor.compare_exchange(current_cursor, max_committable_sequence) {
                    for i in current_cursor + 1..=max_committable_sequence {
                        self.sequence_tracker.unset(i);
                    }
                    self.wait_strategy.signal();
                    break;
                } else {
                    current_cursor = self.cursor.load();
                }
            } else {
                self.wait_strategy.signal();
                break;
            }
        }
    }

    fn create_barrier(&self, mut gating_sequences: Vec<Arc<AtomicSequence>>) -> Self::Barrier {
        gating_sequences.push(self.cursor.clone());
        DefaultSequenceBarrier::new(
            gating_sequences,
            self.wait_strategy.clone(),
            self.is_done.clone(),
        )
    }

    fn add_gating_sequence(&mut self, gating_sequence: Arc<AtomicSequence>) {
        self.gating_sequences.push(gating_sequence);
    }

    fn get_cursor(&self) -> Arc<AtomicSequence> {
        self.cursor.clone()
    }

    fn drain(&self) {
        self.is_done.store(true, Ordering::SeqCst);
        self.wait_strategy.signal();
    }
}

#[cfg(test)]
mod tests_sequencer {
    use super::*;
    use crate::coordinator::{BlockingWaitStrategy, BusySpinWaitStrategy};
    use std::sync::Arc;
    use crate::traits::{AtomicSequence, Sequencer, Sequence, WaitStrategy};
    
    fn create_new_dating_sequence(seq: Sequence) -> Arc<AtomicSequence> {
        Arc::new(AtomicSequence::from(seq))
    }
    
    #[test]
    fn test_next_range() {
        let wait_strategy = BlockingWaitStrategy::new();
        let sequencer = SingleProducerSequencer::new(wait_strategy, 8);
        
        let gating_seq = create_new_dating_sequence(100);
        let sequencer = {
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
        let sequencer = {
            let mut s = sequencer;
            s.add_gating_sequence(gating_seq.clone());
            s
        };
        
        let (low, high) = sequencer.next(5);
        sequencer.publish(low, high);
        
        let cursor = sequencer.get_cursor();
        assert_eq!(cursor.load(), high);
    }
}
