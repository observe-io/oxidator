use std::cell::Cell;
use std::cmp::min;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::coordinator::DefaultSequenceBarrier;
use crate::traits::{AtomicSequence, Sequence, Sequencer, WaitStrategy};
use crate::utils::min_sequence;
use crate::SequenceBarrier;

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

impl<W: WaitStrategy> Clone for SingleProducerSequencer<W> {
    fn clone(&self) -> Self {
        Self {
            cursor: self.cursor.clone(),
            wait_strategy: self.wait_strategy.clone(),
            gating_sequences: self.gating_sequences.clone(),
            is_done: self.is_done.clone(),
            buffer_size: self.buffer_size,
            current_producer_sequence: Cell::new(self.current_producer_sequence.get()),
            slowest_consumer_sequence: Cell::new(self.slowest_consumer_sequence.get()),
        }
    }
}

impl<W:WaitStrategy> Sequencer for SingleProducerSequencer<W> {
    type Barrier = DefaultSequenceBarrier<W>;
    fn next(&self, count: usize) -> (Sequence, Sequence) {
        let curr_producer_idx = self.current_producer_sequence.take();
        let mut consumer_idx = self.slowest_consumer_sequence.take();

        let low = curr_producer_idx + 1;
        let high = curr_producer_idx + count as i64;

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

    fn create_barrier(&self, gating_sequences: Vec<Arc<AtomicSequence>>) -> Self::Barrier {
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
        // while min_sequence(&self.gating_sequences) < self.current_producer_sequence.take() {
        //     self.wait_strategy.signal();
        // }
        // self.is_done.store(true, Ordering::SeqCst);

        self.is_done.store(true, Ordering::SeqCst);

        if !self.gating_sequences.is_empty() {
            let target = self.current_producer_sequence.take();
            let mut min_seq = min_sequence(&self.gating_sequences);

            let mut attempts = 0;
            while min_seq < target && attempts < 10 {
                self.wait_strategy.signal();
                std::thread::sleep(std::time::Duration::from_millis(50));
                min_seq = min_sequence(&self.gating_sequences);
                attempts += 1;
            }
        }

        for _ in 0..5 {
            self.wait_strategy.signal();
        }
    }
}

pub struct MultiProducerSequencer<W: WaitStrategy> {
    wait_strategy: Arc<W>,
    gating_sequences: Vec<Arc<AtomicSequence>>,
    buffer_size: usize,
    is_done: Arc<AtomicBool>,
    cursor: Arc<AtomicSequence>,

    highest_claimed_sequence: AtomicSequence,
    sequence_tracker: Arc<Mutex<Vec<i64>>>,
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
            sequence_tracker: Arc::new(Mutex::new(vec![0; buffer_size])),
        }
    }

    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }

    fn buffer_has_capacity(&self, count: usize) -> bool {
        let highest_claimed = self.highest_claimed_sequence.load();
        let min_seq = min_sequence(&self.gating_sequences);

        if highest_claimed < min_seq {
            return count <= self.buffer_size;
        }

        let used_slots = (highest_claimed - min_seq) as usize;

        self.buffer_size >= used_slots + count
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
            highest_claimed_sequence: AtomicSequence::default(),
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
                    return  (low, high);
                }
            }
        }
    }

    fn publish(&self, low: Sequence, high: Sequence) {
        let buffer_size = self.buffer_size as i64;

        let mut sequence_tracker = self.sequence_tracker.lock().unwrap();

        for i in low..=high {
            let idx = (i % buffer_size) as usize;
            sequence_tracker[idx] = 1;
        }

        let mut max_committable_sequence = self.cursor.load();
        for i in self.cursor.load() + 1..=self.highest_claimed_sequence.load() {
            let idx = (i % buffer_size) as usize;
            if sequence_tracker[idx] == 1 {
                max_committable_sequence = i;
            } else {
                break;
            }
        }

        if max_committable_sequence > self.cursor.load() {
            for i in self.cursor.load() + 1..=max_committable_sequence {
                let idx = (i % buffer_size) as usize;
                sequence_tracker[idx] = 0;
            }

            let mut curr = self.cursor.load();
            while curr < max_committable_sequence && !self.cursor.compare_exchange(curr, max_committable_sequence) {
                curr = self.cursor.load();
            }
        }

        drop(sequence_tracker);

        self.wait_strategy.signal();
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
        // while min_sequence(&self.gating_sequences) < self.cursor.load() {
        //     self.wait_strategy.signal();
        // }

        // self.wait_strategy.signal();
        // self.is_done.store(true, Ordering::SeqCst);
        self.is_done.store(true, Ordering::SeqCst);

        if !self.gating_sequences.is_empty() {
            let target = self.highest_claimed_sequence.load();
            let mut min_seq = min_sequence(&self.gating_sequences);

            let mut attempts = 0;
            while min_seq < target && attempts < 10 {
                self.wait_strategy.signal();
                std::thread::sleep(std::time::Duration::from_millis(50));
                min_seq = min_sequence(&self.gating_sequences);
                attempts += 1;
            }
        }

        for _ in 0..5 {
            self.wait_strategy.signal();
        }
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
        assert_eq!(cursor.load(), high);
    }
}
