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
        let result = self.wait_strategy.wait_for(s, &*self.gating_sequences, || {
            let is_done = self.is_done.load(Ordering::Relaxed);
            is_done
        });
        result
    }

    fn signal(&self) {
        self.wait_strategy.signal();
    }
}

#[cfg(test)]
mod barrier_tests {
    use std::sync::Barrier;
    use super::*;
    use std::thread;
    use std::time::Duration;
    use crate::coordinator::{BlockingWaitStrategy, BusySpinWaitStrategy, YieldingWaitStrategy};
    use crate::traits::*;
    
    #[test]
    fn test_wait_returns_value_when_not_alerted() {
        let dep = Arc::new(AtomicSequence::from(15));
        let wait_strategy = Arc::new(BlockingWaitStrategy::new());
        let is_done = Arc::new(AtomicBool::new(false));
        let barrier = DefaultSequenceBarrier::new(vec![dep.clone()], wait_strategy, is_done);
        let input_sequence = 10;
        let result = barrier.wait_for(input_sequence);
        assert_eq!(result, Some(15));
    }
    
    #[test]
    fn test_wait_returns_none_when_alerted() {
        let dep = Arc::new(AtomicSequence::from(15));
        let wait_strategy = Arc::new(BlockingWaitStrategy::new());
        let is_done = Arc::new(AtomicBool::new(true));
        let barrier = DefaultSequenceBarrier::new(vec![dep.clone()], wait_strategy, is_done);
        let result = barrier.wait_for(11);
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_signal() {
        let dep = Arc::new(AtomicSequence::from(5));
        let wait_strategy = Arc::new(BlockingWaitStrategy::new());
        let is_done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(DefaultSequenceBarrier::new(vec![dep.clone()], wait_strategy, is_done));
        let barrier_clone = barrier.clone();
        
        let handle = thread::spawn(move || {
            loop {
                if let Some(seq) = barrier_clone.wait_for(10) {
                    break seq;
                }
            }
        });
        thread::sleep(Duration::from_millis(100));
        dep.store(15);
        barrier.signal();
        let result = handle.join().unwrap();
        assert_eq!(result, 15);
    }
}
