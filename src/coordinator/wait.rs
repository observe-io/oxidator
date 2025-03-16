use std::sync::{Condvar, Mutex};
use std::thread;
use crate::traits::{AtomicSequence, Sequence, WaitStrategy};
use crate::utils::min_sequence;

pub struct BlockingWaitStrategy {
    guard: Mutex<bool>,
    cvar: Condvar,
}

pub struct BusySpinWaitStrategy;

pub struct YieldingWaitStrategy;

impl WaitStrategy for BlockingWaitStrategy {
    fn new() -> Self {
        Self {
            guard: Mutex::new(false),
            cvar: Condvar::new()
        }
    }

    fn wait_for<T: AsRef<AtomicSequence>, F: Fn() -> bool>(
        &self,
        sequence: Sequence,
        dependencies: &[T],
        check_alert: F) -> Option<Sequence>
    {
        // TODO:
        // 1. acquire lock
        let blocked = self.guard.lock().unwrap();
        // 2. check if alert has been triggered by producer/sequencer, if yes, return
        if check_alert() {
            return None
        }
        // 3. check slowest cursor among dependencies
        let slowest_dependency = min_sequence(dependencies);
        // 3. 1. if slowest dependency is less than or equal to current sequence, return slowest dependency
        if slowest_dependency >= sequence {
            return  Some(slowest_dependency)
        } else { // 3. 2. else 'wait' on condition var to be true
            let _unused = self.cvar.wait(blocked).unwrap();
        }
        
        // 4. release lock
        None
    }

    fn signal(&self) {
        let blocked = self.guard.lock().unwrap();
        self.cvar.notify_all();
        drop(blocked);
    }
}

impl WaitStrategy for BusySpinWaitStrategy {
    fn new() -> Self {
        Self{}
    }

    fn wait_for<T: AsRef<AtomicSequence>, F: Fn() -> bool>(
        &self,
        sequence: Sequence,
        dependencies: &[T],
        check_alert: F
    ) -> Option<Sequence> {
        // 1. check if alert has been triggered by producer/sequencer, if yes, return
        if check_alert() {
            return None
        }
        // 2. check slowest cursor among dependencies
        let slowest_dependency = min_sequence(dependencies);
        // 2. 1. if slowest dependency is less than or equal to current sequence, return slowest dependency
        if slowest_dependency >= sequence {
            return Some(slowest_dependency);
        }
        // 2. 2. else 'wait' on condition var to be true
        None
    }

    fn signal(&self) {}
}

impl WaitStrategy for YieldingWaitStrategy {
    fn new() -> Self {
        Self{}
    }

    fn wait_for<T: AsRef<AtomicSequence>, F: Fn() -> bool>(
        &self,
        sequence: Sequence,
        dependencies: &[T],
        check_alert: F
    ) -> Option<Sequence> {
        // 1. check if alert has been triggered by producer/sequencer, if yes, return
        if check_alert() {
            return None
        }
        // 2. check slowest cursor among dependencies
        let slowest_dependency = min_sequence(dependencies);
        // 3. 1. if slowest dependency is greater than or equal to current sequence, return slowest dependency
        if slowest_dependency >= sequence {
            return Some(slowest_dependency)
        }
        // 3. 2. else yield thread and return None
        thread::yield_now();
        None
    }

    fn signal(&self) {}
}

#[cfg(test)]
mod test_wait {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    
    // Tests for BlockingWaitStrategy Tests
    
    #[test]
    fn test_blocking_wait_strategy_ready() {
        let wait_strategy = BlockingWaitStrategy::new();
        let dep = AtomicSequence::from(10);
        let dependencies = vec![&dep];
        let result = wait_strategy.wait_for(5, &dependencies, || false);
        assert_eq!(result, Some(10));
    }
    
    #[test]
    fn test_blocking_wait_strategy_alert() {
        let wait_strategy = BlockingWaitStrategy::new();
        let dep = AtomicSequence::from(10);
        let dependencies = vec![&dep];
        let result = wait_strategy.wait_for(5, &dependencies, || true);
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_blocking_wait_strategy_signal() {
        let wait_strategy = Arc::new(BlockingWaitStrategy::new());
        let wait_strategy_clone = wait_strategy.clone();
        let handle = thread::spawn(move || {
            let dep = AtomicSequence::from(3);
            let dependencies = vec![&dep];
            let start = Instant::now();
            let result = wait_strategy_clone.wait_for(5, &dependencies, || false);
            assert!(start.elapsed() >= Duration::from_millis(50));
            result
        });
        
        thread::sleep(Duration::from_millis(100));
        wait_strategy.signal();
        let res = handle.join().unwrap();
        assert_eq!(res, None);
    }
    
    #[test]
    fn test_blocking_wait_strategy_eventually_ready() {
        let dep = Arc::new(AtomicSequence::from(3));
        let wait_strategy = Arc::new(BlockingWaitStrategy::new());
        
        let wait_strategy_clone = wait_strategy.clone();
        let dep_clone = dep.clone();
        let handle = thread::spawn(move || {
            loop {
                let dependency = vec![dep_clone.as_ref()];
                if let Some(seq) = wait_strategy_clone.wait_for(5, &dependency, || false) {
                    return seq;
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        
        thread::sleep(Duration::from_millis(50));
        dep.store(10);
        wait_strategy.signal();
        let res = handle.join().unwrap();
        assert_eq!(res, 10);
    }
    
    // BusySpinWaitStrategy Tests
    
    #[test]
    fn test_busy_spin_wait_strategy_alert() {
        let wait_strategy = Arc::new(BlockingWaitStrategy::new());
        let dep = AtomicSequence::from(15);
        let dependencies = vec![&dep];
        let result = wait_strategy.wait_for(10, &dependencies, || true);
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_busy_spin_wait_strategy_ready() {
        let wait_strategy = Arc::new(BlockingWaitStrategy::new());
        let dep = AtomicSequence::from(15);
        let dependencies = vec![&dep];
        let result = wait_strategy.wait_for(10, &dependencies, || false);
        assert_eq!(result, Some(15));
    }
    
    #[test]
    fn test_busy_spin_wait_strategy_not_ready() {
        let wait_strategy = Arc::new(BusySpinWaitStrategy::new());
        let dep = AtomicSequence::from(5);
        let dependencies = vec![&dep];
        let result = wait_strategy.wait_for(10, &dependencies, || false);
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_busy_spin_wait_strategy_eventually_ready() {
        let wait_strategy = Arc::new(BusySpinWaitStrategy::new());
        let wait_strategy_clone = wait_strategy.clone();
        let dep = Arc::new(AtomicSequence::from(3));
        let dep_clone = dep.clone();
        
        let handle = thread::spawn(move || {
            loop {
                let dependency = vec![dep_clone.as_ref()];
                if let Some(seq) = wait_strategy_clone.wait_for(10, &dependency, || false) {
                    return seq;
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        
        thread::sleep(Duration::from_millis(50));
        dep.store(10);
        let result = handle.join().unwrap();
        assert_eq!(result, 10);
    }
    
    // YieldingWaitStrategy Tests
    
    #[test]
    fn test_yielding_wait_strategy_ready() {
        let wait_strategy = Arc::new(YieldingWaitStrategy::new());
        let dep = AtomicSequence::from(20);
        let dependencies = vec![&dep];
        let rendencies = vec![&dep];
        let result = wait_strategy.wait_for(10, &dependencies, || true);
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_yielding_wait_strategy_not_ready() {
        let wait_strategy = Arc::new(YieldingWaitStrategy::new());
        let dep = AtomicSequence::from(5);
        let dependencies = vec![&dep];
        let result = wait_strategy.wait_for(10, &dependencies, || false);
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_yielding_wait_strategy_eventually_ready() {
        let wait_strategy = Arc::new(YieldingWaitStrategy::new());
        let wait_strategy_clone = wait_strategy.clone();
        let dep = Arc::new(AtomicSequence::from(3));
        let dep_clone = dep.clone();
        let handle = thread::spawn(move || {
            loop {
                let dependency = vec![dep_clone.as_ref()];
                if let Some(seq) = wait_strategy_clone.wait_for(10, &dependency, || false) {
                    return seq;
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        
        thread::sleep(Duration::from_millis(50));
        dep.store(10);
        let result = handle.join().unwrap();
        assert_eq!(result, 10);
    }
}
