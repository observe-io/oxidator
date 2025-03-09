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
        Self{
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
            return Some(slowest_dependency)
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


