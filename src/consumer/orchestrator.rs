use crate::traits::{Orchestrator, Worker, WorkerHandle};
use std::thread::JoinHandle;
pub struct ConsumerOrchestrator {
    workers: Vec<Box<dyn Worker>>   
}

impl Orchestrator for ConsumerOrchestrator {
    fn with_workers(workers: Vec<Box<dyn Worker>>) -> Self {
        Self {
            workers,
        }
    }

    fn start(self) -> impl WorkerHandle {
        let mut handles = Vec::new();
        
        for worker in self.workers.into_iter() {
            let b = unsafe {
                std::mem::transmute::<Box<dyn Worker>, Box<dyn Worker + 'static>>(worker)
            };
            let handle = std::thread::spawn(move || b.run());
            handles.push(handle);
        }
        
        ConsumerWorkerHandle::new(handles)
    }
}

pub struct ConsumerWorkerHandle {
    thread_handles: Vec<JoinHandle<()>>
}

impl ConsumerWorkerHandle {
    pub fn new(handles: Vec<JoinHandle<()>>) -> Self {
        Self {
            thread_handles: handles
        }
    }
}

impl WorkerHandle for ConsumerWorkerHandle {
    fn join(&mut self) {
        let mut workers = std::mem::take(&mut self.thread_handles);
        for worker in workers.into_iter() {
            worker.join().unwrap();
        }
    }
}

impl Drop for ConsumerWorkerHandle {
    fn drop(&mut self) {
        self.join();
    }
}




