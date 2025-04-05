use std::marker::PhantomData;
use std::sync::{Arc};
use crate::traits::{AtomicSequence, DataStorage, EventConsumer, SequenceBarrier, Task, Worker};

pub struct Consumer<T, F: Task<T>, D: DataStorage<T>, S: SequenceBarrier> {
    cursor: Arc<AtomicSequence>,
    data_storage: Arc<D>,
    sequence_barrier: S,
    task: F,
    phantom_data: PhantomData<T>,
}

impl<T: Send, F: Task<T> + Send, D: DataStorage<T>, S: SequenceBarrier> Worker for Consumer<T, F, D, S> {
    fn run(&self) {
        // TODO:
        // 1. get current cursor
        let cursor  = self.cursor.clone();
        let next = cursor.load() + 1;
        // 2. loop
        loop {
            // 3. get sequence number till which i'm allowed to process using barrier
            if let Some(upper_limit) = self.sequence_barrier.wait_for(next) {
                // 4. for all slots returned, i.e. from current cursor + 1, till avaialble, run Handler business logic
                for i in next..=upper_limit {
                    unsafe {
                        let slot = self.data_storage.get_data(i);
                        self.task.execute_task(&slot, i, i==upper_limit);
                    }
                }
                // 5. reset cursor
                self.cursor.store(upper_limit);
                // 6. signal to barrier, lock can be released, if any
                self.sequence_barrier.signal();
            } else {
                return; // early return without updating consumer cursor
            }
        }
    }
}

impl<T: Send, F: Task<T> + Send, D: DataStorage<T>, S: SequenceBarrier> EventConsumer<T> for Consumer<T, F, D, S> {
    type ConsumerWorker = Self;
    type Task = F;
    type DataStorage = D;
    type Barrier = S;

    fn init_concurrent_task(
        task: Self::Task,
        barrier: Self::Barrier,
        data_storage: Arc<Self::DataStorage>,
    ) -> Self::ConsumerWorker
    {
        Self {
            cursor: Arc::new(AtomicSequence::default()),
            task,
            data_storage,
            sequence_barrier: barrier,
            phantom_data: PhantomData::default(),
        }
    }

    fn get_consumer_cursor(&self) -> Arc<AtomicSequence> {
        self.cursor.clone()
    }
}
