use std::marker::PhantomData;
use std::sync::{Arc};
use crate::traits::{AtomicSequence, Concurrent, DataStorage, EventConsumer, SequenceBarrier, Task};

struct Consumer<T, H: Task<T>, D: DataStorage<T>, S: SequenceBarrier> {
    handler: H,
    cursor: Arc<AtomicSequence>,
    phantom_data: PhantomData<T>,
    data_storage: D,
    sequence_barrier: S
}

impl<T, H: Task<T>, D: DataStorage<T>, S: SequenceBarrier> Concurrent for Consumer<T, H, D, S> {
    fn run(&self) {
        // TODO:
        // 1. loop
        // 2. get current cursor
        // 3. get sequence number till which i'm allowed to process using barrier
        // 4. for all slots returned, i.e. from current cursor + 1, till avaialble, run Handler business logic
        // 5. reset cursor
        // 6. signal to barrier, lock can be released, if any
    }
}


// impl<T, H: Task<T>, D: DataStorage<T>, S: SequenceBarrier, C: Concurrent> EventConsumer<S> for Consumer<T, H, D, S> {
//     type ConcurrentTask = C;
//
//     fn init_concurrent_task(
//         &self,
//         barrier: S,
//         data_storage: D
//     ) -> Self::ConcurrentTask {
//
//     }
//
//     fn get_consumer_cursor(&self) -> Arc<AtomicSequence> {
//         todo!()
//     }
// }