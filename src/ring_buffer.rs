use std::cell::UnsafeCell;
use crate::traits::{Sequence, DataStorage};

#[derive(Debug)]
pub struct RingBuffer<T> {
    storage: Vec<UnsafeCell<T>>,
    size: usize,
}

impl <T: Send + Sync + Default> RingBuffer<T> {
    pub fn new(size: usize) -> Self {
        assert!(size != 0 && size & (size - 1) == 0, "ring buffer should be power of 2");
        let mut data = Vec::with_capacity(size);

        for _ in 0..size {
            data.push(UnsafeCell::new(T::default()));
        }

        Self {
            storage: data,
            size
        }
    }
}

impl<'a, T: Send + Sync> DataStorage<'a, T> for RingBuffer<T> {
    unsafe fn get_data(&self, s: Sequence) -> &T {
        let index = (s as usize) & (self.size - 1);
        let data = self.storage.get_unchecked(index);
        &*data.get()
    }

    unsafe fn get_data_mut(&self, s: Sequence) -> &'a mut T {
        let index = (s as usize) & (self.size - 1);
        let data = self.storage.get_unchecked(index);
        &mut *data.get()
    }

    fn capacity(&self) -> usize {
        self.size
    }

    fn len(&self) -> usize {
        self.size
    }
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Sync> Sync for RingBuffer<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    #[should_panic(expected = "ring buffer should be power of 2")]
    fn incorrect_initialization() {
        const SIZE: usize = 5;
        let ring_buffer: RingBuffer<()> = RingBuffer::new(SIZE);
    }

    #[test]
    fn initialization() {
        const SIZE: usize = 8;
        let ring_buffer: RingBuffer<()> = RingBuffer::new(SIZE);
        assert_eq!(ring_buffer.capacity(), 8);
    }

    #[test]
    fn single_thread_enqueue_dequeue() {
        let ring_buffer: RingBuffer<i32> = RingBuffer::new(8);
        for i in 0..8 {
            unsafe {
                let cell = ring_buffer.get_data_mut(i as Sequence);
                *cell = i;
            };
        }

        for i in 0..8 {
            unsafe {
                let cell = ring_buffer.get_data(i as Sequence);
                assert_eq!(*cell, i);
            };
        }
    }

    #[test]
    fn thread_safety() {
        let ring_buffer: RingBuffer<i64> = RingBuffer::new(8);
        let shared_buffer = Arc::new(ring_buffer);

        let buffer_producer = shared_buffer.clone();
        let producer = thread::spawn(move|| {
            for i in 0..9 {
                unsafe {
                    let cell = buffer_producer.get_data_mut(i);
                    *cell = i;
                };
            }
        });

        let buffer_consumer = shared_buffer.clone();
        let consumer = thread::spawn(move|| {
            thread::park();
            for i in 1..9 {
                unsafe {
                    let cell =  buffer_consumer.get_data_mut(i);
                    assert_eq!(*cell, i);
                };
            }
        });

        producer.join().unwrap();
        consumer.thread().unpark();
        consumer.join().unwrap();
    }
}