use std::{iter::repeat_with, sync::atomic::{AtomicU64, Ordering}};
use crate::traits::Sequence;

pub struct BitMap {
    slots: Vec<AtomicU64>,
    index_mask: i64,
    index_shift: i64,
    word_bits_mask: usize,
}

impl BitMap {
    pub fn new(capacity: usize) -> Self {
        let word_bits = std::mem::size_of::<AtomicU64>() * 8;
        let remainder = capacity % word_bits;
        let len = capacity / word_bits + if remainder == 0 { 0 } else { 1 };
        let slots = Vec::from_iter(repeat_with(AtomicU64::default).take(len));
        Self {
            slots,
            index_mask: (capacity - 1) as i64,
            index_shift: (word_bits as usize).ilog2() as i64,
            word_bits_mask: word_bits - 1,
        }
    }

    #[inline(always)]
    fn get_slot_and_bit_index(&self, sequence: Sequence) -> (&AtomicU64, usize) {
        let slot_index = ((sequence & self.index_mask) >> self.index_shift) as usize;
        let bit_index_in_slot = (sequence & self.index_mask) as usize & self.word_bits_mask;
        let slot = unsafe { self.slots.get_unchecked(slot_index) };
        (slot, bit_index_in_slot)
    }

    pub fn is_set(&self, sequence: Sequence) -> bool {
        let (slot, bit_index) = self.get_slot_and_bit_index(sequence);
        (slot.load(Ordering::SeqCst) & (1u64 << bit_index)) != 0
    }

    pub fn set(&self, sequence: Sequence) {
        let (slot, bit_index) = self.get_slot_and_bit_index(sequence);
        slot.fetch_or(1u64 << bit_index, Ordering::SeqCst);
    }

    pub fn unset(&self, sequence: Sequence) {
        let (slot, bit_index) = self.get_slot_and_bit_index(sequence);
        slot.fetch_and(!(1u64 << bit_index), Ordering::SeqCst);
    }
}
