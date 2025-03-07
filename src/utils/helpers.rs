use crate::traits::{AtomicSequence, Sequence};

pub fn min_sequence<T: AsRef<AtomicSequence>>(sequences: &[T]) -> Sequence {
    sequences.iter().map(|s| s.as_ref().load()).min().unwrap_or_default()
}
