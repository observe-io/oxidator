
pub type Sequence = i64;



pub trait DataStorage<'a, T>: Send + Sync {
    unsafe fn get_data(&self, s: Sequence) -> &T;
    unsafe fn get_data_mut(&self, s: Sequence) -> &'a mut T;
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;
}








