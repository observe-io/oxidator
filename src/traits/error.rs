use std::fmt;

#[derive(Debug)]
pub enum SizeError {
    CapacityExceeded { capacity: usize },
}

impl fmt::Display for SizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SizeError::CapacityExceeded { capacity } => {
                write!(f, "Vector capacity exceeded. Maximum capacity: {}", capacity)
            }
        }
    }
}

impl std::error::Error for SizeError {}
