use std::marker::PhantomData;
use std::ops::{Deref, Index as StdIndex};

#[allow(unused_imports)]
use bytes::BufMut as _;

mod default;
pub mod directory;
mod error;
mod file;
pub mod joined_index;
pub use error::IndexError;

pub use file::IndexFile;

pub trait Timestamped {
    fn timestamp(&self) -> u64;
}

pub trait WrapBytes<'a> {
    fn wrap(bytes: &'a [u8]) -> Self;
}

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Default)]
pub struct IndexesView<'a> {
    buffer: &'a [u8],
    element_size: usize,
}

impl<'a> IndexesView<'a> {
    /// Creates a new empty container
    pub fn empty() -> Self {
        Self {
            buffer: &[0; 0],
            element_size: 0,
        }
    }

    /// Gets the number of indexes in the container
    pub fn count(&self) -> usize {
        println!("Len: {}", self.buffer.len());
        println!("Size: {}", self.element_size);
        self.buffer.len() / self.element_size
    }

    /// Gets a view of the DefaultIndex at the specified index
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if index >= self.count() {
            return None;
        }

        let start = index as usize * self.element_size;
        let end = start + self.element_size;

        if end <= self.buffer.len() {
            Some(&self.buffer[start..end])
        } else {
            None
        }
    }

    /// Gets a last index
    pub fn last(&self) -> Option<&[u8]> {
        if self.count() == 0 {
            return None;
        }

        Some(&self.buffer[(self.count() - 1) as usize * self.element_size..])
    }

    // // Finds an index by timestamp using binary search
    // /// If an exact match isn't found, returns the index with the nearest timestamp
    // /// that is greater than or equal to the requested timestamp
    // pub fn find_by_timestamp(&self, timestamp: u64) -> Option<T> {
    //     if self.count() == 0 {
    //         return None;
    //     }

    //     let first_idx = self.get(0)?;
    //     if timestamp <= first_idx.timestamp() {
    //         return Some(first_idx);
    //     }

    //     let last_saved_idx = self.get(self.count() - 1)?;
    //     if timestamp > last_saved_idx.timestamp() {
    //         return None;
    //     }

    //     let mut left = 0;
    //     let mut right = self.count() as isize - 1;
    //     let mut result: Option<T> = None;

    //     while left <= right {
    //         let mid = left + (right - left) / 2;
    //         let view = self.get(mid as u32).unwrap();
    //         let current_timestamp = view.timestamp();

    //         match current_timestamp.cmp(&timestamp) {
    //             std::cmp::Ordering::Equal => {
    //                 result = Some(view);
    //                 right = mid - 1;
    //             }
    //             std::cmp::Ordering::Less => {
    //                 left = mid + 1;
    //             }
    //             std::cmp::Ordering::Greater => {
    //                 result = Some(view);
    //                 right = mid - 1;
    //             }
    //         }
    //     }

    //     result
    // }
}
impl<'a> StdIndex<usize> for IndexesView<'a> {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let start = index * self.element_size;
        let end = start + self.element_size;
        &self.buffer[start..end]
    }
}

impl<'a> Deref for IndexesView<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}
