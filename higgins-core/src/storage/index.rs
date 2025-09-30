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
use rkyv::Portable;

pub use file::IndexFile;

pub trait Timestamped {
    fn timestamp(&self) -> u64;
}

fn get_archived_value<T: Portable>(buf: &[u8]) -> &T {
    // #SAFETY: These bytes are always guaranteed to be protected from external changes.
    unsafe { rkyv::access_unchecked::<T>(buf) }
}

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Default)]
pub struct IndexesMut<'a, T> {
    buffer: &'a [u8],
    _t: PhantomData<T>,
}

impl<'a, T: Portable + Timestamped> IndexesMut<'a, T> {
    /// Creates a new empty container
    pub fn empty() -> Self {
        Self {
            buffer: &[0; 0],
            _t: PhantomData,
        }
    }

    /// Gets the number of indexes in the container
    pub fn count(&self) -> u32 {
        println!("Len: {}", self.buffer.len());
        println!("Size: {}", size_of::<T>());
        self.buffer.len() as u32 / size_of::<T>() as u32
    }

    /// Checks if the container is empty
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Gets a view of the DefaultIndex at the specified index
    pub fn get(&self, index: u32) -> Option<&T> {
        if index >= self.count() {
            return None;
        }

        let start = index as usize * size_of::<T>();
        let end = start + size_of::<T>();

        if end <= self.buffer.len() {
            Some(get_archived_value(&self.buffer[start..end]))
        } else {
            None
        }
    }

    /// Gets a last index
    pub fn last(&self) -> Option<&T> {
        if self.count() == 0 {
            return None;
        }

        Some(get_archived_value(
            &self.buffer[(self.count() - 1) as usize * size_of::<T>()..],
        ))
    }

    /// Finds an index by timestamp using binary search
    /// If an exact match isn't found, returns the index with the nearest timestamp
    /// that is greater than or equal to the requested timestamp
    pub fn find_by_timestamp(&self, timestamp: u64) -> Option<&T> {
        if self.count() == 0 {
            return None;
        }

        let first_idx = self.get(0)?;
        if timestamp <= first_idx.timestamp() {
            return Some(first_idx);
        }

        let last_saved_idx = self.get(self.count() - 1)?;
        if timestamp > last_saved_idx.timestamp() {
            return None;
        }

        let mut left = 0;
        let mut right = self.count() as isize - 1;
        let mut result: Option<&T> = None;

        while left <= right {
            let mid = left + (right - left) / 2;
            let view = self.get(mid as u32).unwrap();
            let current_timestamp = view.timestamp();

            match current_timestamp.cmp(&timestamp) {
                std::cmp::Ordering::Equal => {
                    result = Some(view);
                    right = mid - 1;
                }
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    result = Some(view);
                    right = mid - 1;
                }
            }
        }

        result
    }
}
impl<'a, T> StdIndex<usize> for IndexesMut<'a, T> {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let start = index * size_of::<T>();
        let end = start + size_of::<T>();
        &self.buffer[start..end]
    }
}

impl<'a, T> Deref for IndexesMut<'a, T> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::default::{ArchivedDefaultIndex, DefaultIndex};
    use super::*;

    #[test]
    fn test_index_methods() {
        let index = DefaultIndex {
            offset: 1,
            position: 100,
            timestamp: 1234567890,
            object_key: [0; 16],
            size: 12,
        };
        assert_eq!(index.timestamp, 1234567890);
        assert_eq!(index.position, 100);
    }

    #[test]
    fn test_indexes_mut_empty() {
        let indexes: IndexesMut<'_, ArchivedDefaultIndex> = IndexesMut::empty();
        assert_eq!(indexes.count(), 0);
        assert!(indexes.is_empty());
        assert!(indexes.get(0).is_none());
        assert!(indexes.last().is_none());
    }
}
