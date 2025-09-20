use std::marker::PhantomData;
use std::ops::{Deref, Index as StdIndex};

#[allow(unused_imports)]
use bytes::BufMut as _;

mod default;
pub mod directory;
mod error;
mod file;

pub use error::IndexError;
use rkyv::Portable;

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
        tracing::trace!("Len: {}", self.buffer.len());
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
    use super::default::{DefaultIndex, ArchivedDefaultIndex};
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

    // #[test]
    // fn test_index_view_new() {
    //     let buffer = make_index_buffer(1, [0; 16], 100, 1234567890, 1);

    //     let view: IndexView<'_, DefaultIndex> = IndexView::new(&buffer[..]);
    //     assert_eq!(view.offset(), 1);
    //     assert_eq!(view.position(), 100);
    //     assert_eq!(view.timestamp(), 1234567890);
    // }

    #[test]
    fn test_indexes_mut_empty() {
        let indexes: IndexesMut<'_, ArchivedDefaultIndex> = IndexesMut::empty();
        assert_eq!(indexes.count(), 0);
        assert!(indexes.is_empty());
        assert!(indexes.get(0).is_none());
        assert!(indexes.last().is_none());
    }

    //     #[test]
    //     fn test_indexes_mut_insert_and_get() {
    //         let mut indexes = IndexesMut::empty();
    //         indexes.insert(1, [0; 16], 100, 1234567890, 1);
    //         indexes.insert(2, [0; 16], 200, 1234567891, 1);

    //         assert_eq!(indexes.count(), 2);
    //         assert!(!indexes.is_empty());

    //         let view1 = indexes.get(0).unwrap();
    //         assert_eq!(view1.offset(), 1);
    //         assert_eq!(view1.position(), 100);
    //         assert_eq!(view1.timestamp(), 1234567890);

    //         let view2 = indexes.get(1).unwrap();
    //         assert_eq!(view2.offset(), 2);
    //         assert_eq!(view2.position(), 200);
    //         assert_eq!(view2.timestamp(), 1234567891);

    //         assert!(indexes.get(2).is_none());
    //     }

    //     #[test]
    //     fn test_indexes_mut_append_slice() {
    //         let mut indexes = IndexesMut::empty();

    //         let buffer = make_index_buffer(1, [0; 16], 100, 1234567890, 1);

    //         indexes.append_slice(&buffer[..]);
    //         assert_eq!(indexes.count(), 1);

    //         let view = indexes.get(0).unwrap();
    //         assert_eq!(view.offset(), 1);
    //         assert_eq!(view.position(), 100);
    //         assert_eq!(view.timestamp(), 1234567890);
    //     }

    //     #[test]
    //     fn test_indexes_mut_last() {
    //         let mut indexes = IndexesMut::empty();
    //         indexes.insert(1, [0; 16], 100, 1234567890, 1);
    //         indexes.insert(2, [0; 16], 200, 1234567891, 1);

    //         let last = indexes.last().unwrap();
    //         assert_eq!(last.offset(), 2);
    //         assert_eq!(last.position(), 200);
    //         assert_eq!(last.timestamp(), 1234567891);
    //     }

    //     #[test]
    //     fn test_indexes_mut_find_by_timestamp() {
    //         let mut indexes = IndexesMut::empty();
    //         indexes.insert(1, [0; 16], 100, 1000, 1);
    //         indexes.insert(2, [0; 16], 200, 2000, 1);
    //         indexes.insert(3, [0; 16], 300, 3000, 1);
    //         indexes.insert(4, [0; 16], 400, 4000, 1);

    //         // Exact match
    //         let view = indexes.find_by_timestamp(2000).unwrap();
    //         assert_eq!(view.timestamp(), 2000);

    //         // Find closest greater or equal
    //         let view = indexes.find_by_timestamp(2500).unwrap();
    //         assert_eq!(view.timestamp(), 3000);

    //         // Before first
    //         let view = indexes.find_by_timestamp(500).unwrap();
    //         assert_eq!(view.timestamp(), 1000);

    //         // After last
    //         assert!(indexes.find_by_timestamp(5000).is_none());

    //         // Empty indexes
    //         let empty = IndexesMut::empty();
    //         assert!(empty.find_by_timestamp(1000).is_none());
    //     }

    //     #[test]
    //     fn test_indexes_mut_index_and_deref() {
    //         let mut indexes = IndexesMut::empty();
    //         indexes.insert(1, [0; 16], 100, 1000, 1);

    //         let slice = &indexes[0];
    //         let view = IndexView::new(slice);
    //         assert_eq!(view.offset(), 1);
    //         assert_eq!(view.position(), 100);
    //         assert_eq!(view.timestamp(), 1000);

    //         let deref_slice: &[u8] = &indexes;
    //         assert_eq!(deref_slice.len(), std::mem::size_of::<T>());
    //     }
    // }
}
