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

use crate::storage::dereference::Reference;
use crate::storage::index::default::DefaultIndex;
use crate::storage::index::joined_index::JoinedIndex;
use crate::topography::{FunctionType, StreamDefinition};

/// A data type representing all of the different indexes that may be represented in higgins.
pub struct Index<'a> {
    index_type: IndexType,
    data: &'a [u8],
}

#[derive(Default, Clone)]
pub enum IndexType {
    #[default]
    Default,
    Join,
}

impl<'a> Index<'a> {
    // Constructors
    pub fn of(data: &'a [u8], index_type: IndexType) -> Self {
        Self { index_type, data }
    }

    /// Query for the timestamp of this given
    pub fn timestamp(&self) -> u64 {
        match self.index_type {
            IndexType::Default => DefaultIndex::of(self.data).timestamp(),
            IndexType::Join => JoinedIndex::of(self.data).timestamp(),
        }
    }

    /// Retrieve the underlying Reference data of this index.
    pub fn get_reference(&self) -> Reference {
        match self.index_type {
            IndexType::Default => DefaultIndex::of(self.data).reference(),
            IndexType::Join => JoinedIndex::of(self.data).reference(),
        }
    }
}

/// Returns the index size indicated by the stream definition. Each Stream definition will
/// decide which index to use, and therefore will decide how large each
pub fn index_size_from_stream_definition(def: &StreamDefinition) -> usize {
    match def.stream_type.as_ref() {
        Some(t) if matches!(t, FunctionType::Join) => {
            // JoinedIndex::size_of(def.)
            // TODO: we need to determine the amount of joins from the StreamDefinition, which is not implemented yet.
            todo!()
        }
        _ => DefaultIndex::size_of(),
    }
}

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Default)]
pub struct IndexesView<'a> {
    buffer: &'a [u8],
    element_size: usize,
    index_type: IndexType,
}

impl<'a> IndexesView<'a> {
    /// Creates a new empty container
    pub fn empty(index_type: IndexType) -> Self {
        Self {
            buffer: &[0; 0],
            element_size: 0,
            index_type,
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

    // Finds an index by timestamp using binary search
    /// If an exact match isn't found, returns the index with the nearest timestamp
    /// that is greater than or equal to the requested timestamp
    pub fn find_by_timestamp(&self, timestamp: u64) -> Option<Index> {
        if self.count() == 0 {
            return None;
        }

        let first_idx = self
            .get(0)
            .map(|data| Index::of(data, self.index_type.clone()))?;
        if timestamp <= first_idx.timestamp() {
            return Some(first_idx);
        }

        let last_saved_idx = self
            .get(self.count() - 1)
            .map(|data| Index::of(data, self.index_type.clone()))?;
        if timestamp > last_saved_idx.timestamp() {
            return None;
        }

        let mut left = 0;
        let mut right = self.count() as usize - 1;
        let mut result: Option<Index> = None;

        while left <= right {
            let mid = left + (right - left) / 2;
            let view = self
                .get(mid)
                .map(|data| Index::of(data, self.index_type.clone()))
                .unwrap();
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
