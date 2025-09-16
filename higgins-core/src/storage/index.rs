use std::fmt::{self, Debug};
use std::ops::{Deref, Index as StdIndex};

use bytes::{Buf, BufMut as _, BytesMut};

mod default;
pub mod directory;
mod error;
mod file;
mod index_reader;
use default::DefaultIndex;

pub use error::IndexError;

/// A view into a slice of bytes that represent a
/// packed DefaultIndex.
pub struct IndexView<'a>(&'a [u8]);

impl<'a> IndexView<'a> {
    /// Creates a new index view from a byte slice
    /// Slice must be exactly DefaultIndex::size() (16 bytes) long
    pub fn new(data: &'a [u8]) -> Self {
        debug_assert!(
            data.len() == DefaultIndex::size(),
            "DefaultIndex data must be exactly {} bytes",
            DefaultIndex::size()
        );
        Self(data)
    }

    pub fn offset(&self) -> u32 {
        let mut buf = &self.0[0..4];
        buf.get_u32()
    }

    pub fn object_key(&self) -> [u8; 16] {
        self.0[4..20].try_into().unwrap()
    }

    pub fn position(&self) -> u32 {
        let mut buf = &self.0[20..24];
        buf.get_u32()
    }

    pub fn timestamp(&self) -> u64 {
        let mut buf = &self.0[24..32];
        buf.get_u64()
    }

    pub fn size(&self) -> u64 {
        let mut buf = &self.0[32..40];
        buf.get_u64()
    }

    pub fn to_index(self) -> DefaultIndex {
        DefaultIndex {
            offset: self.offset(),
            position: self.position(),
            object_key: self.object_key(),
            timestamp: self.timestamp(),
            size: self.size(),
        }
    }
}

impl std::fmt::Display for IndexView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "offset: {}, position: {}, timestamp: {}, object_key: {}",
            self.offset(),
            self.position(),
            self.timestamp(),
            uuid::Uuid::from_bytes(self.object_key())
        )
    }
}

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Default)]
pub struct IndexesMut {
    buffer: BytesMut,
    // saved_count: u32,
    // base_position: u32,
}

impl IndexesMut {
    /// Creates a new empty container
    pub fn empty() -> Self {
        Self {
            buffer: BytesMut::new(),
            // saved_count: 0,
            // base_position: 0,
        }
    }

    /// Creates indexes from bytes
    pub fn from_bytes(indexes: BytesMut) -> Self {
        Self {
            buffer: indexes,
            // saved_count: 0,
            // base_position,
        }
    }

    /// Inserts a new index at the end of buffer
    #[cfg(test)]
    pub fn insert(
        &mut self,
        offset: u32,
        object_key: [u8; 16],
        position: u32,
        timestamp: u64,
        size: u64,
    ) {
        self.buffer.put_u32(offset);
        self.buffer.put_slice(&object_key);
        self.buffer.put_u32(position);
        self.buffer.put_u64(timestamp);
        self.buffer.put_u64(size);
    }

    /// Appends another slice of indexes to this one.
    #[cfg(test)]
    pub fn append_slice(&mut self, other: &[u8]) {
        self.buffer.put_slice(other);
    }

    /// Gets the number of indexes in the container
    pub fn count(&self) -> u32 {
        tracing::trace!("Len: {}", self.buffer.len());
        self.buffer.len() as u32 / DefaultIndex::size() as u32
    }

    /// Checks if the container is empty
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Gets a view of the DefaultIndex at the specified index
    pub fn get(&self, index: u32) -> Option<IndexView> {
        if index >= self.count() {
            return None;
        }

        let start = index as usize * DefaultIndex::size();
        let end = start + DefaultIndex::size();

        if end <= self.buffer.len() {
            Some(IndexView::new(&self.buffer[start..end]))
        } else {
            None
        }
    }

    /// Gets a last index
    pub fn last(&self) -> Option<IndexView> {
        if self.count() == 0 {
            return None;
        }

        Some(IndexView::new(
            &self.buffer[(self.count() - 1) as usize * DefaultIndex::size()..],
        ))
    }

    /// Finds an index by timestamp using binary search
    /// If an exact match isn't found, returns the index with the nearest timestamp
    /// that is greater than or equal to the requested timestamp
    pub fn find_by_timestamp(&self, timestamp: u64) -> Option<IndexView> {
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
        let mut result: Option<IndexView> = None;

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
impl StdIndex<usize> for IndexesMut {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let start = index * DefaultIndex::size();
        let end = start + DefaultIndex::size();
        &self.buffer[start..end]
    }
}

impl Deref for IndexesMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl fmt::Debug for IndexesMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.count();

        if count == 0 {
            return write!(f, "IndexesMut {{ count: 0, indexes: [] }}");
        }

        writeln!(f, "IndexesMut {{")?;
        writeln!(f, "    count: {count},")?;
        writeln!(f, "    indexes: [")?;

        for i in 0..count {
            if let Some(index) = self.get(i) {
                writeln!(
                    f,
                    "        {{ offset: {}, position: {}, timestamp: {} }},",
                    index.offset(),
                    index.position(),
                    index.timestamp()
                )?;
            }
        }

        writeln!(f, "    ]")?;
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn create_index_view(
        offset: u32,
        object_key: [u8; 16],
        position: u32,
        timestamp: u64,
        size: u64,
    ) -> IndexView<'static> {
        // Use a Vec<u8> to hold the data and leak it to get a 'static lifetime
        let mut buffer = Vec::with_capacity(DefaultIndex::size());
        buffer.extend_from_slice(&offset.to_be_bytes());
        buffer.extend_from_slice(&object_key);
        buffer.extend_from_slice(&position.to_be_bytes());
        buffer.extend_from_slice(&timestamp.to_be_bytes());
        buffer.extend_from_slice(&size.to_be_bytes());
        let buffer: &'static [u8] = Box::leak(Box::new(buffer)).as_slice();
        IndexView::new(buffer)
    }

    fn make_index_buffer(
        offset: u32,
        object_key: [u8; 16],
        position: u32,
        timestamp: u64,
        size: u64,
    ) -> BytesMut {
        let mut buffer = BytesMut::with_capacity(DefaultIndex::size());
        buffer.put_u32(offset);
        buffer.put_slice(&object_key);
        buffer.put_u32(position);
        buffer.put_u64(timestamp);
        buffer.put_u64(size);
        buffer
    }

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
    fn test_index_view_new() {
        let buffer = make_index_buffer(1, [0; 16], 100, 1234567890, 1);

        let view = IndexView::new(&buffer[..]);
        assert_eq!(view.offset(), 1);
        assert_eq!(view.position(), 100);
        assert_eq!(view.timestamp(), 1234567890);
    }

    #[test]
    fn test_index_view_to_index() {
        let view = create_index_view(1, [0; 16], 100, 1234567890, 1);
        let index = view.to_index();
        assert_eq!(index.offset, 1);
        assert_eq!(index.position, 100);
        assert_eq!(index.timestamp, 1234567890);
    }

    #[test]
    fn test_indexes_mut_empty() {
        let indexes = IndexesMut::empty();
        assert_eq!(indexes.count(), 0);
        assert!(indexes.is_empty());
        assert!(indexes.get(0).is_none());
        assert!(indexes.last().is_none());
    }

    #[test]
    fn test_indexes_mut_insert_and_get() {
        let mut indexes = IndexesMut::empty();
        indexes.insert(1, [0; 16], 100, 1234567890, 1);
        indexes.insert(2, [0; 16], 200, 1234567891, 1);

        assert_eq!(indexes.count(), 2);
        assert!(!indexes.is_empty());

        let view1 = indexes.get(0).unwrap();
        assert_eq!(view1.offset(), 1);
        assert_eq!(view1.position(), 100);
        assert_eq!(view1.timestamp(), 1234567890);

        let view2 = indexes.get(1).unwrap();
        assert_eq!(view2.offset(), 2);
        assert_eq!(view2.position(), 200);
        assert_eq!(view2.timestamp(), 1234567891);

        assert!(indexes.get(2).is_none());
    }

    #[test]
    fn test_indexes_mut_append_slice() {
        let mut indexes = IndexesMut::empty();

        let buffer = make_index_buffer(1, [0; 16], 100, 1234567890, 1);

        indexes.append_slice(&buffer[..]);
        assert_eq!(indexes.count(), 1);

        let view = indexes.get(0).unwrap();
        assert_eq!(view.offset(), 1);
        assert_eq!(view.position(), 100);
        assert_eq!(view.timestamp(), 1234567890);
    }

    #[test]
    fn test_indexes_mut_last() {
        let mut indexes = IndexesMut::empty();
        indexes.insert(1, [0; 16], 100, 1234567890, 1);
        indexes.insert(2, [0; 16], 200, 1234567891, 1);

        let last = indexes.last().unwrap();
        assert_eq!(last.offset(), 2);
        assert_eq!(last.position(), 200);
        assert_eq!(last.timestamp(), 1234567891);
    }

    #[test]
    fn test_indexes_mut_find_by_timestamp() {
        let mut indexes = IndexesMut::empty();
        indexes.insert(1, [0; 16], 100, 1000, 1);
        indexes.insert(2, [0; 16], 200, 2000, 1);
        indexes.insert(3, [0; 16], 300, 3000, 1);
        indexes.insert(4, [0; 16], 400, 4000, 1);

        // Exact match
        let view = indexes.find_by_timestamp(2000).unwrap();
        assert_eq!(view.timestamp(), 2000);

        // Find closest greater or equal
        let view = indexes.find_by_timestamp(2500).unwrap();
        assert_eq!(view.timestamp(), 3000);

        // Before first
        let view = indexes.find_by_timestamp(500).unwrap();
        assert_eq!(view.timestamp(), 1000);

        // After last
        assert!(indexes.find_by_timestamp(5000).is_none());

        // Empty indexes
        let empty = IndexesMut::empty();
        assert!(empty.find_by_timestamp(1000).is_none());
    }

    #[test]
    fn test_indexes_mut_index_and_deref() {
        let mut indexes = IndexesMut::empty();
        indexes.insert(1, [0; 16], 100, 1000, 1);

        let slice = &indexes[0];
        let view = IndexView::new(slice);
        assert_eq!(view.offset(), 1);
        assert_eq!(view.position(), 100);
        assert_eq!(view.timestamp(), 1000);

        let deref_slice: &[u8] = &indexes;
        assert_eq!(deref_slice.len(), DefaultIndex::size());
    }
}
