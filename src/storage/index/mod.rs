/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/***
 * THIS IS NOT MY WORK -> THIS IS BORROWED FROM THE APACHE IGGY PROJECT.
 */

use std::fmt;
use std::ops::{Deref, Index as StdIndex};

use bytes::{Buf, BufMut, BytesMut};

pub mod index_reader;
pub mod index_writer;

pub struct Index {
    pub offset: u32,
    pub object_key: [u8; 16],
    pub position: u32,
    pub timestamp: u64,
    pub size: u64,
}

impl Index {
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn object_key(&self) -> [u8; 16] {
        self.object_key
    }

    pub fn position(&self) -> u32 {
        self.position
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(INDEX_SIZE);

        buf.put_u32(self.offset);
        buf.put_slice(&self.object_key);
        buf.put_u32(self.position);
        buf.put_u64(self.timestamp);
        buf.put_u64(self.size);

        buf
    }
}

/// A view into a slice of bytes that represent a
/// packed Index.
///
pub struct IndexView<'a>(&'a [u8]);

impl<'a> IndexView<'a> {
    /// Creates a new index view from a byte slice
    /// Slice must be exactly INDEX_SIZE (16 bytes) long
    pub fn new(data: &'a [u8]) -> Self {
        debug_assert!(
            data.len() == INDEX_SIZE,
            "Index data must be exactly {INDEX_SIZE} bytes"
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

    pub fn to_index(self) -> Index {
        Index {
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
const INDEX_SIZE: usize = std::mem::size_of::<u32>() // offset
    + std::mem::size_of::<u32>() // position
    + std::mem::size_of::<u64>() // timestamp
    + std::mem::size_of::<[u8; 16]>() // object_key
    + std::mem::size_of::<u64>(); // size

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Default)]
pub struct IndexesMut {
    buffer: BytesMut,
    saved_count: u32,
    base_position: u32,
}

impl IndexesMut {
    /// Creates a new empty container
    pub fn empty() -> Self {
        Self {
            buffer: BytesMut::new(),
            saved_count: 0,
            base_position: 0,
        }
    }

    /// Creates indexes from bytes
    pub fn from_bytes(indexes: BytesMut, base_position: u32) -> Self {
        Self {
            buffer: indexes,
            saved_count: 0,
            base_position,
        }
    }

    /// Decompose the container into its components
    pub fn decompose(mut self) -> (u32, BytesMut) {
        let base_position = self.base_position;
        let buffer = std::mem::replace(&mut self.buffer, BytesMut::new());
        (base_position, buffer)
    }

    /// Gets the size of all indexes messages
    pub fn messages_size(&self) -> u32 {
        self.last_position() - self.base_position
    }

    /// Gets the base position of the indexes
    pub fn base_position(&self) -> u32 {
        self.base_position
    }

    /// Sets the base position of the indexes
    pub fn set_base_position(&mut self, base_position: u32) {
        self.base_position = base_position;
    }

    /// Helper method to get the last index position
    pub fn last_position(&self) -> u32 {
        self.count()
            .checked_sub(1)
            .and_then(|index| self.get(index).map(|index_view| index_view.position()))
            .unwrap_or(0)
    }

    /// Creates a new container with the specified capacity
    pub fn with_capacity(capacity: usize, base_position: u32) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity * INDEX_SIZE),
            saved_count: 0,
            base_position,
        }
    }

    /// Gets the capacity of the buffer
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Inserts a new index at the end of buffer
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
    pub fn append_slice(&mut self, other: &[u8]) {
        self.buffer.put_slice(other);
    }

    /// Gets the number of indexes in the container
    pub fn count(&self) -> u32 {
        println!("Len: {}", self.buffer.len());
        self.buffer.len() as u32 / INDEX_SIZE as u32
    }

    /// Checks if the container is empty
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Gets the size of the buffer in bytes
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Gets a view of the Index at the specified index
    pub fn get(&self, index: u32) -> Option<IndexView> {
        if index >= self.count() {
            return None;
        }

        let start = index as usize * INDEX_SIZE;
        let end = start + INDEX_SIZE;

        if end <= self.buffer.len() {
            Some(IndexView::new(&self.buffer[start..end]))
        } else {
            None
        }
    }

    // Set the offset at the given index position
    pub fn set_offset_at(&mut self, index: u32, offset: u32) {
        let pos = index as usize * INDEX_SIZE;
        self.buffer[pos..pos + 4].copy_from_slice(&offset.to_be_bytes());
    }

    // Set the position at the given index
    pub fn set_position_at(&mut self, index: u32, position: u32) {
        let pos = (index as usize * INDEX_SIZE) + 4 + 16;
        self.buffer[pos..pos + 4].copy_from_slice(&position.to_be_bytes());
    }

    // Set the timestamp at the given index
    pub fn set_timestamp_at(&mut self, index: u32, timestamp: u64) {
        let pos = (index as usize * INDEX_SIZE) + 4 + 4 + 16;
        self.buffer[pos..pos + 8].copy_from_slice(&timestamp.to_be_bytes());
    }

    /// Gets a last index
    pub fn last(&self) -> Option<IndexView> {
        if self.count() == 0 {
            return None;
        }

        Some(IndexView::new(
            &self.buffer[(self.count() - 1) as usize * INDEX_SIZE..],
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

    /// Clears the container, removing all indexes but preserving already allocated buffer capacity
    pub fn clear(&mut self) {
        self.saved_count = 0;
        self.buffer.clear();
    }

    /// Gets the number of unsaved indexes
    pub fn unsaved_count(&self) -> u32 {
        self.count().saturating_sub(self.saved_count)
    }

    /// Gets the unsaved part of the index buffer
    pub fn unsaved_slice(&self) -> &[u8] {
        let start_pos = self.saved_count as usize * INDEX_SIZE;
        &self.buffer[start_pos..]
    }

    /// Mark all indexes as saved to disk
    pub fn mark_saved(&mut self) {
        self.saved_count = self.count();
    }

    /// Slices the container to return a view of a specific range of indexes
    pub fn slice_by_offset(&self, relative_start_offset: u32, count: u32) -> Option<IndexesMut> {
        let available_count = self.count().saturating_sub(relative_start_offset);
        let actual_count = std::cmp::min(count, available_count);

        if actual_count == 0 || relative_start_offset >= self.count() {
            return None;
        }

        let end_pos = relative_start_offset + actual_count;

        let start_byte = relative_start_offset as usize * INDEX_SIZE;
        let end_byte = end_pos as usize * INDEX_SIZE;
        let slice = BytesMut::from(&self.buffer[start_byte..end_byte]);

        if relative_start_offset == 0 {
            Some(IndexesMut::from_bytes(slice, self.base_position))
        } else {
            let position_offset = self.get(relative_start_offset - 1).unwrap().position();
            Some(IndexesMut::from_bytes(slice, position_offset))
        }
    }

    /// Loads indexes from cache based on timestamp
    pub fn slice_by_timestamp(&self, timestamp: u64, count: u32) -> Option<IndexesMut> {
        if self.count() == 0 {
            return None;
        }

        let start_index_pos = self.binary_search_position_for_timestamp_sync(timestamp)?;

        let available_count = self.count().saturating_sub(start_index_pos);
        let actual_count = std::cmp::min(count, available_count);

        if actual_count == 0 {
            return None;
        }

        let end_pos = start_index_pos + actual_count;

        let start_byte = start_index_pos as usize * INDEX_SIZE;
        let end_byte = end_pos as usize * INDEX_SIZE;
        let slice = BytesMut::from(&self.buffer[start_byte..end_byte]);

        let base_position = if start_index_pos > 0 {
            self.get(start_index_pos - 1).unwrap().position()
        } else {
            0
        };

        Some(IndexesMut::from_bytes(slice, base_position))
    }

    /// Find the position of the index with timestamp closest to (but not exceeding) the target
    fn binary_search_position_for_timestamp_sync(&self, target_timestamp: u64) -> Option<u32> {
        if self.count() == 0 {
            return None;
        }

        let last_index = self.get(self.count() - 1)?;
        if target_timestamp > last_index.timestamp() {
            return Some(self.count() - 1);
        }

        let first_index = self.get(0)?;
        if target_timestamp <= first_index.timestamp() {
            return Some(0);
        }

        let mut low = 0;
        let mut high = self.count() - 1;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_index = self.get(mid)?;
            let mid_timestamp = mid_index.timestamp();

            match mid_timestamp.cmp(&target_timestamp) {
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    high = mid - 1;
                }
            }
        }

        Some(low)
    }
}

impl StdIndex<usize> for IndexesMut {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let start = index * INDEX_SIZE;
        let end = start + INDEX_SIZE;
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
        writeln!(f, "    count: {},", count)?;
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
        let mut buffer = Vec::with_capacity(INDEX_SIZE);
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
        let mut buffer = BytesMut::with_capacity(INDEX_SIZE);
        buffer.put_u32(offset);
        buffer.put_slice(&object_key);
        buffer.put_u32(position);
        buffer.put_u64(timestamp);
        buffer.put_u64(size);
        buffer
    }

    #[test]
    fn test_index_methods() {
        let index = Index {
            offset: 1,
            position: 100,
            timestamp: 1234567890,
            object_key: [0; 16],
            size: 12,
        };
        assert_eq!(index.timestamp(), 1234567890);
        assert_eq!(index.position(), 100);
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
        assert_eq!(index.position(), 100);
        assert_eq!(index.timestamp(), 1234567890);
    }

    #[test]
    fn test_indexes_mut_empty() {
        let indexes = IndexesMut::empty();
        assert_eq!(indexes.count(), 0);
        assert!(indexes.is_empty());
        assert_eq!(indexes.size(), 0);
        assert_eq!(indexes.base_position(), 0);
        assert_eq!(indexes.last_position(), 0);
        assert!(indexes.get(0).is_none());
        assert!(indexes.last().is_none());
    }

    #[test]
    fn test_indexes_mut_with_capacity() {
        let indexes = IndexesMut::with_capacity(10, 100);
        assert_eq!(indexes.capacity(), 10 * INDEX_SIZE);
        assert_eq!(indexes.base_position(), 100);
        assert_eq!(indexes.count(), 0);
    }

    #[test]
    fn test_indexes_mut_insert_and_get() {
        let mut indexes = IndexesMut::empty();
        indexes.insert(1, [0; 16], 100, 1234567890, 1);
        indexes.insert(2, [0; 16], 200, 1234567891,1 );

        assert_eq!(indexes.count(), 2);
        assert_eq!(indexes.size(), 2 * INDEX_SIZE as u32);
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
    fn test_indexes_mut_set_methods() {
        let mut indexes = IndexesMut::empty();
        indexes.insert(1, [0; 16], 100, 1234567890, 1);

        indexes.set_offset_at(0, 10);
        indexes.set_position_at(0, 200);
        indexes.set_timestamp_at(0, 9876543210);

        let view = indexes.get(0).unwrap();
        assert_eq!(view.offset(), 10);
        assert_eq!(view.position(), 200);
        assert_eq!(view.timestamp(), 9876543210);
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
        indexes.insert(2, [0; 16], 200, 1234567891,1 );

        let last = indexes.last().unwrap();
        assert_eq!(last.offset(), 2);
        assert_eq!(last.position(), 200);
        assert_eq!(last.timestamp(), 1234567891);
    }

    #[test]
    fn test_indexes_mut_find_by_timestamp() {
        let mut indexes = IndexesMut::empty();
        indexes.insert(1, [0; 16], 100, 1000,1);
        indexes.insert(2, [0; 16], 200, 2000,1);
        indexes.insert(3, [0; 16], 300, 3000,1 );
        indexes.insert(4, [0; 16], 400, 4000,1 );

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
    fn test_indexes_mut_slice_by_offset() {
        let mut indexes = IndexesMut::with_capacity(3, 100);
        indexes.insert(1, [0; 16], 100, 1000,1 );
        indexes.insert(2, [0; 16], 200, 2000,1 );
        indexes.insert(3, [0; 16], 300, 3000,1);

        // Valid slice
        let slice = indexes.slice_by_offset(1, 2).unwrap();
        assert_eq!(slice.count(), 2);
        assert_eq!(slice.base_position(), 100);
        let view = slice.get(0).unwrap();
        assert_eq!(view.offset(), 2);
        assert_eq!(view.position(), 200);

        // Invalid offset
        assert!(indexes.slice_by_offset(4, 1).is_none());

        // Zero count
        assert!(indexes.slice_by_offset(1, 0).is_none());
    }

    #[test]
    fn test_indexes_mut_slice_by_timestamp() {
        let mut indexes = IndexesMut::with_capacity(3, 100);
        indexes.insert(1, [0; 16], 100, 1000,1 );
        indexes.insert(2, [0; 16], 200, 2000,1 );
        indexes.insert(3, [0; 16], 300, 3000,1 );

        // Valid slice
        let slice = indexes.slice_by_timestamp(1500, 2).unwrap();
        assert_eq!(slice.count(), 2);
        assert_eq!(slice.base_position(), 100);
        let view = slice.get(0).unwrap();
        assert_eq!(view.timestamp(), 2000);

        // Timestamp after last
        let slice = indexes.slice_by_timestamp(3500, 1).unwrap();
        assert_eq!(slice.count(), 1);
        assert_eq!(slice.get(0).unwrap().timestamp(), 3000);

        // Empty indexes
        let empty = IndexesMut::empty();
        assert!(empty.slice_by_timestamp(1000, 1).is_none());
    }

    #[test]
    fn test_indexes_mut_clear() {
        let mut indexes = IndexesMut::with_capacity(2, 100);
        indexes.insert(1, [0; 16], 100, 1000,1 );
        indexes.insert(2, [0; 16], 200, 2000,1 );

        indexes.clear();
        assert_eq!(indexes.count(), 0);
        assert!(indexes.is_empty());
        assert_eq!(indexes.capacity(), 2 * INDEX_SIZE);
        assert_eq!(indexes.base_position(), 100);
    }

    #[test]
    fn test_indexes_mut_unsaved() {
        let mut indexes = IndexesMut::empty();
        indexes.insert(1, [0; 16], 100, 1000,1 );
        indexes.insert(2, [0; 16], 200, 2000,1);

        assert_eq!(indexes.unsaved_count(), 2);
        assert_eq!(indexes.unsaved_slice().len(), 2 * INDEX_SIZE);

        indexes.mark_saved();
        assert_eq!(indexes.unsaved_count(), 0);
        assert_eq!(indexes.unsaved_slice().len(), 0);
    }

    #[test]
    fn test_indexes_mut_decompose() {
        let mut indexes = IndexesMut::with_capacity(1, 100);
        indexes.insert(1, [0; 16], 100, 1000,1);

        let (base_position, buffer) = indexes.decompose();
        assert_eq!(base_position, 100);
        assert_eq!(buffer.len(), INDEX_SIZE);
    }

    #[test]
    fn test_indexes_mut_index_and_deref() {
        let mut indexes = IndexesMut::empty();
        indexes.insert(1, [0; 16], 100, 1000,1 );

        let slice = &indexes[0];
        let view = IndexView::new(slice);
        assert_eq!(view.offset(), 1);
        assert_eq!(view.position(), 100);
        assert_eq!(view.timestamp(), 1000);

        let deref_slice: &[u8] = &indexes;
        assert_eq!(deref_slice.len(), INDEX_SIZE);
    }
}
