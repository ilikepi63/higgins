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

    /// Gets the offset value from the view
    pub fn offset(&self) -> u32 {
        let mut buf = &self.0[0..4];
        buf.get_u32()
    }

    /// Gets the position value from the view
    pub fn position(&self) -> u32 {
        let mut buf = &self.0[4..8];
        buf.get_u32()
    }

    /// Gets the timestamp value from the view
    pub fn timestamp(&self) -> u64 {
        let mut buf = &self.0[8..16];
        buf.get_u64()
    }
}

impl std::fmt::Display for IndexView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "offset: {}, position: {}, timestamp: {}",
            self.offset(),
            self.position(),
            self.timestamp()
        )
    }
}
const INDEX_SIZE: usize = 16;

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Default)]
pub struct IggyIndexesMut {
    buffer: BytesMut,
    saved_count: u32,
    base_position: u32,
}

impl IggyIndexesMut {
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
        self.get(self.count() - 1)
            .map(|idx| idx.position())
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
    pub fn insert(&mut self, offset: u32, position: u32, timestamp: u64) {
        self.buffer.put_u32(offset);
        self.buffer.put_u32(position);
        self.buffer.put_u64(timestamp);
    }

    /// Appends another slice of indexes to this one.
    pub fn append_slice(&mut self, other: &[u8]) {
        self.buffer.put_slice(other);
    }

    /// Gets the number of indexes in the container
    pub fn count(&self) -> u32 {
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
        let pos = (index as usize * INDEX_SIZE) + 4;
        self.buffer[pos..pos + 4].copy_from_slice(&position.to_be_bytes());
    }

    // Set the timestamp at the given index
    pub fn set_timestamp_at(&mut self, index: u32, timestamp: u64) {
        let pos = (index as usize * INDEX_SIZE) + 8;
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
    pub fn slice_by_offset(
        &self,
        relative_start_offset: u32,
        count: u32,
    ) -> Option<IggyIndexesMut> {
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
            Some(IggyIndexesMut::from_bytes(slice, self.base_position))
        } else {
            let position_offset = self.get(relative_start_offset - 1).unwrap().position();
            Some(IggyIndexesMut::from_bytes(slice, position_offset))
        }
    }

    /// Loads indexes from cache based on timestamp
    pub fn slice_by_timestamp(&self, timestamp: u64, count: u32) -> Option<IggyIndexesMut> {
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

        Some(IggyIndexesMut::from_bytes(slice, base_position))
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

impl StdIndex<usize> for IggyIndexesMut {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let start = index * INDEX_SIZE;
        let end = start + INDEX_SIZE;
        &self.buffer[start..end]
    }
}

impl Deref for IggyIndexesMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl fmt::Debug for IggyIndexesMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.count();

        if count == 0 {
            return write!(f, "IggyIndexesMut {{ count: 0, indexes: [] }}");
        }

        writeln!(f, "IggyIndexesMut {{")?;
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
