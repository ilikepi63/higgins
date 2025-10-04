use rkyv::{Archive, Serialize};
use serde::Deserialize;

use crate::storage::index::Timestamped;

/// JoinedIndex represents the index metadata that one will use to
/// keep track of both offsets of each stream this is derived from.
#[derive(Serialize, Deserialize, Archive)]
pub struct JoinedIndex {
    /// The offset of the resultant index.
    offset: u64,
    /// The index of the right stream that this was joined from.
    pub right_offset: Option<u64>,
    /// The index of the left stream that this was joined from.
    pub left_offset: Option<u64>,
    /// The object key holding the resultant data from the joining.
    pub object_key: Option<[u8; 16]>,
    /// The timestamp for this index.
    pub timestamp: u64,
}

impl Timestamped for ArchivedJoinedIndex {
    fn timestamp(&self) -> u64 {
        self.timestamp.to_native()
    }
}

impl JoinedIndex {
    /// Creates a new instance of this given a right_offset only.
    pub fn new_with_right_offset(offset: u64, right_offset: u64, timestamp: u64) -> Self {
        Self {
            offset,
            right_offset: Some(right_offset),
            left_offset: None,
            object_key: None,
            timestamp,
        }
    }

    /// Creates a new instance of this given a left_offset.
    pub fn new_with_left_offset(offset: u64, left_offset: u64, timestamp: u64) -> Self {
        Self {
            offset,
            right_offset: None,
            left_offset: Some(left_offset),
            object_key: None,
            timestamp,
        }
    }
}
