//! Subscription implementation for higgins.
//!
//! This is a  file-backed subscription model for effectively keeping track of the watermarks of
//! subcriptions in higgins. These watermarks are tracked per partition inside of the each
//! stream.
mod error;

use rkyv::{Archive, Deserialize, Serialize, deserialize, rancor::Error};
use rocksdb::{DB, Options, TransactionDB};
use std::path::PathBuf;

use crate::subscription::error::SubscriptionError;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
struct Range(u64, u64);

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
struct SubscriptionMetadata {
    max_offset: u64,
    ranges: Vec<Range>,
}

// NB: db is automatically closed at end of lifetime
pub struct Subscription(TransactionDB);

type Offset = u64;

impl Subscription {
    pub fn new(path: &PathBuf) -> Self {
        let db: TransactionDB = TransactionDB::open_default(path).unwrap();

        Self(db)
    }

    /// Add a partition to  this  Subscription, beginning at the given offset.
    pub fn add_partition(
        &self,
        key: &[u8],
        offset: Option<u64>,
        max_offset: Option<u64>,
    ) -> Result<(), SubscriptionError> {
        let txn = self.0.transaction();

        let value = txn.get(key);

        if value.is_ok_and(|val| val.is_some()) {
            return Err(SubscriptionError::SubscriptionPartitionAlreadyExists);
        };

        let mut ranges = Vec::new();

        if let Some(offset) = offset {
            ranges.push(Range(0, offset));
        };

        let metadata = SubscriptionMetadata {
            max_offset: max_offset.unwrap_or(0),
            ranges,
        };

        txn.put(key, rkyv::to_bytes::<rkyv::rancor::Error>(&metadata)?)?;

        txn.commit();

        Ok(())
    }

    /// Acknowledges the offset, adjusting the ranges that appear inside of this given
    /// BTree.
    pub fn acknowledge(&self, key: &[u8], offset: Offset) -> Result<(), SubscriptionError> {
        let txn = self.0.transaction();

        let serde_subscription_metadata = txn.get(key);

        let mut subscription_metadata = match serde_subscription_metadata {
            Ok(Some(val)) => {
                let val = rkyv::from_bytes::<SubscriptionMetadata, rkyv::rancor::Error>(&val)?;

                val
            }
            Ok(None) | Err(_) => {
                return Err(
                    SubscriptionError::AttemptToAcknowledgePartitionThatDoesntExist(
                        key.iter().map(|val| val.to_string()).collect::<String>(),
                        offset,
                    ),
                );
            }
        };

        let existing_ranges = subscription_metadata
            .ranges
            .iter_mut()
            .find(|range| range.0 <= offset + 1 && range.1 >= offset - 1);

        if let Some(range) = existing_ranges {
            apply_offset_to_range(range, offset);
        }

        // Otherwise we create a range inside of the Vec.
        let range = Range(offset, offset);

        subscription_metadata.ranges.push(range);

        // Sort the subcriptions.
        subscription_metadata.ranges.sort();

        Ok(())
    }

    /// Takes the next few offsets of a set of partitions
    pub fn take(count: u64) {}

    /// Sets the maximum offset for a partition.
    pub fn set_max_offset(key: &[u8]) {}
}

fn apply_offset_to_range(range: &mut Range, offset: u64) {
    if offset + 1 == range.0 {
        range.0 -= 1;
    }

    if offset - 1 == range.1 {
        range.1 += 1;
    }
}

/// A function that collapses missing ranges.
fn collapse_ranges(ranges: &mut [Range]) {

    let last_index = ranges.len() - 1;

    for index in 0..last_index {

        let curr_index = index;
        let next_index = index + 1; 

        if next_index == ranges.len() {
            break;
        }

        let (curr_range, next_range) = ranges.split_at_mut(next_index);

        let curr_range = curr_range[curr_range.len() - 1];
        let next_range = &ranges[next_index];

        if curr_range.1 + 1 == next_range.0 {
            curr_range.1 = next_range.1;
        }

    }



}

#[cfg(test)]
mod test {

    use std::env::temp_dir;

    use super::*;

    #[test]
    fn can_create_subscription_without_failure() {
        let path = temp_dir();

        let sub = Subscription::new(&path);
    }
}
