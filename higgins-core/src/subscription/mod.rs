//! Subscription implementation for higgins.
//!
//! This is a  file-backed subscription model for effectively keeping track of the watermarks of
//! subcriptions in higgins. These watermarks are tracked per partition inside of the each
//! stream.
pub mod error;

use rkyv::{Archive, Deserialize, Serialize};
use rocksdb::TransactionDB;
use std::{path::PathBuf, sync::atomic::AtomicU64};
use tokio::sync::Notify;

use crate::subscription::error::SubscriptionError;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Eq, PartialOrd, Ord)]
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

// TODO: should we make a lock per row?
pub struct Subscription {
    db: TransactionDB,
    last_index: u64,
    #[allow(unused)]
    // Allowing for now as we will need this for grabbing this condvar to make more jobs.
    condvar: Notify,
    pub client_counts: Vec<(u64, AtomicU64)>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("last_index", &self.last_index)
            .finish()
    }
}

type Offset = u64;
type Key = Vec<u8>; // Probably not correct to do this..

impl Subscription {
    pub fn new(path: &PathBuf) -> Self {
        // Init the RocksDB implementation.
        let db: TransactionDB = TransactionDB::open_default(path).unwrap();

        Self {
            db,
            last_index: 0,
            condvar: Notify::new(),
            client_counts: vec![],
        }
    }

    /// Add a partition to  this  Subscription, beginning at the given offset.
    pub fn add_partition(
        &self,
        key: &[u8],
        offset: Option<u64>,
        max_offset: Option<u64>,
    ) -> Result<(), SubscriptionError> {
        let txn = self.db.transaction();

        let value = txn.get(key);

        if value.is_ok_and(|val| val.is_some()) {
            return Err(SubscriptionError::SubscriptionPartitionAlreadyExists);
        };

        let ranges = vec![Range(0, offset.unwrap_or(0))];

        let metadata = SubscriptionMetadata {
            max_offset: max_offset.unwrap_or(0),
            ranges,
        };

        txn.put(key, rkyv::to_bytes::<rkyv::rancor::Error>(&metadata)?)?;

        txn.commit()?;

        Ok(())
    }

    /// Acknowledges the offset, adjusting the ranges that appear inside of this given
    /// BTree.
    pub fn acknowledge(&self, key: &[u8], offset: Offset) -> Result<(), SubscriptionError> {
        let txn = self.db.transaction();

        let serde_subscription_metadata = txn.get(key);

        let mut subscription_metadata = match serde_subscription_metadata {
            Ok(Some(val)) => rkyv::from_bytes::<SubscriptionMetadata, rkyv::rancor::Error>(&val)?,
            Ok(None) | Err(_) => {
                return Err(
                    SubscriptionError::AttemptToAcknowledgePartitionThatDoesntExist(
                        key.iter().map(|val| val.to_string()).collect::<String>(),
                        offset,
                    ),
                );
            }
        };

        let existing_ranges = subscription_metadata.ranges.iter_mut().find(|range| {
            range.0 <= offset.saturating_add(1) && range.1 >= offset.saturating_sub(1)
        });

        if let Some(range) = existing_ranges {
            apply_offset_to_range(range, offset);
        }

        // Otherwise we create a range inside of the Vec.
        let range = Range(offset, offset + 1);

        subscription_metadata.ranges.push(range);

        // Sort the subcriptions.
        subscription_metadata.ranges.sort();

        // let ranges = collapse_ranges(&mut subscription_metadata.ranges);

        // subscription_metadata.ranges = ranges;

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&subscription_metadata)?;

        txn.put(key, serialized)?;
        txn.commit()?;

        Ok(())
    }

    /// Takes the next few offsets of a set of partitions
    /// TODO: implement round-robining for this.
    pub fn take(
        &mut self,
        client_id: u64,
        count: u64,
    ) -> Result<Vec<(Key, Offset)>, SubscriptionError> {
        let mut result_vec = Vec::with_capacity(count.try_into().unwrap_or(10));

        let count: &mut AtomicU64 = if let Some((_, count)) = self
            .client_counts
            .iter_mut()
            .find(|(id, _)| *id == client_id)
        {
            tracing::trace!("Found a client count for given count number: {:#?}", count);

            count
        } else {
            let client_count = (client_id, AtomicU64::new(count));
            self.client_counts.push(client_count);

            &mut self
                .client_counts
                .iter_mut()
                .rev()
                .find(|(id, _)| *id == client_id)
                .unwrap()
                .1
        };

        tracing::trace!("Current count for subscription: {:#?}", count);

        // If it is more than zero, we need to iterate a little bit to see if we can retrieve more indices.
        for index in self.db.iterator(rocksdb::IteratorMode::Start) {
            let (key, item) = index?;
            let metadata = deserialize_subscription_metadata_or_else(&item)?;

            let mut extracted_offsets =
                extract_unacknowledged_keys_from_subscription_metadata(*count.get_mut(), &metadata)
                    .iter()
                    .map(|offset| (key.to_vec(), *offset))
                    .collect::<Vec<(Key, Offset)>>();

            tracing::trace!("Extracted offsets length: {}", extracted_offsets.len());
            tracing::trace!("Removed count: {:#?}", count);

            let offsets_length: u64 = extracted_offsets.len().try_into()?;

            let _result = count
                .fetch_update(
                    // TryInto::<u64>::try_into(extracted_offsets.len())?,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Relaxed,
                    |val| {
                        if val < offsets_length {
                            Some(0)
                        } else {
                            Some(val - offsets_length)
                        }
                    },
                )
                .unwrap();

            tracing::trace!("Removed count after subtraction: {:#?}", count);

            result_vec.append(&mut extracted_offsets);

            if count.load(std::sync::atomic::Ordering::Relaxed) < 1 {
                break;
            }
        }

        // // We remove the taken count from the amount to take.
        // self.amount_to_take += TryInto::<u64>::try_into(result_vec.len()).unwrap();

        Ok(result_vec)
    }

    /// Sets the maximum offset for a partition.
    /// Incrementing this effectively adds indexes to the subscription -> How do we then notify the underlying awaiter?
    pub fn set_max_offset(&self, key: &[u8], offset: u64) -> Result<(), SubscriptionError> {
        // How do we make this idempotent?.

        let serde_subscription_metadata = self.db.get(key);

        let mut subscription_metadata = match serde_subscription_metadata {
            Ok(Some(val)) => rkyv::from_bytes::<SubscriptionMetadata, rkyv::rancor::Error>(&val)?,
            Ok(None) | Err(_) => {
                return Err(
                    SubscriptionError::AttemptToAcknowledgePartitionThatDoesntExist(
                        key.iter().map(|val| val.to_string()).collect::<String>(),
                        offset,
                    ),
                );
            }
        };

        if subscription_metadata.max_offset < offset {
            subscription_metadata.max_offset = offset;

            let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&subscription_metadata)?;

            self.db.put(key, serialized)?;
        }

        Ok(())
    }

    pub fn increment_amount_to_take(&mut self, client_id: u64, n: u64) {
        if let Some(count) = self.client_counts.iter_mut().find(|(c, _)| *c == client_id) {
            count.1.fetch_add(n, std::sync::atomic::Ordering::AcqRel);
        } else {
            self.client_counts.push((client_id, AtomicU64::new(n)));
        }
    }
}

fn apply_offset_to_range(range: &mut Range, offset: u64) {
    if offset + 1 == range.0 {
        range.0 -= 1;
    }

    if offset == range.1 {
        range.1 += 1;
    }
}

/// A function that collapses missing ranges.
#[allow(unused)]
fn collapse_ranges(ranges: &[Range]) -> Vec<Range> {
    let last_index = ranges.len() - 1;

    let mut result = Vec::with_capacity(ranges.len());

    for index in 0..last_index {
        let next_index = index + 1;

        if next_index == ranges.len() {
            break;
        }

        let (curr_range, next_range) = ranges.split_at(next_index);

        if let (Some(curr_range), Some(next_range)) = (curr_range.last(), next_range.first()) {
            if curr_range.1 + 1 == next_range.0 {
                let range = Range(curr_range.0, next_range.1);
                result.push(range);
            } else {
                result.push(curr_range.clone());
                result.push(next_range.clone());
            }
        } else if let (None, Some(curr_range)) = (curr_range.last(), next_range.first()) {
            // Generally means that we've come to the last element.
            result.push(curr_range.clone());
        }
    }

    result
}

fn deserialize_subscription_metadata_or_else(
    val: &[u8],
) -> Result<SubscriptionMetadata, SubscriptionError> {
    let val = rkyv::from_bytes::<SubscriptionMetadata, rkyv::rancor::Error>(val)?;

    Ok(val)
}

fn extract_unacknowledged_keys_from_subscription_metadata(
    offsets_to_take: u64,
    metadata: &SubscriptionMetadata,
) -> Vec<Offset> {
    let mut index = 0;
    let mut accumulated_offsets = 0;
    let mut result_vec = Vec::with_capacity(offsets_to_take.try_into().unwrap_or(10));

    'outer: loop {
        let curr = metadata.ranges.get(index);
        let next = metadata.ranges.get(index + 1);

        match (curr, next) {
            (Some(curr), Some(next)) => {
                for r in curr.1..next.0 {
                    result_vec.push(r);
                    accumulated_offsets += 1;

                    if accumulated_offsets == offsets_to_take {
                        break;
                    }
                }

                if accumulated_offsets == offsets_to_take {
                    break 'outer;
                }
            }
            (Some(curr), None) => {
                for r in curr.1..metadata.max_offset {
                    result_vec.push(r);
                    accumulated_offsets += 1;
                    if accumulated_offsets == offsets_to_take {
                        break 'outer;
                    }
                }
            }
            (None, _) => break, // !unreachable()
        }

        index += 1;
    }

    result_vec
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_subscription() -> (Subscription, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let path = temp_dir.path().to_path_buf();
        let sub = Subscription::new(&path);
        (sub, temp_dir)
    }

    #[test]
    fn test_new_subscription() {
        let (sub, _temp_dir) = setup_subscription();
        // Verify that creating a subscription doesn't panic and opens the DB
        let key = b"test".to_vec();
        assert!(sub.db.get(&key).is_ok(), "Database should be accessible");
    }

    #[test]
    fn test_add_partition_success() {
        let (sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add a partition with offset and max_offset
        assert!(sub.add_partition(&key, Some(10), Some(100)).is_ok());

        // Verify the partition was added by checking stored metadata
        let metadata = sub
            .db
            .get(&key)
            .expect("Failed to read DB")
            .expect("Metadata not found");
        let metadata: SubscriptionMetadata =
            rkyv::from_bytes::<_, rkyv::rancor::Error>(&metadata).expect("Failed to deserialize");
        assert_eq!(metadata.max_offset, 100);
        assert_eq!(metadata.ranges, vec![Range(0, 10)]);
    }

    #[test]
    fn test_add_partition_already_exists() {
        let (sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add partition once
        assert!(sub.add_partition(&key, None, None).is_ok());

        // Try adding the same partition again
        matches!(
            sub.add_partition(&key, None, None),
            Err(SubscriptionError::SubscriptionPartitionAlreadyExists)
        );
    }

    #[test]
    fn test_acknowledge_existing_partition() {
        let (sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add partition
        assert!(sub.add_partition(&key, Some(5), Some(100)).is_ok());

        // Acknowledge offset 6 (adjacent to range 0..5)
        assert!(sub.acknowledge(&key, 6).is_ok());

        // Verify the range is updated
        let metadata = sub
            .db
            .get(&key)
            .expect("Failed to read DB")
            .expect("Metadata not found");
        let metadata: SubscriptionMetadata =
            rkyv::from_bytes::<_, rkyv::rancor::Error>(&metadata).expect("Failed to deserialize");
        assert_eq!(metadata.ranges, vec![Range(0, 5), Range(6, 7)]);
    }

    #[test]
    fn test_acknowledge_nonexistent_partition() {
        let (sub, _temp_dir) = setup_subscription();
        let key = b"nonexistent".to_vec();

        // Try acknowledging a partition that doesn't exist
        assert!(matches!(
            sub.acknowledge(&key, 10),
            Err(SubscriptionError::AttemptToAcknowledgePartitionThatDoesntExist(_, 10))
        ));
    }

    #[test]
    fn test_take_offsets() {
        let (mut sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add partition with max_offset 10
        assert!(sub.add_partition(&key, None, Some(10)).is_ok());

        // Take 5 offsets
        let offsets = sub.take(1, 5).expect("Failed to take offsets");
        assert_eq!(offsets.len(), 5);
        assert_eq!(
            offsets,
            vec![
                (key.clone(), 0_u64),
                (key.clone(), 1),
                (key.clone(), 2),
                (key.clone(), 3),
                (key, 4)
            ]
        );
    }

    #[test]
    fn test_take_no_offsets_available() {
        let (mut sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add partition with no unacknowledged offsets (max_offset 0)
        assert!(sub.add_partition(&key, None, Some(0)).is_ok());

        // Try to take offsets
        let offsets = sub.take(1, 5).expect("Failed to take offsets");
        assert!(offsets.is_empty(), "No offsets should be available");
    }

    #[test]
    fn test_set_max_offset_existing_partition() {
        let (sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add partition
        assert!(sub.add_partition(&key, None, Some(50)).is_ok());

        // Set new max_offset
        assert!(sub.set_max_offset(&key, 100).is_ok());

        // Verify the max_offset was updated
        let metadata = sub
            .db
            .get(&key)
            .expect("Failed to read DB")
            .expect("Metadata not found");
        let metadata: SubscriptionMetadata =
            rkyv::from_bytes::<_, rkyv::rancor::Error>(&metadata).expect("Failed to deserialize");
        assert_eq!(metadata.max_offset, 100);
    }

    #[test]
    fn test_set_max_offset_nonexistent_partition() {
        let (sub, _temp_dir) = setup_subscription();
        let key = b"nonexistent".to_vec();

        // Try setting max_offset for a non-existent partition
        assert!(matches!(
            sub.set_max_offset(&key, 100),
            Err(SubscriptionError::AttemptToAcknowledgePartitionThatDoesntExist(_, 100))
        ));
    }

    #[test]
    fn test_acknowledge_and_take_combination() {
        let (mut sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add partition with max_offset 10
        assert!(sub.add_partition(&key, None, Some(10)).is_ok());

        // Acknowledge some offsets
        assert!(sub.acknowledge(&key, 2).is_ok());
        assert!(sub.acknowledge(&key, 4).is_ok());

        // Take 3 offsets (should skip acknowledged offsets 2 and 4)
        let offsets = sub.take(1, 3).expect("Failed to take offsets");

        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets, vec![(key.clone(), 0), (key.clone(), 1), (key, 3)]);
    }

    #[test]
    fn test_multiple_partitions() {
        let (mut sub, _temp_dir) = setup_subscription();
        let key1 = b"partition1".to_vec();
        let key2 = b"partition2".to_vec();

        // Add two partitions
        assert!(sub.add_partition(&key1, None, Some(2)).is_ok());
        assert!(sub.add_partition(&key2, None, Some(2)).is_ok());

        // Take 4 offsets (should distribute across partitions)
        let offsets = sub.take(1, 4).expect("Failed to take offsets");
        println!("{offsets:?}");
        assert_eq!(offsets.len(), 4);
        // Note: Without round-robin logic, exact distribution may vary
        assert!(offsets.iter().any(|(k, o)| k == &key1 && *o == 0));
        assert!(offsets.iter().any(|(k, o)| k == &key2 && *o == 0));
    }
}
