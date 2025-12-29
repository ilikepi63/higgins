//! Subscription implementation for higgins.
//!
//! This is a  file-backed subscription model for effectively keeping track of the watermarks of
//! subcriptions in higgins. These watermarks are tracked per partition inside of the each
//! stream.
pub mod error;
pub mod file;

use std::{path::PathBuf, sync::atomic::AtomicU64};
use tokio::sync::Notify;

use crate::subscription::error::SubscriptionError;

/// Represents the current offset of a partition, as well as the maximum offset for that specific partition.
#[derive(Clone, Debug)]
pub struct PartitionOffsets {
    /// The ID for this specific partition.
    partition_id: Vec<u8>,
    /// The current watermark or offset that has been acknowledged for this offset.
    last_completed_offset: u64,
    /// The max offset, or the largest offset that exists within this partition.
    max_offset: u64,
    /// The amount of offsets that can be taken from this partition, this is effectively = `max_offfset - last_completed_offset`.
    amount_to_take: u64,
}

impl PartialEq for PartitionOffsets {
    fn eq(&self, other: &Self) -> bool {
        self.partition_id == other.partition_id
            && self.amount_to_take == other.amount_to_take
            && self.last_completed_offset == other.last_completed_offset
            && self.max_offset == other.max_offset
    }
}

impl Eq for PartitionOffsets {}

impl PartialOrd for PartitionOffsets {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.amount_to_take.cmp(&other.amount_to_take))
    }
}

impl Ord for PartitionOffsets {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.amount_to_take.cmp(&other.amount_to_take)
    }
}

impl PartitionOffsets {
    // Create this given a partition_id and optional defaults.
    fn of(key: &[u8], offset: Option<u64>, max_offset: Option<u64>) -> Self {
        let last_completed_offset = offset.unwrap_or(0);
        let max_offset = max_offset.unwrap_or(0);
        let mut new_partition = PartitionOffsets {
            partition_id: key.to_owned(),
            last_completed_offset,
            max_offset,
            amount_to_take: 0,
        };

        new_partition.recalculate_amount_to_take();

        new_partition
    }

    // helper method for calculating the amount_to_take.
    fn recalculate_amount_to_take(&mut self) {
        self.amount_to_take = self.max_offset - self.last_completed_offset;
    }

    // Set the last_completed_offset.
    fn set_last_completed_offset(&mut self, offset: u64) {
        self.last_completed_offset = offset;
        self.recalculate_amount_to_take();
    }
}

/// Represents a file that holds ranges of used subscription partitions.
#[allow(unused)]
pub struct SubscriptionPartitionFile {
    file: std::fs::File,
}

impl SubscriptionPartitionFile {
    pub fn create_with() {}
}

// TODO: should we make a lock per row?
pub struct Subscription {
    /// Path of the enclosing directory for this subscription.
    #[allow(unused)]
    path: PathBuf,
    last_index: u64,
    #[allow(unused)]
    // Allowing for now as we will need this for grabbing this condvar to make more jobs.
    condvar: Notify,
    pub client_counts: Vec<(u64, AtomicU64)>,

    // TODO: This will need to be moved to the file, when we decide on a data structure.
    partitions: Vec<PartitionOffsets>,
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
        Self {
            path: path.clone(),
            last_index: 0,
            condvar: Notify::new(),
            client_counts: vec![],
            partitions: vec![],
        }
    }

    /// Add a partition to  this  Subscription, beginning at the given offset.
    pub fn add_partition(
        &mut self,
        key: &[u8],
        offset: Option<u64>,
        max_offset: Option<u64>,
    ) -> Result<(), SubscriptionError> {
        let new_partition = PartitionOffsets::of(key, offset, max_offset);

        self.partitions.push(new_partition);

        Ok(())
    }

    /// Retrieval of the partition for a specific key.
    pub fn get_partition(&self, key: &[u8]) -> Option<PartitionOffsets> {
        self.partitions
            .iter()
            .find(|PartitionOffsets { partition_id, .. }| partition_id == key)
            .map(|p| p.clone())
    }

    /// Acknowledges the offset, adjusting the ranges that appear inside of this given
    /// BTree.
    pub fn acknowledge(&mut self, key: &[u8], offset: Offset) -> Result<(), SubscriptionError> {
        // Retrieve the partition via the key.
        // TODO: This is obviously O(n), might be better to take a look at a hashmap implementation for indexing.

        let partition = self
            .partitions
            .iter_mut()
            .find(|partition| partition.partition_id.iter().eq(key.iter()));

        match partition {
            Some(partition) => {
                // Check that the offset matches, or is offset + 1.
                if offset != partition.last_completed_offset {
                    return Err(SubscriptionError::AttemptToAcknowledgeOffsetWithoutAcknowledgingPreviousOffset(offset, partition.last_completed_offset));
                }

                // then bump the partition
                partition.set_last_completed_offset(offset + 1);

                // sort the partitions
                self.partitions.sort();

                Ok(())
            }
            None => Err(
                SubscriptionError::AttemptToAcknowledgePartitionThatDoesntExist(
                    String::from_utf8(key.to_owned()).unwrap(), // TODO: Probably shouldn't try to do this?
                    offset,
                ),
            ),
        }
    }
    /// Takes the next few offsets of a set of partitions
    /// TODO: implement round-robining for this.
    pub fn take(
        &mut self,
        client_id: u64,
        count: u64,
    ) -> Result<Vec<(Key, Offset)>, SubscriptionError> {
        // Client specific logic.

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

        // subscription specific logic
        // If it is more than zero, we need to iterate a little bit to see if we can retrieve more indices.
        let mut partition_offset_index = 0;
        let mut offset_count = count.load(std::sync::atomic::Ordering::Relaxed);

        let mut results = vec![];

        while offset_count > 0 && partition_offset_index < self.partitions.len() {
            let current_partition = self.partitions.get_mut(partition_offset_index);

            match current_partition {
                Some(partition_offset) => {
                    for i in partition_offset.last_completed_offset.clone()
                        ..partition_offset.max_offset.clone()
                    {
                        // Push the offset on the resultant vec.
                        results.push((partition_offset.partition_id.clone(), i));
                        // Update the current last_completed_offset.
                        partition_offset.set_last_completed_offset(i);

                        // If the offset count has gotten to zero, we break here and continue with the while loop.
                        offset_count -= 1;
                        if offset_count == 0 {
                            break;
                        }
                    }
                }
                None => {}
            }

            partition_offset_index += 1;
        }

        Ok(results)
    }

    /// Sets the maximum offset for a partition.
    /// Incrementing this effectively adds indexes to the subscription -> How do we then notify the underlying awaiter?
    pub fn set_max_offset(&mut self, key: &[u8], offset: u64) -> Result<(), SubscriptionError> {
        // How do we make this idempotent?

        let partition = self
            .partitions
            .iter_mut()
            .find(|PartitionOffsets { partition_id, .. }| partition_id == key);

        match partition {
            Some(partition) => {
                partition.max_offset = offset;
                Ok(())
            }
            None => Err(SubscriptionError::PartitionDoesNotExists),
        }
    }

    pub fn increment_amount_to_take(&mut self, client_id: u64, n: u64) {
        if let Some(count) = self.client_counts.iter_mut().find(|(c, _)| *c == client_id) {
            count.1.fetch_add(n, std::sync::atomic::Ordering::AcqRel);
        } else {
            self.client_counts.push((client_id, AtomicU64::new(n)));
        }
    }
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
    fn test_add_partition_success() {
        let (mut sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add a partition with offset and max_offset
        assert!(sub.add_partition(&key, Some(10), Some(100)).is_ok());

        // Verify the partition was added by checking stored metadata
        let PartitionOffsets {
            partition_id,
            last_completed_offset,
            max_offset,
            ..
        } = sub.get_partition(&key).unwrap();

        assert_eq!(partition_id, key);
        assert_eq!(max_offset, 100);
        assert_eq!(last_completed_offset, 10);
    }

    #[test]
    fn test_add_partition_already_exists() {
        let (mut sub, _temp_dir) = setup_subscription();
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
        let (mut sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add partition
        assert!(sub.add_partition(&key, Some(5), Some(100)).is_ok());

        // Acknowledge offset 6 (adjacent to range 0..5)
        let acknowledge_result = sub.acknowledge(&key, 5);
        assert!(acknowledge_result.is_ok());

        // Verify the range is updated
        let PartitionOffsets {
            last_completed_offset,
            ..
        } = sub.get_partition(&key).unwrap();

        assert_eq!(last_completed_offset, 6);
    }

    #[test]
    fn test_acknowledge_nonexistent_partition() {
        let (mut sub, _temp_dir) = setup_subscription();
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
        let (mut sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add partition
        assert!(sub.add_partition(&key, None, Some(50)).is_ok());

        // Set new max_offset
        assert!(sub.set_max_offset(&key, 100).is_ok());

        // Verify the max_offset was updated
        let PartitionOffsets { max_offset, .. } = sub.get_partition(&key).unwrap();

        assert_eq!(max_offset, 100);
    }

    #[test]
    fn test_set_max_offset_nonexistent_partition() {
        let (mut sub, _temp_dir) = setup_subscription();
        let key = b"nonexistent".to_vec();

        // Try setting max_offset for a non-existent partition
        let max_offset_result = sub.set_max_offset(&key, 100);
        dbg!(&max_offset_result);
        assert!(matches!(
            max_offset_result,
            Err(SubscriptionError::PartitionDoesNotExists)
        ));
    }

    #[test]
    fn test_acknowledge_and_take_combination() {
        let (mut sub, _temp_dir) = setup_subscription();
        let key = b"partition1".to_vec();

        // Add partition with max_offset 10
        assert!(sub.add_partition(&key, None, Some(10)).is_ok());

        // Acknowledge some offsets
        assert!(sub.acknowledge(&key, 0).is_ok());
        assert!(sub.acknowledge(&key, 1).is_ok());

        // Take 3 offsets (should skip acknowledged offsets 2 and 4)
        let offsets = sub.take(1, 2).expect("Failed to take offsets");

        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets, vec![(key.clone(), 2), (key.clone(), 3)]);
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
        assert_eq!(offsets.len(), 4);
        // Note: Without round-robin logic, exact distribution may vary
        assert!(offsets.iter().any(|(k, o)| k == &key1 && *o == 0));
        assert!(offsets.iter().any(|(k, o)| k == &key2 && *o == 0));
    }
}
