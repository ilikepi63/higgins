//! Subscription implementation for higgins.
//!
//! This is a  file-backed subscription model for effectively keeping track of the watermarks of
//! subcriptions in higgins. These watermarks are tracked per partition inside of the each
//! stream.
pub mod error;
pub mod file;

use file::SubscriptionFile;
use std::ops::Range;
use std::{path::PathBuf, sync::atomic::AtomicU64};
use tokio::sync::Notify;

use crate::subscription::error::SubscriptionError;
use higgins_shared::PartitionName;
/// Represents the current offset of a partition, as well as the maximum offset for that specific partition.
#[derive(Clone, Debug)]
pub struct PartitionOffsets {
    /// The ID for this specific partition.
    partition_id: PartitionName,
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
    fn of(key: &PartitionName, offset: Option<u64>, max_offset: Option<u64>) -> Self {
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
    last_index: u64,
    #[allow(unused)]
    // Allowing for now as we will need this for grabbing this condvar to make more jobs.
    condvar: Notify,
    pub client_counts: Vec<(u64, AtomicU64)>,

    // TODO: This will need to be moved to the file, when we decide on a data structure.
    partitions: Vec<PartitionOffsets>,
    file: SubscriptionFile,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("last_index", &self.last_index)
            .finish()
    }
}

type Offset = u64;

impl Subscription {
    pub fn new<P: AsRef<std::path::Path> + ?Sized>(path: &P) -> Self {
        let mut subscription_file = SubscriptionFile::new(path).unwrap();

        let mut partitions = subscription_file
            .get_partition_indexes()
            .unwrap()
            .iter()
            .map(|partition_offsets| {
                let partition_id = partition_offsets.get_partition_name()?;
                let last_completed_offset = partition_offsets.get_last_completed_offset()?;
                let max_offset = partition_offsets.get_max_offset()?;

                let amount_to_take = max_offset - last_completed_offset;

                Ok(PartitionOffsets {
                    partition_id,
                    last_completed_offset,
                    max_offset,
                    amount_to_take,
                })
            })
            .collect::<Result<Vec<PartitionOffsets>, SubscriptionError>>()
            .unwrap();

        partitions.sort();

        Self {
            last_index: 0,
            condvar: Notify::new(),
            client_counts: vec![],
            partitions: partitions,
            file: subscription_file,
        }
    }

    /// Add a partition to  this  Subscription, beginning at the given offset.
    pub fn add_partition(
        &mut self,
        key: &PartitionName,
        offset: Option<u64>,
        max_offset: Option<u64>,
    ) -> Result<(), SubscriptionError> {
        // Create the partition in the file.
        self.file
            .add_partition(key)
            .map_err(|err| SubscriptionError::SubscriptionFileCreationFailure(err.to_string()))?;

        // Set the max_offset and current offset of the partition.
        if let Some(max_offset) = offset {
            self.file.set_max_offset(key, &max_offset)?;
        }

        if let Some(offset) = offset {
            tracing::trace!("Acknowledging offset: {}", offset);
            self.file.acknowledge(
                key,
                &Range {
                    start: 0,
                    end: offset,
                },
            )?;
        }

        // Create and add it to this memory model.
        let new_partition = PartitionOffsets::of(key, offset, max_offset);

        self.partitions.push(new_partition);

        Ok(())
    }

    /// Retrieval of the partition for a specific key.
    pub fn get_partition(&self, key: &PartitionName) -> Option<PartitionOffsets> {
        self.partitions
            .iter()
            .find(|PartitionOffsets { partition_id, .. }| partition_id == key)
            .map(|p| p.clone())
    }

    /// Acknowledges the offset, adjusting the ranges that appear inside of this given
    /// BTree.
    pub fn acknowledge(
        &mut self,
        key: &PartitionName,
        offsets: &Range<u64>,
    ) -> Result<(), SubscriptionError> {
        // TODO: This is obviously O(n), might be better to take a look at a hashmap implementation for indexing.

        let partition = self
            .partitions
            .iter_mut()
            .find(|partition| partition.partition_id == *key);

        match partition {
            Some(partition) => {
                // Check that the offset matches, or is offset + 1.
                if offsets.start != partition.last_completed_offset {
                    return Err(SubscriptionError::AttemptToAcknowledgeOffsetWithoutAcknowledgingPreviousOffset(offsets.start, partition.last_completed_offset));
                }

                // then bump the partition
                partition.set_last_completed_offset(offsets.end);

                // Acknowledge the file backed partition here.
                self.file.acknowledge(key, offsets)?;

                // sort the partitions
                self.partitions.sort();

                Ok(())
            }
            None => Err(
                SubscriptionError::AttemptToAcknowledgePartitionThatDoesntExist(
                    String::from_utf8(key.0.to_vec()).unwrap(), // TODO: Probably shouldn't try to do this?
                    offsets.start,
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
    ) -> Result<Vec<(PartitionName, Offset)>, SubscriptionError> {
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
    pub fn set_max_offset(
        &mut self,
        key: &PartitionName,
        offset: u64,
    ) -> Result<(), SubscriptionError> {
        // How do we make this idempotent?

        self.file.set_max_offset(key, &offset)?;

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

    pub fn clear(&mut self) {
        self.client_counts.clear();
        self.partitions.clear();
    }

    /// Deletes this subscription, including the backing file for it.
    pub fn delete(&mut self) -> Result<(), SubscriptionError> {
        self.clear();

        self.file.delete()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_partition_success() {
        tracing_subscriber::fmt::init();
        let mut sub = Subscription::new("add_partition_success");
        let key = PartitionName::try_from("partition1").unwrap();

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

        sub.delete().unwrap();
    }

    #[test]
    fn test_add_partition_already_exists() {
        let mut sub = Subscription::new("partition_exists");
        let key = PartitionName::try_from("partition1").unwrap();

        // Add partition once
        assert!(sub.add_partition(&key, None, None).is_ok());

        // Try adding the same partition again
        matches!(
            sub.add_partition(&key, None, None),
            Err(SubscriptionError::SubscriptionPartitionAlreadyExists)
        );
        sub.delete().unwrap();
    }

    #[test]
    fn test_acknowledge_existing_partition() {
        let mut sub = Subscription::new("acknowledge_existing_partition");
        let key = PartitionName::try_from("partition1").unwrap();

        // Add partition
        assert!(sub.add_partition(&key, Some(5), Some(100)).is_ok());

        // Acknowledge offset 6 (adjacent to range 0..5)
        let acknowledge_result = sub.acknowledge(&key, &Range { start: 5, end: 6 });
        assert!(acknowledge_result.is_ok());

        // Verify the range is updated
        let PartitionOffsets {
            last_completed_offset,
            ..
        } = sub.get_partition(&key).unwrap();

        assert_eq!(last_completed_offset, 6);
        sub.delete().unwrap();
    }

    #[test]
    fn test_acknowledge_nonexistent_partition() {
        let mut sub = Subscription::new("test_acknowledge_nonexistent_partition");
        let key = PartitionName::try_from("nonexistent").unwrap();

        // Try acknowledging a partition that doesn't exist
        assert!(matches!(
            sub.acknowledge(&key, &Range { start: 10, end: 11 }),
            Err(SubscriptionError::AttemptToAcknowledgePartitionThatDoesntExist(_, 10))
        ));
        sub.delete().unwrap();
    }

    #[test]
    fn test_take_offsets() {
        let mut sub = Subscription::new("test_take_offsets");

        let key = PartitionName::try_from("partition1").unwrap();

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

        sub.delete().unwrap();
    }

    #[test]
    fn test_take_no_offsets_available() {
        let mut sub = Subscription::new("test_take_no_offsets_available");
        let key = PartitionName::try_from("partition1").unwrap();

        // Add partition with no unacknowledged offsets (max_offset 0)
        assert!(sub.add_partition(&key, None, Some(0)).is_ok());

        // Try to take offsets
        let offsets = sub.take(1, 5).expect("Failed to take offsets");
        assert!(offsets.is_empty(), "No offsets should be available");
        sub.delete().unwrap();
    }

    #[test]
    fn test_set_max_offset_existing_partition() {
        let mut sub = Subscription::new("test_set_max_offset_existing_partition");
        let key = PartitionName::try_from("partition1").unwrap();

        // Add partition
        assert!(sub.add_partition(&key, None, Some(50)).is_ok());

        // Set new max_offset
        assert!(sub.set_max_offset(&key, 100).is_ok());

        // Verify the max_offset was updated
        let PartitionOffsets { max_offset, .. } = sub.get_partition(&key).unwrap();

        assert_eq!(max_offset, 100);
        sub.delete().unwrap();
    }

    #[test]
    fn test_set_max_offset_nonexistent_partition() {
        let mut sub = Subscription::new("test_set_max_offset_nonexistent_partition");
        let key = PartitionName::try_from("nonexistent").unwrap();

        // Try setting max_offset for a non-existent partition
        let max_offset_result = sub.set_max_offset(&key, 100);
        dbg!(&max_offset_result);
        assert!(matches!(
            max_offset_result,
            Err(SubscriptionError::PartitionDoesNotExists)
        ));
        sub.delete().unwrap();
    }

    #[test]
    fn test_acknowledge_and_take_combination() {
        let mut sub = Subscription::new("test_acknowledge_and_take_combination");
        let key = PartitionName::try_from("partition1").unwrap();

        // Add partition with max_offset 10
        assert!(sub.add_partition(&key, None, Some(10)).is_ok());

        // Acknowledge some offsets
        assert!(sub.acknowledge(&key, &Range { start: 0, end: 1 }).is_ok());
        assert!(sub.acknowledge(&key, &Range { start: 1, end: 2 }).is_ok());

        // Take 3 offsets (should skip acknowledged offsets 2 and 4)
        let offsets = sub.take(1, 2).expect("Failed to take offsets");

        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets, vec![(key.clone(), 2), (key.clone(), 3)]);
        sub.delete().unwrap();
    }

    #[test]
    fn test_multiple_partitions() {
        let mut sub = Subscription::new("test_multiple_partitions");
        let key1 = PartitionName::try_from("partition1").unwrap();
        let key2 = PartitionName::try_from("partition2").unwrap();

        // Add two partitions
        assert!(sub.add_partition(&key1, None, Some(2)).is_ok());
        assert!(sub.add_partition(&key2, None, Some(2)).is_ok());

        // Take 4 offsets (should distribute across partitions)
        let offsets = sub.take(1, 4).expect("Failed to take offsets");
        assert_eq!(offsets.len(), 4);
        // Note: Without round-robin logic, exact distribution may vary
        assert!(offsets.iter().any(|(k, o)| k == &key1 && *o == 0));
        assert!(offsets.iter().any(|(k, o)| k == &key2 && *o == 0));
        sub.delete().unwrap();
    }

    #[test]
    fn test_can_read_subscription_from_file() {
        let sub_name = "test_can_read_subscription_from_file";

        let mut sub = Subscription::new(sub_name);

        [
            "partition_one",
            "partition_two",
            "partition_three",
            "partition_four",
        ]
        .iter()
        .enumerate()
        .for_each(|(i, partition_name)| {
            let partition_name = PartitionName::try_from(*partition_name).unwrap();
            sub.add_partition(&partition_name, Some(i as u64 * 2), Some(i as u64 * 10))
                .unwrap();
        });

        // Drop the sub.
        drop(sub);

        let sub = Subscription::new(sub_name);

        dbg!(&sub.partitions);

        assert_eq!(sub.client_counts.len(), 0);
        // assert_eq!(sub.partitions.len(), 4);

        for (i, partition) in sub.partitions.iter().enumerate() {
            println!(
                "Partition Name{}",
                String::from_utf8(partition.partition_id.0.to_vec()).unwrap()
            );

            assert_eq!(partition.last_completed_offset, i as u64 * 2);
            assert_eq!(partition.max_offset, i as u64 * 10);
        }
    }
}
