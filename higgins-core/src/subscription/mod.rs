//! Subscription implementation for higgins.
//!
//! This is a  file-backed subscription model for effectively keeping track of the watermarks of
//! subcriptions in higgins. These watermarks are tracked per partition inside of the each
//! stream.
//!
//! The semantics of a Subscription file are as such:
//!
//! - For each partition p that has a range of values already published to it, you will have a PartitionOffsets value inside
//!   of the subscription.
//! - Each PartitionOffsets holds a range, which the `start` of the range denotes the already queried values whilst
//!   the `end` of the range denotes the values that are still to be read.
//!
//! An example is as such:
//!
//! - 0..0 -> you can read 0 from this PartitionOffsets
//! - 0..1 -> you can read 0..=1 from this Partition.
//! - 1..0 -> This partition is `complete`. When a partition gets acknowledged at u64::Max, there should possibly be some form of tomb stoning.
pub mod file;
pub mod helpers;

use file::SubscriptionFile;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The default amount of time a batch of offsets that has been taken from a
/// subscription stays "invisible" before, if it has not been acknowledged, it
/// is reset and made available again for redelivery.
pub const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);

use higgins_shared::{HigginsError, PartitionName, SubscriptionError};

/// Represents the unique ID for a subscription.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct SubscriptionId(Vec<u8>);

impl From<Vec<u8>> for SubscriptionId {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<SubscriptionId> for Vec<u8> {
    fn from(val: SubscriptionId) -> Self {
        val.0
    }
}

/// Represents the current offset of a partition, as well as the maximum offset for that specific partition.
#[derive(Clone, Debug)]
pub struct PartitionOffsets {
    /// The ID for this specific partition.
    pub partition_id: PartitionName,
    /// The current watermark or offset that has been acknowledged for this offset.
    pub start: u64,
    /// The max offset, or the largest offset that exists within this partition.
    pub end: u64,
    /// The highest offset that has been durably acknowledged. This is the
    /// watermark we roll `start` back to when an in-flight (taken but
    /// unacknowledged) batch exceeds its visibility timeout.
    pub committed: u64,
    /// When the current in-flight batch of offsets for this partition was first
    /// handed out to a consumer. `None` means there is nothing in flight.
    pub inflight_since: Option<Instant>,
    /// The client that most recently took the in-flight offsets, so that the
    /// redelivery demand can be restored to it once the visibility timeout
    /// elapses.
    pub inflight_by: Option<u64>,
}

impl PartialEq for PartitionOffsets {
    fn eq(&self, other: &Self) -> bool {
        self.partition_id == other.partition_id
            && self.start == other.start
            && self.end == other.end
    }
}

impl Eq for PartitionOffsets {}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for PartitionOffsets {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.amount_to_take().cmp(&other.amount_to_take()))
    }
}

impl Ord for PartitionOffsets {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.amount_to_take().cmp(&other.amount_to_take())
    }
}

impl PartitionOffsets {
    // Create this given a partition_id and optional defaults.
    fn of(key: &PartitionName, offset: u64, max_offset: u64) -> Self {
        let start = offset;
        let end = max_offset;

        PartitionOffsets {
            partition_id: key.to_owned(),
            start,
            end,
            committed: offset,
            inflight_since: None,
            inflight_by: None,
        }
    }

    // helper method for calculating the amount_to_take.
    pub fn amount_to_take(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    // Set the last_completed_offset.
    fn set_start(&mut self, offset: u64) {
        self.start = offset;
    }

    #[allow(unused)]
    fn set_end(&mut self, offset: u64) {
        self.end = offset;
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
#[derive(Debug)]
pub struct Subscription {
    pub client_counts: Vec<(u64, AtomicU64)>,

    // TODO: This will need to be moved to the file, when we decide on a data structure.
    pub partitions: Vec<PartitionOffsets>,
    file: SubscriptionFile,

    /// How long a batch of taken-but-unacknowledged offsets stays invisible
    /// before it is reset and made available for redelivery.
    visibility_timeout: Duration,
}

type Offset = u64;

impl Subscription {
    pub fn new<P: AsRef<std::path::Path> + ?Sized>(path: &P) -> Result<Self, HigginsError> {
        Self::new_with_timeout(path, DEFAULT_VISIBILITY_TIMEOUT)
    }

    /// Like [`Subscription::new`], but with an explicit visibility timeout.
    pub fn new_with_timeout<P: AsRef<std::path::Path> + ?Sized>(
        path: &P,
        visibility_timeout: Duration,
    ) -> Result<Self, HigginsError> {
        let mut subscription_file = SubscriptionFile::new(path)?;

        let mut partitions = subscription_file
            .get_partition_indexes()?
            .iter()
            .map(|partition_offsets| {
                let partition_id = partition_offsets.get_partition_name()?;
                let start = partition_offsets.get_last_completed_offset()?;
                let end = partition_offsets.get_max_offset()?;

                Ok(PartitionOffsets {
                    partition_id,
                    start,
                    end,
                    committed: start,
                    inflight_since: None,
                    inflight_by: None,
                })
            })
            .collect::<Result<Vec<PartitionOffsets>, SubscriptionError>>()?;

        partitions.sort();

        Ok(Self {
            client_counts: vec![],
            partitions,
            file: subscription_file,
            visibility_timeout,
        })
    }

    /// Returns the visibility timeout configured for this subscription.
    pub fn visibility_timeout(&self) -> Duration {
        self.visibility_timeout
    }

    /// Add a partition to  this  Subscription, beginning at the given offset.
    pub fn add_partition(
        &mut self,
        key: &PartitionName,
        offset: u64,
        max_offset: u64,
    ) -> Result<(), SubscriptionError> {
        tracing::trace!("Adding partition with max_offset: {:#?}", max_offset);

        // Create the partition in the file.
        self.file
            .add_partition(key)
            .map_err(|err| SubscriptionError::SubscriptionFileCreationFailure(err.to_string()))?;

        // Set the max_offset and current offset of the partition.
        // if let Some(max_offset) = max_offset {
        self.file.set_max_offset(key, &max_offset)?;
        // }
        //
        // if let Some(offset) = offset {
        tracing::trace!("Acknowledging offset: {}", offset);
        self.file.acknowledge(
            key,
            &Range {
                start: 0,
                end: offset,
            },
        )?;
        // }

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
            .cloned()
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

        tracing::info!("Retrieved partition: {:#?}", partition);

        match partition {
            Some(partition) => {
                // Check that the offset matches, or is offset + 1.
                // if offsets.start != partition.last_completed_offset {
                //     return Err(SubscriptionError::AttemptToAcknowledgeOffsetWithoutAcknowledgingPreviousOffset(offsets.start, partition.last_completed_offset));
                // }

                // then bump the partition
                // We set this to offsets.end + 1, so that if you acknowledge 0..0, your sub should be at 0..1, which means you'd have 1..1 and nothing
                // to pull here.
                tracing::trace!("offsets.end: {}", offsets.end);
                partition.set_start(offsets.end.saturating_add(1));

                partition.committed = offsets.end.saturating_add(1);

                if partition.start <= partition.committed {
                    partition.inflight_since = None;
                    partition.inflight_by = None;
                }

                tracing::trace!("Partition after acknowledgement: {:#?}", partition);

                // Acknowledge the file backed partition here.
                self.file.acknowledge(key, offsets)?;

                // sort the partitions
                self.partitions.sort();

                Ok(())
            }
            None => Err(
                SubscriptionError::AttemptToAcknowledgePartitionThatDoesntExist(
                    key.to_string().unwrap_or("NO KEY PRESENT".to_string()), // TODO: Probably shouldn't try to do this?
                    offsets.start,
                ),
            ),
        }
    }

    /// Tries to take {count} many offsets from this subscription.
    pub fn take(&mut self, count: u64) -> Result<Vec<(PartitionName, Offset)>, SubscriptionError> {
        tracing::debug!(
            "[SUBSCRIPTION TAKE] Taking {count} from subscription: {:#?}",
            self.partitions
        );

        tracing::debug!("Subscription state: {:#?}", self);

        let mut partition_offset_index = 0;
        let mut offset_count = count;

        let mut results = vec![];

        while offset_count > 0 && partition_offset_index < self.partitions.len() {
            let current_partition = self.partitions.get_mut(partition_offset_index);

            if let Some(partition_offset) = current_partition {
                for i in partition_offset.start..=partition_offset.end {
                    tracing::trace!(
                        "[SUBSCRIPTION TAKE] Taking partition_offset: {:#?}",
                        partition_offset
                    );

                    tracing::trace!(
                        "[SUBSCRIPTION TAKE] Setting last completed offset: {:#?}",
                        i
                    );

                    // Push the offset on the resultant vec.
                    results.push((partition_offset.partition_id.clone(), i));
                    // Update the current last_completed_offset.
                    partition_offset.set_start(i + 1);

                    // If the offset count has gotten to zero, we break here and continue with the while loop.
                    offset_count -= 1;
                    if offset_count == 0 {
                        break;
                    }
                }
            }

            tracing::debug!(
                "[SUBSCRIPTION TAKE] Taking {count} from subscription: {:#?}",
                self.partitions
            );

            partition_offset_index += 1;
        }

        tracing::debug!("Subscription state after take: {:#?}", self);

        tracing::debug!("returning offsets taken from subscription: {:#?}", results);

        Ok(results)
    }

    /// Similar to `take`, but returns a range of offsets as opposed to a partition name/offset pair.
    pub fn take_range(
        &mut self,
        count: u64,
    ) -> Result<Vec<(PartitionName, Range<u64>)>, SubscriptionError> {
        // tracing::debug!(
        //     "[SUBSCRIPTION TAKE] Taking {count} from subscription: {:#?}",
        //     self.partitions
        // );

        let mut partition_offset_index = 0;
        let mut offset_count = count;

        let mut results = vec![];

        while offset_count > 0 && partition_offset_index < self.partitions.len() {
            let current_partition = self.partitions.get_mut(partition_offset_index);

            tracing::info!("Retrieving current partition: {:#?}", current_partition);

            if let Some(partition_offset) = current_partition {
                tracing::trace!("{:#?}", partition_offset);
                tracing::trace!(
                    "{} {} {}",
                    partition_offset.end,
                    partition_offset.start,
                    partition_offset.end > partition_offset.start
                );

                if partition_offset.start > partition_offset.end {
                    tracing::trace!("BREAKING, WE HAVE HAD AN END HERE");
                    // If the end > start, this means by the semantics describe at the top, that this partition has already had everything
                    // consumed.
                    partition_offset_index += 1;
                    continue;
                }

                let end = partition_offset.end;

                tracing::info!("{}", end);

                results.push((
                    partition_offset.partition_id.clone(),
                    Range {
                        start: partition_offset.start,
                        end,
                    },
                ));
                tracing::info!("Pushed result.");

                tracing::info!(
                    "Offset count: {offset_count}, Start:{}  End: {end}",
                    partition_offset.start
                );

                offset_count =
                    offset_count.saturating_sub(end.saturating_sub(partition_offset.start));

                tracing::info!(
                    "AFTER: Offset count: {offset_count}, Start:{}  End: {end}",
                    partition_offset.start
                );
            }

            tracing::debug!(
                "[SUBSCRIPTION TAKE] Taking {count} from subscription: {:#?}",
                self.partitions
            );

            partition_offset_index += 1;
        }

        tracing::trace!("Returning: {:#?}", results);

        Ok(results)
    }

    /// Records that the given offsets have been handed out to `client_id` and
    /// are now in flight (awaiting acknowledgement). This starts the
    /// visibility-timeout clock for each affected partition.
    ///
    /// The clock for a partition is only (re)started when nothing was already in
    /// flight for it, so that the deadline is measured from the *first*
    /// unacknowledged delivery rather than being pushed out by every subsequent
    /// take.
    pub fn mark_inflight(&mut self, client_id: u64, offsets: &[(PartitionName, Offset)]) {
        let now = Instant::now();

        for partition in self.partitions.iter_mut() {
            let was_taken = offsets
                .iter()
                .any(|(key, _)| *key == partition.partition_id);

            if was_taken && partition.start > partition.committed {
                if partition.inflight_since.is_none() {
                    partition.inflight_since = Some(now);
                }
                partition.inflight_by = Some(client_id);
            }
        }
    }

    /// Resets any partition whose in-flight offsets have exceeded the visibility
    /// timeout, rolling its read frontier (`start`) back to the last
    /// acknowledged offset (`committed`) so the offsets become available for
    /// redelivery. The redelivery demand is restored to the client that
    /// originally took the offsets.
    ///
    /// Returns `true` if any partition was reset.
    pub fn reset_expired(&mut self) -> bool {
        let timeout = self.visibility_timeout;

        let mut restorations: Vec<(u64, u64)> = vec![];
        let mut did_reset = false;

        for partition in self.partitions.iter_mut() {
            let expired = partition
                .inflight_since
                .map(|since| since.elapsed() >= timeout)
                .unwrap_or(false);

            if expired && partition.start > partition.committed {
                let count = partition.start - partition.committed;

                tracing::trace!(
                    "[VISIBILITY] Resetting partition {:#?} from {} back to {} for redelivery",
                    partition.partition_id,
                    partition.start,
                    partition.committed
                );

                partition.set_start(partition.committed);

                if let Some(client) = partition.inflight_by {
                    restorations.push((client, count));
                }

                partition.inflight_since = None;
                partition.inflight_by = None;
                did_reset = true;
            }
        }

        if !did_reset {
            return false;
        }

        // Restore the redelivery demand for each affected client so the reactive
        // take loop pushes the reset offsets again.
        for (client, count) in restorations {
            self.increment_amount_to_take(client, count);
        }

        // `amount_to_take` changed for the reset partitions, so re-sort.
        self.partitions.sort();

        true
    }

    /// Removes the client count for a specific set.
    pub fn remove_client_count(&self, client: &u64, count: u64) {
        if let Some((_, value)) = self.client_counts.iter().find(|(c, _)| c == client) {
            value.fetch_sub(count, Ordering::AcqRel);
        }
    }

    /// Sets the maximum offset for a partition.
    /// Incrementing this effectively adds indexes to the subscription -> How do we then notify the underlying awaiter?
    pub fn set_end(&mut self, key: &PartitionName, offset: u64) -> Result<(), SubscriptionError> {
        tracing::trace!("Setting end: {}", offset);

        self.file.set_max_offset(key, &offset)?;

        let partition = self
            .partitions
            .iter_mut()
            .find(|PartitionOffsets { partition_id, .. }| partition_id == key);

        match partition {
            Some(partition) => {
                partition.end = offset;
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
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::expect_used)]
    use super::*;
    use std::panic::catch_unwind;

    #[test]
    fn test_add_partition_success() {
        tracing_subscriber::fmt::init();
        let mut sub = Subscription::new("add_partition_success").unwrap();
        let key = PartitionName::try_from("partition1").unwrap();

        // Add a partition with offset and max_offset
        assert!(sub.add_partition(&key, 10, 100).is_ok());

        // Verify the partition was added by checking stored metadata
        let PartitionOffsets {
            partition_id,
            start,
            end,
            ..
        } = sub.get_partition(&key).unwrap();

        assert_eq!(partition_id, key);
        assert_eq!(end, 100);
        assert_eq!(start, 10);

        sub.delete().unwrap();
    }

    #[test]
    fn test_add_partition_already_exists() {
        let mut sub = Subscription::new("partition_exists").unwrap();
        let key = PartitionName::try_from("partition1").unwrap();

        // Add partition once
        assert!(sub.add_partition(&key, 0, 0).is_ok());

        // Try adding the same partition again
        matches!(
            sub.add_partition(&key, 0, 0),
            Err(SubscriptionError::SubscriptionPartitionAlreadyExists)
        );
        sub.delete().unwrap();
    }

    #[test]
    fn test_acknowledge_existing_partition() {
        let sub_name = "acknowledge_existing_partition";

        let result = catch_unwind(|| {
            let mut sub = Subscription::new(sub_name).unwrap();
            let key = PartitionName::try_from("partition1").unwrap();

            // Add partition
            assert!(sub.add_partition(&key, 5, 100).is_ok());

            // Acknowledge offset 6 (adjacent to range 0..5)
            let acknowledge_result = sub.acknowledge(&key, &Range { start: 5, end: 6 });
            assert!(acknowledge_result.is_ok());

            // Verify the range is updated
            let PartitionOffsets { start, .. } = sub.get_partition(&key).unwrap();

            assert_eq!(start, 7);
        });

        std::fs::remove_file(sub_name).unwrap();

        result.unwrap();
    }

    #[test]
    fn test_acknowledge_nonexistent_partition() {
        let mut sub = Subscription::new("test_acknowledge_nonexistent_partition").unwrap();
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
        let mut sub = Subscription::new("test_take_offsets").unwrap();

        let key = PartitionName::try_from("partition1").unwrap();

        // Add partition with max_offset 10
        assert!(sub.add_partition(&key, 0, 10).is_ok());

        // Take 5 offsets
        let offsets = sub.take(5).expect("Failed to take offsets");
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
        let mut sub = Subscription::new("test_take_no_offsets_available").unwrap();
        let key = PartitionName::try_from("partition1").unwrap();

        // Add partition with no unacknowledged offsets (max_offset 0)
        assert!(sub.add_partition(&key, 0, 0).is_ok());

        // Try to take offsets
        let offsets = sub.take(5).expect("Failed to take offsets");
        assert_eq!(offsets.len(), 1, "No offsets should be available");
        sub.delete().unwrap();
    }

    #[test]
    fn test_set_max_offset_existing_partition() {
        let mut sub = Subscription::new("test_set_max_offset_existing_partition").unwrap();
        let key = PartitionName::try_from("partition1").unwrap();

        // Add partition
        assert!(sub.add_partition(&key, 0, 50).is_ok());

        // Set new max_offset
        assert!(sub.set_end(&key, 100).is_ok());

        // Verify the max_offset was updated
        let PartitionOffsets { end, .. } = sub.get_partition(&key).unwrap();

        assert_eq!(end, 100);
        sub.delete().unwrap();
    }

    #[test]
    fn test_set_max_offset_nonexistent_partition() {
        let mut sub = Subscription::new("test_set_max_offset_nonexistent_partition").unwrap();
        let key = PartitionName::try_from("nonexistent").unwrap();

        // Try setting max_offset for a non-existent partition
        let max_offset_result = sub.set_end(&key, 100);
        dbg!(&max_offset_result);
        assert!(matches!(
            max_offset_result,
            Err(SubscriptionError::PartitionDoesNotExists)
        ));
        sub.delete().unwrap();
    }

    #[test]
    fn test_acknowledge_and_take_combination() {
        let sub_name = "test_acknowledge_and_take_combination";

        let result = catch_unwind(|| {
            let mut sub = Subscription::new(sub_name).unwrap();

            let key = PartitionName::try_from("partition1").unwrap();

            // Add partition with max_offset 10
            assert!(sub.add_partition(&key, 0, 10).is_ok());

            // Acknowledge some offsets
            assert!(sub.acknowledge(&key, &Range { start: 0, end: 1 }).is_ok());
            assert!(sub.acknowledge(&key, &Range { start: 1, end: 2 }).is_ok());

            // Take 3 offsets (should skip acknowledged offsets 2 and 4)
            let offsets = sub.take(2).expect("Failed to take offsets");

            assert_eq!(offsets.len(), 2);
            assert_eq!(offsets, vec![(key.clone(), 3), (key.clone(), 4)]);
        });

        std::fs::remove_file(sub_name).unwrap();

        result.unwrap();
    }

    #[test]
    fn test_multiple_partitions() {
        let mut sub = Subscription::new("test_multiple_partitions").unwrap();
        let key1 = PartitionName::try_from("partition1").unwrap();
        let key2 = PartitionName::try_from("partition2").unwrap();

        // Add two partitions
        assert!(sub.add_partition(&key1, 0, 2).is_ok());
        assert!(sub.add_partition(&key2, 0, 2).is_ok());

        // Take 4 offsets (should distribute across partitions)
        let offsets = sub.take(4).expect("Failed to take offsets");
        assert_eq!(offsets.len(), 4);
        // Note: Without round-robin logic, exact distribution may vary
        assert!(offsets.iter().any(|(k, o)| k == &key1 && *o == 0));
        assert!(offsets.iter().any(|(k, o)| k == &key2 && *o == 0));
        sub.delete().unwrap();
    }

    #[test]
    fn test_can_read_subscription_from_file() {
        let sub_name = "test_can_read_subscription_from_file";

        let result = catch_unwind(|| {
            let mut sub = Subscription::new(sub_name).unwrap();

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
                sub.add_partition(&partition_name, i as u64 * 2, i as u64 * 10)
                    .unwrap();
            });

            // Drop the sub.
            drop(sub);

            let sub = Subscription::new(sub_name).unwrap();

            assert_eq!(sub.client_counts.len(), 0);
            assert_eq!(sub.partitions.len(), 4);

            for (i, partition) in sub.partitions.iter().enumerate() {
                assert_eq!(partition.start, (i as u64 * 2) + 1);
                assert_eq!(partition.end, i as u64 * 10);
            }

            sub
        });
        std::fs::remove_file(sub_name).unwrap();

        result.unwrap();
    }

    #[test]
    fn subscription_file_acknowledgement_works() {
        let sub_name = "subscription_file_acknowledgement_works";

        let result = catch_unwind(|| {
            let mut sub = Subscription::new(sub_name).unwrap();

            let partition_name = PartitionName::try_from("1").unwrap();

            // There is nothing in this subscription at this point.
            sub.add_partition(&partition_name, 0, 0).unwrap();

            let partitions = sub.take(10).unwrap();

            assert_eq!(partitions.len(), 1);

            sub.set_end(&partition_name, 1).unwrap();

            assert_eq!(sub.take(10).unwrap().len(), 1);

            assert_eq!(sub.take(10).unwrap().len(), 0);

            sub.acknowledge(&partition_name, &(0..1)).unwrap();

            assert_eq!(sub.take(10).unwrap().len(), 0);
        });

        std::fs::remove_file(sub_name).unwrap();

        result.unwrap();
    }

    #[test]
    fn test_acknowledge_and_take_combination_range() {
        let sub_name = "test_acknowledge_and_take_combination_range";

        let result = catch_unwind(|| {
            let mut sub = Subscription::new(sub_name).unwrap();

            let key = PartitionName::try_from("partition1").unwrap();

            // Add partition with max_offset 10
            assert!(sub.add_partition(&key, 0, 0).is_ok());

            // Take 3 offsets (should skip acknowledged offsets 2 and 4)
            let offsets = sub.take_range(1).expect("Failed to take offsets");

            assert_eq!(offsets, vec![(key.clone(), 0..0)]);

            // Acknowledge some offsets
            assert!(sub.acknowledge(&key, &Range { start: 0, end: 0 }).is_ok());

            // Take 3 offsets (should skip acknowledged offsets 2 and 4)
            let offsets = sub.take_range(1).expect("Failed to take offsets");

            debug_assert_eq!(offsets.len(), 0);

            sub.set_end(&key, 3).unwrap();

            // Take 3 offsets (should skip acknowledged offsets 2 and 4)
            let offsets = sub.take_range(5).expect("Failed to take offsets");

            dbg!(&offsets);

            assert_eq!(offsets.len(), 1);
            assert_eq!(offsets, vec![(key.clone(), 1..3)]);
        });

        std::fs::remove_file(sub_name).unwrap();

        result.unwrap();
    }

    #[test]
    fn visibility_timeout_resets_unacknowledged_offsets() {
        let sub_name = "visibility_timeout_resets_unacknowledged_offsets";

        let result = catch_unwind(|| {
            let mut sub =
                Subscription::new_with_timeout(sub_name, std::time::Duration::from_millis(50))
                    .unwrap();
            let key = PartitionName::try_from("p1").unwrap();

            // Offset 0 is available.
            sub.add_partition(&key, 0, 0).unwrap();

            // Take it and mark it in flight for client 7.
            let taken = sub.take(1).unwrap();
            assert_eq!(taken, vec![(key.clone(), 0)]);
            sub.mark_inflight(7, &taken);

            // Before the timeout elapses nothing is reset.
            assert!(!sub.reset_expired());
            // ...and the offset is no longer available.
            assert!(sub.take(1).unwrap().is_empty());

            // After the timeout elapses the offset is reset for redelivery.
            std::thread::sleep(std::time::Duration::from_millis(80));
            assert!(sub.reset_expired());

            // The redelivery demand was restored to client 7.
            let restored = sub
                .client_counts
                .iter()
                .find(|(c, _)| *c == 7)
                .map(|(_, n)| n.load(Ordering::Relaxed));
            assert_eq!(restored, Some(1));

            // The offset is available again.
            assert_eq!(sub.take(1).unwrap(), vec![(key.clone(), 0)]);
        });

        std::fs::remove_file(sub_name).unwrap();

        result.unwrap();
    }

    #[test]
    fn visibility_timeout_does_not_reset_acknowledged_offsets() {
        let sub_name = "visibility_timeout_does_not_reset_acknowledged_offsets";

        let result = catch_unwind(|| {
            let mut sub =
                Subscription::new_with_timeout(sub_name, std::time::Duration::from_millis(50))
                    .unwrap();
            let key = PartitionName::try_from("p1").unwrap();

            sub.add_partition(&key, 0, 0).unwrap();

            let taken = sub.take(1).unwrap();
            assert_eq!(taken, vec![(key.clone(), 0)]);
            sub.mark_inflight(7, &taken);

            // Acknowledge the offset before the timeout elapses.
            sub.acknowledge(&key, &Range { start: 0, end: 0 }).unwrap();

            // After the timeout, nothing is reset because it was acknowledged.
            std::thread::sleep(std::time::Duration::from_millis(80));
            assert!(!sub.reset_expired());

            // And the offset is not redelivered.
            assert!(sub.take(1).unwrap().is_empty());
        });

        std::fs::remove_file(sub_name).unwrap();

        result.unwrap();
    }
}
