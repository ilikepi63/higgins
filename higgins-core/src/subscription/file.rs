//! File-related utilites for managing Subscriptions.

use higgins_shared::PartitionName;
use std::{fs::File, path::PathBuf};

/// Represents the header for this specific file.
struct SubscriptionFileHeader {
    // The index that separates readable from unreadable, where separator_index will be the first unreadable partition,
    // whilst separator_index - 1 will be the last readable partition.
    separator_index: usize,
}

/// Represents the current offset of a partition, as well as the maximum offset for that specific partition.
#[derive(Clone, Debug)]
pub struct PartitionOffsetsSerde;
static LAST_COMPLETED_OFFSET: usize = size_of::<PartitionName>();
static MAX_OFFSET: usize = LAST_COMPLETED_OFFSET + size_of::<u64>();
static AMOUNT_TO_TAKE_OFFSET: usize = MAX_OFFSET + size_of::<u64>();

impl PartitionOffsetsSerde {
    pub fn write_to(
        partition_id: PartitionName,
        last_completed_offset: u64,
        max_offset: u64,
        amount_to_take: u64,
        dest: &mut [u8],
    ) {
        dest[0..LAST_COMPLETED_OFFSET].clone_from_slice(&partition_id.0);
        dest[LAST_COMPLETED_OFFSET..MAX_OFFSET]
            .clone_from_slice(&last_completed_offset.to_be_bytes());
        dest[MAX_OFFSET..AMOUNT_TO_TAKE_OFFSET].clone_from_slice(&max_offset.to_be_bytes());
        dest[AMOUNT_TO_TAKE_OFFSET..AMOUNT_TO_TAKE_OFFSET + size_of::<u64>()]
            .clone_from_slice(&amount_to_take.to_be_bytes());
    }
}

struct SubscriptionFileTail {}

pub struct SubscriptionFile {
    handle: File,
}

impl SubscriptionFile {
    /// Acknowledge the given offsets for this specific file/partition.
    pub fn acknowledge(&self, partition: &PartitionName, offsets: &[u64]) {

        // Read the header for where the indexes are.
        // Iterate through the body, finding this partition.
        // acknowledge the given offsets.
        // If the current partition is converted from readable to unreadable, swap to an unreadable destination.
    }

    /// Increment the max offset for a partition.
    pub fn set_max_offset(&self, file: File, partition: &PartitionName, max_offset: &[u64]) {
        // Read the header file for the indexes.
        // Iterate through the body, finding the partition we need to Adjust.
        // Set the max_offset of this partition.
        // It will then have become readable, so if it has changed from readable to unreadable, we swap it with a readable value
    }
}
