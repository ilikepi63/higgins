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
pub struct PartitionOffsetsSerde {
    /// The ID for this specific partition.
    partition_id: PartitionName,
    /// The current watermark or offset that has been acknowledged for this offset.
    last_completed_offset: u64,
    /// The max offset, or the largest offset that exists within this partition.
    max_offset: u64,
    /// The amount of offsets that can be taken from this partition, this is effectively = `max_offfset - last_completed_offset`.
    amount_to_take: u64,
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
