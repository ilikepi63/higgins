/// Name of the partition.
///
/// The reason for choosing 32 is because:
/// - The need for a fixed size buffer.
/// - A long enough buffer for users to be able to store human-readable names.
#[derive(Debug, Clone)]
pub struct PartitionName([u8; 32]);
