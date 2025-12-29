/// Name of the partition.
///
/// The reason for choosing 32 is because:
/// - The need for a fixed size buffer.
/// - A long enough buffer for users to be able to store human-readable names.
#[derive(Debug, Clone)]
pub struct PartitionName(pub [u8; 32]);

impl Into<Vec<u8>> for PartitionName {
    fn into(self) -> Vec<u8> {
        self.0.to_vec()
    }
}
