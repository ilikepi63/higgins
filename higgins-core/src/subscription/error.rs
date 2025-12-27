use std::num::TryFromIntError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SubscriptionError {
    #[error("Attempt to create a subscription tracker which already exists.")]
    SubscriptionPartitionAlreadyExists,
    #[error(
        "Attempting to acknowledge offset without acknowleding previous index. Offset: {0}, Previous Offset: {1}"
    )]
    AttemptToAcknowledgeOffsetWithoutAcknowledgingPreviousOffset(u64, u64),
    #[error("Attempt to retrieve partition that does not exist.")]
    PartitionDoesNotExists,
    #[error("Attempt to acknowledge a partition/offset that doesn't exist: {0} {1}.")]
    AttemptToAcknowledgePartitionThatDoesntExist(String, u64),
    #[error("Failed to deserialize SubscriptionMetadata.")]
    FailureToDeserializeSubscriptionMetadata,
    #[error("Error occurred in RocksDB: {0}")]
    RocksDbError(#[from] rocksdb::Error),
    #[error("Error occurred with Rkyv serde: {0}")]
    RkyvError(#[from] rkyv::rancor::Error),
    #[error("Failue to convert from Integer.")]
    TryFromIntError(#[from] TryFromIntError),
    #[error("Unknown Subscription Error")]
    Unknown,
}
