use higgins_shared::PartitionName;
use thiserror::Error;

use crate::storage::index::IndexError;
use crate::subscription::error::SubscriptionError;
use crate::topography::StreamName;
use crate::topography::errors::TopographyError;

#[derive(Error, Debug)]
pub enum HigginsError {
    #[error("Stream/Subscription does not exist: {0} {1}")]
    SubscriptionForStreamDoesNotExist(String, String),

    #[error("Error occurred with Subscriptions.")]
    SubscriptionError(#[from] SubscriptionError),

    #[error("Attempted to retrieve subscription that does not exist.")]
    SubscriptionRetrievalFailed,

    #[error("IO Error")]
    StdIOError(#[from] std::io::Error),

    #[error("Error occurred with Typography.")]
    TopographyError(#[from] TopographyError),

    #[error("The specified index was not found: stream: {0}, partition: {1}, offset: {2}")]
    IndexNotFoundError(StreamName, PartitionName, u64),

    #[error("Error occurred with Indexing.")]
    IndexError(#[from] IndexError),

    #[error("PartitionNameError")]
    PartitionNameError(#[from] higgins_shared::PartitionNameError),

    #[error("Attempted to place data at a null reference. ")]
    UnableToPlaceDataAtNullReference,

    #[error("Attemt to write data to s3 failed.")]
    S3PutDataFailure,

    #[error("Attempt to dereference null Reference.")]
    NullDereferenceError,

    #[error("Dereference Error: {0}.")]
    DereferenceError(String),

    #[error("Attempt to retrieve object from object store resulted in a failure: {0}")]
    ObjectStoreRetrievalError(String),

    #[error("Attempt to retrieve object store but one was not configured. ")]
    ObjectStoreNotConfigured,

    #[error("Unknown Error")]
    Unknown,

    #[error("Too many clients, could not connect.")]
    TooManyClientsConnnectedToBroker,
}
