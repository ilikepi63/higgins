use thiserror::Error;

use crate::subscription::error::SubscriptionError;
use crate::topography::errors::TopographyError;

#[derive(Error, Debug)]
pub enum HigginsError {
    #[error("Stream/Subscription does not exist: {0} {1}")]
    SubscriptionForStreamDoesNotExist(String, String),

    #[error("Error occurred with Subscriptions.")]
    SubscriptionError(#[from] SubscriptionError),

    #[error("Attempted to retrieve subscription that does not exist.")]
    SubscriptionRetrievalFailed,
    #[error("Error occurred with Typography.")]
    TopographyError(#[from] TopographyError),

    #[error("Attempted to place data at a null reference. ")]
    UnableToPlaceDataAtNullReference,

    #[error("Attemt to write data to s3 failed.")]
    S3PutDataFailure,

    #[error("Attempt to dereference null Reference.")]
    NullDereferenceError,

    #[error("Attempt to retrieve object from object store resulted in a failure: {0}")]
    ObjectStoreRetrievalError(String),

    #[error("Unknown Error")]
    Unknown,
}
