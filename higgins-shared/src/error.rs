use crate::{PartitionName, PartitionNameError, StreamName};
use arrow::error::ArrowError;
use prost::{DecodeError, EncodeError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HigginsError {
    #[error("No message body was sent")]
    MissingPayload,

    #[error("Encoding Error")]
    EncodeError(#[from] EncodeError),

    #[error("Decoding protobuf Error")]
    DecodeError(#[from] DecodeError),

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

    #[error("PartitionNameError")]
    PartitionNameError(#[from] PartitionNameError),

    #[error("ArrowError")]
    ArrowError(#[from] ArrowError),

    #[error("Stream Definition not found")]
    StreamDefinitionNotFound(String),

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

    #[error("Index Error")]
    IndexError(#[from] IndexError),

    /// Called when there is no class, but the error can be explained from the string.
    #[error("{0}")]
    Arbitrary(String),

    #[error("Too many clients, could not connect.")]
    TooManyClientsConnnectedToBroker,

    #[error("TryFromIntError")]
    TryFromIntError(#[from] TryFromIntError),

    #[error("TaskError")]
    TaskError(#[from] HigginsTaskError),
}

use std::array::TryFromSliceError;
use std::num::TryFromIntError;

#[derive(Error, Debug)]
pub enum SubscriptionError {
    #[error("PartitionNameError")]
    PartitionNameError(#[from] PartitionNameError),
    #[error("Attempt to create a subscription tracker which already exists.")]
    SubscriptionPartitionAlreadyExists,
    #[error("Failed to create subscription file for subscription: {0}")]
    SubscriptionFileCreationFailure(String),
    #[error("Failure to convert from Slice.")]
    TryFromSliceError(#[from] TryFromSliceError),
    #[error("IOError: {0}")]
    IOError(#[from] std::io::Error),
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
    #[error("Error occurred with Rkyv serde: {0}")]
    RkyvError(#[from] rkyv::rancor::Error),
    #[error("Failue to convert from Integer.")]
    TryFromIntError(#[from] TryFromIntError),
    #[error("Subscription not found.")]
    SubscriptionNotFound,
    #[error("Unknown Subscription Error")]
    Unknown,
}

#[derive(Error, Debug)]
pub enum TopographyError {
    #[error("The entry at the designated key is already occupied: {0}")]
    Occupied(String),
    #[error("Attempted to retrieve a stream that doesn't exist: {0}")]
    StreamNotFound(String),
    #[error("Derivative stream not found in topography: {0}")]
    DerivativeNotFound(String),
    #[error("No base key defined for derivative stream: {0}")]
    NoBaseKeyDefinedForDerivativeStream(String),
    #[error("Incorrect Stream definition: {0}")]
    IncorrectStreamDefinition(String),
    #[error("Schema not found in topography: {0}")]
    SchemaNotFound(String),
    #[error("The given join definition did not describe any joined streams.")]
    NoJoinsInJoinDefinition,
    #[error("An Attempt to join on a stream that does not exist.")]
    JoinStreamDoesNotExist,
    #[error("Attempt to Join a stream with no mapping attributes.")]
    JoinStreamWithoutMappingAttributes,
    #[error("IO Error")]
    IOError(#[from] std::io::Error),
    #[error("JSON Serialization Error")]
    SerdeJsonError(#[from] serde_json::error::Error),
    #[error("TOML Serialization Error")]
    SerdeTomlError(#[from] toml::ser::Error),
    #[error("Conversion to Utf8 String Error")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Attempt to open a non-directory index file.")]
    IndexFileIsNotADirectory,
    #[error("IO Error")]
    IOError(#[from] std::io::Error),
    #[error("Attempt to swap out index with incorrectly sized byte array.")]
    IndexSwapSizeError,
    #[error("TryFromInt Error")]
    TryFromIntError(#[from] TryFromIntError),
    #[error("Rancor Error")]
    RancorError(#[from] rkyv::rancor::Error),
    #[error("The given index does not exist in the JoinedIndex.")]
    IndexInJoinedIndexNotFound,
    #[error("Index out of bounds for the JoinedIndex.")]
    IndexGivenOutOfBoundsForJoinedIndex,
    #[error("Put index out of range")]
    PutIndexOutOfRange,
    #[error("Attempted to overwrite an index that already exists.")]
    IndexAlreadyExists(u64, u64),
    #[error("Unknown Index Error")]
    Unknown,
}

#[derive(Error, Debug)]
pub enum HigginsTaskError {
    #[error("Attempt to retrieve a task hierarchy that does not exist.")]
    TaskHierarchyDoesNotExist,
}
