use thiserror::Error;

#[derive(Error, Debug)]
pub enum TopographyError {
    #[error("The entry at the designated key is already occupied: {0}")]
    Occupied(String),
    #[error("Derivative stream not found in topography: {0}")]
    DerivativeNotFound(String),
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
