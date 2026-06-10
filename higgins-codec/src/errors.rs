use std::num::TryFromIntError;

use prost::{DecodeError, EncodeError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HigginsCodecError {
    #[error("IO Error.")]
    IOError(#[from] std::io::Error),
    #[error("Encoding Error")]
    EncodeError(#[from] EncodeError),
    #[error("Decoding protobuf Error")]
    DecodeError(#[from] DecodeError),
    #[error("Error when trying to convert from an Integer.")]
    TryFromIntError(#[from] TryFromIntError),
}
