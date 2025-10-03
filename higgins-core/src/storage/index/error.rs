use std::num::TryFromIntError;

use thiserror::Error;

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
    #[error("Unknown Index Error")]
    Unknown,
}
