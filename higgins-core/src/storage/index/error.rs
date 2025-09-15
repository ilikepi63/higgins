use std::num::TryFromIntError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Attempt to open a non-directory index file.")]
    IndexFileIsNotADirectory,
    #[error("IO Error")]
    IOError(#[from] std::io::Error),

    #[error("TryFromInt Error")]
    TryFromIntError(#[from] TryFromIntError),

    #[error("Unknown Index Error")]
    Unknown,
}
