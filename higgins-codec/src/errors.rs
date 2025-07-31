use std::num::TryFromIntError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum HigginsCodecError {
    #[error("IO Error.")]
    IOError(#[from] std::io::Error),

    #[error("Error when trying to convert from an Integer.")]
    TryFromIntError(#[from] TryFromIntError),
}
