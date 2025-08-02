use std::{num::TryFromIntError, string::FromUtf8Error};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum HigginsFunctionError {
    #[error("Failed to convert Integer types.")]
    TryFromIntError(#[from] TryFromIntError),

    #[error("Failed to convert from UTF8.")]
    FromUtf8Error(#[from] FromUtf8Error),
}
