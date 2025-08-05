use std::{num::TryFromIntError, string::FromUtf8Error};

use arrow::error::ArrowError;
use ffi_convert::AsRustError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HigginsFunctionError {
    #[error("Failed to convert Integer types.")]
    TryFromIntError(#[from] TryFromIntError),

    #[error("Failed to convert from UTF8.")]
    FromUtf8Error(#[from] FromUtf8Error),

    #[error("Attempt at dereferencing null ptr.")]
    DereferenceNullPtr,

    #[error("Atempt to convert CArray to Rust Vec failed.")]
    AsRustError(#[from] AsRustError),

    #[error("ArrowError ocurred.")]
    ArrowError(#[from] ArrowError)
}
