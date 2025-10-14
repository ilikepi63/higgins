//! All logic related to holding dereferencable data and how to dereference it.

use std::io::Write;

/// Represents composite data that will be:
///
/// 1. Embedded into an Index and
/// 2. Read to allow for the dereferencing of a byte vector from the underlying storage implementation.
pub enum Dereferencable {
    S3(S3Dereferencable),
}

impl Dereferencable {
    /// Write this struct to bytes.
    pub fn to_bytes(&self, mut w: &mut [u8]) {
        match self {
            Self::S3(data) => {
                w.write_all(&1_u16.to_be_bytes());
                w.write_all(&data.object_key)
            }
        };
    }

    /// Read this struct from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let t = u16::from_be_bytes(data[0..2].try_into()?);

        match t {
            1 => {
                let object_key: [u8; 16] = data[2..19].try_into()?;

                Ok(Self::S3(S3Dereferencable { object_key }))
            }
            _ => {
                tracing::error!("Unable to interpret byte array for Dereferencable. ");
                unimplemented!();
            }
        }
    }

    /// The general size of this struct if it is written to bytes.
    ///
    /// This is a static value that represents the largest amount of metadata that can be written to this
    pub fn size_of() -> usize {
        S3Dereferencable::size_of()
    }
}

pub struct S3Dereferencable {
    pub object_key: [u8; 16],
}

impl S3Dereferencable {
    /// This is always the amount of a bytes that this data will use once it
    /// has been written to a byte array.
    pub fn size_of() -> usize {
        16 // The size of the embedded buffer.
    }
}
