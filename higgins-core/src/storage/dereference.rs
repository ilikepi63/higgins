//! All logic related to holding dereferencable data and how to dereference it.

use std::io::Write;

/// Dereference a given reference into the underlying data.
pub async fn dereference(references: Vec<Reference>) -> Vec<u8> {}

/// Represents composite data that will be:
///
/// 1. Embedded into an Index and
/// 2. Read to allow for the dereferencing of a byte vector from the underlying storage implementation.
#[derive(Debug)]
pub enum Reference {
    Null,
    S3(S3Reference),
}

impl Reference {
    /// Write this struct to bytes.
    pub fn to_bytes(&self, mut w: &mut [u8]) {
        match self {
            Self::S3(data) => {
                w.write_all(&1_u16.to_be_bytes()).unwrap();
                w.write_all(&data.object_key).unwrap();
                w.write_all(&data.position.to_be_bytes()).unwrap();
                w.write_all(&data.size.to_be_bytes()).unwrap();
            }
            Self::Null => {
                w.write_all(&1_u16.to_be_bytes()).unwrap();
            }
        };
    }

    /// Read this struct from bytes.
    pub fn from_bytes(data: &[u8]) -> Self {
        let t = u16::from_be_bytes(data[0..2].try_into().unwrap());

        match t {
            0 => Self::Null,
            1 => {
                let object_key: [u8; 16] = data[2..19].try_into().unwrap();
                let position: u64 = u64::from_be_bytes(data[19..27].try_into().unwrap());
                let size: u64 = u64::from_be_bytes(data[27..35].try_into().unwrap());

                Self::S3(S3Reference {
                    object_key,
                    position,
                    size,
                })
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
    pub const fn size_of() -> usize {
        S3Reference::size_of()
    }
}

#[derive(Debug)]
pub struct S3Reference {
    pub object_key: [u8; 16],
    pub position: u64,
    pub size: u64,
}

impl S3Reference {
    /// This is always the amount of a bytes that this data will use once it
    /// has been written to a byte array.
    pub const fn size_of() -> usize {
        16 // The size of the embedded buffer.
    }
}
