//! All logic related to holding dereferencable data and how to dereference it.

use std::io::Write;

use crate::{broker::Broker, error::HigginsError};
use tokio::sync::RwLock;

use riskless::object_store::path::Path;

static NULL_DISCRIMINATOR: u16 = 0;
static OBJECT_STORE_DISCRIMINATOR: u16 = 1;

/// Dereference a given reference into the underlying data.
pub async fn dereference(
    reference: Reference,
    broker: std::sync::Arc<RwLock<Broker>>,
) -> Result<Vec<u8>, HigginsError> {
    match reference {
        Reference::S3(reference_object_store) => {
            // Retrieve the object store reference.
            let object_store = {
                let broker = broker.read().await;
                broker.object_store.clone()
            };

            let object_name = uuid::Uuid::from_bytes(reference_object_store.object_key).to_string();

            let get_object_result = object_store.get(&Path::from(object_name.as_str())).await;

            match get_object_result {
                Ok(get_result) => {
                    if let Ok(b) = get_result.bytes().await {
                        // index into the bytes.
                        let start: usize = (reference_object_store.position).try_into().unwrap();
                        let end: usize = (reference_object_store.position
                            + reference_object_store.size)
                            .try_into()
                            .unwrap();

                        let data = b.slice(start..end);

                        Ok(data.to_vec())
                    } else {
                        let error_message = format!(
                            "Could not retrieve bytes for given GetObject query: {}",
                            object_name
                        );
                        tracing::trace!(error_message);
                        Err(HigginsError::ObjectStoreRetrievalError(error_message))
                    }
                }
                Err(err) => {
                    let error_message = format!(
                        "An error occurred trying to retrieve the object with key {}. Error: {:#?}",
                        object_name, err
                    );
                    tracing::trace!(error_message);
                    Err(HigginsError::ObjectStoreRetrievalError(error_message))
                }
            }
        }
        Reference::Null => Err(HigginsError::NullDereferenceError),
    }
}

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
    pub fn to_bytes(&self, mut w: &mut [u8]) -> Result<(), std::io::Error> {
        match self {
            Self::S3(data) => {
                w.write_all(&OBJECT_STORE_DISCRIMINATOR.to_be_bytes())?;
                w.write_all(&data.object_key)?;
                w.write_all(&data.position.to_be_bytes())?;
                w.write_all(&data.size.to_be_bytes())?;
            }
            Self::Null => {
                w.write_all(&NULL_DISCRIMINATOR.to_be_bytes())?;
            }
        };

        Ok(())
    }

    /// Read this struct from bytes.
    pub fn from_bytes(data: &[u8]) -> Self {
        let t = u16::from_be_bytes(data[0..2].try_into().unwrap());

        match t {
            0 => Self::Null,
            1 => {
                println!("Data: {:#?}", data);
                println!("Data: {:#?}", data.len());

                let object_key: [u8; 16] = data[2..(2 + 16)].try_into().unwrap();
                let position: u64 = u64::from_be_bytes(data[18..26].try_into().unwrap());
                let size: u64 = u64::from_be_bytes(data[26..(26 + 8)].try_into().unwrap());

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
        size_of::<u16>() + S3Reference::size_of()
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
        size_of::<[u8; 16]>() + size_of::<u64>() + size_of::<u64>() // The size of the embedded buffer.
    }
}
