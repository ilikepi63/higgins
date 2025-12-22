//! All logic related to holding dereferencable data and how to dereference it.

use std::io::Write;

use crate::{broker::Broker, error::HigginsError};

use riskless::object_store::path::Path;

static NULL_DISCRIMINATOR: u16 = 0;
static OBJECT_STORE_DISCRIMINATOR: u16 = 1;

/// Dereference a given reference into the underlying data.
pub async fn dereference(reference: Reference, broker: &Broker) -> Result<Vec<u8>, HigginsError> {
    match reference {
        Reference::S3(reference_object_store) => {
            // Retrieve the object store reference.
            let object_store = {
                // let broker = broker.read().await;
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
                w[0..size_of::<u16>()].copy_from_slice(&NULL_DISCRIMINATOR.to_be_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;

    // Placeholder for Broker and HigginsError; assuming they are defined in the crate
    // and can be instantiated for testing purposes (e.g., Broker::default() or similar).
    // If Broker requires additional setup, adjust accordingly.
    // For dereference S3 tests, a mock object store would be ideal (e.g., an in-memory
    // implementation of the object store trait from riskless).

    #[test]
    fn test_reference_null_to_bytes() {
        let mut buffer = [0u8; Reference::size_of()];
        let reference = Reference::Null;
        let result = reference.to_bytes(&mut buffer);
        assert!(result.is_ok());

        // Verify discriminator
        let discriminator = u16::from_be_bytes(buffer[0..2].try_into().unwrap());
        assert_eq!(discriminator, NULL_DISCRIMINATOR);
    }

    #[test]
    fn test_reference_s3_to_bytes() {
        let object_key = [0x01u8; 16];
        let position = 100u64;
        let size = 50u64;
        let reference = Reference::S3(S3Reference {
            object_key,
            position,
            size,
        });

        let mut buffer = [0u8; Reference::size_of()];
        let result = reference.to_bytes(&mut buffer);
        assert!(result.is_ok());

        // Verify discriminator
        let discriminator = u16::from_be_bytes(buffer[0..2].try_into().unwrap());
        assert_eq!(discriminator, OBJECT_STORE_DISCRIMINATOR);

        // Verify object_key
        let read_key: [u8; 16] = buffer[2..18].try_into().unwrap();
        assert_eq!(read_key, object_key);

        // Verify position
        let read_position = u64::from_be_bytes(buffer[18..26].try_into().unwrap());
        assert_eq!(read_position, position);

        // Verify size
        let read_size = u64::from_be_bytes(buffer[26..34].try_into().unwrap());
        assert_eq!(read_size, size);
    }

    #[test]
    fn test_reference_null_from_bytes() {
        let mut buffer = [0u8; Reference::size_of()];
        let original = Reference::Null;
        original.to_bytes(&mut buffer).unwrap();

        let deserialized = Reference::from_bytes(&buffer);
        assert!(matches!(deserialized, Reference::Null));
    }

    #[test]
    fn test_reference_s3_from_bytes() {
        let object_key = [0x42u8; 16];
        let position = 42u64;
        let size = 24u64;
        let original = Reference::S3(S3Reference {
            object_key,
            position,
            size,
        });

        let mut buffer = [0u8; Reference::size_of()];
        original.to_bytes(&mut buffer).unwrap();

        let deserialized = Reference::from_bytes(&buffer);
        match deserialized {
            Reference::S3(s3_ref) => {
                assert_eq!(s3_ref.object_key, object_key);
                assert_eq!(s3_ref.position, position);
                assert_eq!(s3_ref.size, size);
            }
            _ => panic!("Deserialized as Null instead of S3"),
        }
    }

    #[test]
    fn test_reference_s3_roundtrip() {
        let object_key = uuid::Uuid::new_v4()
            .as_bytes()
            .to_owned()
            .try_into()
            .unwrap(); // Use a real UUID for variety
        let position = 123u64;
        let size = 456u64;
        let original = Reference::S3(S3Reference {
            object_key,
            position,
            size,
        });

        let mut buffer = [0u8; Reference::size_of()];
        original.to_bytes(&mut buffer).unwrap();

        let deserialized = Reference::from_bytes(&buffer);
        match deserialized {
            Reference::S3(s3_ref) => {
                // assert_eq!(s3_ref, original.as_ref().unwrap_s3()); // Assuming a helper method; adjust if needed
                // Or manually:
                assert_eq!(s3_ref.object_key, object_key);
                assert_eq!(s3_ref.position, position);
                assert_eq!(s3_ref.size, size);
            }
            _ => panic!("Roundtrip failed: deserialized as Null"),
        }
    }

    #[test]
    fn test_reference_size_of() {
        let expected_size = std::mem::size_of::<u16>() + S3Reference::size_of();
        assert_eq!(Reference::size_of(), expected_size);
        assert_eq!(Reference::size_of(), 34); // 2 + 16 + 8 + 8
    }

    #[test]
    fn test_s3reference_size_of() {
        let expected_size = std::mem::size_of::<[u8; 16]>()
            + std::mem::size_of::<u64>()
            + std::mem::size_of::<u64>();
        assert_eq!(S3Reference::size_of(), expected_size);
        assert_eq!(S3Reference::size_of(), 32); // 16 + 8 + 8
    }

    // #[tokio::test]
    // async fn test_dereference_null() {
    //     // Placeholder: Create a broker instance. Adjust based on actual Broker implementation.
    //     // For example: let broker = Arc::new(RwLock::new(Broker::new(mock_object_store)));
    //     let broker = Arc::new(RwLock::new(Broker {})); // Compile-time placeholder; replace with valid init

    //     let result = dereference(Reference::Null, broker).await;
    //     assert!(result.is_err());
    //     // Verify specific error variant
    //     if let Err(HigginsError::NullDereferenceError) = result {
    //         // Expected
    //     } else {
    //         panic!("Expected NullDereferenceError");
    //     }
    // }

    // Note: Full testing of dereference(Reference::S3) requires a mock object store that returns
    // predictable bytes() results. Below is a skeleton for such a test, assuming an in-memory
    // object store implementation is available (e.g., via riskless or a test double).
    /*
    #[tokio::test]
    async fn test_dereference_s3_success() {
        // Setup: Create mock object store with known data
        let mock_data = vec![0u8; 100]; // 100 bytes of known data
        let mock_object_store = MockObjectStore::new(mock_data); // Hypothetical mock
        let broker = Arc::new(RwLock::new(Broker {
            object_store: mock_object_store,
            ..Default::default()
        }));

        let object_key = uuid::Uuid::new_v4().as_bytes().to_owned().try_into().unwrap();
        let position = 10u64;
        let size = 20u64;
        let reference = Reference::S3(S3Reference {
            object_key,
            position,
            size,
        });

        let result = dereference(reference, broker).await;
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.len(), size as usize);
        // Verify slice matches expected bytes from mock_data[position..position + size]
        // e.g., assert_eq!(data, &mock_data[10..30]);
    }

    #[tokio::test]
    async fn test_dereference_s3_invalid_position() {
        // Test overflow or invalid slice; adjust mock to return large enough bytes
        // ...
    }

    #[tokio::test]
    async fn test_dereference_s3_object_not_found() {
        // Mock store returns Err on get
        // ...
    }
    */
}
