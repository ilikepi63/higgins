#[allow(unused_imports)] // No idea why this is throwing a warning.
use bytes::{BufMut as _, BytesMut};
use std::io::Write as _;

use crate::storage::dereference::Reference;

const OFFSET_INDEX: usize = 0;
const OBJECT_KEY_INDEX: usize = OFFSET_INDEX + size_of::<u64>();
const POSITION_INDEX: usize = OBJECT_KEY_INDEX + Reference::size_of();
const TIMESTAMP_INDEX: usize = POSITION_INDEX + size_of::<u32>();
const SIZE_INDEX: usize = TIMESTAMP_INDEX + size_of::<u64>();

/// A default index for us with generic streams.
#[derive(Debug)]
pub struct DefaultIndex<'a>(&'a [u8]);
//     pub offset: u64,
//     pub object_key: [u8; 16],
//     pub position: u32,
//     pub timestamp: u64,
//     pub size: u64,

impl<'a> DefaultIndex<'a> {
    /// Creates a new instance of this index, wrapping the old.
    pub fn of(data: &'a [u8]) -> Self {
        Self(data)
    }

    /// Returns the offset as a u64, converted from big-endian bytes.
    pub fn offset(&self) -> u64 {
        u64::from_be_bytes(self.0[OFFSET_INDEX..OBJECT_KEY_INDEX].try_into().unwrap())
    }

    /// Returns the position as a u32, converted from big-endian bytes.
    pub fn position(&self) -> u32 {
        u32::from_be_bytes(self.0[POSITION_INDEX..TIMESTAMP_INDEX].try_into().unwrap())
    }

    /// Returns the size as a u64, converted from big-endian bytes.
    pub fn size(&self) -> u64 {
        let end = SIZE_INDEX + size_of::<u64>();
        u64::from_be_bytes(self.0[SIZE_INDEX..end].try_into().unwrap())
    }

    pub fn to_bytes(&self) -> BytesMut {
        let buf = BytesMut::from(self.0);

        buf
    }

    pub const fn size_of() -> usize {
        SIZE_INDEX + size_of::<u64>() + 1
    }

    /// Puts the data into the mutable slice, returning this struct as a reference over it.
    pub fn put(
        offset: u64,
        reference: Reference,
        position: u32,
        timestamp: u64,
        size: u64,
        mut data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        (&mut data[0..]).write_all(offset.to_be_bytes().as_slice())?;

        reference.to_bytes(&mut data[OBJECT_KEY_INDEX..])?;

        (&mut data[POSITION_INDEX..]).write_all(position.to_be_bytes().as_slice())?;

        (&mut data[TIMESTAMP_INDEX..]).write_all(timestamp.to_be_bytes().as_slice())?;

        (&mut data[SIZE_INDEX..]).write_all(size.to_be_bytes().as_slice())?;

        Ok(())
    }

    pub fn timestamp(&self) -> u64 {
        u64::from_be_bytes(self.0[TIMESTAMP_INDEX..SIZE_INDEX].try_into().unwrap())
    }

    /// Retrieve the reference of this Index.
    pub fn reference(&self) -> Reference {
        Reference::from_bytes(&self.0[OBJECT_KEY_INDEX..OBJECT_KEY_INDEX + Reference::size_of()])
    }

    /// Update the reference for this.
    pub fn put_reference(&self, reference: Reference) -> Vec<u8> {
        let mut cloned = self.0.to_vec();

        reference.to_bytes(&mut cloned[OBJECT_KEY_INDEX..OBJECT_KEY_INDEX + Reference::size_of()]);

        cloned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::dereference::{Reference, S3Reference};

    // Helper function to create a test S3 Reference
    fn create_test_s3_reference() -> Reference {
        Reference::S3(S3Reference {
            object_key: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            position: 100,
            size: 200,
        })
    }

    // Helper function to create another distinct test reference
    fn create_alternate_s3_reference() -> Reference {
        Reference::S3(S3Reference {
            object_key: [
                255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240,
            ],
            position: 999,
            size: 1024,
        })
    }

    // Helper function to create a null reference
    fn create_null_reference() -> Reference {
        Reference::Null
    }

    // Helper function to create a buffer with the correct size
    fn create_buffer() -> Vec<u8> {
        vec![0u8; DefaultIndex::size_of()]
    }

    // Helper to compare references by serializing them
    fn references_equal(r1: &Reference, r2: &Reference) -> bool {
        let mut buf1 = vec![0u8; Reference::size_of()];
        let mut buf2 = vec![0u8; Reference::size_of()];
        r1.to_bytes(&mut buf1);
        r2.to_bytes(&mut buf2);
        buf1 == buf2
    }

    #[test]
    fn test_size_of() {
        // SIZE_INDEX + size_of::<u64>() + 1
        let expected = size_of::<u64>()
            + Reference::size_of()
            + size_of::<u32>()
            + size_of::<u64>()
            + size_of::<u64>()
            + 1;
        assert_eq!(DefaultIndex::size_of(), expected);
    }

    #[test]
    fn test_put_and_read_offset() {
        let mut buffer = create_buffer();
        let offset = 0x123456789ABCDEFu64;
        let reference = create_test_s3_reference();
        let position = 42u32;
        let timestamp = 1234567890u64;
        let size = 9876543210u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        assert_eq!(index.offset(), offset);
    }

    #[test]
    fn test_put_and_read_position() {
        let mut buffer = create_buffer();
        let offset = 100u64;
        let reference = create_test_s3_reference();
        let position = 0xDEADBEEFu32;
        let timestamp = 1000u64;
        let size = 2000u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        assert_eq!(index.position(), position);
    }

    #[test]
    fn test_put_and_read_timestamp() {
        let mut buffer = create_buffer();
        let offset = 0u64;
        let reference = create_test_s3_reference();
        let position = 0u32;
        let timestamp = 0xFEDCBA9876543210u64;
        let size = 500u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        assert_eq!(index.timestamp(), timestamp);
    }

    #[test]
    fn test_put_and_read_size() {
        let mut buffer = create_buffer();
        let offset = 999u64;
        let reference = create_test_s3_reference();
        let position = 123u32;
        let timestamp = 456u64;
        let size = 0x0102030405060708u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        assert_eq!(index.size(), size);
    }

    #[test]
    fn test_put_and_read_all_fields() {
        let mut buffer = create_buffer();
        let offset = 0xAAAAAAAAAAAAAAAAu64;
        let reference = create_test_s3_reference();
        let position = 0xBBBBBBBBu32;
        let timestamp = 0xCCCCCCCCCCCCCCCCu64;
        let size = 0xDDDDDDDDDDDDDDDDu64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        assert_eq!(index.offset(), offset);
        assert_eq!(index.position(), position);
        assert_eq!(index.timestamp(), timestamp);
        assert_eq!(index.size(), size);
    }

    #[test]
    fn test_to_bytes() {
        let mut buffer = create_buffer();
        let offset = 12345u64;
        let reference = create_test_s3_reference();
        let position = 67890u32;
        let timestamp = 11111u64;
        let size = 22222u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        let bytes = index.to_bytes();

        assert_eq!(bytes.len(), buffer.len());
        assert_eq!(&bytes[..], &buffer[..]);
    }

    #[test]
    fn test_reference_roundtrip_s3() {
        let mut buffer = create_buffer();
        let offset = 1u64;
        let reference = create_test_s3_reference();
        let position = 2u32;
        let timestamp = 3u64;
        let size = 4u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        let retrieved_ref = index.reference();

        // Verify the reference matches
        assert!(references_equal(
            &retrieved_ref,
            &create_test_s3_reference()
        ));

        // For S3Reference, check specific fields
        match retrieved_ref {
            Reference::S3(s3_ref) => {
                assert_eq!(
                    s3_ref.object_key,
                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
                );
                assert_eq!(s3_ref.position, 100);
                assert_eq!(s3_ref.size, 200);
            }
            Reference::Null => panic!("Expected S3Reference, got Null"),
        }
    }

    #[test]
    fn test_reference_roundtrip_null() {
        let mut buffer = create_buffer();
        let offset = 1u64;
        let reference = create_null_reference();
        let position = 2u32;
        let timestamp = 3u64;
        let size = 4u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        let retrieved_ref = index.reference();

        // Verify we get back a Null reference
        match retrieved_ref {
            Reference::Null => {} // Success
            Reference::S3(_) => panic!("Expected Null reference, got S3Reference"),
        }
    }

    #[test]
    fn test_put_reference() {
        let mut buffer = create_buffer();
        let offset = 100u64;
        let original_reference = create_test_s3_reference();
        let position = 200u32;
        let timestamp = 300u64;
        let size = 400u64;

        DefaultIndex::put(
            offset,
            original_reference,
            position,
            timestamp,
            size,
            &mut buffer,
        )
        .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);

        // Create a new reference with different data
        let new_reference = create_alternate_s3_reference();
        let updated_buffer = index.put_reference(new_reference);

        let updated_index = DefaultIndex::of(&updated_buffer);

        // Verify other fields remain unchanged
        assert_eq!(updated_index.offset(), offset);
        assert_eq!(updated_index.position(), position);
        assert_eq!(updated_index.timestamp(), timestamp);
        assert_eq!(updated_index.size(), size);

        // Verify reference was updated
        let updated_ref = updated_index.reference();
        match updated_ref {
            Reference::S3(s3_ref) => {
                assert_eq!(
                    s3_ref.object_key,
                    [
                        255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241,
                        240
                    ]
                );
                assert_eq!(s3_ref.position, 999);
                assert_eq!(s3_ref.size, 1024);
            }
            Reference::Null => panic!("Expected S3Reference, got Null"),
        }
    }

    #[test]
    fn test_put_reference_from_s3_to_null() {
        let mut buffer = create_buffer();
        let offset = 100u64;
        let original_reference = create_test_s3_reference();
        let position = 200u32;
        let timestamp = 300u64;
        let size = 400u64;

        DefaultIndex::put(
            offset,
            original_reference,
            position,
            timestamp,
            size,
            &mut buffer,
        )
        .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);

        // Update to Null reference
        let new_reference = create_null_reference();
        let updated_buffer = index.put_reference(new_reference);

        let updated_index = DefaultIndex::of(&updated_buffer);

        // Verify other fields remain unchanged
        assert_eq!(updated_index.offset(), offset);
        assert_eq!(updated_index.position(), position);
        assert_eq!(updated_index.timestamp(), timestamp);
        assert_eq!(updated_index.size(), size);

        // Verify reference was updated to Null
        match updated_index.reference() {
            Reference::Null => {} // Success
            Reference::S3(_) => panic!("Expected Null reference, got S3Reference"),
        }
    }

    #[test]
    fn test_zero_values() {
        let mut buffer = create_buffer();
        let offset = 0u64;
        let reference = Reference::S3(S3Reference {
            object_key: [0u8; 16],
            position: 0,
            size: 0,
        });
        let position = 0u32;
        let timestamp = 0u64;
        let size = 0u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        assert_eq!(index.offset(), 0);
        assert_eq!(index.position(), 0);
        assert_eq!(index.timestamp(), 0);
        assert_eq!(index.size(), 0);

        // Verify reference fields are also zero
        match index.reference() {
            Reference::S3(s3_ref) => {
                assert_eq!(s3_ref.object_key, [0u8; 16]);
                assert_eq!(s3_ref.position, 0);
                assert_eq!(s3_ref.size, 0);
            }
            Reference::Null => panic!("Expected S3Reference, got Null"),
        }
    }

    #[test]
    fn test_max_values() {
        let mut buffer = create_buffer();
        let offset = u64::MAX;
        let reference = Reference::S3(S3Reference {
            object_key: [255u8; 16],
            position: u64::MAX,
            size: u64::MAX,
        });
        let position = u32::MAX;
        let timestamp = u64::MAX;
        let size = u64::MAX;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        assert_eq!(index.offset(), u64::MAX);
        assert_eq!(index.position(), u32::MAX);
        assert_eq!(index.timestamp(), u64::MAX);
        assert_eq!(index.size(), u64::MAX);

        // Verify reference fields are also max
        match index.reference() {
            Reference::S3(s3_ref) => {
                assert_eq!(s3_ref.object_key, [255u8; 16]);
                assert_eq!(s3_ref.position, u64::MAX);
                assert_eq!(s3_ref.size, u64::MAX);
            }
            Reference::Null => panic!("Expected S3Reference, got Null"),
        }
    }

    #[test]
    fn test_buffer_too_small() {
        let mut buffer = vec![0u8; 10]; // Too small buffer
        let offset = 1u64;
        let reference = create_test_s3_reference();
        let position = 2u32;
        let timestamp = 3u64;
        let size = 4u64;

        let result = DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer);
        assert!(result.is_err(), "Should fail with buffer too small");
    }

    #[test]
    fn test_multiple_indices_independence() {
        let mut buffer1 = create_buffer();
        let mut buffer2 = create_buffer();

        let offset1 = 111u64;
        let reference1 = create_test_s3_reference();
        let position1 = 222u32;
        let timestamp1 = 333u64;
        let size1 = 444u64;

        let offset2 = 555u64;
        let reference2 = create_alternate_s3_reference();
        let position2 = 666u32;
        let timestamp2 = 777u64;
        let size2 = 888u64;

        DefaultIndex::put(
            offset1,
            reference1,
            position1,
            timestamp1,
            size1,
            &mut buffer1,
        )
        .expect("Failed to put data to buffer1");
        DefaultIndex::put(
            offset2,
            reference2,
            position2,
            timestamp2,
            size2,
            &mut buffer2,
        )
        .expect("Failed to put data to buffer2");

        let index1 = DefaultIndex::of(&buffer1);
        let index2 = DefaultIndex::of(&buffer2);

        assert_eq!(index1.offset(), offset1);
        assert_eq!(index2.offset(), offset2);
        assert_ne!(index1.offset(), index2.offset());

        // Verify references are different
        match (index1.reference(), index2.reference()) {
            (Reference::S3(s3_1), Reference::S3(s3_2)) => {
                assert_ne!(s3_1.object_key, s3_2.object_key);
                assert_ne!(s3_1.position, s3_2.position);
                assert_ne!(s3_1.size, s3_2.size);
            }
            _ => panic!("Expected both references to be S3References"),
        }
    }

    #[test]
    fn test_s3_reference_with_specific_object_key() {
        let mut buffer = create_buffer();
        let offset = 1000u64;
        let object_key = [
            0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
            0x99, 0x00,
        ];
        let reference = Reference::S3(S3Reference {
            object_key,
            position: 5000,
            size: 6000,
        });
        let position = 2000u32;
        let timestamp = 3000u64;
        let size = 4000u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        let index = DefaultIndex::of(&buffer);
        match index.reference() {
            Reference::S3(s3_ref) => {
                assert_eq!(s3_ref.object_key, object_key);
                assert_eq!(s3_ref.position, 5000);
                assert_eq!(s3_ref.size, 6000);
            }
            Reference::Null => panic!("Expected S3Reference, got Null"),
        }
    }

    #[test]
    fn test_index_wrapper_preserves_original_buffer() {
        let mut buffer = create_buffer();

        println!("Buffer: {:#?}", buffer);
        println!("Buffer size: {:#?}", buffer.len());
        let offset = 100u64;
        let reference = create_test_s3_reference();
        let position = 200u32;
        let timestamp = 300u64;
        let size = 400u64;

        DefaultIndex::put(offset, reference, position, timestamp, size, &mut buffer)
            .expect("Failed to put data");

        println!("Buffer: {:#?}", buffer);
        println!("Buffer size: {:#?}", buffer.len());

        // Create index wrapper
        let index = DefaultIndex::of(&buffer);

        // Verify reading doesn't modify the buffer
        let _ = index.offset();
        let _ = index.position();
        let _ = index.timestamp();
        let _ = index.size();
        let _ = index.reference();

        // Create another index from same buffer and verify values match
        let index2 = DefaultIndex::of(&buffer);
        assert_eq!(index.offset(), index2.offset());
        assert_eq!(index.position(), index2.position());
        assert_eq!(index.timestamp(), index2.timestamp());
        assert_eq!(index.size(), index2.size());
    }
}
