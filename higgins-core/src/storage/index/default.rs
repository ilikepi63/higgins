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

    /// Returns the object key as [u8; 16].
    pub fn object_key(&self) -> [u8; 16] {
        self.0[OBJECT_KEY_INDEX..POSITION_INDEX].try_into().unwrap()
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
        object_key: [u8; 16],
        position: u32,
        timestamp: u64,
        size: u64,
        mut data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        data.write_all(offset.to_be_bytes().as_slice())?;
        data.write_all(object_key.as_slice())?;
        data.write_all(position.to_be_bytes().as_slice())?;
        data.write_all(timestamp.to_be_bytes().as_slice())?;
        data.write_all(size.to_be_bytes().as_slice())?;

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
    pub fn put_reference(&mut self, reference: Reference) {
        reference.to_bytes(&mut self.0[OBJECT_KEY_INDEX..OBJECT_KEY_INDEX + Reference::size_of()]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap() {
        let data = vec![0u8; DefaultIndex::size_of()];
        let index = DefaultIndex::of(&data[..]);
        assert_eq!(index.0, data.as_slice());
    }

    #[test]
    fn test_to_bytes() {
        let data = vec![0xAAu8; DefaultIndex::size_of()];
        let index = DefaultIndex::of(&data[..]);
        let buf = index.to_bytes();
        assert_eq!(buf.as_ref(), data.as_slice());
    }

    #[test]
    fn test_size() {
        let expected_size = SIZE_INDEX + size_of::<u64>() + 1;
        assert_eq!(DefaultIndex::size_of(), expected_size);
    }

    #[test]
    fn test_offset() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_offset: u64 = 0x123456789ABCDEF0;
        data[OFFSET_INDEX..OBJECT_KEY_INDEX].copy_from_slice(&expected_offset.to_be_bytes());

        let index = DefaultIndex::of(&data[..]);
        assert_eq!(index.offset(), expected_offset);
    }

    #[test]
    fn test_object_key() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_key = [0xAA; 16];
        data[OBJECT_KEY_INDEX..POSITION_INDEX].copy_from_slice(&expected_key);

        let index = DefaultIndex::of(&data[..]);
        assert_eq!(index.object_key(), expected_key);
    }

    #[test]
    fn test_position() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_position: u32 = 0x12345678;
        data[POSITION_INDEX..TIMESTAMP_INDEX].copy_from_slice(&expected_position.to_be_bytes());

        let index = DefaultIndex::of(&data[..]);
        assert_eq!(index.position(), expected_position);
    }

    #[test]
    fn test_timestamp() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_timestamp: u64 = 0xFEDCBA9876543210;
        data[TIMESTAMP_INDEX..SIZE_INDEX].copy_from_slice(&expected_timestamp.to_be_bytes());

        let index = DefaultIndex::of(&data[..]);
        assert_eq!(index.timestamp(), expected_timestamp);
    }

    #[test]
    fn test_size_getter() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_size: u64 = 0xABCDEF0123456789;
        let end = SIZE_INDEX + size_of::<u64>();
        data[SIZE_INDEX..end].copy_from_slice(&expected_size.to_be_bytes());

        let index = DefaultIndex::of(&data[..]);
        assert_eq!(index.size(), expected_size);
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_offset_panics_on_short_slice() {
        let short_data = vec![0u8; OBJECT_KEY_INDEX - 1];
        let index = DefaultIndex::of(&short_data[..]);
        let _ = index.offset();
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_object_key_panics_on_short_slice() {
        let short_data = vec![0u8; POSITION_INDEX - 1];
        let index = DefaultIndex::of(&short_data[..]);
        let _ = index.object_key();
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_position_panics_on_short_slice() {
        let short_data = vec![0u8; TIMESTAMP_INDEX - 1];
        let index = DefaultIndex::of(&short_data[..]);
        let _ = index.position();
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_timestamp_panics_on_short_slice() {
        let short_data = vec![0u8; SIZE_INDEX - 1];
        let index = DefaultIndex::of(&short_data[..]);
        let _ = index.timestamp();
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_size_getter_panics_on_short_slice() {
        let short_data = vec![0u8; SIZE_INDEX + size_of::<u64>() - 1];
        let index = DefaultIndex::of(&short_data[..]);
        let _ = index.size();
    }

    #[test]
    fn test_debug() {
        let data = vec![0xBBu8; DefaultIndex::size_of()];
        let index = DefaultIndex::of(&data[..]);
        let debug_str = format!("{:?}", index);
        assert!(debug_str.starts_with("DefaultIndex("));
        assert!(debug_str.contains("["));
    }

    #[test]
    fn test_put_writes_correctly() {
        let offset: u64 = 0x123456789ABCDEF0;
        let object_key = [0xAA; 16];
        let position: u32 = 0x12345678;
        let timestamp: u64 = 0xFEDCBA9876543210;
        let size: u64 = 0xABCDEF0123456789;

        let expected_len =
            size_of::<u64>() + size_of::<[u8; 16]>() + size_of::<u32>() + 2 * size_of::<u64>();
        let mut data = vec![0u8; expected_len];

        let result = DefaultIndex::put(offset, object_key, position, timestamp, size, &mut data);
        assert!(result.is_ok());

        // Verify written data
        let read_offset = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let read_key: [u8; 16] = data[8..24].try_into().unwrap();
        let read_position = u32::from_be_bytes(data[24..28].try_into().unwrap());
        let read_timestamp = u64::from_be_bytes(data[28..36].try_into().unwrap());
        let read_size = u64::from_be_bytes(data[36..44].try_into().unwrap());

        assert_eq!(read_offset, offset);
        assert_eq!(read_key, object_key);
        assert_eq!(read_position, position);
        assert_eq!(read_timestamp, timestamp);
        assert_eq!(read_size, size);
    }

    #[test]
    fn test_put_returns_ok_on_success() {
        let offset: u64 = 0;
        let object_key = [0u8; 16];
        let position: u32 = 0;
        let timestamp: u64 = 0;
        let size: u64 = 0;

        let mut data = vec![0u8; DefaultIndex::size_of()];

        let result = DefaultIndex::put(offset, object_key, position, timestamp, size, &mut data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_put_errors_on_short_buffer() {
        let offset: u64 = 0;
        let object_key = [0u8; 16];
        let position: u32 = 0;
        let timestamp: u64 = 0;
        let size: u64 = 0;

        let mut short_data = vec![0u8; 40]; // Less than 44 bytes

        let result = DefaultIndex::put(
            offset,
            object_key,
            position,
            timestamp,
            size,
            &mut short_data,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WriteZero);
    }

    #[test]
    fn test_put_partial_write_errors() {
        // Test error during middle write, e.g., if buffer is long enough for first few but not all
        let offset: u64 = 0;
        let object_key = [0u8; 16];
        let position: u32 = 0;
        let timestamp: u64 = 0;
        let size: u64 = 0;

        // Buffer long enough for offset + key + position + timestamp (8+16+4+8=36), but short for size
        let mut partial_data = vec![0u8; 40];

        let result = DefaultIndex::put(
            offset,
            object_key,
            position,
            timestamp,
            size,
            &mut partial_data,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WriteZero);
    }
}
