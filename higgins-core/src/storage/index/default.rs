use crate::storage::index::{Timestamped, WrapBytes};
#[allow(unused_imports)] // No idea why this is throwing a warning.
use bytes::{BufMut as _, BytesMut};

const OFFSET_INDEX: usize = 0;
const OBJECT_KEY_INDEX: usize = OFFSET_INDEX + size_of::<u64>();
const POSITION_INDEX: usize = OBJECT_KEY_INDEX + size_of::<[u8; 16]>();
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
}

impl<'a> Timestamped for DefaultIndex<'a> {
    fn timestamp(&self) -> u64 {
        u64::from_be_bytes(self.0[TIMESTAMP_INDEX..SIZE_INDEX].try_into().unwrap())
    }
}

impl<'a> WrapBytes<'a> for DefaultIndex<'a> {
    fn wrap(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFSET_INDEX: usize = 0;
    const OBJECT_KEY_INDEX: usize = OFFSET_INDEX + size_of::<u64>();
    const POSITION_INDEX: usize = OBJECT_KEY_INDEX + size_of::<[u8; 16]>();
    const TIMESTAMP_INDEX: usize = POSITION_INDEX + size_of::<u32>();
    const SIZE_INDEX: usize = TIMESTAMP_INDEX + size_of::<u64>();

    #[test]
    fn test_wrap() {
        let data = vec![0u8; DefaultIndex::size_of()];
        let index = DefaultIndex::wrap(&data[..]);
        assert_eq!(index.0, data.as_slice());
    }

    #[test]
    fn test_to_bytes() {
        let data = vec![0xAAu8; DefaultIndex::size_of()];
        let index = DefaultIndex::wrap(&data[..]);
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

        let index = DefaultIndex::wrap(&data[..]);
        assert_eq!(index.offset(), expected_offset);
    }

    #[test]
    fn test_object_key() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_key = [0xAA; 16];
        data[OBJECT_KEY_INDEX..POSITION_INDEX].copy_from_slice(&expected_key);

        let index = DefaultIndex::wrap(&data[..]);
        assert_eq!(index.object_key(), expected_key);
    }

    #[test]
    fn test_position() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_position: u32 = 0x12345678;
        data[POSITION_INDEX..TIMESTAMP_INDEX].copy_from_slice(&expected_position.to_be_bytes());

        let index = DefaultIndex::wrap(&data[..]);
        assert_eq!(index.position(), expected_position);
    }

    #[test]
    fn test_timestamp() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_timestamp: u64 = 0xFEDCBA9876543210;
        data[TIMESTAMP_INDEX..SIZE_INDEX].copy_from_slice(&expected_timestamp.to_be_bytes());

        let index = DefaultIndex::wrap(&data[..]);
        assert_eq!(index.timestamp(), expected_timestamp);
    }

    #[test]
    fn test_timestamped_trait() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_timestamp: u64 = 0xFEDCBA9876543210;
        data[TIMESTAMP_INDEX..SIZE_INDEX].copy_from_slice(&expected_timestamp.to_be_bytes());

        let index = DefaultIndex::wrap(&data[..]);
        assert_eq!(Timestamped::timestamp(&index), expected_timestamp);
    }

    #[test]
    fn test_size_getter() {
        let mut data = vec![0u8; DefaultIndex::size_of()];
        let expected_size: u64 = 0xABCDEF0123456789;
        let end = SIZE_INDEX + size_of::<u64>();
        data[SIZE_INDEX..end].copy_from_slice(&expected_size.to_be_bytes());

        let index = DefaultIndex::wrap(&data[..]);
        assert_eq!(index.size(), expected_size);
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_offset_panics_on_short_slice() {
        let short_data = vec![0u8; OBJECT_KEY_INDEX - 1];
        let index = DefaultIndex::wrap(&short_data[..]);
        let _ = index.offset();
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_object_key_panics_on_short_slice() {
        let short_data = vec![0u8; POSITION_INDEX - 1];
        let index = DefaultIndex::wrap(&short_data[..]);
        let _ = index.object_key();
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_position_panics_on_short_slice() {
        let short_data = vec![0u8; TIMESTAMP_INDEX - 1];
        let index = DefaultIndex::wrap(&short_data[..]);
        let _ = index.position();
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_timestamp_panics_on_short_slice() {
        let short_data = vec![0u8; SIZE_INDEX - 1];
        let index = DefaultIndex::wrap(&short_data[..]);
        let _ = index.timestamp();
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_size_getter_panics_on_short_slice() {
        let short_data = vec![0u8; SIZE_INDEX + size_of::<u64>() - 1];
        let index = DefaultIndex::wrap(&short_data[..]);
        let _ = index.size();
    }

    #[test]
    fn test_debug() {
        let data = vec![0xBBu8; DefaultIndex::size_of()];
        let index = DefaultIndex::wrap(&data[..]);
        let debug_str = format!("{:?}", index);
        assert!(debug_str.starts_with("DefaultIndex("));
        assert!(debug_str.contains("["));
    }
}
