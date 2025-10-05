use crate::storage::index::{Timestamped, WrapBytes};
use std::io::Write;

/// JoinedIndex represents the index metadata that one will use to
/// keep track of both offsets of each stream this is derived from.
pub struct JoinedIndex<'a>(&'a [u8]);
// /// The offset of the resultant index.
// pub offset: u64,
// /// The timestamp for this index.
// pub timestamp: u64,
// /// The object key holding the resultant data from the joining.
// pub object_key: Option<[u8; 16]>,
// /// The offsets of the derivative streams.
// pub offsets: T,

const OFFSET_INDEX: usize = 0;
const TIMESTAMP_INDEX: usize = OFFSET_INDEX + size_of::<u64>();
const OBJECT_KEY_OPTIONAL_INDEX: usize = TIMESTAMP_INDEX + size_of::<u64>();
const OBJECT_KEY_INDEX: usize = OBJECT_KEY_OPTIONAL_INDEX + size_of::<bool>();
const INDEXES_INDEX: usize = OBJECT_KEY_INDEX + size_of::<[u8; 16]>();

impl<'a> JoinedIndex<'a> {
    // Properties.
    /// Offset
    pub fn offset(&self) -> u64 {
        u64::from_be_bytes(self.0[OFFSET_INDEX..OBJECT_KEY_INDEX].try_into().unwrap())
    }
    pub fn object_key(&self) -> [u8; 16] {
        self.0[OBJECT_KEY_INDEX..TIMESTAMP_INDEX]
            .try_into()
            .unwrap()
    }

    // Constructors
    /// Puts the data into the mutable slice, returning this struct as a reference over it.
    fn put(
        &mut self,
        offset: u64,
        object_key: Option<[u8; 16]>,
        timestamp: u64,
        indexes: &[u64],
        mut data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        data.write_all(offset.to_be_bytes().as_slice())?;
        data.write_all(timestamp.to_be_bytes().as_slice())?;

        match object_key {
            Some(object_key) => {
                data.write_all(&u8::to_be_bytes(1));
                data.write_all(object_key.as_slice())?;
            }
            None => {
                data.write_all(&u8::to_be_bytes(0));
                data.write_all([0_u8; 16].as_slice())?;
            }
        }

        for index in indexes {
            data.write_all(index.to_be_bytes().as_slice())?;
        }

        Ok(())
    }
}

impl<'a> From<&'a [u8]> for JoinedIndex<'a> {
    fn from(src: &'a [u8]) -> Self {
        Self(src)
    }
}

impl<'a> Timestamped for JoinedIndex<'a> {
    fn timestamp(&self) -> u64 {
        u64::from_be_bytes(self.0[TIMESTAMP_INDEX..INDEXES_INDEX].try_into().unwrap())
    }
}

impl<'a> WrapBytes<'a> for JoinedIndex<'a> {
    fn wrap(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_slice() {
        let data = vec![0u8; INDEXES_INDEX];
        let index = JoinedIndex::from(&data[..]);
        assert_eq!(index.0, data.as_slice());
    }

    #[test]
    fn test_offset() {
        let mut data = vec![0u8; INDEXES_INDEX];
        let expected_offset: u64 = 0x123456789ABCDEF0;
        data[OFFSET_INDEX..OBJECT_KEY_INDEX].copy_from_slice(&expected_offset.to_be_bytes());

        let index = JoinedIndex::from(&data[..]);
        assert_eq!(index.offset(), expected_offset);
    }

    #[test]
    fn test_object_key() {
        let mut data = vec![0u8; INDEXES_INDEX];
        let expected_key = [0xAA; 16];
        data[OBJECT_KEY_INDEX..TIMESTAMP_INDEX].copy_from_slice(&expected_key);

        let index = JoinedIndex::from(&data[..]);
        assert_eq!(index.object_key(), expected_key);
    }

    #[test]
    fn test_timestamp() {
        let mut data = vec![0u8; INDEXES_INDEX];
        let expected_timestamp: u64 = 0xFEDCBA9876543210;
        data[TIMESTAMP_INDEX..INDEXES_INDEX].copy_from_slice(&expected_timestamp.to_be_bytes());

        let index = JoinedIndex::from(&data[..]);
        assert_eq!(index.timestamp(), expected_timestamp);
    }

    #[test]
    fn test_timestamped_trait() {
        let mut data = vec![0u8; INDEXES_INDEX];
        let expected_timestamp: u64 = 0xFEDCBA9876543210;
        data[TIMESTAMP_INDEX..INDEXES_INDEX].copy_from_slice(&expected_timestamp.to_be_bytes());

        let index = JoinedIndex::from(&data[..]);
        assert_eq!(Timestamped::timestamp(&index), expected_timestamp);
    }

    #[test]
    fn test_put_returns_error_on_short_data() {
        let mut index = JoinedIndex::from(&[0u8; 0][..]); // Empty slice for mut self
        let mut short_data = vec![0u8; 10]; // Too short
        let result = index.put(0, Some([0u8; 16]), 0, &[], &mut short_data);

        assert!(matches!(result, Err(_)));
    }

    #[test]
    fn test_put_writes_correctly() {
        let mut index = JoinedIndex::from(&[0u8; 32][..]); // Dummy for mut self, ignored
        let offset: u64 = 0x123456789ABCDEF0;
        let object_key = [0xAA; 16];
        let timestamp: u64 = 0xFEDCBA9876543210;
        let indexes = vec![0x1111111111111111u64, 0x2222222222222222u64];

        let mut data = vec![0u8; 32 + indexes.len() * 8];
        index.put(offset, Some(object_key), timestamp, &indexes, &mut data);

        // Verify written data
        let read_offset = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let read_key: [u8; 16] = data[8..24].try_into().unwrap();
        let read_timestamp = u64::from_be_bytes(data[24..32].try_into().unwrap());
        let read_index1 = u64::from_be_bytes(data[32..40].try_into().unwrap());
        let read_index2 = u64::from_be_bytes(data[40..48].try_into().unwrap());

        assert_eq!(read_offset, offset);
        assert_eq!(read_key, object_key);
        assert_eq!(read_timestamp, timestamp);
        assert_eq!(read_index1, indexes[0]);
        assert_eq!(read_index2, indexes[1]);
    }
}
