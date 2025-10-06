use crate::storage::index::{IndexError, Timestamped, WrapBytes};
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
        self.0[OBJECT_KEY_INDEX..INDEXES_INDEX].try_into().unwrap()
    }

    // Constructors
    /// Puts the data into the mutable slice, returning this struct as a reference over it.
    fn put(
        &mut self,
        offset: u64,
        object_key: Option<[u8; 16]>,
        timestamp: u64,
        offsets: &[Option<u64>],
        mut data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        data.write_all(offset.to_be_bytes().as_slice())?;
        data.write_all(timestamp.to_be_bytes().as_slice())?;

        match object_key {
            Some(object_key) => {
                data.write_all(&u8::to_be_bytes(1))?;
                data.write_all(object_key.as_slice())?;
            }
            None => {
                data.write_all(&u8::to_be_bytes(0))?;
                data.write_all([0_u8; 16].as_slice())?;
            }
        }

        for offset in offsets {
            match offset {
                Some(offset) => {
                    data.write_all(&u8::to_be_bytes(1))?;
                    data.write_all(offset.to_be_bytes().as_slice())?;
                }
                None => {
                    data.write_all(&u8::to_be_bytes(0))?;
                    // Write an empty byte array.
                    data.write_all([0_u8; 8].as_slice())?;
                }
            }
        }

        Ok(())
    }

    /// Gets the offset at the specified index.
    pub fn get_offset(&self, index: usize) -> Result<u64, IndexError> {
        match self.within_bounds(index) {
            true => {
                let relative_index = (index * (size_of::<u8>() + size_of::<u64>())) + INDEXES_INDEX;

                let offset =
                    &self.0[relative_index..relative_index + (size_of::<u8>() + size_of::<u64>())];

                let (optional, offset) = offset.split_at(1);

                if (u8::from_be_bytes(optional.try_into().unwrap()) == 1) {
                    Ok(u64::from_be_bytes(offset.try_into().unwrap()))
                } else {
                    Err(IndexError::IndexInJoinedIndexNotFound)
                }
            }
            false => Err(IndexError::IndexGivenOutOfBoundsForJoinedIndex),
        }
    }

    /// Puts the offset at the specified index.
    pub fn put_offset() {}

    // Helpers
    pub fn size_of(n_offsets: usize) -> usize {
        // last index (add one to make length), plus the amount of indexes times the size of the optional and the size of the offset.
        INDEXES_INDEX + 1 + (n_offsets * (size_of::<u8>() + size_of::<u64>()))
    }

    /// Checks whether an index given is within the specific bounds of this JoinedIndex.
    fn within_bounds(&self, index: usize) -> bool {
        index + INDEXES_INDEX < (self.0.len() - 1)
            && (index + (size_of::<u8>() + size_of::<u64>())) < (self.0.len() - 1)
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
