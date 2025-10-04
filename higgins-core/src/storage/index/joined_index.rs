use crate::storage::index::Timestamped;
use std::io::Write;

/// JoinedIndex represents the index metadata that one will use to
/// keep track of both offsets of each stream this is derived from.
pub struct JoinedIndex<'a>(&'a [u8]);

// /// The offset of the resultant index.
// pub offset: u64,
// /// The object key holding the resultant data from the joining.
// pub object_key: Option<[u8; 16]>,
// /// The timestamp for this index.
// pub timestamp: u64,
// /// The offsets of the derivative streams.
// pub offsets: T,

const OFFSET_INDEX: usize = 0;
const OBJECT_KEY_INDEX: usize = OFFSET_INDEX + size_of::<u64>();
const TIMESTAMP_INDEX: usize = OBJECT_KEY_INDEX + size_of::<[u8; 16]>();
const INDEXES_INDEX: usize = TIMESTAMP_INDEX + size_of::<u64>();

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
    // pub fn offset(&self) -> u64 {
    //     u64::from_be_bytes(self.0[OFFSET_INDEX..OBJECT_KEY_INDEX].try_into().unwrap())
    // }
    //
    // Constructors
    /// Puts the data into the mutable slice, returning this struct as a reference over it.
    fn put(
        &mut self,
        offset: u64,
        object_key: [u8; 16],
        timestamp: u64,
        indexes: &[u64],
        mut data: &mut [u8],
    ) {
        data.write_all(offset.to_be_bytes().as_slice());
        data.write_all(object_key.as_slice());
        data.write_all(timestamp.to_be_bytes().as_slice());

        for index in indexes {
            data.write_all(index.to_be_bytes().as_slice());
        }
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
