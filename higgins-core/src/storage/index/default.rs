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
    pub fn to_bytes(&self) -> BytesMut {
        let buf = BytesMut::from(self.0);

        buf
    }

    pub const fn size() -> usize {
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
