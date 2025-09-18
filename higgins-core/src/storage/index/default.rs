#[allow(unused_imports)] // No idea why this is throwing a warning.
use bytes::{BufMut as _, BytesMut};
use rkyv::{Archive, Deserialize, Serialize};
use crate::storage::index::Timestamped;

/// A default index for us with generic streams.
#[derive(Archive, Debug, Deserialize, Serialize)]
pub struct DefaultIndex {
    pub offset: u32,
    pub object_key: [u8; 16],
    pub position: u32,
    pub timestamp: u64,
    pub size: u64,
}

impl DefaultIndex {
    pub fn to_bytes(&self) -> BytesMut {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap();

        let buf = BytesMut::from(bytes.as_slice());

        buf
    }

    pub const fn size() -> usize {
        std::mem::size_of::<ArchivedDefaultIndex>()
    }
}


impl Timestamped for ArchivedDefaultIndex {
    fn timestamp(&self) -> u64 {
        self.timestamp.to_native()
    }
}