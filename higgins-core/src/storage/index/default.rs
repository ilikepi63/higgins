use bytes::{BufMut as _, BytesMut};

pub struct Index {
    pub offset: u32,
    pub object_key: [u8; 16],
    pub position: u32,
    pub timestamp: u64,
    pub size: u64,
}

impl Index {
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn object_key(&self) -> [u8; 16] {
        self.object_key
    }

    pub fn position(&self) -> u32 {
        self.position
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(INDEX_SIZE);

        buf.put_u32(self.offset);
        buf.put_slice(&self.object_key);
        buf.put_u32(self.position);
        buf.put_u64(self.timestamp);
        buf.put_u64(self.size);

        buf
    }

    pub const fn size() -> usize {
        INDEX_SIZE
    }
}

pub const INDEX_SIZE: usize = std::mem::size_of::<u32>() // offset
    + std::mem::size_of::<u32>() // position
    + std::mem::size_of::<u64>() // timestamp
    + std::mem::size_of::<[u8; 16]>() // object_key
    + std::mem::size_of::<u64>(); // size

#[repr(C, packed)]
pub struct DefaultIndex {
    pub offset: u32,
    pub object_key: [u8; 16],
    pub position: u32,
    pub timestamp: u64,
    pub size: u64,
}

impl DefaultIndex {
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn object_key(&self) -> [u8; 16] {
        self.object_key
    }

    pub fn position(&self) -> u32 {
        self.position
    }

    fn as_slice(&self) -> &[u8; INDEX_SIZE] {
        // SAFETY:
        // * `MyStruct` has the same size as `[u8; MY_STRUCT_SIZE]`.
        // * `[u8; MY_STRUCT_SIZE]` has no alignment requirement.
        // * Since it is packed, this type has no padding.
        unsafe { &*(self as *const DefaultIndex as *const [u8; INDEX_SIZE]) }
    }

    pub fn to_bytes(&self) -> BytesMut {
        BytesMut::from(self.as_slice().as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    // Helper function to create test instances
    fn create_test_instances() -> (Index, DefaultIndex) {
        let object_key = [1u8; 16];
        let index = Index {
            offset: 42,
            object_key,
            position: 100,
            timestamp: 1234567890,
            size: 1024,
        };
        let default_index = DefaultIndex {
            offset: 42,
            object_key,
            position: 100,
            timestamp: 1234567890,
            size: 1024,
        };
        (index, default_index)
    }

    #[test]
    fn test_method_timestamp() {
        let (index, default_index) = create_test_instances();
        assert_eq!(
            index.timestamp(),
            default_index.timestamp(),
            "timestamp() should return the same value"
        );
    }

    #[test]
    fn test_method_object_key() {
        let (index, default_index) = create_test_instances();
        assert_eq!(
            index.object_key(),
            default_index.object_key(),
            "object_key() should return the same value"
        );
    }

    #[test]
    fn test_method_position() {
        let (index, default_index) = create_test_instances();
        assert_eq!(
            index.position(),
            default_index.position(),
            "position() should return the same value"
        );
    }

    #[test]
    fn test_to_bytes_equality() {
        let (index, default_index) = create_test_instances();
        let index_bytes = index.to_bytes();
        let default_index_bytes = default_index.to_bytes();

        assert_eq!(
            index_bytes, default_index_bytes,
            "to_bytes() should produce identical BytesMut"
        );
        assert_eq!(
            index_bytes.len(),
            INDEX_SIZE,
            "Index to_bytes length should match INDEX_SIZE"
        );
        assert_eq!(
            default_index_bytes.len(),
            INDEX_SIZE,
            "DefaultIndex to_bytes length should match INDEX_SIZE"
        );
    }

    #[test]
    fn test_different_values() {
        let index = Index {
            offset: 42,
            object_key: [1u8; 16],
            position: 100,
            timestamp: 1234567890,
            size: 1024,
        };
        let default_index = DefaultIndex {
            offset: 43, // Different offset
            object_key: [1u8; 16],
            position: 100,
            timestamp: 1234567890,
            size: 1024,
        };

        assert_ne!(
            index.to_bytes(),
            default_index.to_bytes(),
            "to_bytes() should differ when structs differ"
        );
    }
}
