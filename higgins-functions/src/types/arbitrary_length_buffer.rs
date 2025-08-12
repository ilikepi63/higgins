pub struct ArbitraryLengthBuffer(Vec<u8>);

impl From<&[u8]> for ArbitraryLengthBuffer {
    fn from(value: &[u8]) -> Self {
        let length = value.len() as i64;

        // TODO: Maybe we don't want to reallocate here?
        let mut vec = length.to_be_bytes().to_vec();

        vec.extend_from_slice(value);

        Self(vec)
    }
}

impl From<*const u8> for ArbitraryLengthBuffer {
    fn from(value: *const u8) -> Self {
        let length_ptr = value as *const [u8; 8];

        let length = u64::from_be_bytes(unsafe { *length_ptr });

        let val_ptr = unsafe { length_ptr.add(1) };

        Self(unsafe { Vec::from_raw_parts(val_ptr as *mut u8, length as usize, length as usize) })
    }
}

impl ArbitraryLengthBuffer {
    pub fn new(v: Vec<u8>) -> Self {
        Self(v)
    }

    pub fn data(&self) -> &[u8] {
        &self.0[8..&self.0.len() - 1]
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}
