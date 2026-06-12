use std::fmt::Display;

use serde::{Deserialize, Serialize};
use std::ffi::CStr;
use thiserror::Error;
/// Name of the partition.
///
/// The reason for choosing 32 is because:
/// - The need for a fixed size buffer.
/// - A long enough buffer for users to be able to store human-readable names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionName([u8; 32]);

impl Display for PartitionName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&String::from_utf8_lossy(&self.0))
    }
}

impl PartitionName {
    /// Attempts to convert this key name into a String.
    ///
    /// TODO: This should ideally error out but also a key name should always ideally be a
    /// Sized string, so we'd need to change the internals of this struct.
    pub fn to_string(&self) -> Option<String> {
        let cstr = CStr::from_bytes_until_nul(&self.0).ok()?;

        Some(cstr.to_str().ok()?.to_string())
    }

    /// Equality comparison between bytes and these inner bytes.
    pub fn eq_bytes(&self, bytes: &[u8]) -> bool {
        self.0.iter().eq(bytes)
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl From<PartitionName> for Vec<u8> {
    fn from(val: PartitionName) -> Self {
        val.0.to_vec()
    }
}

impl TryFrom<&[u8]> for PartitionName {
    type Error = PartitionNameError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() > size_of::<Self>() {
            return Err(PartitionNameError::TooManyBytesForPartitionName(
                bytes.len(),
            ));
        }

        let mut result = [0; 32];

        result[0..bytes.len()].copy_from_slice(bytes);

        Ok(Self(result))
    }
}

impl TryFrom<&str> for PartitionName {
    type Error = PartitionNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.as_bytes())
    }
}

#[derive(Error, Debug)]
pub enum PartitionNameError {
    #[error("Too many bytes for partition name: {0}")]
    TooManyBytesForPartitionName(usize),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_try_from_empty_string() {
        let name = PartitionName::try_from("").unwrap();
        assert_eq!(name.0, [0u8; 32]);
    }

    #[test]
    fn test_try_from_short_string() {
        let input = "hello";
        let name = PartitionName::try_from(input).unwrap();
        assert_eq!(&name.0[..input.len()], input.as_bytes());
        assert_eq!(&name.0[input.len()..], &[0u8; 27]);
    }

    #[test]
    fn test_try_from_exact_length() {
        let input = "a".repeat(32);
        let name = PartitionName::try_from(input.as_str()).unwrap();
        assert_eq!(name.0, input.as_bytes());
    }

    #[test]
    fn test_try_from_too_long() {
        let input = "a".repeat(33);
        let err = PartitionName::try_from(input.as_str()).unwrap_err();
        match err {
            PartitionNameError::TooManyBytesForPartitionName(len) => {
                assert_eq!(len, 33);
            }
        }
    }

    #[test]
    fn test_into_vec() {
        let input = "test";
        let name = PartitionName::try_from(input).unwrap();
        let vec: Vec<u8> = name.into();
        assert_eq!(vec.len(), 32);
        assert_eq!(&vec[..input.len()], input.as_bytes());
        assert_eq!(&vec[input.len()..], &[0u8; 28]);
    }

    #[test]
    fn test_clone() {
        let original = PartitionName::try_from("clone-test").unwrap();
        let cloned = original.clone();
        assert_eq!(original.0, cloned.0);
    }
}
