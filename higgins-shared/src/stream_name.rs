use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Eq, Ord, PartialOrd, Clone, Serialize, Deserialize)]
pub struct StreamName(String);

impl StreamName {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl PartialEq for StreamName {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Display for StreamName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&[u8]> for StreamName {
    fn from(value: &[u8]) -> Self {
        Self(String::from_utf8_lossy(value).to_string())
    }
}

impl From<Vec<u8>> for StreamName {
    fn from(value: Vec<u8>) -> Self {
        Self::from(value.as_slice())
    }
}

impl From<&str> for StreamName {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for StreamName {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<StreamName> for String {
    fn from(val: StreamName) -> Self {
        val.0
    }
}
