use std::fmt::Display;

#[derive(Debug, Eq, Ord, PartialOrd, Clone)]
pub struct StreamName(String);

impl StreamName {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
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

impl From<&str> for StreamName {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl Into<String> for StreamName {
    fn into(self) -> String {
        self.0
    }
}
