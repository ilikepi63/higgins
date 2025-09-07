use super::Broker;

use std::{path::PathBuf, str::FromStr};

impl Default for Broker {
    fn default() -> Self {
        Self::new(PathBuf::from_str("higgins_data").unwrap())
    }
}
