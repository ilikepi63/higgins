//! File-related operations for a given typography.
use serde::Serialize;
use super::errors::TopographyError;

use std::io::Write as _;
pub struct TypographyFile(std::path::Path);




impl TypographyFile{

    /// Appends an item to this file.
    fn add_item<T:Serialize>(&self, val: T) -> Result<(), TopographyError> {

        let mut file = std::fs::OpenOptions::new().append(true).create(true).open(&self.0)?;

        let serialized = serde_json::to_vec(&val)?;

        file.write_all(&serialized)?;

        file.flush()?;

        Ok(())
    }

}
