//! File-related operations for a given typography.
use super::errors::TopographyError;
use serde::{Deserialize, Serialize};

use {std::io::Read as _, std::io::Write as _};
pub struct TypographyFile(std::path::Path);

impl TypographyFile {
    /// Appends an item to this file.
    fn add_item<T: Serialize>(&self, val: T) -> Result<(), TopographyError> {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.0)?;

        let serialized = serde_json::to_vec(&val)?;

        file.write_all(&serialized)?;

        file.flush()?;

        Ok(())
    }

    /// Reads the entirety of this into the determined header.
    pub fn read<T, V: TryFrom<Vec<T>>>(&self) -> Result<V, TopographyError>
    where
        for<'a> T: Deserialize<'a>,
        TopographyError: From<<V as TryFrom<Vec<T>>>::Error>,
    {
        let mut file = std::fs::OpenOptions::new().read(true).open(&self.0)?;

        let mut data = Vec::new();

        file.read_to_end(&mut data)?;

        let data_string = String::from_utf8(data)?;

        let values: Vec<T> = data_string
            .split("\\n")
            .map(|t_string| {
                let t = serde_json::from_str(&t_string);
                t
            })
            .collect::<Result<Vec<T>, serde_json::error::Error>>()?;

        let result = V::try_from(values)?;

        Ok(result)
    }
}
