//! File-related operations for a given typography.
use higgins_shared::TopographyError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use {std::io::Read as _, std::io::Write as _};

static FILE_NAME: &str = "topography.jsonl";

#[derive(Debug)]
pub struct TopographyFile(PathBuf);

impl TopographyFile {
    pub fn new(mut path: PathBuf) -> Self {
        path.push(FILE_NAME);
        Self(path)
    }

    /// Appends an item to this file.
    pub fn add_item<T: Serialize>(&self, val: T) -> Result<(), TopographyError> {
        let (mut file, created) = {
            match std::fs::exists(&self.0)? {
                true => {
                    let file = std::fs::OpenOptions::new().append(true).open(&self.0)?;

                    (file, false)
                }
                false => {
                    let mut dir_path = self.0.clone();

                    if dir_path.pop() {
                        std::fs::create_dir_all(dir_path)?;
                    }

                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&self.0)?;

                    (file, true)
                }
            }
        };

        let serialized = serde_json::to_string(&val)?;

        let value = {
            match created {
                true => serialized,
                false => {
                    let mut v = "\\n".to_string();
                    v.push_str(&serialized);
                    v
                }
            }
        };

        file.write_all(value.as_bytes())?;

        file.flush()?;

        Ok(())
    }

    /// Reads the entirety of this into the determined header.
    pub fn read<T>(&self) -> Result<Vec<T>, TopographyError>
    where
        for<'a> T: Deserialize<'a>,
    {
        let mut file = std::fs::OpenOptions::new().read(true).open(&self.0)?;

        let mut data = Vec::new();

        file.read_to_end(&mut data)?;

        let data_string = String::from_utf8(data)?;

        let values: Vec<T> = data_string
            .split("\\n")
            .map(|t_string| serde_json::from_str(t_string))
            .collect::<Result<Vec<T>, serde_json::error::Error>>()?;

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::TopographyFile;
    use serde_json::json;
    use std::path::PathBuf;
    use std::str::FromStr;

    #[test]
    fn add_item_writes_json_and_read_single() {
        let temp_file_name = PathBuf::from_str(&uuid::Uuid::new_v4().to_string())?;

        let moved_temp_file_name = temp_file_name.clone();

        let result = std::panic::catch_unwind(|| {
            let typography_file = TopographyFile::new(moved_temp_file_name);

            typography_file
                .add_item(json!({ "a": 1 }))
                .expect("add item");

            let values: Vec<serde_json::Value> = typography_file.read().expect("read values");
            assert_eq!(values, vec![json!({ "a": 1 })]);
        });

        std::fs::remove_dir_all(temp_file_name)?;

        result?;
    }

    #[test]
    fn read_multiple_items_from_newline_separated_json() {
        let temp_file_name = PathBuf::from_str(&uuid::Uuid::new_v4().to_string())?;

        let moved_temp_file_name = temp_file_name.clone();

        let result = std::panic::catch_unwind(|| {
            let typography_file = TopographyFile::new(moved_temp_file_name);

            typography_file
                .add_item(json!({ "a": 1 }))
                .expect("add item");
            typography_file
                .add_item(json!({ "b": 2 }))
                .expect("add item");

            let values: Vec<serde_json::Value> = typography_file.read().expect("read values");
            assert_eq!(values, vec![json!({ "a": 1 }), json!({"b": 2})]);
        });

        std::fs::remove_dir_all(temp_file_name)?;

        result?;
    }
}
