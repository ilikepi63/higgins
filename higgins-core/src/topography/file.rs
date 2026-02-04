//! File-related operations for a given typography.
use super::errors::TopographyError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use {std::io::Read as _, std::io::Write as _};

pub struct TypographyFile(PathBuf);

impl TypographyFile {

    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }

    /// Appends an item to this file.
    fn add_item<T: Serialize>(&self, val: T) -> Result<(), TopographyError> {

        println!("Calling..");

        let (mut file, created) = {
            match std::fs::exists(&self.0).inspect_err(|e| {
                println!("Yes, error here");
            })? {
                true => {
                    println!("Exists!");
                    let  file = std::fs::OpenOptions::new()
                            .append(true)
                           .open(&self.0)?;

                    (file, false)
                },
                false => {
                    println!("Doesn't exist!");
                    let file = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                           .open(&self.0)?;


                    (file, true)

                }
            }
        };

        let serialized = serde_json::to_string(&val)?;
        println!("Writing {:#?}", serialized);

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

        println!("Writing {:#?}", value);

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
            .map(|t_string| {
                let t = serde_json::from_str(&t_string);
                t
            })
            .collect::<Result<Vec<T>, serde_json::error::Error>>()?;

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::TypographyFile;
    use super::super::errors::TopographyError;
    use serde_json::json;
    use std::path::PathBuf;
    use std::str::FromStr;


    #[test]
    fn add_item_writes_json_and_read_single() {
        let temp_file_name =  PathBuf::from_str(&uuid::Uuid::new_v4().to_string()).unwrap();

        let moved_temp_file_name = temp_file_name.clone();

        let result = std::panic::catch_unwind(|| {
            let typography_file = TypographyFile::new(moved_temp_file_name);

            typography_file.add_item(json!({ "a": 1 })).expect("add item");

            let values: Vec<serde_json::Value> = typography_file.read().expect("read values");
            assert_eq!(values, vec![json!({ "a": 1 })]);
        });

        std::fs::remove_file(temp_file_name).unwrap();

        result.unwrap();
    }

    #[test]
    fn read_multiple_items_from_newline_separated_json() {
        let temp_file_name =  PathBuf::from_str(&uuid::Uuid::new_v4().to_string()).unwrap();

        let moved_temp_file_name = temp_file_name.clone();

        let result = std::panic::catch_unwind(|| {
            let typography_file = TypographyFile::new(moved_temp_file_name);

            typography_file.add_item(json!({ "a": 1 })).expect("add item");
            typography_file.add_item(json!({ "b": 2 })).expect("add item");

            let values: Vec<serde_json::Value> = typography_file.read().expect("read values");
            assert_eq!(values, vec![json!({ "a": 1 }), json!({"b": 2})]);
        });

        std::fs::remove_file(temp_file_name).unwrap();

        result.unwrap();

    }
}
