use std::{
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

use higgins_shared::HigginsError;

#[derive(Debug)]
pub struct FunctionCollection {
    base_dir: PathBuf,
}

impl FunctionCollection {
    pub fn new(path: PathBuf) -> Self {
        Self { base_dir: path }
    }

    pub async fn put_function(&self, name: &str, module: Vec<u8>) -> Result<(), HigginsError> {
        let path = {
            let mut path = self.base_dir.clone();
            path.push(name);

            path
        };

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        file.write_all(&module)?;

        Ok(())
    }

    pub async fn get_function(&self, name: &str) -> Result<Vec<u8>, HigginsError> {
        let path = {
            let mut path = self.base_dir.clone();
            path.push(name);

            path
        };

        tracing::info!("Reading function: {:#?}", path);

        let mut file = std::fs::OpenOptions::new().read(true).open(path)?;

        tracing::trace!("File Metadata: {:#?}", file.metadata()?.size());

        let mut buffer = vec![0; file.metadata()?.size().try_into()?];

        file.read_exact(&mut buffer)?;

        Ok(buffer)
    }
}
