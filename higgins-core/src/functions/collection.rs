use std::{
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

#[derive(Debug)]
pub struct FunctionCollection {
    base_dir: PathBuf,
}

impl FunctionCollection {
    pub fn new(path: PathBuf) -> Self {
        Self { base_dir: path }
    }

    pub async fn put_function(&self, name: &str, module: Vec<u8>) {
        let path = {
            let mut path = self.base_dir.clone();
            path.push(name);

            path
        };

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            ?;

        file.write_all(&module)?;
    }

    pub async fn get_function(&self, name: &str) -> Vec<u8> {
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

        buffer
    }
}
