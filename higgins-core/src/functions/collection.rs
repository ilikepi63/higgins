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
            .unwrap();

        file.write_all(&module).unwrap();
    }

    pub async fn get_function(&self, name: &str) -> Vec<u8> {
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
            .unwrap();

        let mut buffer = Vec::with_capacity(file.metadata().unwrap().size().try_into().unwrap());

        file.read(&mut buffer).unwrap();

        buffer
    }
}
