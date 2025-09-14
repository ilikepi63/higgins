use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Attempt to open a non-directory index file.")]
    IndexFileIsNotADirectory,
    #[error("Unknown Index Error")]
    Unknown,
}
