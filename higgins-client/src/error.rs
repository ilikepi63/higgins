use thiserror::Error;

#[derive(Error, Debug)]
pub enum HigginsClientError {
    #[error("Unknown Error")]
    Unknown,
}
