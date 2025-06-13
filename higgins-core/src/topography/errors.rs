use thiserror::Error;

#[derive(Error, Debug)]
pub enum TopographyError {
    #[error("The entry at the designated key is already occupied: {0}")]
    Occupied(String),
}