use thiserror::Error;

#[derive(Error, Debug)]
pub enum TopographyError {
    #[error("The entry at the designated key is already occupied: {0}")]
    Occupied(String),
    #[error("Derivative stream not found in topography: {0}")]
    DerivativeNotFound(String),
    #[error("Schema not found in topography: {0}")]
    SchemaNotFound(String),
}
