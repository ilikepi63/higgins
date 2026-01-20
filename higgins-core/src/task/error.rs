use thiserror::Error;

#[derive(Error, Debug)]
pub enum HigginsTaskError {
    #[error("Attempt to retrieve a task hierarchy that does not exist.")]
    TaskHierarchyDoesNotExist,
}
