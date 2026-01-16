use thiserror::Error;

use crate::subscription::error::SubscriptionError;
use crate::topography::errors::TopographyError;

#[derive(Error, Debug)]
pub enum HigginsTaskError {
    #[error("An Infallible error has seemed to occur.")]
    Infallible,
}
