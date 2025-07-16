use thiserror::Error;

use crate::subscription::error::SubscriptionError;
use crate::topography::errors::TopographyError;

#[derive(Error, Debug)]
pub enum HigginsError {
    #[error("Stream/Subscription does not exist: {0} {1}")]
    SubscriptionForStreamDoesNotExist(String, String),

    #[error("Error occurred with Subscriptions.")]
    SubscriptionError(#[from] SubscriptionError),

    #[error("Error occurred with Typography.")]
    TopographyError(#[from] TopographyError),

    #[error("Unknown Error")]
    Unknown,
}
