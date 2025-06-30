use thiserror::Error;

use crate::subscription::error::SubscriptionError;

#[derive(Error, Debug)]
pub enum HigginsError {
    #[error("Stream/Subscription does not exist: {0} {1}")]
    SubscriptionForStreamDoesNotExist(String, String),

    #[error("Error occurred with Subscriptions.")]
    SubscriptionError(#[from] SubscriptionError),
    #[error("Unknown Error")]
    Unknown,
}
