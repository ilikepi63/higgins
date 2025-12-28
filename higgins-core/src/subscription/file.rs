//! File-related ustilites for managing Subscriptions.

use std::path::PathBuf;

pub struct SubscriptionDirectory {
    /// File that holds all of the subscriptions and the ordering thereof.
    partition_file: PathBuf,
}
