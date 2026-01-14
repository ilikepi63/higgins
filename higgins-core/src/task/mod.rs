//! The primitives for handling asynchronous tasks inside of higgins.

use std::panic::UnwindSafe;

use futures::FutureExt;

/// Primary structure for handling the creation and deletion of tasks.
pub struct TaskHandler;

impl TaskHandler {
    /// Spawn a future inside of this task handle.
    pub fn spawn<F>(future: F)
    where
        F: Future + Send + 'static + UnwindSafe,
        F::Output: Send + 'static,
    {
        tokio::spawn(async move { future.catch_unwind().await });
    }
}
