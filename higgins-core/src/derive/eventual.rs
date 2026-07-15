//! An Eventual is a structure that can represent a value present or not,
//! where another arbitrary writer can write to.

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Notify;

use higgins_shared::HigginsError;

pub fn eventual<T>() -> (Eventual<T>, Setter<T>) {
    let data = Arc::new((Mutex::new(None), Notify::new()));
    (Eventual(data.clone()), Setter(data))
}

#[derive(Clone, Debug)]
pub struct Eventual<T>(Arc<(Mutex<Option<T>>, Notify)>);

impl<T: Clone> Eventual<T> {
    pub async fn get(&self) -> Result<T, HigginsError> {
        loop {
            tracing::debug!("Retrieving guard..");
            let guard = self.0.0.lock().await;
            tracing::debug!("Retrieved guard.");

            if let Some(val) = guard.as_ref() {
                tracing::debug!("Returning value..");

                return Ok(val.clone());
            }

            drop(guard);

            let notified = self.0.1.notified();

            notified.await;

            tracing::debug!("Get value has been notified..");
        }
    }
}

#[derive(Debug)]
pub struct Setter<T>(Arc<(Mutex<Option<T>>, Notify)>);

impl<T> Setter<T> {
    pub async fn set(&self, val: T) {
        *self.0.0.lock().await = Some(val);
        tracing::debug!("Notifying waiters..");

        self.0.1.notify_waiters();
    }
}
