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
            let notified = self.0.1.notified();

            if let Some(val) = self.0.0.lock().await.as_ref() {
                return Ok(val.clone());
            }

            notified.await;
        }
    }
}

pub struct Setter<T>(Arc<(Mutex<Option<T>>, Notify)>);

impl<T> Setter<T> {
    pub async fn set(&self, val: T) {
        *self.0.0.lock().await = Some(val);
        self.0.1.notify_waiters();
    }
}
