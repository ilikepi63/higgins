//! An Eventual is a structure that can represent a value present or not,
//! where another arbitrary writer can write to.

use std::sync::{
    Arc,
    atomic::{AtomicPtr, Ordering},
};

use tokio::sync::Notify;

use crate::error::HigginsError;

pub fn eventual<T>() -> (Eventual<T>, Setter<T>) {
    let data = Arc::new((AtomicPtr::new(std::ptr::null_mut()), Notify::new()));

    (Eventual(data.clone()), Setter(data))
}

#[derive(Debug)]
pub struct Eventual<T>(Arc<(AtomicPtr<T>, Notify)>);

impl<T> Eventual<T> {
    pub async fn get(&self) -> Result<T, HigginsError> {
        let ptr = self.0.0.load(Ordering::SeqCst);

        if ptr.is_null() {
            drop(ptr);
            self.0.1.notified().await;
        }
        let ptr = self.0.0.load(Ordering::SeqCst);
        // SAFETY: If notify has been notified, this means this value is present.
        Ok(unsafe { std::ptr::read(ptr) })
    }
}

pub struct Setter<T>(Arc<(AtomicPtr<T>, Notify)>);

impl<T> Setter<T> {
    pub fn set(&self, val: T) {
        let set = Box::new(val);

        let ptr = Box::into_raw(set);

        self.0.0.store(ptr, Ordering::SeqCst);

        self.0.1.notify_waiters();
    }
}
