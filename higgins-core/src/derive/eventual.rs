//! An Eventual is a structure that can represent a value present or not,
//! where another arbitrary writer can write to.

use std::sync::{
    Arc,
    atomic::{AtomicPtr, Ordering},
};

use tokio::sync::Notify;

use crate::error::HigginsError;

pub fn eventual<T: Clone + Copy>() -> (Eventual<T>, Setter<T>) {
    let data = Arc::new((AtomicPtr::new(std::ptr::null_mut()), Notify::new()));

    (Eventual(data.clone()), Setter(data))
}

pub struct Eventual<T: Clone + Copy>(Arc<(AtomicPtr<T>, Notify)>);

impl<T: Clone + Copy> Eventual<T> {
    pub async fn get(self) -> Result<T, HigginsError> {
        let ptr = self.0.0.load(Ordering::SeqCst);

        if ptr.is_null() {
            self.0.1.notified().await;
        }
        // SAFETY: If notify has been notified, this means this value is present.
        Ok(unsafe { *ptr })
    }
}

pub struct Setter<T: Clone + Copy>(Arc<(AtomicPtr<T>, Notify)>);

impl<T: Clone + Copy> Setter<T> {
    pub fn set(&self, val: T) {
        let set = Box::new(val);

        let ptr = Box::into_raw(set);

        self.0.0.store(ptr, Ordering::SeqCst);

        self.0.1.notify_waiters();
    }
}
