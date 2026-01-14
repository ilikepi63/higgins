//! The primitives for handling asynchronous tasks inside of higgins.

use std::panic::UnwindSafe;

use futures::FutureExt;
use tokio::task::JoinHandle;

use crate::error::HigginsError;

/// Primary structure for handling the creation and deletion of tasks.
pub struct TaskHandler {
    handle: TaskHandle,
}

impl TaskHandler {
    /// Spawn a future inside of this task handle.
    pub fn spawn<F>(task_description: TaskDescription, future: F)
    where
        F: Future + Send + 'static + UnwindSafe,
        F::Output: Send + 'static,
    {
        tokio::spawn(async move {
            let unwind_result = future.catch_unwind().await;
        });
    }
}

/// The description of a given task.
///
/// This includes the logic that surrounds how layering of tasks are sorted out.
pub struct TaskDescription(String);

impl TaskDescription {
    pub fn push(&mut self, layer: &str) -> Result<(), HigginsError> {
        if layer.contains("::") {
            panic!(); // TODO: A real error here
        }

        self.0.push_str(layer);

        Ok(())
    }
}

/// Renaming struct for easier reference.
///
/// Generally, these tasks are considering long running tasks
/// and should only be returning unit or !.
type TaskHandle = tokio::task::JoinHandle<()>;

/// A pointer to either a task handle
/// or another set of task handles.
///
/// TODO: This might be better handled as a union type?
pub struct TaskPtr {
    name: String,
    handle: Option<TaskHandle>,
    tasks: Option<Vec<TaskPtr>>,
}

impl TaskPtr {
    pub fn of_vec(name: String, v: Vec<TaskPtr>) -> Self {
        Self {
            name,
            handle: None,
            tasks: Some(v),
        }
    }

    pub fn of_task(name: String, t: JoinHandle<()>) -> Self {
        Self {
            name,
            handle: Some(t),
            tasks: None,
        }
    }
}
