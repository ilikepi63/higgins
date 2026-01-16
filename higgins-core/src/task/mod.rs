//! The primitives for handling asynchronous tasks inside of higgins.

use std::{collections::VecDeque, convert::Infallible, panic::UnwindSafe};

use futures::FutureExt;
use tokio::task::JoinHandle;

use crate::error::HigginsError;
use error::HigginsTaskError;
pub mod error;

/// Primary structure for handling the creation and deletion of tasks.
pub struct TaskHandler {
    root: TaskPtr,
}

impl TaskHandler {
    pub fn new() -> Self {
        Self {
            root: TaskPtr {
                name: "root".to_string(),
                handle: None,
                tasks: Some(vec![]),
            },
        }
    }

    /// Spawn a future inside of this task handle.
    pub fn spawn<F>(task_description: TaskDescription, future: F) -> Result<(), HigginsTaskError>
    where
        F: Future + Send + 'static + UnwindSafe,
        F::Output: Send + 'static,
    {
        tokio::spawn(async move {
            let unwind_result = future.catch_unwind().await;
        });

        Ok(())
    }

    /// Given a task description, retrieves the vector in which this task needs to
    /// be placed.
    pub fn get_task_handle_vec(
        &mut self,
        task_description: TaskDescription,
    ) -> Result<&mut TaskPtr, HigginsTaskError> {
        let description_layers = task_description.layers();

        loop {
            if description_layers.len() == 1 {
                // If the layer is one length, we basically just create a taskhandle on the root layer.
                return Ok(&mut self.root);
            }
        }
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

    pub fn layers(&self) -> VecDeque<String> {
        // TODO: should like fix this to not make so many allocations.
        self.0.split("::").map(|s| s.to_owned()).collect()
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_task_handler_happy_path() {
        let task_handler = TaskHandler::new();
    }
}
