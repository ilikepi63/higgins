//! The primitives for handling asynchronous tasks inside of higgins.

use std::{collections::VecDeque, convert::Infallible, panic::UnwindSafe};

use futures::FutureExt;
use tokio::task::JoinHandle;

use crate::error::HigginsError;
use error::HigginsTaskError;
pub mod error;

/// Primary structure for handling the creation and deletion of tasks.
#[derive(Debug)]
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
    pub fn spawn<F>(
        &self,
        task_description: TaskDescription,
        future: F,
    ) -> Result<(), HigginsTaskError>
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
        let mut description_layers = task_description.layers();

        let mut current_task_ptr = &mut self.root;

        loop {
            let next_layer = description_layers.pop_front();

            match next_layer {
                Some(next_layer) => {
                    let existing_task = current_task_ptr
                        .tasks
                        .as_mut()
                        .map(|v| v.iter_mut().find(|task| task.name == next_layer))
                        .flatten();

                    match existing_task {
                        Some(task) => {
                            current_task_ptr = task;
                            continue;
                        }
                        None => return Err(HigginsTaskError::TaskHierarchyDoesNotExist),
                    }
                }
                None => return Ok(current_task_ptr),
            }
        }
    }
}

/// The description of a given task.
///
/// This includes the logic that surrounds how layering of tasks are sorted out.
#[derive(Debug)]
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
#[derive(Debug)]
pub struct TaskPtr {
    name: String,
    handle: Option<TaskHandle>,
    tasks: Option<Vec<TaskPtr>>,
}

impl PartialEq for TaskPtr {
    fn eq(&self, other: &Self) -> bool {
        other.name == self.name
    }
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

    #[tokio::test]
    async fn basic_task_handler_happy_path() {
        let task_handler = TaskHandler::new();

        let result = task_handler.spawn(
            TaskDescription("some::hierarchy".to_string()),
            async move {},
        );

        assert!(result.is_ok());

        // Now we assert the hierarchy.

        let task_handler_tasks = task_handler.root.tasks.unwrap();

        let some_level = task_handler_tasks
            .iter()
            .find(|task| task.name == "some")
            .unwrap();

        assert!(some_level.handle.is_some());
    }

    #[tokio::test]
    async fn get_task_works_correctly() {
        tracing_subscriber::fmt::init();

        let mut task_handler = TaskHandler {
            root: TaskPtr {
                name: "root".to_string(),
                handle: None,
                tasks: Some(vec![TaskPtr {
                    name: "some".to_string(),
                    handle: Some(tokio::spawn(async move {})),
                    tasks: Some(vec![TaskPtr {
                        name: "hierarchy".to_string(),
                        handle: Some(tokio::spawn(async move {})),
                        tasks: None,
                    }]),
                }]),
            },
        };

        assert_eq!(
            task_handler
                .get_task_handle_vec(TaskDescription("some::hierarchy".to_string()))
                .unwrap(),
            &mut TaskPtr {
                name: "hierarchy".to_string(),
                handle: Some(tokio::spawn(async move {})),
                tasks: None,
            }
        );
    }

    #[tokio::test]
    async fn get_task_fails_correctly() {
        tracing_subscriber::fmt::init();

        let mut task_handler = TaskHandler {
            root: TaskPtr {
                name: "root".to_string(),
                handle: None,
                tasks: None,
            },
        };

        assert!(matches!(
            task_handler.get_task_handle_vec(TaskDescription("some::hierarchy".to_string())),
            Err(HigginsTaskError::TaskHierarchyDoesNotExist)
        ));
    }
}
