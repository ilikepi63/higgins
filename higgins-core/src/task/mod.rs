//! The primitives for handling asynchronous tasks inside of higgins.

use std::{collections::VecDeque, panic::UnwindSafe};

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
        &mut self,
        task_description: &TaskDescription,
        future: F,
    ) -> Result<(), HigginsTaskError>
    where
        F: Future + Send + 'static + UnwindSafe,
        F::Output: Send + 'static,
    {
        let mut layers = task_description.layers();

        let mut current_task_ptr = &mut self.root;

        loop {
            let next_layer = layers.pop_front();

            match next_layer {
                Some(next_layer) => {
                    let exist = {
                        let vec_exists = current_task_ptr.tasks.is_some();
                        let task_exists = current_task_ptr.tasks.as_ref().is_some_and(|vec| {
                            vec.iter().find(|task| task.name == next_layer).is_some()
                        });

                        (vec_exists, task_exists)
                    };

                    current_task_ptr = match exist {
                        // or If there is alre ady a vec, add a task to the vec
                        (true, false) => {
                            let index = current_task_ptr
                                .tasks
                                .as_ref()
                                .map(|v| v.len())
                                .unwrap_or(0);

                            current_task_ptr.tasks.as_mut().unwrap().push(TaskPtr {
                                name: next_layer,
                                handle: None, //Some(Self::spawn_task(future)),
                                tasks: None,
                            });
                            current_task_ptr
                                .tasks
                                .as_mut()
                                .unwrap()
                                .get_mut(index)
                                .unwrap()
                        }
                        // or if there is no vec, create a vec and add a task to it, making the current pointer point to it.
                        (false, _) => {
                            current_task_ptr.tasks.replace(vec![TaskPtr {
                                name: next_layer,
                                handle: None, //Some(Self::spawn_task(future)),
                                tasks: None,
                            }]);

                            current_task_ptr
                                .tasks
                                .as_mut()
                                .unwrap()
                                .first_mut()
                                .unwrap()
                        }
                        // If there already exists a task, make that task the one we are pointing to.
                        (true, true) => current_task_ptr
                            .tasks
                            .as_mut()
                            .unwrap()
                            .iter_mut()
                            .find(|val| val.name == next_layer)
                            .unwrap(),
                    };
                }
                None => {
                    let task_handle = Self::spawn_task(future);
                    current_task_ptr.handle = Some(task_handle);
                    break;
                }
            }
        }

        Ok(())
    }

    fn spawn_task<F>(fut: F) -> JoinHandle<()>
    where
        F: Future + Send + 'static + UnwindSafe,
        F::Output: Send + 'static,
    {
        tokio::spawn(async move {
            let unwind_result = fut.catch_unwind().await;
        })
    }

    /// Given a task description, retrieves the vector in which this task needs to
    /// be placed.
    pub fn get_task_handle_vec(
        &mut self,
        task_description: &TaskDescription,
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

    pub fn get_container_task(
        &mut self,
        task_description: &TaskDescription,
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
                        .flatten()
                        .is_some();

                    match existing_task {
                        true => {
                            if description_layers.len() < 1 {
                                return Ok(current_task_ptr);
                            } else {
                                current_task_ptr = current_task_ptr
                                    .tasks
                                    .as_mut()
                                    .map(|v| v.iter_mut().find(|task| task.name == next_layer))
                                    .flatten()
                                    .unwrap();
                            }
                        }
                        false => return Err(HigginsTaskError::TaskHierarchyDoesNotExist),
                    }
                }
                None => return Err(HigginsTaskError::TaskHierarchyDoesNotExist),
            }
        }
    }

    /// Aborts the given task identified by the hierarchy,
    /// recursively aborting every task that is it's subordinate.
    pub fn abort(&mut self, task_description: TaskDescription) -> Result<(), HigginsTaskError> {
        let layers = task_description.layers();

        let mut task = self.get_task_handle_vec(&task_description)?;

        Self::abort_recursive(task);

        let container_task = self.get_container_task(&task_description);

        Ok(())
    }

    fn abort_recursive(task: &mut TaskPtr) {
        if let Some(sub_tasks) = task.tasks.as_mut() {
            for sub_task in sub_tasks.iter_mut() {
                Self::abort_recursive(sub_task);
            }

            if let Some(handle) = task.handle.as_mut() {
                handle.abort();
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
        let mut task_handler = TaskHandler::new();

        let result = task_handler.spawn(
            &TaskDescription("some::hierarchy".to_string()),
            async move {},
        );

        assert!(result.is_ok());

        dbg!(&task_handler);

        assert_eq!(
            task_handler
                .get_task_handle_vec(&&TaskDescription("some::hierarchy".to_string()))
                .unwrap(),
            &mut TaskPtr {
                name: "hierarchy".to_string(),
                handle: Some(tokio::spawn(async move {})),
                tasks: None,
            }
        );
    }

    #[tokio::test]
    async fn task_handler_task_hierarchy_spawning() {
        let mut task_handler = TaskHandler::new();

        let result = task_handler.spawn(
            &TaskDescription("some::hierarchy".to_string()),
            async move {},
        );

        assert!(result.is_ok());

        dbg!(&task_handler);

        let task_ptr = task_handler
            .get_task_handle_vec(&TaskDescription("some".to_string()))
            .unwrap();

        assert_eq!(task_ptr.name, "some".to_string());
        assert!(task_ptr.handle.is_none());

        let result = task_handler.spawn(&TaskDescription("some".to_string()), async move {});

        assert!(result.is_ok());

        let some_handler = task_handler
            .get_task_handle_vec(&TaskDescription("some".to_string()))
            .unwrap();

        assert_eq!(some_handler.name, "some".to_string());
        assert!(some_handler.handle.is_some());

        let handler = task_handler
            .get_task_handle_vec(&TaskDescription("some::hierarchy".to_string()))
            .unwrap();

        assert_eq!(handler.name, "hierarchy".to_string());
        assert!(handler.handle.is_some());
    }

    #[tokio::test]
    async fn task_handler_task_hierarchy_side_spawning() {
        let mut task_handler = TaskHandler::new();

        println!("hierarchy");
        let result = task_handler.spawn(
            &TaskDescription("some::hierarchy".to_string()),
            async move {},
        );

        assert!(result.is_ok());

        let task_ptr = task_handler
            .get_task_handle_vec(&TaskDescription("some".to_string()))
            .unwrap();

        assert_eq!(task_ptr.name, "some".to_string());
        assert!(task_ptr.handle.is_none());

        println!("thing");

        let result = task_handler.spawn(&TaskDescription("some::thing".to_string()), async move {});

        assert!(result.is_ok());

        println!("thingelse");

        let result = task_handler.spawn(
            &TaskDescription("some::thingelse".to_string()),
            async move {},
        );

        assert!(result.is_ok());

        dbg!(&task_handler);

        let some_handler = task_handler
            .get_task_handle_vec(&TaskDescription("some".to_string()))
            .unwrap();

        assert_eq!(some_handler.name, "some".to_string());
        assert!(some_handler.handle.is_none());

        let handler = task_handler
            .get_task_handle_vec(&TaskDescription("some::hierarchy".to_string()))
            .unwrap();

        assert_eq!(handler.name, "hierarchy".to_string());
        assert!(handler.handle.is_some());

        let handler = task_handler
            .get_task_handle_vec(&TaskDescription("some::thing".to_string()))
            .unwrap();

        assert_eq!(handler.name, "thing".to_string());
        assert!(handler.handle.is_some());

        let handler = task_handler
            .get_task_handle_vec(&TaskDescription("some::thingelse".to_string()))
            .unwrap();

        assert_eq!(handler.name, "thingelse".to_string());
        assert!(handler.handle.is_some());

        dbg!(&task_handler);
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
                .get_task_handle_vec(&TaskDescription("some::hierarchy".to_string()))
                .unwrap(),
            &mut TaskPtr {
                name: "hierarchy".to_string(),
                handle: Some(tokio::spawn(async move {})),
                tasks: None,
            }
        );
    }

    #[tokio::test]
    async fn get_task_container_works_correctly() {
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
                .get_container_task(&TaskDescription("some::hierarchy".to_string()))
                .unwrap(),
            &mut TaskPtr {
                name: "some".to_string(),
                handle: Some(tokio::spawn(async move {})),
                tasks: None,
            }
        );

        assert_eq!(
            task_handler
                .get_container_task(&TaskDescription("some".to_string()))
                .unwrap(),
            &mut TaskPtr {
                name: "root".to_string(),
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
            task_handler.get_task_handle_vec(&TaskDescription("some::hierarchy".to_string())),
            Err(HigginsTaskError::TaskHierarchyDoesNotExist)
        ));
    }
}
