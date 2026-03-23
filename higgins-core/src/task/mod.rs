//! The primitives for handling asynchronous tasks inside of higgins.

use std::collections::VecDeque;

use tokio::task::JoinHandle;

use crate::error::HigginsError;
use error::HigginsTaskError;
pub mod error;

/// Primary structure for handling the creation and deletion of tasks.
#[derive(Debug)]
pub struct TaskHandler {
    root: TaskPtr,
}

impl Default for TaskHandler {
    fn default() -> Self {
        Self::new()
    }
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
    pub fn spawn<F>(&mut self, config: &SpawnTaskConfig, future: F) -> Result<(), HigginsTaskError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tracing::trace!("Starting the task..");

        let mut layers = config.description.layers();

        let mut current_task_ptr = &mut self.root;

        loop {
            let next_layer = layers.pop_front();

            match next_layer {
                Some(next_layer) => {
                    let exist = {
                        let vec_exists = current_task_ptr.tasks.is_some();
                        let task_exists = current_task_ptr
                            .tasks
                            .as_ref()
                            .is_some_and(|vec| vec.iter().any(|task| task.name == next_layer));

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

                            current_task_ptr.add_sub_task(TaskPtr {
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
                            current_task_ptr.add_sub_task(TaskPtr {
                                name: next_layer,
                                handle: None, //Some(Self::spawn_task(future)),
                                tasks: None,
                            });

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
                    self.spawn_task(config, future);
                    break;
                }
            }
        }

        Ok(())
    }

    // fn update_task(&mut self, task_description: &TaskDescription, task_handle: JoinHandle<()>) {}

    fn spawn_task<F>(&mut self, config: &SpawnTaskConfig, fut: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tracing::trace!("Calling spawn_task..");

        let task_description_for_task = config.description.clone();

        let mut handler_ptr = TaskHandlerReference::from(self);

        let handle = tokio::spawn(async move {
            tracing::trace!("{:#?} spawning..", task_description_for_task);

            let _result = fut.await;

            tracing::trace!(
                "{:#?} completed, Removing from tree..",
                task_description_for_task
            );

            unsafe { handler_ptr.abort(&task_description_for_task) };
        });

        tracing::trace!("Updating the handle in the task_description..");

        let task_description = config.description.clone();

        match config.unique {
            true => {
                if let Err(e) = self.get_task_handle_vec(&task_description).map(|task| {
                    let name = task.get_unique_sub_task_name();

                    task.add_sub_task(TaskPtr {
                        name,
                        handle: Some(handle),
                        tasks: None,
                    });
                }) {
                    tracing::error!("Task creation failed: {:#?}", e);
                };
            }
            false => {
                if let Err(e) = self.get_task_handle_vec(&task_description).map(|task| {
                    task.handle = Some(handle);
                }) {
                    tracing::error!("Task creation failed: {:#?}", e);
                };
            }
        }

        // handle
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
                        .and_then(|v| v.iter_mut().find(|task| task.name == next_layer));

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
                        .and_then(|v| v.iter_mut().find(|task| task.name == next_layer))
                        .is_some();

                    match existing_task {
                        true => {
                            if description_layers.is_empty() {
                                return Ok(current_task_ptr);
                            } else {
                                current_task_ptr = current_task_ptr
                                    .tasks
                                    .as_mut()
                                    .and_then(|v| v.iter_mut().find(|task| task.name == next_layer))
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
    pub fn abort(&mut self, task_description: &TaskDescription) -> Result<(), HigginsTaskError> {
        let aborted_task_name = {
            let task = self.get_task_handle_vec(task_description)?;

            Self::abort_recursive(task);

            task.name.clone()
        };

        let container_task = self.get_container_task(task_description).unwrap();

        if let Some(sub_tasks) = container_task.tasks.as_mut() {
            let index = sub_tasks
                .iter()
                .enumerate()
                .find(|(_, t)| t.name == aborted_task_name)
                .map(|(i, _)| i);

            if let Some(i) = index {
                sub_tasks.swap_remove(i);
            }
        }

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

/// A TaskHandlerReference that allows a task to keep a reference to the handle.
///
/// A task will never outlive the TaskHandler it belongs to, and therefore dereferencing this ptr
/// should never be UB.
struct TaskHandlerReference(*mut TaskHandler);

impl TaskHandlerReference {
    pub fn from(handler: &mut TaskHandler) -> Self {
        Self(std::ptr::from_mut(handler))
    }

    pub unsafe fn abort(&mut self, task_description: &TaskDescription) {
        if let Err(err) = unsafe { (*(self.0)).abort(task_description) } {
            tracing::error!("Error attempting to abort task: {:#?}", err);
        }
    }
}

/// SAFETY: Tasks that reference this handle will never outlive the struct it points to.
unsafe impl Send for TaskHandlerReference {}

/// The description of a given task.
///
/// This includes the logic that surrounds how layering of tasks are sorted out.
#[derive(Debug, Clone)]
pub struct TaskDescription(String);

impl std::fmt::Display for TaskDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)?;
        Ok(())
    }
}

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

    /// Add a subtask to this TaskPtr.
    ///
    /// If the Task doesn't have a vec assigned to it, this creates one.
    pub fn add_sub_task(&mut self, ptr: TaskPtr) {
        match self.tasks.as_mut() {
            Some(tasks) => {
                tasks.push(ptr);
            }
            None => {
                let _ = self.tasks.insert(vec![ptr]);
            }
        }
    }

    /// Retrieve a unique name for a task ptr before adding it into
    /// this TaskPtr.
    ///
    /// NOTE: This is not a great method, as we could be doing a lot of
    /// lookups depending on how often tasks get spawned/deallocated.
    pub fn get_unique_sub_task_name(&self) -> String {
        let mut length = self.tasks.as_ref().map(|t| t.len()).unwrap_or(0);

        loop {
            if self
                .tasks
                .as_ref()
                .map(|tasks| {
                    tasks
                        .iter()
                        .all(|sub_task| sub_task.name != length.to_string())
                })
                .unwrap_or(true)
            {
                return length.to_string();
            }

            length += 1;
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpawnTaskConfig {
    description: TaskDescription,
    /// Whether or not this task should be spawned uniquely.
    ///
    /// This will either create the task inside of the current hierarchy, or add it to the hierarchy's vec
    /// with a generated id. This is toggled usually if you are spawning many tasks that sit side-by-side,
    /// but the specific ID's of those tasks are not important for the hierarchical nature of the TaskHandler.
    unique: bool,
}

impl SpawnTaskConfig {
    pub fn new(description: &str, unique: bool) -> Self {
        Self {
            description: TaskDescription(description.to_string()),
            unique,
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
            &SpawnTaskConfig {
                description: TaskDescription("some::hierarchy".to_string()),
                unique: false,
            },
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
            &SpawnTaskConfig {
                description: TaskDescription("some::hierarchy".to_string()),
                unique: false,
            },
            async move {},
        );

        assert!(result.is_ok());

        dbg!(&task_handler);

        let task_ptr = task_handler
            .get_task_handle_vec(&TaskDescription("some".to_string()))
            .unwrap();

        assert_eq!(task_ptr.name, "some".to_string());
        assert!(task_ptr.handle.is_none());

        let result = task_handler.spawn(
            &SpawnTaskConfig {
                description: TaskDescription("some".to_string()),
                unique: false,
            },
            async move {},
        );

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

        let result = task_handler.spawn(
            &SpawnTaskConfig {
                description: TaskDescription("some::hierarchy".to_string()),
                unique: false,
            },
            async move {},
        );

        assert!(result.is_ok());

        let task_ptr = task_handler
            .get_task_handle_vec(&TaskDescription("some".to_string()))
            .unwrap();

        assert_eq!(task_ptr.name, "some".to_string());
        assert!(task_ptr.handle.is_none());

        let result = task_handler.spawn(
            &SpawnTaskConfig {
                description: TaskDescription("some::thing".to_string()),
                unique: false,
            },
            async move {},
        );

        assert!(result.is_ok());

        let result = task_handler.spawn(
            &SpawnTaskConfig {
                description: TaskDescription("some::thingelse".to_string()),
                unique: false,
            },
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
    async fn task_handler_task_hierarchy_side_spawning_abort() {
        let mut task_handler = TaskHandler::new();

        let result = task_handler.spawn(
            &SpawnTaskConfig {
                description: TaskDescription("some::hierarchy".to_string()),
                unique: false,
            },
            async move {},
        );

        assert!(result.is_ok());

        let task_ptr = task_handler
            .get_task_handle_vec(&TaskDescription("some".to_string()))
            .unwrap();

        assert_eq!(task_ptr.name, "some".to_string());
        assert!(task_ptr.handle.is_none());

        let result = task_handler.spawn(
            &SpawnTaskConfig {
                description: TaskDescription("some::hierarchy".to_string()),
                unique: false,
            },
            async move {},
        );

        assert!(result.is_ok());

        let result = task_handler.spawn(
            &SpawnTaskConfig {
                description: TaskDescription("some::thingelse".to_string()),
                unique: false,
            },
            async move {},
        );

        assert!(result.is_ok());

        let result = task_handler.spawn(
            &SpawnTaskConfig {
                description: TaskDescription("other::thing".to_string()),
                unique: false,
            },
            async move {},
        );

        assert!(result.is_ok());

        dbg!(&task_handler);

        task_handler
            .abort(&TaskDescription("some".to_string()))
            .unwrap();

        dbg!(&task_handler);

        assert!(matches!(
            task_handler.get_task_handle_vec(&TaskDescription("some::hierarchy".to_string())),
            Err(HigginsTaskError::TaskHierarchyDoesNotExist)
        ));

        let other_thing_task = task_handler
            .get_task_handle_vec(&TaskDescription("other::thing".to_string()))
            .unwrap();

        assert!(other_thing_task.handle.is_some());
        assert_eq!(other_thing_task.name, "thing".to_string());
    }

    #[tokio::test]
    async fn get_task_works_correctly() {
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

    #[tokio::test]
    async fn task_aborts_after_some_time() {
        let mut task_handler = TaskHandler::new();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        task_handler
            .spawn(
                &SpawnTaskConfig {
                    description: TaskDescription("some::".to_string()),
                    unique: false,
                },
                async move {
                    tx.send(()).unwrap(); // send to this channel so we know that this was executed.
                },
            )
            .unwrap();

        // Pause this task so that the spawned one can operate.
        rx.await.unwrap(); // Await the spawn.

        // Assert that the task handle vec for this hierarchy has
        // an empty task (the "what" task ptr has removed itself.)
        assert_eq!(
            task_handler
                .get_task_handle_vec(&TaskDescription("some".to_string()))
                .unwrap()
                .tasks,
            Some(vec![])
        );
    }

    #[tokio::test]
    async fn task_unique_name_retrievable() {
        let mut task_ptr = TaskPtr {
            name: "root".to_string(),
            handle: None,
            tasks: None,
        };

        let unique_name = task_ptr.get_unique_sub_task_name();

        task_ptr.add_sub_task(TaskPtr {
            name: unique_name,
            handle: None,
            tasks: None,
        });

        assert_eq!(
            task_ptr.tasks,
            Some(vec![TaskPtr {
                name: "0".to_string(),
                handle: None,
                tasks: None,
            },],),
        );

        let unique_name = task_ptr.get_unique_sub_task_name();

        task_ptr.add_sub_task(TaskPtr {
            name: unique_name,
            handle: None,
            tasks: None,
        });

        assert_eq!(
            task_ptr.tasks,
            Some(vec![
                TaskPtr {
                    name: "0".to_string(),
                    handle: None,
                    tasks: None,
                },
                TaskPtr {
                    name: "1".to_string(),
                    handle: None,
                    tasks: None,
                },
            ],),
        );

        let unique_name = task_ptr.get_unique_sub_task_name();

        task_ptr.add_sub_task(TaskPtr {
            name: unique_name,
            handle: None,
            tasks: None,
        });

        assert_eq!(
            task_ptr.tasks,
            Some(vec![
                TaskPtr {
                    name: "0".to_string(),
                    handle: None,
                    tasks: None,
                },
                TaskPtr {
                    name: "1".to_string(),
                    handle: None,
                    tasks: None,
                },
                TaskPtr {
                    name: "2".to_string(),
                    handle: None,
                    tasks: None,
                },
            ],),
        );

        // Remove a task.
        task_ptr.tasks.as_mut().unwrap().remove(1);

        assert_eq!(
            task_ptr.tasks,
            Some(vec![
                TaskPtr {
                    name: "0".to_string(),
                    handle: None,
                    tasks: None,
                },
                TaskPtr {
                    name: "2".to_string(),
                    handle: None,
                    tasks: None,
                },
            ],),
        );
        let unique_name = task_ptr.get_unique_sub_task_name();

        task_ptr.add_sub_task(TaskPtr {
            name: unique_name,
            handle: None,
            tasks: None,
        });
        assert_eq!(
            task_ptr.tasks,
            Some(vec![
                TaskPtr {
                    name: "0".to_string(),
                    handle: None,
                    tasks: None,
                },
                TaskPtr {
                    name: "2".to_string(),
                    handle: None,
                    tasks: None,
                },
                TaskPtr {
                    name: "3".to_string(),
                    handle: None,
                    tasks: None,
                },
            ],),
        );
    }
}
