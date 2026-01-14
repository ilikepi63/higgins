# Higgins Task Handling

## Introduction

Higgins requires a robust task handling runtime for arbitrary tasks that are spawned and run asyncrhonously within the system. These tasks need to:

- Gracefully handle panics or shutdowns. 
- Be able to be inspected for failure scenarios. 
- Allow for linking of different tasks together into a hierarchical structure. This doesn't mean that tasks spawn other tasks, but more likely a set of tasks can be pinned to a long running piece of data and freed when necessary. 
- Assert that certain classes of tasks do not exist concurrently.
