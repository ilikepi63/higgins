//! Operations are abstractions over each action of a derived stream.
//!
//! If we consider the streams as vertices in a graph, operations would be the edges between those vertices. It is necessary to have an
//! abstraction over these edges as it is necessary to execute these independently of one another.

// pub enum Step {
//     Init,
//     Prepare,
//     Commit,
// }

// enum Operation {
//     Map(MapOperation),
//     Reduce(ReduceOperation),
//     Window(WindowOperation),
//     Join(JoinOperation),
// }
