//! Operations are abstractions over each action of a derived stream.
//!
//! If we consider the streams as vertices in a graph, operations would be the edges between those vertices. It is necessary to have an
//! abstraction over these edges as it is necessary to execute these independently of one another.
use super::{
    joining::JoinOperation, map::MapOperation, reduce::ReduceOperation, windowed::WindowOperation,
};
use crate::error::HigginsError;

#[allow(unused)]
pub enum Step {
    Init,
    Prepare,
    Commit,
}

#[allow(unused)]
enum Operation {
    Map(MapOperation),
    Reduce(ReduceOperation),
    Window(WindowOperation),
    Join(JoinOperation),
}

#[allow(unused)]
impl Operation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        match self {
            Self::Map(o) => o.init().await,
            Self::Join(o) => o.init().await,
            Self::Window(o) => o.init().await,
            Self::Reduce(o) => o.init().await,
        }
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        match self {
            Self::Map(o) => o.prepare().await,
            Self::Join(o) => o.prepare().await,
            Self::Window(o) => o.prepare().await,
            Self::Reduce(o) => o.prepare().await,
        }
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        match self {
            Self::Map(o) => o.commit().await,
            Self::Join(o) => o.commit().await,
            Self::Window(o) => o.commit().await,
            Self::Reduce(o) => o.commit().await,
        }
    }
}
