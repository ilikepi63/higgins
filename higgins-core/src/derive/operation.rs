//! Operations are abstractions over each action of a derived stream.
//!
//! If we consider the streams as vertices in a graph, operations would be the edges between those vertices. It is necessary to have an
//! abstraction over these edges as it is necessary to execute these independently of one another.
use arrow::array::RecordBatch;
use higgins_shared::{PartitionName, read_arrow};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{
    joining::JoinOperation, map::MapOperation, reduce::ReduceOperation, windowed::WindowOperation,
};
use crate::{
    broker::{Broker, ProduceOperation},
    derive::{joining::join::JoinDefinition, windowed::definition::WindowedStreamDefinition},
    error::HigginsError,
    storage::dereference::Reference,
    subscription::Subscription,
    topography::{FunctionType, StreamDefinition, StreamName},
};

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
    Produce(ProduceOperation),
}

#[allow(unused)]
impl Operation {
    pub async fn try_new(
        // passed in dynamically.
        broker: Arc<RwLock<Broker>>,
        offsets: Range<u64>,
        references: Option<Vec<Reference>>,
        records: Vec<(Vec<u8>, u64)>,

        // Can be kept in relation.
        stream_name: StreamName,
        definition: StreamDefinition,
        partition: PartitionName,
        subscription: Arc<RwLock<Subscription>>,
        join_index: Option<u64>,
    ) -> Result<Self, HigginsError> {
        Ok(match definition.stream_type {
            Some(FunctionType::Window) => Operation::Window(WindowOperation {
                broker,
                stream: stream_name.clone().into(),
                definition: WindowedStreamDefinition::try_from((stream_name, definition))?,
                partition,
                offsets,
                subscription,
            }),
            Some(FunctionType::Map) => Operation::Map(MapOperation {
                broker,
                stream_name: stream_name.into(),
                stream_def: definition,
                partition,
                offset: offsets,
                references,
                subscription,
                records,
            }),
            Some(FunctionType::Reduce) => Operation::Reduce(ReduceOperation {
                broker,
                stream_name: stream_name.into(),
                stream_def: definition,
                partition,
                offsets,
                references,
                subscription,
                records,
            }),
            Some(FunctionType::Join) => {
                let definition = {
                    let broker_guard = broker.write().await;

                    JoinDefinition::try_from((stream_name.into(), definition, &*broker_guard))?
                };

                Operation::Join(JoinOperation {
                    broker,
                    index: join_index.ok_or(HigginsError::Unknown)?,
                    definition,
                    partition,
                    offsets,
                    optimistic_index: None,
                    optimistic_offset: None,
                })
            }
            Some(FunctionType::Aggregate) => todo!(),
            None => Operation::Produce({
                let records = records
                    .iter()
                    .map(|(record, index)| record)
                    .map(|record| read_arrow(record).next())
                    .map(|record_batch| {
                        record_batch
                            .map(|rb| rb.ok())
                            .flatten()
                            .ok_or(HigginsError::Unknown)
                    })
                    .collect::<Result<Vec<RecordBatch>, HigginsError>>()?;

                ProduceOperation {
                    broker,
                    stream: stream_name.into(),
                    partition,
                    references,
                    records,
                }
            }),
        })
    }

    pub fn broker(&self) -> Arc<RwLock<Broker>> {
        match self {
            Self::Map(o) => o.broker.clone(),
            Self::Join(o) => o.broker.clone(),
            Self::Window(o) => o.broker.clone(),
            Self::Reduce(o) => o.broker.clone(),
            Self::Produce(o) => o.broker.clone(),
        }
    }

    pub async fn init(&mut self) -> Result<(), HigginsError> {
        match self {
            Self::Map(o) => o.init().await,
            Self::Join(o) => o.init().await,
            Self::Window(o) => o.init().await,
            Self::Reduce(o) => o.init().await,
            Self::Produce(o) => o.init().await,
        }?;

        // Retrieve the relationship between this operation and other operations.
        let broker_lock = self.broker();
        let broker_guard = broker_lock.write().await;

        // broker_guard.topo

        // broker

        // start an operation and run init.

        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        match self {
            Self::Map(o) => o.prepare().await,
            Self::Join(o) => o.prepare().await,
            Self::Window(o) => o.prepare().await,
            Self::Reduce(o) => o.prepare().await,
            Self::Produce(o) => o.prepare().await,
        }
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        match self {
            Self::Map(o) => o.commit().await,
            Self::Join(o) => o.commit().await,
            Self::Window(o) => o.commit().await,
            Self::Reduce(o) => o.commit().await,
            Self::Produce(o) => o.commit().await,
        }
    }
}
