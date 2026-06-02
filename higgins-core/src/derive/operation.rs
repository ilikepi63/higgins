//! Operations are abstractions over each action of a derived stream.
//!
//! If we consider the streams as vertices in a graph, operations would be the edges between those vertices. It is necessary to have an
//! abstraction over these edges as it is necessary to execute these independently of one another.
use arrow::array::RecordBatch;
use higgins_shared::{PartitionName, read_arrow};
use std::sync::Arc;
use std::{ops::Range, sync::atomic::AtomicU8};
use tokio::sync::{Notify, RwLock};

use super::{
    joining::JoinOperation, map::MapOperation, reduce::ReduceOperation, windowed::WindowOperation,
};
use crate::{
    broker::{Broker, ProduceOperation},
    derive::{joining::join::JoinDefinition, windowed::definition::WindowedStreamDefinition},
    error::HigginsError,
    storage::dereference::Reference,
    subscription::Subscription,
    task::SpawnTaskConfig,
    topography::{FunctionType, StreamDefinition, StreamName},
};

#[repr(u8)]
pub enum Step {
    Pre,
    Init,
    Prepare,
    Commit,
}

impl Into<u8> for Step {
    fn into(self) -> u8 {
        self as u8
    }
}

/// A represntation of a
pub struct NodeOperation {
    operation: Operation,
    condvar: Notify,
    step: AtomicU8,
}

impl NodeOperation {
    pub fn of(operation: Operation) -> Self {
        Self {
            operation,
            condvar: Notify::new(),
            step: AtomicU8::new(Step::Pre.into()),
        }
    }

    pub async fn init(&mut self) -> Result<(), HigginsError> {
        self.operation.init().await?;
        self.step
            .store(Step::Init.into(), std::sync::atomic::Ordering::SeqCst);

        // Retrieve the relationship between this operation and other operations.

        let relations = {
            let broker_lock = self.operation.broker();
            let broker_guard = broker_lock.write().await;
            broker_guard
                .get_relation_for_stream(&self.operation.stream())
                .clone()
        };

        for relation in relations {
            let moved_values = {
                (
                    self.operation.broker().clone(),
                    relation.definition.clone(),
                    relation.join_index.clone(),
                    relation.stream_name.clone(),
                    relation.subscription.clone(),
                    self.operation.offsets().clone(),
                    self.operation.records().clone(),
                    self.operation.partition().clone(),
                )
            };

            let broker_lock = self.operation.broker();
            let mut broker_guard = broker_lock.write().await;
            drop(broker_guard);

            tokio::spawn(async move {
                let (
                    broker,
                    definition,
                    join_index,
                    stream_name,
                    subscription,
                    offsets,
                    records,
                    partition,
                ) = moved_values;

                let mut operation = Operation::try_new(
                    broker,
                    offsets,
                    None,
                    records.unwrap_or(vec![]), // TODO: Join and Window need to create their records in init.
                    stream_name,
                    definition,
                    partition,
                    Some(subscription), // Always Some as this would always be a relation.
                    join_index,
                )
                .await
                .inspect_err(|err| tracing::error!("{:#?}", err))
                .unwrap();

                let operation = NodeOperation::of(operation);

                operation.init().await.unwrap();
            });
        }

        Ok(())
    }
}

pub enum Operation {
    Map(MapOperation),
    Reduce(ReduceOperation),
    Window(WindowOperation),
    Join(JoinOperation),
    Produce(ProduceOperation),
}

// unsafe impl Send for Operation {}

#[allow(unused)]
impl Operation {
    pub async fn try_new(
        // passed in dynamically.
        broker: Arc<RwLock<Broker>>,
        offsets: Range<u64>,
        references: Option<Vec<Reference>>,
        records: Vec<RecordBatch>,

        // Can be kept in relation.
        stream_name: StreamName,
        definition: StreamDefinition,
        partition: PartitionName,
        subscription: Option<Arc<RwLock<Subscription>>>,
        join_index: Option<u64>,
    ) -> Result<Self, HigginsError> {
        Ok(match definition.stream_type {
            Some(FunctionType::Window) => Operation::Window(WindowOperation {
                broker,
                stream: stream_name.clone().into(),
                definition: WindowedStreamDefinition::try_from((stream_name, definition))?,
                partition,
                offsets,
                subscription: subscription.ok_or(HigginsError::Unknown)?,
            }),
            Some(FunctionType::Map) => Operation::Map(MapOperation {
                broker,
                stream_name: stream_name.into(),
                stream_def: definition,
                partition,
                offset: offsets,
                references,
                subscription: subscription.ok_or(HigginsError::Unknown)?,
                records,
            }),
            Some(FunctionType::Reduce) => Operation::Reduce(ReduceOperation {
                broker,
                stream_name: stream_name.into(),
                stream_def: definition,
                partition,
                offsets,
                references,
                subscription: subscription.ok_or(HigginsError::Unknown)?,
                records,
            }),
            Some(FunctionType::Join) => {
                let definition = {
                    let broker_guard = broker.write().await;

                    JoinDefinition::try_from((
                        stream_name.clone().into(),
                        definition,
                        &*broker_guard,
                    ))?
                };

                Operation::Join(JoinOperation {
                    stream: stream_name.clone(),
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
            None => Operation::Produce(ProduceOperation {
                broker,
                stream: stream_name.into(),
                partition,
                references,
                records,
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

    pub fn stream(&self) -> StreamName {
        match self {
            Self::Map(o) => StreamName::from(o.stream_name.clone()),
            Self::Join(o) => o.stream.clone(),
            Self::Window(o) => StreamName::from(o.stream.as_str()),
            Self::Reduce(o) => StreamName::from(o.stream_name.clone()),
            Self::Produce(o) => StreamName::from(o.stream.as_str()),
        }
    }

    // records,
    // partition,
    pub fn offsets(&self) -> Range<u64> {
        match self {
            Self::Map(o) => o.offset.clone(),
            Self::Join(o) => o.offsets.clone(),
            Self::Window(o) => o.offsets.clone(),
            Self::Reduce(o) => o.offsets.clone(),
            Self::Produce(o) => 0..0, // Not required.
        }
    }

    pub fn references(&self) -> Option<Vec<Reference>> {
        match self {
            Self::Map(o) => o.references.clone(),
            Self::Join(o) => None,
            Self::Window(o) => None,
            Self::Reduce(o) => o.references.clone(),
            Self::Produce(o) => o.references.clone(),
        }
    }

    pub fn records(&self) -> Option<Vec<RecordBatch>> {
        match self {
            Self::Map(o) => Some(o.records.clone()),
            Self::Join(o) => None,
            Self::Window(o) => None,
            Self::Reduce(o) => Some(o.records.clone()),
            Self::Produce(o) => Some(o.records.clone()),
        }
    }

    pub fn partition(&self) -> PartitionName {
        match self {
            Self::Map(o) => o.partition.clone(),
            Self::Join(o) => o.partition.clone(),
            Self::Window(o) => o.partition.clone(),
            Self::Reduce(o) => o.partition.clone(),
            Self::Produce(o) => o.partition.clone(),
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

fn _assert_send<T: Send>() {}
fn _assert_operation_send() {
    _assert_send::<Operation>();
    _assert_send::<NodeOperation>();
}
