//! Operations are abstractions over each action of a derived stream.
//!
//! If we consider the streams as vertices in a graph, operations would be the edges between those vertices. It is necessary to have an
//! abstraction over these edges as it is necessary to execute these independently of one another.
use crate::derive::eventual;
use arrow::array::RecordBatch;
use higgins_shared::{HigginsError, PartitionName, StreamName};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::{ops::Range, sync::atomic::AtomicU8};
use tokio::sync::{Notify, RwLock};

use super::{
    joining::JoinOperation, map::MapOperation, reduce::ReduceOperation, windowed::WindowOperation,
};
use crate::{
    broker::{Broker, ProduceOperation},
    derive::joining::join::JoinDefinition,
    storage::dereference::Reference,
    subscription::Subscription,
    topography::{FunctionType, StreamDefinition},
};

/// Data that every operation will need to complete.
#[derive(Debug)]
pub struct OperationData {
    // passed in dynamically
    pub broker: Arc<RwLock<Broker>>,
    pub offsets: eventual::Eventual<Range<u64>>,
    pub records: eventual::Eventual<Vec<RecordBatch>>,
    pub offsets_setter: eventual::Setter<Range<u64>>,
    pub records_setter: eventual::Setter<Vec<RecordBatch>>,
    pub references: Option<Vec<Reference>>,

    // Can be kept in relation.
    pub stream: StreamName,
    pub definition: StreamDefinition,
    pub partition: PartitionName,
    pub subscription: Option<Arc<RwLock<Subscription>>>,
    pub join_index: Option<u64>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub enum Step {
    Pre = 0,
    Init = 1,
    Prepare = 2,
    Commit = 3,
}

impl From<&AtomicU8> for Step {
    fn from(value: &AtomicU8) -> Self {
        match value.load(std::sync::atomic::Ordering::SeqCst) {
            1 => Self::Init,
            2 => Self::Prepare,
            3 => Self::Commit,
            _ => Self::Pre,
        }
    }
}

impl From<Step> for u8 {
    fn from(val: Step) -> Self {
        val as u8
    }
}

#[derive(Clone, Debug)]
pub struct StepSync {
    pub inner: Arc<(AtomicU8, Notify)>,
}

impl StepSync {
    pub fn new() -> Self {
        Self {
            inner: Arc::new((AtomicU8::new(Step::Pre.into()), Notify::new())),
        }
    }

    pub fn read_step(&self) -> Step {
        Step::from(&self.inner.0)
    }

    pub fn write_step(&self, step: Step) {
        self.inner.0.store(step as u8, Ordering::SeqCst);
    }
}

#[derive(Clone, Debug)]
pub struct ProducerStepSync(StepSync);

impl ProducerStepSync {
    pub fn new() -> Self {
        Self(StepSync::new())
    }
    pub async fn await_step(&self, step: Step) {
        loop {
            if self.0.read_step() >= step {
                break;
            }
            self.0.inner.1.notified().await;
        }
    }
    pub fn consumer(&self) -> ConsumerStepSync {
        ConsumerStepSync(self.0.clone())
    }
}

#[derive(Debug)]
pub struct ConsumerStepSync(StepSync);

impl ConsumerStepSync {
    pub fn new() -> Self {
        Self(StepSync::new())
    }
    pub fn set_step(&mut self, step: Step) {
        self.0.write_step(step);
        self.0.inner.1.notify_waiters();
    }
    pub fn producer(&self) -> ProducerStepSync {
        ProducerStepSync(self.0.clone())
    }
}

pub async fn produce_operation(
    stream: StreamName,
    partition: PartitionName,
    definition: StreamDefinition,
    records: &[RecordBatch],
    broker: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    let (records_eventual, record_setter) = eventual::eventual();
    let (offsets_eventual, offsets_setter) = eventual::eventual();

    record_setter.set(records.to_vec()).await;

    tracing::trace!("Initializing the Operation.");
    let mut operation = Operation::try_new(
        broker.clone(),
        offsets_eventual.clone(),
        None,
        records_eventual.clone(),
        offsets_setter,
        eventual::eventual().1, // This shouldn't matter. It is not writing to this value.
        stream.clone(),
        definition.clone(),
        partition.clone(),
        None,
        None,
    )
    .await?;

    let producer_step_sync = ProducerStepSync::new();

    let mut consumer_step_sync = producer_step_sync.consumer();

    tracing::trace!("Generating Relation graph.");

    generate_relation_tasks_from_stream(
        stream,
        partition,
        offsets_eventual,
        records_eventual,
        broker,
        producer_step_sync,
    )
    .await?;

    operation.init().await?;
    consumer_step_sync.set_step(Step::Init);

    operation.prepare().await?;
    consumer_step_sync.set_step(Step::Prepare);

    operation.commit().await?;
    consumer_step_sync.set_step(Step::Commit);

    Ok(())
}

pub async fn generate_relation_tasks_from_stream(
    stream: StreamName,
    partition: PartitionName,
    offsets_eventual: eventual::Eventual<Range<u64>>,
    records_eventual: eventual::Eventual<Vec<RecordBatch>>,
    broker: Arc<RwLock<Broker>>,
    producer_step_sync: ProducerStepSync,
) -> Result<(), HigginsError> {
    let mut relations = VecDeque::new();

    tracing::trace!("Querying relations with key {}", stream);

    let current_relations = {
        let broker_guard = broker.write().await;
        broker_guard.get_relation_for_stream(&stream).clone()
    };

    tracing::trace!(
        "Found {} relations for stream {}. Relations: {:#?}",
        current_relations.len(),
        stream,
        current_relations
    );

    // // We retrieve the producer so that we can produce to this stream for underlying streams.
    // let consumer_step_sync = producer_step_sync.consume();

    relations.push_back(
        current_relations
            .iter()
            .map(|relation| {
                (
                    broker.clone(),
                    relation.definition.clone(),
                    relation.join_index,
                    relation.stream_name.clone(),
                    relation.subscription.clone(),
                    partition.clone(),
                    producer_step_sync.clone(),
                    records_eventual.clone(),
                    offsets_eventual.clone(),
                )
            })
            .collect::<Vec<_>>(),
    );

    loop {
        let current_relations = relations.pop_front();

        if let Some(current_relations) = current_relations {
            for relation in current_relations {
                // First get the relations for this stream and push it into our ring buffer.
                let current_relations = {
                    let broker_guard = broker.write().await;
                    broker_guard.get_relation_for_stream(&relation.3).clone()
                };

                // Create the consumer for this task.
                let mut consumer_step_sync = ConsumerStepSync::new();

                let (records_eventual, records_setter) = eventual::eventual();
                let (offsets_eventual, offsets_setter) = eventual::eventual();

                relations.push_back(
                    current_relations
                        .iter()
                        .map(|relation| {
                            (
                                broker.clone(),
                                relation.definition.clone(),
                                relation.join_index,
                                relation.stream_name.clone(),
                                relation.subscription.clone(),
                                partition.clone(),
                                // forward the producer to the relational tasks.
                                consumer_step_sync.producer(),
                                records_eventual.clone(),
                                offsets_eventual.clone(),
                            )
                        })
                        .collect::<Vec<_>>(),
                );

                // Then we run the task.
                tokio::spawn(async move {
                    tracing::debug!("Spawning operation for {:#?}", relation);

                    let (
                        broker,
                        definition,
                        join_index,
                        stream,
                        subscription,
                        partition,
                        producer_step_sync,
                        records,
                        offsets,
                    ) = relation;

                    let stream_type = definition.stream_type.clone();

                    match Operation::try_new(
                        broker.clone(),
                        offsets.clone(),
                        None,
                        records.clone(),
                        offsets_setter,
                        records_setter,
                        stream.clone(),
                        definition.clone(),
                        partition.clone(),
                        Some(subscription), // Always Some as this would always be a relation.
                        join_index,
                    )
                    .await
                    {
                        Ok(mut operation) => {
                            producer_step_sync.await_step(Step::Prepare).await;
                            tracing::debug!("Preparing..");
                            match stream_type {
                                Some(FunctionType::Window | FunctionType::Join) => {}
                                _ => {
                                    #[allow(clippy::unwrap_used)]
                                    operation.init().await.unwrap();
                                    #[allow(clippy::unwrap_used)]
                                    operation.prepare().await.unwrap();

                                    consumer_step_sync.set_step(Step::Prepare);
                                }
                            }

                            tracing::debug!("Awaiting Commit step..");

                            producer_step_sync.await_step(Step::Commit).await;
                            tracing::debug!(" Committing..");

                            if let Some(FunctionType::Window | FunctionType::Join) = stream_type {
                                #[allow(clippy::unwrap_used)]
                                operation.init().await.unwrap();
                                #[allow(clippy::unwrap_used)]
                                operation.prepare().await.unwrap();
                            }

                            #[allow(clippy::unwrap_used)]
                            operation.commit().await.unwrap();
                            tracing::debug!("Committed..");

                            consumer_step_sync.set_step(Step::Commit);
                        }
                        Err(err) => {
                            tracing::error!("{:#?}", err)
                        }
                    }
                });
            }
        } else {
            break;
        }
    }

    Ok(())
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
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
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new(
        // passed in dynamically.
        broker: Arc<RwLock<Broker>>,
        offsets: eventual::Eventual<Range<u64>>,
        references: Option<Vec<Reference>>,
        records: eventual::Eventual<Vec<RecordBatch>>,
        offsets_setter: eventual::Setter<Range<u64>>,
        records_setter: eventual::Setter<Vec<RecordBatch>>,

        // Can be kept in relation.
        stream_name: StreamName,
        definition: StreamDefinition,
        partition: PartitionName,
        subscription: Option<Arc<RwLock<Subscription>>>,
        join_index: Option<u64>,
    ) -> Result<Self, HigginsError> {
        Ok(match definition.stream_type {
            Some(FunctionType::Window) => {
                tracing::debug!("Spawning window operation..");
                Operation::Window(WindowOperation(OperationData {
                    broker,
                    stream: stream_name,
                    definition,
                    partition,
                    offsets,
                    references,
                    subscription,
                    records,
                    join_index: None,
                    offsets_setter,
                    records_setter,
                }))
            }

            Some(FunctionType::Map) => {
                tracing::debug!("Spawning map operation..");
                Operation::Map(MapOperation(OperationData {
                    broker,
                    stream: stream_name,
                    definition,
                    partition,
                    offsets,
                    references,
                    subscription,
                    records,
                    join_index: None,
                    offsets_setter,
                    records_setter,
                }))
            }
            Some(FunctionType::Reduce) => Operation::Reduce(ReduceOperation(OperationData {
                broker,
                stream: stream_name,
                definition,
                partition,
                offsets,
                references,
                subscription,
                records,
                join_index: None,
                offsets_setter,
                records_setter,
            })),
            Some(FunctionType::Join) => {
                let join_definition = {
                    let broker_guard = broker.write().await;

                    JoinDefinition::try_from((
                        stream_name.clone(),
                        definition.clone(),
                        &*broker_guard,
                    ))?
                };

                Operation::Join(JoinOperation {
                    data: OperationData {
                        broker,
                        stream: stream_name,
                        definition: definition.clone(),
                        partition,
                        offsets,
                        references,
                        subscription,
                        records,
                        join_index,
                        offsets_setter,
                        records_setter,
                    },
                    definition: join_definition,
                    optimistic_index: None,
                    optimistic_offset: None,
                })
            }
            // Some(FunctionType::Aggregate) => todo!(),
            None => Operation::Produce(ProduceOperation(OperationData {
                broker,
                stream: stream_name,
                definition,
                partition,
                offsets,
                references,
                subscription,
                records,
                join_index: None,
                offsets_setter,
                records_setter,
            })),
            _ => todo!(),
        })
    }

    pub fn broker(&self) -> Arc<RwLock<Broker>> {
        match self {
            Self::Map(o) => o.0.broker.clone(),
            Self::Join(o) => o.data.broker.clone(),
            Self::Window(o) => o.0.broker.clone(),
            Self::Reduce(o) => o.0.broker.clone(),
            Self::Produce(o) => o.0.broker.clone(),
        }
    }

    pub fn stream(&self) -> StreamName {
        match self {
            Self::Map(o) => o.0.stream.clone(),
            Self::Join(o) => o.data.stream.clone(),
            Self::Window(o) => o.0.stream.clone(),
            Self::Reduce(o) => o.0.stream.clone(),
            Self::Produce(o) => o.0.stream.clone(),
        }
    }

    pub async fn init(&mut self) -> Result<(), HigginsError> {
        tracing::debug!("Initting for {:#?}", self);
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
        tracing::debug!("Preparing for {:#?}", self);
        match self {
            Self::Map(o) => o.prepare().await,
            Self::Join(o) => o.prepare().await,
            Self::Window(o) => o.prepare().await,
            Self::Reduce(o) => o.prepare().await,
            Self::Produce(o) => o.prepare().await,
        }
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        tracing::debug!("Committing for {:#?}", self);
        match self {
            Self::Map(o) => o.commit().await,
            Self::Join(o) => o.commit().await,
            Self::Window(o) => o.commit().await,
            Self::Reduce(o) => o.commit().await,
            Self::Produce(o) => o.commit().await,
        }
    }
}
