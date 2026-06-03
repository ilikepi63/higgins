//! Operations are abstractions over each action of a derived stream.
//!
//! If we consider the streams as vertices in a graph, operations would be the edges between those vertices. It is necessary to have an
//! abstraction over these edges as it is necessary to execute these independently of one another.
use arrow::array::RecordBatch;
use higgins_shared::{PartitionName, read_arrow};
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
    derive::{joining::join::JoinDefinition, windowed::definition::WindowedStreamDefinition},
    error::HigginsError,
    storage::dereference::Reference,
    subscription::Subscription,
    topography::{FunctionType, StreamDefinition, StreamName},
};

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

impl Step {
    /// Checks if this related step can allow the derivative algorithm to
    /// prepare it's data for commit.
    pub fn can_prepare(&self) -> bool {
        *self > Self::Pre
    }

    /// A check to see if the transaction can commit.
    pub fn can_commit(&self) -> bool {
        *self > Self::Commit
    }
}

impl Into<u8> for Step {
    fn into(self) -> u8 {
        self as u8
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
    tracing::trace!("Initializing the Operation.");
    let mut operation = Operation::try_new(
        broker.clone(),
        0..0, // Doesn't matter..
        None,
        records.to_vec(),
        StreamName::from(stream.clone()),
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
        0..0,
        definition,
        Some(records),
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
    offsets: Range<u64>,
    _definition: StreamDefinition,
    records: Option<&[RecordBatch]>,
    broker: Arc<RwLock<Broker>>,
    producer_step_sync: ProducerStepSync,
) -> Result<(), HigginsError> {
    let mut relations = VecDeque::new();

    tracing::trace!("Querying relations with key {}", stream);

    let current_relations = {
        let broker_guard = broker.write().await;
        tracing::debug!("Broker state: {:#?}", broker_guard);
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
                    relation.join_index.clone(),
                    relation.stream_name.clone(),
                    relation.subscription.clone(),
                    offsets.clone(),
                    records.map(|records| records.to_vec()).unwrap_or(vec![]), // TODO: Join and Window need to create their records in init.
                    partition.clone(),
                    producer_step_sync.clone(),
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

                relations.push_back(
                    current_relations
                        .iter()
                        .map(|relation| {
                            (
                                broker.clone(),
                                relation.definition.clone(),
                                relation.join_index.clone(),
                                relation.stream_name.clone(),
                                relation.subscription.clone(),
                                offsets.clone(),
                                records.map(|records| records.to_vec()).unwrap_or(vec![]), // TODO: Join and Window need to create their records in init.
                                partition.clone(),
                                // forward the producer to the relational tasks.
                                consumer_step_sync.producer(),
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
                        offsets,
                        records,
                        partition,
                        producer_step_sync,
                    ) = relation;

                    let stream_type = definition.stream_type.clone();

                    let mut operation = Operation::try_new(
                        broker.clone(),
                        offsets.clone(),
                        None,
                        records.clone(),
                        stream.clone(),
                        definition.clone(),
                        partition.clone(),
                        Some(subscription), // Always Some as this would always be a relation.
                        join_index,
                    )
                    .await
                    .inspect_err(|err| tracing::error!("{:#?}", err))
                    .unwrap();
                    tracing::debug!("Awaiting prepare step.");

                    producer_step_sync.await_step(Step::Prepare).await;
                    tracing::debug!("Preparing..");
                    match stream_type {
                        Some(FunctionType::Window | FunctionType::Join) => {}
                        _ => {
                            operation.init().await.unwrap();
                            operation.prepare().await.unwrap();

                            consumer_step_sync.set_step(Step::Prepare);
                        }
                    }

                    tracing::debug!("Awaiting Commit step..");

                    producer_step_sync.await_step(Step::Commit).await;
                    tracing::debug!(" Committing..");

                    match stream_type {
                        Some(FunctionType::Window | FunctionType::Join) => {
                            operation.init().await.unwrap();
                            operation.prepare().await.unwrap();
                        }
                        _ => {}
                    }

                    operation.commit().await.unwrap();
                    tracing::debug!("Committed..");

                    consumer_step_sync.set_step(Step::Commit);
                });
            }
        } else {
            break;
        }
    }

    Ok(())
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

        let sync_values = Arc::new((Notify::new(), AtomicU8::new(Step::Pre.into())));

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
                    sync_values.clone(),
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
                    sync_values,
                ) = moved_values;

                let stream_type = definition.stream_type.clone();

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

                if Step::from(&sync_values.1).can_prepare() {
                    operation.init().await.unwrap();
                    operation.prepare().await.unwrap(); // TODO: handle failure for commit.

                    match stream_type {
                        Some(FunctionType::Window | FunctionType::Join) => {}
                        _ => {
                            sync_values
                                .1
                                .store(Step::Prepare as u8, std::sync::atomic::Ordering::SeqCst);
                            sync_values.0.notify_waiters();
                        }
                    }
                }

                if Step::from(&sync_values.1).can_commit() {
                    operation.commit().await.unwrap();
                    sync_values
                        .1
                        .store(Step::Commit as u8, std::sync::atomic::Ordering::SeqCst);
                    sync_values.0.notify_waiters();
                }
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
        offsets: Option<Range<u64>>,
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
            Some(FunctionType::Map) => {
                tracing::debug!("Spawning map operation..");
                Operation::Map(MapOperation {
                    broker,
                    stream_name: stream_name.into(),
                    stream_def: definition,
                    partition,
                    offset: offsets,
                    references,
                    subscription: subscription.ok_or(HigginsError::Unknown)?,
                    records,
                })
            }
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
