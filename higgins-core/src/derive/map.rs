use super::utils::ColumnName;
use crate::derive::operation::Eventual;
use crate::derive::subscription::create_derived_stream_subscription;
use crate::subscription::Subscription;
use crate::{
    broker::Broker,
    derive::{
        joining::opts::eager_range_take_or_wait,
        utils::{get_partition_key_from_record_batch, put_default_index_at_range},
    },
    error::HigginsError,
    functions::map::run_map_function,
    storage::dereference::Reference,
    topography::{Key, StreamDefinition},
};
use arrow::array::RecordBatch;
use higgins_shared::{PartitionName, read_arrow};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct MapOperation {
    /// Broker  Reference.
    pub broker: Arc<RwLock<Broker>>,
    /// This resultant stream's stream name.
    pub stream_name: Key,
    /// This resultant streams stream definition.
    pub stream_def: StreamDefinition,
    /// The partition we've received offsets on.
    pub partition: PartitionName,
    /// The offsets.
    pub offset: Eventual<Range<u64>>,
    /// The references - We want to use these to commit so we have to save them over init and commit branches.
    pub references: Option<Vec<Reference>>,
    /// The subscription that controls how this stream is tracked.
    pub subscription: Arc<RwLock<Subscription>>,
    /// The underlying records that this operation is based on.
    /// Vec<(
    ///   Vec<u8> - IPC record batch.
    ///   u64 - The offset to which it belongs.
    /// )>
    pub records: Eventual<Vec<RecordBatch>>,
}

impl MapOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        tracing::trace!("[MAP] Retrieved records: {:#?}", self.records);

        let mut references = vec![];

        for record_batch in self.records.iter() {
            tracing::trace!("[MAP] Received consume Response");

            tracing::trace!("[MAP] Iterating through batches..");

            tracing::trace!("[MAP] Awaiting the broker lock..");

            let broker_lock = self.broker.write().await;

            tracing::trace!("[MAP] We are reading the stream values in..");

            for _ in 0..record_batch.num_rows() {
                let partition_val = get_partition_key_from_record_batch(
                    &record_batch,
                    &ColumnName::from(&self.stream_def),
                );

                let engine = &broker_lock.wasm_engine;
                let module = broker_lock
                    .wasm_modules
                    .iter()
                    .find(|(n, _)| n == self.stream_def.function_name.as_ref().unwrap())
                    .map(|(_, m)| m)
                    .unwrap();

                tracing::trace!("[MAP] We have fetched the module.");

                let mapped_record_batch = run_map_function(&record_batch, engine, module);

                tracing::trace!("[MAP] Result from mapping: {:#?}", mapped_record_batch);

                tracing::trace!("[MAP] Producing to the stream..");

                {
                    let stream = String::from_utf8_lossy(self.stream_name.as_bytes()).to_string();
                    let partition = &PartitionName::try_from(&partition_val[..])?;

                    // CREATE REFERENCE
                    let reference = broker_lock
                        .put_data_store(stream.clone(), partition, mapped_record_batch)
                        .await?;

                    references.push(reference);
                }
            }

            drop(broker_lock);
        }

        self.references = Some(references);

        Ok(())

        // PREPARE
        // COMMIT
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        match self.references.as_ref() {
            Some(references) => {
                let mut broker_guard = self.broker.write().await;

                let stream = String::from_utf8_lossy(self.stream_name.as_bytes()).to_string();

                put_default_index_at_range(
                    stream,
                    &self.partition,
                    self.offset.clone(),
                    &mut broker_guard,
                    references,
                )
                .await?;

                let mut lock = self.subscription.write().await;

                lock.acknowledge(&self.partition, &self.offset)?;

                drop(lock);

                Ok(())
            }
            None => Err(HigginsError::Unknown),
        }
    }
}

pub async fn create_mapped_stream_from_definition(
    stream_name: Key,
    stream_def: StreamDefinition,
    base_stream: (Key, StreamDefinition),
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    tracing::debug!(
        "Base stream: {}",
        String::from_utf8_lossy(base_stream.0.as_bytes())
    );
    tracing::debug!(
        "Derivative stream: {}",
        String::from_utf8_lossy(stream_name.as_bytes())
    );

    let (client_id, condvar, subscription) = {
        tracing::trace!("Attempting to input client_id.");

        let client_id = broker
            .clients
            .insert(crate::client::ClientRef::NoOp)
            .ok_or(HigginsError::Unknown)?;

        tracing::trace!("Retrieved client_id.");
        let subscription = broker.create_subscription(base_stream.0.as_bytes());

        tracing::trace!("Successfully created the subscription.");

        let (notify, subscription) = broker
            .get_subscription_by_key(base_stream.0.as_bytes(), &subscription)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)?;

        tracing::trace!("Retrieved the notification for said subscription.");

        (client_id, notify, subscription)
    };

    tokio::task::spawn(async move {
        tracing::trace!("[MAP] We are being initiated");
        let result: Result<(), HigginsError> = async {
            loop {
                let offsets =
                    eager_range_take_or_wait(subscription.clone(), condvar.clone(), client_id)
                        .await?;

                tracing::info!("[MAP] Retrieved offsets in map {:#?}", offsets);

                for (partition, offset) in offsets {
                    let records = {
                        let mut broker_guard = broker_ref.write().await;
                        tracing::info!("[MAP] Retrieved broker lock.");
                        broker_guard
                            .get_range(base_stream.0.as_bytes(), &partition, offset.clone())
                            .await?
                            .into_iter()
                            .filter_map(std::convert::identity)
                            .collect::<Vec<_>>()
                    };

                    let records = records
                        .iter()
                        .map(|data| {
                            read_arrow(data)
                                .next()
                                .map(|result| result.ok())
                                .flatten()
                                .ok_or(HigginsError::Unknown)
                        })
                        .collect::<Result<Vec<_>, HigginsError>>()?;

                    let mut operation = MapOperation {
                        broker: broker_ref.clone(),
                        stream_name: stream_name.clone(),
                        stream_def: stream_def.clone(),
                        partition: partition.clone(),
                        offset: offset.clone(),
                        references: None,
                        subscription: subscription.clone(),
                        records,
                    };

                    tracing::info!("[MAP] Created operation.");

                    operation.init().await.unwrap();
                    operation.prepare().await.unwrap();
                    operation.commit().await.unwrap();
                }
            }
        }
        .await;

        if let Err(err) = result {
            tracing::error!("An error occurred whilst mapping the stream: {:#?}", err);
        }
    });

    Ok(())
}
