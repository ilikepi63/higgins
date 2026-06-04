use super::utils::ColumnName;
use crate::derive::eventual::eventual;
use crate::derive::operation::OperationData;
use crate::derive::subscription::create_derived_stream_subscription;
use crate::subscription::Subscription;
use crate::topography::StreamName;
use crate::{
    broker::Broker,
    derive::{
        joining::opts::eager_range_take_or_wait,
        utils::{get_partition_key_from_record_batch, put_default_index_at_range},
    },
    error::HigginsError,
    functions::map::run_map_function,
    topography::{Key, StreamDefinition},
};
use higgins_shared::{PartitionName, read_arrow};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct MapOperation(pub OperationData);

impl MapOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        tracing::trace!("[MAP] Retrieved records: {:#?}", self.0.records);

        let mut references = vec![];

        let records = self.0.records.get().await?;

        tracing::debug!("[MAP] Received records: {:#?}", records);

        for record_batch in records.iter() {
            tracing::trace!("[MAP] Awaiting the broker lock..");

            let broker_lock = self.0.broker.write().await;

            tracing::trace!("[MAP] We are reading the stream values in..");

            for _ in 0..record_batch.num_rows() {
                let partition_val = get_partition_key_from_record_batch(
                    &record_batch,
                    &ColumnName::from(&self.0.definition),
                );

                let engine = &broker_lock.wasm_engine;
                let module = broker_lock
                    .wasm_modules
                    .iter()
                    .find(|(n, _)| n == self.0.definition.function_name.as_ref().unwrap())
                    .map(|(_, m)| m)
                    .unwrap();

                tracing::trace!("[MAP] We have fetched the module.");

                let mapped_record_batch = run_map_function(&record_batch, engine, module);

                tracing::trace!("[MAP] Result from mapping: {:#?}", mapped_record_batch);

                tracing::trace!("[MAP] Producing to the stream..");

                {
                    let stream = self.0.stream.to_string();
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

        self.0.references = Some(references);

        Ok(())

        // PREPARE
        // COMMIT
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        match self.0.references.as_ref() {
            Some(references) => {
                let mut broker_guard = self.0.broker.write().await;

                let offsets = self.0.offsets.get().await?;

                put_default_index_at_range(
                    self.0.stream.to_string(),
                    &self.0.partition,
                    offsets.clone(),
                    &mut broker_guard,
                    references,
                )
                .await?;

                let mut lock = self.0.subscription.as_mut().unwrap().write().await;

                lock.acknowledge(&self.0.partition, &offsets)?;

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

                    let (records_eventual, record_setter) = eventual();
                    let (offsets, offsets_setter) = eventual();

                    offsets_setter.set(offset).await;
                    record_setter.set(records).await;

                    let mut operation = MapOperation(OperationData {
                        broker: broker_ref.clone(),
                        stream: StreamName::from(stream_name.clone()).clone(),
                        definition: stream_def.clone(),
                        partition: partition.clone(),
                        offsets: offsets,
                        references: None,
                        subscription: Some(subscription.clone()),
                        records: records_eventual,
                        join_index: None,
                        offsets_setter: todo!(),
                        records_setter: todo!(),
                    });

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
