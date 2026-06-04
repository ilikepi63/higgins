use super::joining::opts::eager_range_take_or_wait;
use super::utils::put_default_index_at_range;
use crate::derive::eventual::eventual;
use crate::derive::operation::OperationData;
use crate::storage::dereference::Reference;
use crate::subscription::Subscription;
use crate::topography::StreamName;
use crate::{
    broker::Broker,
    error::HigginsError,
    functions::reduce::run_reduce_function,
    topography::{Key, StreamDefinition},
};
use arrow::array::RecordBatch;
use higgins_shared::PartitionName;
use higgins_shared::read_arrow;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ReduceOperation(pub OperationData);

impl ReduceOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        // tracing::debug!("Retrieved {} records for reduction.", self.records.len());

        let offsets = self.0.offsets.get().await?;
        // In order to begin the reduction for these records, we need to
        // retrieve the first record's previous record.
        let mut prev_record = match offsets.start {
            0 => None,
            _ => {
                let mut broker_guard = self.0.broker.write().await;
                broker_guard
                    .get_at(
                        self.0.stream.as_bytes(),
                        &self.0.partition,
                        offsets.start - 1, // TODO: This should be impossible to fail as the invariant forces > 0, perhaps there is a better technique to be used here
                    )
                    .await
                    .inspect_err(|err| {
                        tracing::error!("Failed to retrieve offset with error: {:#?}", err)
                    })
                    .ok()
                    .flatten()
                    .map(|arrow_bytes| {
                        // tracing::trace!("bytes: {:#?}", arrow_bytes);
                        let mut batches = read_arrow(&arrow_bytes);
                        tracing::trace!("batches: {:#?}", batches);
                        batches
                            .next()
                            .inspect(|val| {
                                tracing::trace!(
                                    "Correctly retrieved a value from the batches: {:#?}",
                                    val
                                );
                            })
                            .and_then(|result| result.ok())
                    })
            }
        }
        .flatten();

        let mut references = vec![];

        let records = self.0.records.get().await;

        for batches in records.iter() {
            for batch in batches.iter() {
                tracing::trace!("[REDUCE] Awaiting the broker lock..");

                let broker_lock = self.0.broker.write().await;

                tracing::trace!("[REDUCE] We are reading the stream values in..");

                tracing::debug!("Retrieved current value: {:#?}", batch);
                tracing::debug!("Previous value: {:#?}", prev_record);

                match prev_record.as_ref() {
                    Some(prev_record) => {
                        tracing::info!("Using previous record..");

                        let module = broker_lock
                            .wasm_modules
                            .iter()
                            .find(|(n, _)| n == self.0.definition.function_name.as_ref().unwrap())
                            .map(|(_, m)| m)
                            .unwrap();

                        tracing::trace!("Applying the function..");

                        let reduced_record_batch = run_reduce_function(
                            &batch,
                            &prev_record,
                            &broker_lock.wasm_engine,
                            module,
                        );

                        tracing::trace!("Reduced Record batch: {:#?}", reduced_record_batch);

                        {
                            // CREATE REFERENCE
                            let reference = broker_lock
                                .put_data_store(
                                    self.0.stream.to_string(),
                                    &self.0.partition,
                                    reduced_record_batch,
                                )
                                .await?;

                            references.push(reference);
                        }
                    }
                    None => {
                        tracing::trace!(
                            "No previous index found. Producing to stream {} key {} ",
                            self.0.stream.to_string(),
                            String::from_utf8_lossy(&self.0.partition.0)
                        );

                        // CREATE REFERENCE
                        let reference = broker_lock
                            .put_data_store(
                                self.0.stream.to_string(),
                                &self.0.partition,
                                batch.clone(),
                            )
                            .await?;

                        references.push(reference);
                    }
                }

                tracing::trace!("Setting previous record to current value.");
                prev_record = Some(batch.clone());
            }
        }

        self.0.references = Some(references);

        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        tracing::trace!("Writing the values.");

        if let Some(references) = self.0.references.as_ref() {
            let offsets = self.0.offsets.get().await?;

            {
                let mut broker_guard = self.0.broker.write().await;

                tracing::trace!("Writing the offsets.");

                put_default_index_at_range(
                    self.0.stream.to_string(),
                    &self.0.partition,
                    offsets.clone(),
                    &mut broker_guard,
                    references,
                )
                .await?;
            }
            tracing::trace!(
                "Wrote the offsets to {:#?}. References: {:#?}",
                offsets,
                references
            );

            if let Some(subscription) = self.0.subscription.as_ref() {
                let mut lock = subscription.write().await;

                lock.acknowledge(&self.0.partition, &offsets)?;
            }
        } else {
            tracing::error!("Attempt to commit without any referencs on Reduce stream.")
        }

        Ok(())
    }
}

pub async fn create_reduced_stream_from_definition(
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
        tracing::trace!("[REDUCE] We are being initiated");
        let result: Result<(), HigginsError> = async {
            loop {
                let offsets =
                    eager_range_take_or_wait(subscription.clone(), condvar.clone(), client_id)
                        .await?;

                tracing::info!("[REDUCE] Retrieved offsets in REDUCE {:#?}", offsets);

                for (partition, offset) in offsets {
                    tracing::debug!("[REDUCE] Iterating offsets");
                    let records = {
                        tracing::debug!("[REDUCE] Awaiting broker lock");

                        let mut broker_guard = broker_ref.write().await;

                        tracing::debug!("[REDUCE] Acquired broker lock");

                        let range = broker_guard
                            .get_range(base_stream.0.as_bytes(), &partition, offset.clone())
                            .await
                            .inspect_err(|err| {
                                tracing::error!("Retrieved error on try: {:#?}", err)
                            })?;

                        tracing::debug!("Retrieved range: {:#?}", range);

                        range
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
                        .collect::<Result<Vec<RecordBatch>, HigginsError>>()?;

                    let (records_eventual, record_setter) = eventual();
                    let (offsets, offsets_setter) = eventual();

                    offsets_setter.set(offset);
                    record_setter.set(records);

                    let mut operation = ReduceOperation(OperationData {
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

                    operation.init().await.unwrap();
                    operation.prepare().await.unwrap();
                    operation.commit().await.unwrap();
                }
            }
        }
        .await;

        if let Err(err) = result {
            tracing::error!("An error occurred whilst REDUCEping the stream: {:#?}", err);
        }
    });

    Ok(())
}
