use super::joining::opts::eager_range_take_or_wait;
use super::utils::put_default_index_at_range;
use crate::storage::dereference::Reference;
use crate::subscription::Subscription;
use crate::{
    broker::Broker,
    error::HigginsError,
    functions::reduce::run_reduce_function,
    topography::{Key, StreamDefinition},
};
use higgins_shared::PartitionName;
use higgins_shared::read_arrow;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ReduceOperation {
    /// Broker  Reference.
    broker: Arc<RwLock<Broker>>,
    /// This resultant stream's stream name.
    stream_name: Key,
    /// This resultant streams stream definition.
    stream_def: StreamDefinition,
    /// The partition we've received offsets on.
    partition: PartitionName,
    /// The offsets.
    offsets: Range<u64>,
    /// The references - We want to use these to commit so we have to save them over init and commit branches.
    references: Option<Vec<Reference>>,
    /// The subscription that controls how this stream is tracked.
    subscription: Arc<RwLock<Subscription>>,
    /// The underlying records that this operation is based on.
    /// Vec<(
    ///   Vec<u8> - IPC record batch.
    ///   u64 - The offset to which it belongs.
    /// )>
    records: Vec<(Vec<u8>, u64)>,
}

impl ReduceOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        tracing::debug!("Retrieved {} records for reduction.", self.records.len());
        // In order to begin the reduction for these records, we need to
        // retrieve the first record's previous record.
        let mut prev_record = match self.offsets.start {
            0 => None,
            _ => {
                let mut broker_guard = self.broker.write().await;
                broker_guard
                    .get_at(
                        self.stream_name.as_bytes(),
                        &self.partition,
                        self.offsets.start - 1, // TODO: This should be impossible to fail as the invariant forces > 0, perhaps there is a better technique to be used here
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

        for (data, _) in self.records.iter() {
            tracing::trace!("[REDUCE] Awaiting the broker lock..");

            let broker_lock = self.broker.write().await;

            tracing::trace!("[REDUCE] We are reading the stream values in..");

            let batch = {
                let mut stream_reader = read_arrow(&data);

                let batch = if let Some(batch) = stream_reader.next() {
                    batch
                        .inspect_err(|err| tracing::error!("{:#?}", err))
                        .unwrap()
                } else {
                    tracing::error!("No batch returned for current value.   ");
                    panic!();
                };
                // TODO: We need to ensure that these batches are merged if there are more than one.
                batch
            };

            tracing::debug!("Retrieved current value: {:#?}", batch);
            tracing::debug!("Previous value: {:#?}", prev_record);

            match prev_record.as_ref() {
                Some(prev_record) => {
                    tracing::info!("Using previous record..");

                    let module = broker_lock
                        .wasm_modules
                        .iter()
                        .find(|(n, _)| n == self.stream_def.function_name.as_ref().unwrap())
                        .map(|(_, m)| m)
                        .unwrap();

                    tracing::trace!("Applying the function..");

                    let reduced_record_batch =
                        run_reduce_function(&batch, &prev_record, &broker_lock.wasm_engine, module);

                    tracing::trace!("Reduced Record batch: {:#?}", reduced_record_batch);

                    {
                        let stream =
                            String::from_utf8_lossy(self.stream_name.as_bytes()).to_string();

                        // CREATE REFERENCE
                        let reference = broker_lock
                            .put_data_store(stream.clone(), &self.partition, reduced_record_batch)
                            .await?;

                        references.push(reference);
                    }
                }
                None => {
                    tracing::trace!(
                        "No previous index found. Producing to stream {} key {} ",
                        String::from_utf8_lossy(self.stream_name.as_bytes()),
                        String::from_utf8_lossy(&self.partition.0)
                    );

                    let stream = String::from_utf8_lossy(self.stream_name.as_bytes()).to_string();

                    // CREATE REFERENCE
                    let reference = broker_lock
                        .put_data_store(stream.clone(), &self.partition, batch.clone())
                        .await?;

                    references.push(reference);
                }
            }

            tracing::trace!("Setting previous record to current value.");
            prev_record = Some(batch);
        }

        self.references = Some(references);

        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        tracing::trace!("Writing the values.");

        if let Some(references) = self.references.as_ref() {
            {
                let mut broker_guard = self.broker.write().await;

                let stream = String::from_utf8_lossy(self.stream_name.as_bytes()).to_string();

                tracing::trace!("Writing the offsets.");

                put_default_index_at_range(
                    stream,
                    &self.partition,
                    self.offsets.clone(),
                    &mut broker_guard,
                    references,
                )
                .await?;
            }
            tracing::trace!(
                "Wrote the offsets to {:#?}. References: {:#?}",
                self.offsets,
                references
            );

            let mut lock = self.subscription.write().await;

            lock.acknowledge(&self.partition, &self.offsets)?;

            drop(lock);
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
                            .zip(offset.start..=offset.end)
                            .collect::<Vec<_>>()
                    };

                    let mut operation = ReduceOperation {
                        broker: broker_ref.clone(),
                        stream_name: stream_name.clone(),
                        stream_def: stream_def.clone(),
                        partition: partition.clone(),
                        offsets: offset.clone(),
                        references: None,
                        subscription: subscription.clone(),
                        records,
                    };

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
