use super::Broker;
use crate::broker::subscriptions::{OffsetPayload, write_offsets_to_client};
use crate::derive::operation::{OperationData, produce_operation};
use crate::storage::backing_store::BackingStore;
use crate::storage::index::IndexType;
use crate::storage::index::default::DefaultIndex;
use crate::utils::epoch;
use arrow::array::RecordBatch;
use higgins_codec::message::Type;
use higgins_codec::{Message, TakeRecordsRequest, TakeRecordsResponse};
use higgins_shared::{PartitionName, StreamName};
use riskless::messages::ProduceRequest;

use crate::storage::{
    dereference::{Reference, S3Reference},
    index::Index,
};
use higgins_shared::{HigginsError, write_arrow};
use prost::Message as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::RwLock;

pub struct ProduceOperation(pub OperationData);

impl ProduceOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        tracing::debug!("Running init on produce.");
        let broker = self.0.broker.write().await;
        tracing::debug!("Retrieved broker lock.");

        let mut references = vec![];

        let records = self.0.records.get().await?;

        self.0.records_setter.set(records.clone()).await;

        let backing_store = broker
            .backing_store
            .as_ref()
            .ok_or(HigginsError::ObjectStoreNotConfigured)?
            .clone();

        drop(broker);

        for record in records {
            references.push(
                Broker::put_data_store(
                    backing_store.clone(),
                    self.0.stream.to_string(),
                    &self.0.partition.clone(),
                    record.clone(),
                )
                .await
                .inspect_err(|err| tracing::error!("{:#?}", err))?,
            );
        }

        tracing::debug!("Returning references.");

        self.0.references = Some(references);

        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        let mut broker = self.0.broker.write().await;
        tracing::debug!("Running commit.");

        let mut index_file_lock =
            broker.get_index_file(self.0.stream.clone(), &self.0.partition.clone())?;

        let mut index_file_guard = index_file_lock.lock().await;

        let file_len = index_file_guard.len()?;

        if let Some(references) = self.0.references.as_ref() {
            let offset = file_len..file_len;
            let setter_offset = file_len as u64..file_len as u64;
            let mut buf = vec![0_u8; DefaultIndex::size_of() * references.len()];

            buf.chunks_mut(DefaultIndex::size_of())
                .zip(references)
                .zip(offset.start..=offset.end)
                .map(|((buf, reference), offset)| {
                    DefaultIndex::put(offset as u64, reference.clone(), 0, epoch(), 0, buf)
                })
                .collect::<Result<Vec<_>, std::io::Error>>()?;

            index_file_guard.range_put_at(
                offset.start as usize..offset.end.saturating_add(1) as usize,
                &mut buf,
            )?;

            self.0.offsets_setter.set(setter_offset).await;

            // get the subscriptions for the stream.
            let subscription = broker.get_subscriptions_for_stream(&self.0.stream);

            let stream_name = self.0.stream.clone();

            // If there are subscriptions, produce to them.
            if let Some(subscriptions) = subscription {
                for (_, subscription) in subscriptions.values() {
                    let mut subscription_guard = subscription.write().await;

                    tracing::trace!(
                        "[PRODUCE] Found a subscription for this produce request: {:#?}",
                        subscription_guard
                    );

                    if subscription_guard
                        .partitions
                        .iter()
                        .find(|sub_key| sub_key.partition_id == self.0.partition)
                        .is_some()
                    {
                        subscription_guard.set_end(&self.0.partition, offset.end as u64)?;
                    } else {
                        subscription_guard.add_partition(
                            &self.0.partition,
                            0,
                            offset.end as u64,
                        )?;
                    };

                    tracing::trace!(
                        "Set the end of this given subscription: {:#?}",
                        subscription_guard
                    );

                    let client_ids = subscription_guard
                        .client_counts
                        .iter()
                        .map(|(client_id, _)| client_id.clone())
                        .collect::<Vec<_>>();

                    tracing::trace!("Clients: {:#?}", client_ids);

                    for client_id in client_ids {
                        let client_ref = if let Some(r) = broker.get_client_by_id(client_id.clone())
                        {
                            r
                        } else {
                            continue;
                        };

                        tracing::trace!("Retrieved client ref: {:#?}", client_ref);

                        let n = match subscription_guard
                            .client_counts
                            .binary_search_by(|(id, _)| client_id.cmp(id))
                            .map(|index| subscription_guard.client_counts.get(index))
                            .ok()
                            .flatten()
                        {
                            Some(c) => c.1.load(Ordering::Relaxed),
                            None => continue,
                        };

                        tracing::trace!("[TAKE] Taking the amount: {n}");

                        let offsets = subscription_guard.take(n);

                        tracing::trace!("{:#?}", &offsets);

                        if let Ok(offsets) = offsets.as_ref() {
                            subscription_guard
                                .remove_client_count(&client_id, offsets.len() as u64);
                        }

                        if let Ok(offsets) = offsets {
                            //Get payloads from offsets.
                            for (partition, offset) in offsets {
                                let consumption = {
                                    let mut results = vec![];

                                    let consumption = broker
                                        .consume(&stream_name, &partition, offset, 50_000)
                                        .await;

                                    if let Ok(consumption) = consumption {
                                        for result in consumption {
                                            // tracing::trace!(
                                            //     "RECEIVED DATA FOR SUBSCRIPTION: {:#?}",
                                            //     result
                                            // );

                                            if let Ok(result) = result {
                                                results.push(OffsetPayload {
                                                    stream: stream_name.clone(),
                                                    key: partition.clone(),
                                                    offset,
                                                    bytes: result, // TODO: wrap this in a conversion function and filter out errors.
                                                });
                                            }
                                        }
                                    }

                                    results
                                };

                                for val in consumption {
                                    let resp = TakeRecordsResponse {
                                        records: vec![{ val.into() }],
                                    };

                                    tracing::trace!("[TAKE] Writing the amount back to client.");

                                    client_ref
                                        .send(Message {
                                            r#type: Type::Takerecordsresponse as i32,
                                            take_records_response: Some(resp),
                                            ..Default::default()
                                        })
                                        .await
                                        .map_err(|err| {
                                            HigginsError::Arbitrary(format!(
                                                "Failed to write offsets to client: {:#?}",
                                                err
                                            ))
                                        })?;
                                }
                            }
                        };
                    }
                }
            }
        } else {
            tracing::error!("Attempt to place without errors.");
        }

        Ok(())
    }
}

impl Broker {
    /// Produce a data set onto the named stream.
    pub async fn produce(
        // &mut self,
        stream_name: &StreamName,
        partition: &PartitionName,
        record_batch: RecordBatch,
        broker: Arc<RwLock<Broker>>,
    ) -> Result<(), HigginsError> {
        tracing::trace!(
            "[PRODUCE] Producing to stream: {}, data: {:#?}",
            stream_name,
            record_batch
        );

        let definition = {
            let broker_guard = broker.write().await;
            broker_guard
                .get_topography_stream(stream_name)
                .map(|(_, definition)| definition.clone())?
        };

        tracing::trace!("Initializing produce operation.");
        produce_operation(
            stream_name.clone(),
            partition.clone(),
            definition,
            &[record_batch],
            broker,
        )
        .await?;
        tracing::trace!("Completed produce operation.");

        Ok(())
    }

    /// Places data in the backing store, returning a `Reference` to where it was placed.
    pub async fn put_data_store(
        backing_store: Arc<dyn BackingStore<Error = HigginsError>>,
        stream: String,
        partition: &PartitionName,
        data: RecordBatch,
    ) -> Result<Reference, HigginsError> {
        let data = write_arrow(&data)?;

        let request = ProduceRequest {
            request_id: 1,
            topic: stream,
            partition: partition.to_vec(),
            data,
        };

        let response = backing_store.put(request)?;

        let response = response
            .recv()
            .await
            .map_err(|e| HigginsError::Arbitrary(e.to_string()))?;

        let reference = Reference::S3(S3Reference {
            object_key: response.object_key,
            position: response.offset,
            size: response.size.into(),
        });

        Ok(reference)
    }

    /// Takes a record batch and places it given the current Index.
    ///
    /// An {Index} will always have the given {Reference}, and therefore will always
    /// put the data in a place referenceable by the given reference. If the given reference
    /// does not explicitly have a "place" yet, this will generate data to fulfill that.
    pub async fn put_data<'a>(
        &self,
        stream: String,
        partition: &PartitionName,
        index: &mut Index<'a>,
        data: RecordBatch,
    ) -> Result<Vec<u8>, HigginsError> {
        tracing::trace!("[PRODUCE] Producing to stream: {}", stream);

        let data = write_arrow(&data)?;

        let request = ProduceRequest {
            request_id: 1,
            topic: stream,
            partition: partition.to_vec(),
            data,
        };

        let response = self
            .backing_store
            .as_ref()
            .ok_or(HigginsError::ObjectStoreNotConfigured)?
            .put(request)?;

        let response = response
            .recv()
            .await
            .map_err(|e| HigginsError::Arbitrary(e.to_string()))?;

        let reference = Reference::S3(S3Reference {
            object_key: response.object_key,
            position: response.offset,
            size: response.size.into(),
        });

        let mut reference_bytes = [0_u8; Reference::size_of()];

        reference.to_bytes(&mut reference_bytes)?;

        tracing::trace!("Reference: {:#?}", reference_bytes);

        let index = index.put_reference(Reference::S3(S3Reference {
            object_key: response.object_key,
            position: response.offset,
            size: response.size.into(),
        }))?;

        tracing::trace!(
            "Successfully written to the index: {:#?}",
            Index::of(&index, IndexType::Join)
        );

        Ok(index)
    }
}
