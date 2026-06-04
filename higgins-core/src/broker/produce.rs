use super::Broker;
use crate::derive::operation::{OperationData, produce_operation};
use crate::storage::index::default::DefaultIndex;
use crate::topography::{Key, StreamName};
use crate::utils::epoch;
use crate::{derive::operation::Operation, storage::index::IndexType};
use arrow::array::RecordBatch;
use futures::Stream;
use higgins_shared::PartitionName;
use riskless::messages::ProduceRequest;

use crate::{
    error::HigginsError,
    storage::{
        dereference::{Reference, S3Reference},
        index::Index,
    },
};
use higgins_shared::write_arrow;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;
pub struct ProduceOperation(pub OperationData);
// /// Broker  Reference.
// pub broker: Arc<RwLock<Broker>>,
// /// Stream that this value is being produced to.
// pub stream: String,
// /// The partition we've received offsets on.
// pub partition: PartitionName,
// /// The offsets at which we are optimistic of placing these values.
// pub offsets: Range<u64>,
// /// The underlying records that this operation is based on.
// /// Vec<(
// ///   Vec<u8> - IPC record batch.
// ///   u64 - The offset to which it belongs.
// /// )>
// pub records: Vec<RecordBatch>,
// /// The References that have previously been created.
// pub references: Option<Vec<Reference>>,

impl ProduceOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        tracing::debug!("Running init on produce.");
        let mut broker = self.0.broker.write().await;
        tracing::debug!("Retrieved broker lock.");

        let mut references = vec![];

        let records = self.0.records.get().await?;

        for record in records {
            references.push(
                broker
                    .put_data_store(
                        self.0.stream.to_string(),
                        &self.0.partition.clone(),
                        record.clone(),
                    )
                    .await
                    .inspect_err(|err| tracing::error!("{:#?}", err))?,
            );
        }

        tracing::debug!("Returning references.");

        let reference_count = references.len() as u64;

        self.0.references = Some(references);

        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        let mut broker = self.0.broker.write().await;
        tracing::debug!("Running commit.");

        let mut index_file_lock = broker
            .get_index_file(self.0.stream.to_string(), &self.0.partition.clone())
            .unwrap();

        let mut index_file_guard = index_file_lock.lock().await;

        if let Some(references) = self.0.references.as_ref() {
            let offset = self.0.offsets.get().await?;

            let mut buf = vec![0_u8; DefaultIndex::size_of() * references.len()];

            buf.chunks_mut(DefaultIndex::size_of())
                .zip(references)
                .zip(offset.start..=offset.end)
                .map(|((buf, reference), offset)| {
                    DefaultIndex::put(
                        offset.try_into().unwrap(),
                        reference.clone(),
                        0,
                        epoch(),
                        0,
                        buf,
                    )
                })
                .collect::<Result<Vec<_>, std::io::Error>>()?;

            index_file_guard.range_put_at(
                offset.start as usize..offset.end.saturating_add(1) as usize,
                &mut buf,
            )?;

            let subscription = broker.get_subscriptions_for_stream(&self.0.stream.to_string());

            if let Some(subscriptions) = subscription {
                tracing::trace!("[PRODUCE] Found a subscription for this produce request.");

                for (notify, subscription) in subscriptions.values() {
                    let mut subscription = subscription.write().await;

                    tracing::trace!(
                        "[PRODUCE] Notifying the subscription. Offsets: {:#?}",
                        offset
                    );

                    if subscription
                        .partitions
                        .iter()
                        .find(|sub_key| sub_key.partition_id == self.0.partition)
                        .is_some()
                    {
                        subscription.set_end(&self.0.partition, offset.end as u64)?;
                    } else {
                        subscription.add_partition(&self.0.partition, 0, offset.end as u64)?;
                    };

                    tracing::info!("SUBSCRIPTION{:#?}", subscription);

                    // Notify the tasks awaiting this subscription.
                    notify.notify_waiters();
                    tracing::trace!("[PRODUCE] Notified the subscription.");
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
        stream_name: &[u8],
        partition: &PartitionName,
        record_batch: RecordBatch,
        broker: Arc<RwLock<Broker>>,
    ) -> Result<(), HigginsError> {
        tracing::trace!(
            "[PRODUCE] Producing to stream: {}, data: {:#?}",
            String::from_utf8(stream_name.to_vec()).unwrap(),
            record_batch
        );

        let definition = {
            let broker_guard = broker.write().await;
            broker_guard
                .get_topography_stream(&Key::from(stream_name))
                .map(|(_, definition)| definition.clone())
                .ok_or(HigginsError::Unknown)?
        };

        tracing::trace!("Initializing produce operation.");
        produce_operation(
            StreamName::from(stream_name),
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
        &self,
        stream: String,
        partition: &PartitionName,
        data: RecordBatch,
    ) -> Result<Reference, HigginsError> {
        let data = write_arrow(&data);

        let request = ProduceRequest {
            request_id: 1,
            topic: stream,
            partition: partition.0.to_vec(),
            data,
        };

        let response = self
            .backing_store
            .as_ref()
            .ok_or(HigginsError::ObjectStoreNotConfigured)?
            .put(request);

        let response = response.recv().await.unwrap();

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

        let data = write_arrow(&data);

        let request = ProduceRequest {
            request_id: 1,
            topic: stream,
            partition: partition.0.to_vec(),
            data,
        };

        let response = self
            .backing_store
            .as_ref()
            .ok_or(HigginsError::ObjectStoreNotConfigured)?
            .put(request);

        let response = response.recv().await.unwrap();

        let reference = Reference::S3(S3Reference {
            object_key: response.object_key,
            position: response.offset,
            size: response.size.into(),
        });

        let mut reference_bytes = [0_u8; Reference::size_of()];

        reference.to_bytes(&mut reference_bytes).unwrap();

        tracing::trace!("Reference: {:#?}", reference_bytes);

        let index = index.put_reference(Reference::S3(S3Reference {
            object_key: response.object_key,
            position: response.offset,
            size: response.size.into(),
        }));

        tracing::trace!(
            "Successfully written to the index: {:#?}",
            Index::of(&index, IndexType::Join)
        );

        Ok(index)
    }
}
