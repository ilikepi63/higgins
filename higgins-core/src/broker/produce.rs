use super::Broker;
use crate::storage::index::IndexType;
use crate::storage::index::default::DefaultIndex;
use crate::utils::epoch;
use arrow::array::RecordBatch;
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
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ProduceOperation {
    /// Broker  Reference.
    broker: Arc<RwLock<Broker>>,
    /// Stream that this value is being produced to.
    stream: String,
    /// The partition we've received offsets on.
    partition: PartitionName,
    /// The underlying records that this operation is based on.
    /// Vec<(
    ///   Vec<u8> - IPC record batch.
    ///   u64 - The offset to which it belongs.
    /// )>
    records: Vec<RecordBatch>,
    /// The References that have previously been created.
    references: Option<Vec<Reference>>,
}

impl ProduceOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        tracing::debug!("Running init on produce.");
        let broker = self.broker.write().await;
        tracing::debug!("Retrieved broker lock.");

        let mut references = vec![];

        for record in &self.records {
            references.push(
                broker
                    .put_data_store(self.stream.clone(), &self.partition.clone(), record.clone())
                    .await
                    .inspect_err(|err| tracing::error!("{:#?}", err))?,
            );
        }

        tracing::debug!("Returning references.");

        self.references = Some(references);

        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        let mut broker = self.broker.write().await;
        tracing::debug!("Running commit.");

        let mut index_file_lock = broker
            .get_index_file(self.stream.clone(), &self.partition.clone())
            .unwrap();

        let mut index_file_guard = index_file_lock.lock().await;

        let file_len = index_file_guard.len().unwrap();

        if let Some(references) = self.references.as_ref() {
            let offset = file_len..file_len;

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

            index_file_guard.range_put_at(offset.start..offset.end.saturating_add(1), &mut buf)?;

            let subscription = broker.get_subscriptions_for_stream(&self.stream);

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
                        .find(|sub_key| sub_key.partition_id == self.partition)
                        .is_some()
                    {
                        subscription.set_end(&self.partition, offset.end as u64)?;
                    } else {
                        subscription.add_partition(&self.partition, 0, offset.end as u64)?;
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

        let mut operation = ProduceOperation {
            stream: String::from_utf8_lossy(stream_name).to_string(),
            partition: partition.clone(),
            broker,
            references: None,
            records: vec![record_batch],
        };

        operation.init().await?;
        operation.prepare().await?;
        operation.commit().await?;

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
