use super::Broker;
use crate::storage::index::IndexType;
use arrow::array::RecordBatch;
use higgins_shared::PartitionName;
use riskless::messages::ProduceRequest;

use crate::{
    error::HigginsError,
    storage::{
        arrow_ipc::write_arrow,
        dereference::{Reference, S3Reference},
        index::Index,
    },
};

impl Broker {
    /// Produce a data set onto the named stream.
    pub async fn produce(
        &mut self,
        stream_name: &[u8],
        partition: &PartitionName,
        record_batch: RecordBatch,
    ) -> Result<(), HigginsError> {
        tracing::trace!(
            "[PRODUCE] Producing to stream: {}",
            String::from_utf8(stream_name.to_vec()).unwrap()
        );

        let data = write_arrow(&record_batch);

        let request = ProduceRequest {
            request_id: 1,
            topic: String::from_utf8(stream_name.to_vec()).unwrap(),
            partition: partition.0.to_vec(),
            data,
        };

        let response = self
            .backing_store
            .as_ref()
            .ok_or(HigginsError::ObjectStoreNotConfigured)?
            .put(request);

        // Await the response from flushing.
        let response = response.recv().await.unwrap();

        // Create a new reference given the data.
        let reference = Reference::S3(S3Reference {
            object_key: response.object_key,
            position: response.offset,
            size: response.size.into(),
        });

        let (index_type, stream_def) = {
            let (_, stream_def) = self
                .get_topography_stream(&crate::topography::Key::try_from(stream_name).unwrap())
                .unwrap();

            (
                IndexType::try_from(stream_def).unwrap(),
                stream_def.to_owned(),
            )
        };

        let offset = self
            .indexes
            .put_default_index(
                String::from_utf8(stream_name.to_owned()).unwrap(),
                partition,
                reference,
                response,
                &index_type,
                &stream_def,
            )
            .await;

        tracing::trace!("Offset: {:#?}", offset);

        // Watermark the subscription.
        let subscription = self.subscriptions.get(stream_name);

        if let Some(subscriptions) = subscription {
            tracing::trace!("[PRODUCE] Found a subscription for this produce request.");

            for (_, (notify, subscription)) in subscriptions {
                let mut subscription = subscription.write().await;

                tracing::trace!(
                    "[PRODUCE] Notifying the subscription. Subscription end: {}",
                    offset + 1
                );

                // Set the max offset of the subscription.
                subscription.set_end(partition, offset + 1)?;

                tracing::info!("{:#?}", subscription);

                // Notify the tasks awaiting this subscription.
                notify.notify_waiters();
                tracing::trace!("[PRODUCE] Notified the subscription.");
            }
        }

        Ok(())
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
