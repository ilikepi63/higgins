use super::Broker;
use crate::storage::{batch_coordinate::BatchCoordinate, index::IndexType};
use arrow::array::RecordBatch;
use riskless::messages::ProduceRequest;

use crate::{
    error::HigginsError,
    storage::{
        arrow_ipc::write_arrow,
        dereference::{Reference, S3Reference},
        index::Index,
    },
    utils::request_response::Request,
};

impl Broker {
    /// Produce a data set onto the named stream.
    pub async fn produce(
        &mut self,
        stream_name: &[u8],
        partition: &[u8],
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
            partition: partition.to_vec(),
            data,
        };

        let (request, response) = Request::<ProduceRequest, BatchCoordinate>::new(request);

        // Add the request to the buffer for flushing.
        {
            let mut buffer_lock = self.collection.write().await;

            let _ = buffer_lock.0.collect(request.inner().clone());

            buffer_lock.1.push(request);

            // TODO: This is currently hardcoded to 50kb, but we possibly want to make
            if buffer_lock.0.size() > 50_000 {
                let _ = self.flush_tx.as_ref().unwrap().send(()).await;
            }

            drop(buffer_lock);
        }

        // Await the response from flushing.
        let response = response.recv().await.unwrap();

        tracing::trace!("Retrieved the response for flushing: {:#?}", response);

        // Create a new reference given the data.
        let reference = Reference::S3(S3Reference {
            object_key: response.object_key,
            position: response.offset,
            size: response.size.into(),
        });

        let (index_type, stream_def) = {
            let (_, stream_def) = self
                .get_topography_stream(&crate::topography::Key(stream_name.to_owned()))
                .unwrap();

            (
                IndexType::try_from(stream_def).unwrap(),
                stream_def.to_owned(),
            )
        };

        let offset = response.offset.clone();

        self.indexes
            .put_default_index(
                String::from_utf8(stream_name.to_owned()).unwrap(),
                partition,
                reference,
                response,
                &index_type,
                &stream_def,
            )
            .await;

        // Watermark the subscription.
        let subscription = self.subscriptions.get(stream_name);

        if let Some(subscriptions) = subscription {
            tracing::trace!("[PRODUCE] Found a subscription for this produce request.");

            for (subscription_id, (notify, subscription)) in subscriptions {
                let subscription = subscription.write().await;

                tracing::trace!("[PRODUCE] Notifying the subscrition.");

                // Set the max offset of the subscription.
                subscription.set_max_offset(partition, offset)?;

                // Notify the tasks awaiting this subscription.
                notify.notify_waiters();
                tracing::trace!("[PRODUCE] Notified the subscrition.");

                tracing::info!(
                    "Updated Max offset for subscription: {}, watermark: {}",
                    subscription_id
                        .iter()
                        .map(u8::to_string)
                        .collect::<String>(),
                    offset
                );
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
        partition: &[u8],
        index: &mut Index<'a>,
        data: RecordBatch,
    ) -> Result<Vec<u8>, HigginsError> {
        match index.get_reference() {
            Reference::S3(_) => {
                tracing::trace!("[PRODUCE] Producing to stream: {}", stream);

                let data = write_arrow(&data);

                let request = ProduceRequest {
                    request_id: 1,
                    topic: stream,
                    partition: partition.to_vec(),
                    data,
                };

                let (request, response) = Request::<ProduceRequest, BatchCoordinate>::new(request);

                let mut buffer_lock = self.collection.write().await;

                let _ = buffer_lock.0.collect(request.inner().clone());

                buffer_lock.1.push(request);

                // TODO: This is currently hardcoded to 50kb, but we possibly want to make
                if buffer_lock.0.size() > 50_000 {
                    let _ = self.flush_tx.as_ref().unwrap().send(()).await;
                }

                drop(buffer_lock);

                let response = response.recv().await.unwrap();

                let index = index.put_reference(Reference::S3(S3Reference {
                    object_key: response.object_key,
                    position: response.offset,
                    size: response.size.into(),
                }));

                Ok(index)
            }
            Reference::Null => Err(HigginsError::UnableToPlaceDataAtNullReference),
        }
    }
}
