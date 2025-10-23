use super::Broker;

use arrow::array::RecordBatch;
use riskless::messages::{ProduceRequest, ProduceResponse};
use uuid::Uuid;

use crate::{
    error::HigginsError,
    storage::{arrow_ipc::write_arrow, dereference::Reference, index::Index},
    utils::request_response::Request,
};

impl Broker {
    /// Produce a data set onto the named stream.
    pub async fn produce(
        &mut self,
        stream_name: &[u8],
        partition: &[u8],
        record_batch: RecordBatch,
    ) -> Result<ProduceResponse, HigginsError> {
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

        let (request, response) = Request::<ProduceRequest, ProduceResponse>::new(request);

        let mut buffer_lock = self.collection.write().await;

        let _ = buffer_lock.0.collect(request.inner().clone());

        buffer_lock.1.push(request);

        // TODO: This is currently hardcoded to 50kb, but we possibly want to make
        if buffer_lock.0.size() > 50_000 {
            let _ = self.flush_tx.as_ref().unwrap().send(()).await;
        }

        drop(buffer_lock);

        let response = response.recv().await.unwrap();

        // TODO: fix this to actually return the error?
        if !response.errors.is_empty() {
            tracing::error!(
                "Error when attempting to write out to producer. Request: {}",
                response.request_id
            );
        } else {
            // Watermark the subscription.
            let subscription = self.subscriptions.get(stream_name);

            if let Some(subscriptions) = subscription {
                tracing::trace!("[PRODUCE] Found a subscription for this produce request.");

                for (subscription_id, (notify, subscription)) in subscriptions {
                    let subscription = subscription.write().await;

                    tracing::trace!("[PRODUCE] Notifying the subscrition.");

                    // Set the max offset of the subscription.
                    subscription.set_max_offset(partition, response.batch.offset)?;

                    // Notify the tasks awaiting this subscription.
                    notify.notify_waiters();
                    tracing::trace!("[PRODUCE] Notified the subscrition.");

                    tracing::info!(
                        "Updated Max offset for subscription: {}, watermark: {}",
                        subscription_id
                            .iter()
                            .map(u8::to_string)
                            .collect::<String>(),
                        response.batch.offset
                    );
                }
            }
        }

        Ok(response)
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
    ) -> Result<&mut Index<'a>, HigginsError> {
        match index.get_reference() {
            Reference::S3(s3_reference) => {
                tracing::trace!("[PRODUCE] Producing to stream: {}", stream);

                let data = write_arrow(&data);

                let request = ProduceRequest {
                    request_id: 1,
                    topic: stream,
                    partition: partition.to_vec(),
                    data,
                };

                let (request, response) = Request::<ProduceRequest, ProduceResponse>::new(request);

                let mut buffer_lock = self.collection.write().await;

                let _ = buffer_lock.0.collect(request.inner().clone());

                buffer_lock.1.push(request);

                // TODO: This is currently hardcoded to 50kb, but we possibly want to make
                if buffer_lock.0.size() > 50_000 {
                    let _ = self.flush_tx.as_ref().unwrap().send(()).await;
                }

                drop(buffer_lock);

                let response = response.recv().await.unwrap();

                // TODO: fix this to actually return the error?
                if !response.errors.is_empty() {
                    tracing::error!(
                        "An Error occurred trying to put data into object storage: {:#?}",
                        response.errors
                    );
                    Err(HigginsError::S3PutDataFailure)
                } else {
                    Ok(())
                }
            }
            Null => Err(HigginsError::UnableToPlaceDataAtNullReference),
        }
    }
}
