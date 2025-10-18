use super::Broker;

use riskless::{
    batch_coordinator::FindBatchResponse,
    messages::{ConsumeRequest, ConsumeResponse},
    object_store::ObjectStore,
};
use std::sync::Arc;

use crate::error::HigginsError;
use crate::{broker::object_store::path::Path, storage::index::IndexType};
use riskless::messages::ConsumeBatch;
use std::collections::HashSet;

impl Broker {
    pub async fn consume(
        &self,
        topic: &[u8],
        partition: &[u8],
        offset: u64,
        max_partition_fetch_bytes: u32,
    ) -> tokio::sync::mpsc::Receiver<ConsumeResponse> {
        let object_store = self.object_store.clone();
        let indexes = self.indexes.clone();

        riskless::consume(
            ConsumeRequest {
                topic: String::from_utf8_lossy(topic).to_string(),
                partition: partition.to_vec(),
                offset,
                max_partition_fetch_bytes,
            },
            object_store,
            indexes,
        )
        .await
        .unwrap()
    }
    pub async fn get_by_timestamp(
        &self,
        stream: &[u8],
        partition: &[u8],
        timestamp: u64,
    ) -> Option<ConsumeResponse> {
        let stream_def = self
            .topography
            .get_stream_definition_by_key(String::from_utf8(stream.to_owned()).unwrap())
            .unwrap();

        let find_batch_responses = self
            .indexes
            .get_by_timestamp(
                stream,
                partition,
                timestamp,
                IndexType::try_from(stream_def).unwrap(),
            )
            .await;

        self.dereference_find_batch_response(find_batch_responses)
            .await
            .unwrap()
            .recv()
            .await
    }

    pub async fn get_latest(
        &self,
        stream: &[u8],
        partition: &[u8],
    ) -> Result<tokio::sync::mpsc::Receiver<ConsumeResponse>, HigginsError> {
        let stream_def = self
            .topography
            .get_stream_definition_by_key(String::from_utf8(stream.to_owned()).unwrap())
            .unwrap();

        let find_batch_responses = self
            .indexes
            .get_latest_offset(stream, partition, IndexType::try_from(stream_def).unwrap())
            .await;

        self.dereference_find_batch_response(find_batch_responses)
            .await
    }

    pub async fn dereference_find_batch_response(
        &self,
        batch_responses: Vec<FindBatchResponse>,
    ) -> Result<tokio::sync::mpsc::Receiver<ConsumeResponse>, HigginsError> {
        let object_storage = self.object_store.clone();

        let objects_to_retrieve = batch_responses
            .iter()
            .flat_map(|resp| resp.batches.clone())
            .map(|batch_info| batch_info.object_key)
            .collect::<HashSet<_>>();

        // We create a
        let (batch_response_tx, batch_reponse_rx) =
            tokio::sync::mpsc::channel(if !objects_to_retrieve.is_empty() {
                objects_to_retrieve.len()
            } else {
                1
            });

        let batch_responses = Arc::new(batch_responses);

        for object_name in objects_to_retrieve {
            let batch_response_tx = batch_response_tx.clone();
            let object_name = object_name.clone();
            let object_store = object_storage.clone();
            let batch_responses = batch_responses.clone();

            tokio::spawn(async move {
                let get_object_result = object_store.get(&Path::from(object_name.as_str())).await;

                let result = match get_object_result {
                    Ok(get_result) => {
                        if let Ok(b) = get_result.bytes().await {
                            // Retrieve the current fetch Responses by name.

                            batch_responses
                                .iter()
                                .flat_map(|res| {
                                    res.batches
                                        .iter()
                                        .filter(|batch| batch.object_key == *object_name)
                                        .map(|batch| (res.clone(), batch))
                                })
                                .inspect(|val| {
                                    tracing::trace!("Result returned for query: {:#?}", val);
                                })
                                .filter_map(|(res, batch)| {
                                    ConsumeBatch::try_from((res, batch, &b)).ok()
                                })
                                .collect::<Vec<_>>()
                        } else {
                            tracing::trace!(
                                "Could not retrieve bytes for given GetObject query: {}",
                                object_name
                            );
                            vec![]
                        }
                    }
                    Err(err) => {
                        tracing::error!(
                            "An error occurred trying to retrieve the object with key {}. Error: {:#?}",
                            object_name,
                            err
                        );
                        vec![]
                    }
                };

                if !result.is_empty() {
                    if let Err(e) = batch_response_tx
                        .send(ConsumeResponse { batches: result })
                        .await
                    {
                        tracing::error!("Failed to send consume response: {:#?}", e);
                    };
                } else {
                    tracing::trace!("No ConsumeBatches found for query.");
                };
            });
        }

        Ok(batch_reponse_rx)
    }
}
