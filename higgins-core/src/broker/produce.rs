use super::Broker;

use arrow::{array::RecordBatch, datatypes::Schema};
use bytes::BytesMut;
use higgins_codec::{Message, Record, TakeRecordsResponse, message::Type};
use prost::Message as _;
use riskless::{
    batch_coordinator::FindBatchResponse,
    messages::{
        ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceRequestCollection, ProduceResponse,
    },
    object_store::{self, ObjectStore},
};
use std::{
    collections::BTreeMap,
    fs::create_dir,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

use crate::{
    broker::object_store::path::Path,
    derive::{
        joining::create_joined_stream_from_definition, map::create_mapped_stream_from_definition,
        reduce::create_reduced_stream_from_definition,
    },
    functions::collection::FunctionCollection,
    topography::FunctionType,
};
use crate::{
    client::ClientCollection,
    error::HigginsError,
    storage::{
        arrow_ipc::{read_arrow, write_arrow},
        indexes::IndexDirectory,
    },
    subscription::Subscription,
    topography::{Topography, apply_configuration_to_topography, config::from_toml},
    utils::request_response::Request,
};
use riskless::messages::ConsumeBatch;
use std::collections::HashSet;

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

type MutableCollection = Arc<
    RwLock<(
        ProduceRequestCollection,
        Vec<Request<ProduceRequest, ProduceResponse>>,
    )>,
>;

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
            let _ = self.flush_tx.send(()).await;
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
}
