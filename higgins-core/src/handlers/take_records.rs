use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{Message, TakeRecordsRequest};
use higgins_shared::StreamName;
use tokio::sync::RwLock;

use crate::broker::Broker;
use tokio::sync::mpsc::Sender;

pub async fn handle_take_records(
    broker: Arc<RwLock<Broker>>,
    message: Message,
    client_id: u64,
    writer_tx: Sender<BytesMut>,
) {
    tracing::trace!("[TAKE] Received a TakeRecordsRequest!");
    let broker_ref = broker.clone();

    let TakeRecordsRequest {
        n,
        stream_name,
        subscription_id,
    } = message.take_records_request.unwrap();

    // TODO: Wrap this behind test cfg flag.
    tracing::info!(
        "Sub ID: {:#?}",
        uuid::Uuid::from_slice(&subscription_id).unwrap()
    );

    let mut broker = broker.write().await;
    let stream_name = StreamName::from(stream_name);

    broker
        .take_from_subscription(
            client_id,
            &stream_name,
            &subscription_id,
            writer_tx.clone(),
            broker_ref,
            n,
        )
        .await
        .unwrap();
}
