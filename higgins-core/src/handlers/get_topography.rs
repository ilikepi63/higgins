use std::sync::Arc;

use crate::storage::arrow_ipc::read_arrow;
use bytes::BytesMut;
use higgins_codec::{
    Error, GetCurrentTopographyRequest, GetIndexResponse, Message, Record, message::Type,
};
use prost::Message as _;
use tokio::sync::RwLock;

use crate::broker::Broker;
use higgins_shared::PartitionName;
use tokio::sync::mpsc::Sender;

pub async fn handle_get_topography(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) {
    tracing::info!("We're trying to get the lock.");

    let broker_ref = broker.clone();

    let mut broker = broker.write().await;

    tracing::info!("Applying configuration..");

    let mut result = BytesMut::new();

    todo!();

    Message {
        r#type: Type::Getcurrenttopographyresponse as i32,
        get_current_topography_response: Some(create_configuration_response),
        ..Default::default()
    }
    .encode(&mut result)
    .unwrap();

    let _ = writer_tx.send(result).await;
}
