use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{
    CreateSubscriptionRequest, CreateSubscriptionResponse, Message, message::Type,
};
use prost::Message as _;
use tokio::sync::RwLock;

use crate::broker::Broker;
use tokio::sync::mpsc::Sender;

pub async fn handle_create_subscription(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) {
    tracing::trace!(
        "Received CreateSubscriptionRequest: {:#?}",
        message.create_subscription_request
    );

    let CreateSubscriptionRequest { stream_name, .. } =
        message.create_subscription_request.unwrap();

    let mut broker = broker.write().await;

    let subscription_id = broker.create_subscription(&stream_name);

    let resp = CreateSubscriptionResponse {
        errors: vec![],
        subscription_id: Some(subscription_id),
    };

    let mut result = BytesMut::new();

    Message {
        correlation_id: message.correlation_id,
        r#type: Type::Createsubscriptionresponse as i32,
        create_subscription_response: Some(resp),
        ..Default::default()
    }
    .encode(&mut result)
    .unwrap();

    writer_tx.send(result).await.unwrap();
}
