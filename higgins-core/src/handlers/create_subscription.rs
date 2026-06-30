use std::{sync::Arc, time::Duration};

use bytes::BytesMut;
use higgins_codec::{
    CreateSubscriptionRequest, CreateSubscriptionResponse, Message, message::Type,
};
use higgins_shared::{HigginsError, StreamName};
use prost::Message as _;
use tokio::sync::RwLock;

use crate::broker::Broker;
use tokio::sync::mpsc::Sender;

/// The default amount of time a subscription's shadow acknowledgement takes.
pub const DEFAULT_SUBSCRIPTION_TIMEOUT: u64 = 500;

pub async fn handle_create_subscription(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) -> Result<(), HigginsError> {
    tracing::trace!(
        "Received CreateSubscriptionRequest: {:#?}",
        message.create_subscription_request
    );

    let CreateSubscriptionRequest {
        stream_name,
        timeout_ms,
        ..
    } = message
        .create_subscription_request
        .ok_or(HigginsError::MissingPayload)?;

    let stream_name = StreamName::from(stream_name);

    let mut broker = broker.write().await;

    let subscription_id = broker.create_subscription(
        &stream_name,
        Duration::from_millis(timeout_ms.unwrap_or(DEFAULT_SUBSCRIPTION_TIMEOUT)),
    )?;

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
    .encode(&mut result)?;

    writer_tx
        .send(result)
        .await
        .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

    Ok(())
}
