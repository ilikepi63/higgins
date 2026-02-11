use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{
    ClientCount, GetSubscriptionRequest, GetSubscriptionResponse, KeyOffset, Message, message::Type,
};
use prost::Message as _;
use tokio::sync::RwLock;
use zerocopy::IntoBytes;

use crate::broker::Broker;
use tokio::sync::mpsc::Sender;

pub async fn handle_get_subscription(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) {
    let broker = broker.read().await;

    if let Some(GetSubscriptionRequest {
        subscription_id,
        stream,
    }) = message.get_subscription_request
    {
        if let Some((_, subscription_data)) =
            broker.get_subscription_by_key(stream.as_bytes(), &subscription_id)
        {
            let subscription_data = subscription_data.read().await;

            let mut result = BytesMut::new();

            Message {
                r#type: Type::Getsubscriptionresponse as i32,
                get_subscription_response: Some(GetSubscriptionResponse {
                    errors: vec![],
                    stream: Some(stream),
                    subscription_id: Some(subscription_id),
                    offsets: subscription_data
                        .partitions
                        .iter()
                        .map(|key| KeyOffset {
                            key: key.partition_id.0.as_bytes().to_owned(),
                            max_offset: key.max_offset,
                            last_completed_offset: key.last_completed_offset,
                            amount_to_take: key.amount_to_take,
                        })
                        .collect(),
                    client_counts: subscription_data
                        .client_counts
                        .iter()
                        .map(|client_count| ClientCount {
                            client_id: client_count.0,
                            count: client_count.1.load(std::sync::atomic::Ordering::Relaxed),
                        })
                        .collect(),
                }),
                ..Default::default()
            }
            .encode(&mut result)
            .unwrap();

            writer_tx.send(result).await.unwrap();
        };
    }
}
