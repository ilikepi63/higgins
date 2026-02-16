use std::{ops::Range, sync::Arc};

use bytes::BytesMut;
use higgins_codec::{
    AcknowledgeSubscriptionOffsetsRequest, AcknowledgeSubscriptionOffsetsResponse, ClientCount,
    GetSubscriptionRequest, GetSubscriptionResponse, KeyOffset, Message, Offset, message::Type,
};
use higgins_shared::PartitionName;
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

pub async fn handle_acknowledge(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) {
    let broker = broker.read().await;

    if let Some(AcknowledgeSubscriptionOffsetsRequest {
        stream,
        subscription_id,
        offsets,
    }) = message.acknowledge_request
    {
        let mut failed_offsets = vec![];

        if let Some((_, subscription)) =
            broker.get_subscription_by_key(stream.as_bytes(), &subscription_id)
        {
            let mut subscription = subscription.write().await;

            for Offset { key, range } in offsets.iter() {
                let unwrapped_range: Range<u64> = range
                    .map(|range| Range {
                        start: range.start,
                        end: range.end,
                    })
                    .unwrap();

                match subscription.acknowledge(
                    &PartitionName::try_from(key.as_bytes()).unwrap(),
                    &unwrapped_range,
                ) {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::error!("Failed to acknowledge partitions: {:#?}", err);
                        failed_offsets.push(Offset {
                            key: key.to_owned(),
                            range: range.clone(),
                        })
                    }
                };
            }
        };

        let mut result = BytesMut::new();

        Message {
            r#type: Type::Acknowledgeresponse as i32,
            acknowledge_response: Some(AcknowledgeSubscriptionOffsetsResponse {
                stream,
                subscription_id,
                failed_offsets,
                error: String::new(),
            }),
            ..Default::default()
        }
        .encode(&mut result)
        .unwrap();

        writer_tx.send(result).await.unwrap();
    }
}
