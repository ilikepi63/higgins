use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{
    AcknowledgeSubscriptionOffsetsRequest, AcknowledgeSubscriptionOffsetsResponse, ClientCount,
    GetSubscriptionRequest, GetSubscriptionResponse, KeyOffset, Message, Offset, message::Type,
};
use higgins_shared::{PartitionName, StreamName};
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
        && let Some((_, subscription_data)) =
            broker.get_subscription_by_key(&StreamName::from(stream.clone()), &subscription_id)
    {
        let subscription_data = subscription_data.read().await;

        let mut result = BytesMut::new();

        Message {
            correlation_id: message.correlation_id,
            r#type: Type::Getsubscriptionresponse as i32,
            get_subscription_response: Some(GetSubscriptionResponse {
                errors: vec![],
                stream: Some(stream),
                subscription_id: Some(subscription_id),
                offsets: subscription_data
                    .partitions
                    .iter()
                    .map(|key| KeyOffset {
                        key: key.partition_id.to_vec(),
                        max_offset: key.end,
                        last_completed_offset: key.start,
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
        .encode(&mut result)?;

        writer_tx.send(result).await?;
    };
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
        let offsets = offsets
            .iter()
            .map(|Offset { key, range }| {
                (
                    PartitionName::try_from(key.as_bytes())?,
                    std::ops::Range {
                        start: range?.start,
                        end: range?.end,
                    },
                )
            })
            .collect::<Vec<_>>();

        let stream = StreamName::from(stream);

        let (error, failed_offsets) = match broker
            .acknowledge(stream.clone(), subscription_id.clone(), offsets)
            .await
        {
            Ok(v) => v,
            Err(err) => (err.to_string(), vec![]),
        };

        let mut result = BytesMut::new();

        Message {
            correlation_id: message.correlation_id,
            r#type: Type::Acknowledgeresponse as i32,
            acknowledge_response: Some(AcknowledgeSubscriptionOffsetsResponse {
                stream: stream.into(),
                subscription_id,
                failed_offsets: failed_offsets
                    .iter()
                    .map(|offset| Offset {
                        key: offset.0.to_vec(),
                        range: Some(higgins_codec::Range {
                            start: offset.1.start,
                            end: offset.1.end,
                        }),
                    })
                    .collect(),
                error,
            }),
            ..Default::default()
        }
        .encode(&mut result)?;

        writer_tx.send(result).await?;
    }
}
