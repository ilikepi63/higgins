use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{
    Error, GetCurrentTopographyResponse, GetSubscriptionRequest, Message, message::Type,
};
use prost::Message as _;
use tokio::sync::RwLock;

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

            // match topography_config {
            //     Ok(topography_config) => {
            //         Message {
            //             r#type: Type::Getcurrenttopographyresponse as i32,
            //             get_current_topography_response: Some(GetCurrentTopographyResponse {
            //                 data: topography_config.into_bytes(),
            //             }),
            //             ..Default::default()
            //         }
            //         .encode(&mut result)
            //         .unwrap();

            //         let _ = writer_tx.send(result).await;
            //     }
            //     Err(err) => {
            //         tracing::error!("Error occurred when trying to get topography: {:#?}", err);
            //         Message {
            //             r#type: Type::Error as i32,
            //             error: Some(Error { r#type: 2 }),
            //             ..Default::default()
            //         }
            //         .encode(&mut result)
            //         .unwrap();

            //         let _ = writer_tx.send(result).await;
            //     }
            // }
        };
    }
}
