use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{Error, GetCurrentTopographyResponse, Message, message::Type};
use prost::Message as _;
use tokio::sync::RwLock;

use crate::broker::Broker;
use tokio::sync::mpsc::Sender;

pub async fn handle_get_topography(broker: Arc<RwLock<Broker>>, writer_tx: Sender<BytesMut>) {
    tracing::info!("We're trying to get the lock.");

    let broker = broker.read().await;

    tracing::info!("Applying configuration..");

    let mut result = BytesMut::new();

    let topography_config = broker.get_topography_as_config_string();

    match topography_config {
        Ok(topography_config) => {
            Message {
                r#type: Type::Getcurrenttopographyresponse as i32,
                get_current_topography_response: Some(GetCurrentTopographyResponse {
                    data: topography_config.into_bytes(),
                }),
                ..Default::default()
            }
            .encode(&mut result)
            .unwrap();

            let _ = writer_tx.send(result).await;
        }
        Err(err) => {
            tracing::error!("Error occurred when trying to get topography: {:#?}", err);
            Message {
                r#type: Type::Error as i32,
                error: Some(Error { r#type: 2 }),
                ..Default::default()
            }
            .encode(&mut result)
            .unwrap();

            let _ = writer_tx.send(result).await;
        }
    }
}
