use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{Message, UploadModuleRequest, UploadModuleResponse, message::Type};
use prost::Message as _;
use tokio::sync::RwLock;

use crate::broker::Broker;
use tokio::sync::mpsc::Sender;

pub async fn handle_upload_module(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) {
    tracing::trace!("Received Upload Module Request.");

    let UploadModuleRequest { name, value } = message
        .upload_module_request
        .expect("Marked Upload Module Request without a body.");

    let broker_lock = broker.write().await;

    broker_lock.functions.put_function(&name, value).await;

    let mut result = BytesMut::new();

    let response = UploadModuleResponse::default();

    Message {
        r#type: Type::Uploadmoduleresponse as i32,
        upload_module_response: Some(response),
        ..Default::default()
    }
    .encode(&mut result)
    .unwrap();

    writer_tx.send(result).await.unwrap();
}
