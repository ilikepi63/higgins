use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{Message, UploadModuleRequest, UploadModuleResponse, message::Type};
use higgins_functions::wasmtime::Module;
use higgins_shared::HigginsError;
use prost::Message as _;
use tokio::sync::RwLock;

use crate::broker::Broker;
use tokio::sync::mpsc::Sender;

pub async fn handle_upload_module(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) -> Result<(), HigginsError> {
    tracing::trace!("Received Upload Module Request.");

    let UploadModuleRequest { name, value } = message
        .upload_module_request
        .expect("Marked Upload Module Request without a body.");

    let mut broker_lock = broker.write().await;

    let module = Module::new(&broker_lock.wasm_engine, value.clone())
        .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

    broker_lock.wasm_modules.push((name.to_owned(), module));
    broker_lock.functions.put_function(&name, value).await?;

    let mut result = BytesMut::new();

    let response = UploadModuleResponse::default();

    Message {
        correlation_id: message.correlation_id,
        r#type: Type::Uploadmoduleresponse as i32,
        upload_module_response: Some(response),
        ..Default::default()
    }
    .encode(&mut result)?;

    writer_tx
        .send(result)
        .await
        .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

    Ok(())
}
