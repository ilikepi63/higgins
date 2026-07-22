use std::sync::Arc;

use crate::broker::Broker;
use arrow_schema::DataType;
use bytes::BytesMut;
use higgins_codec::{Message, ProduceRequest, ProduceResponse, message::Type};
use higgins_shared::HigginsError;
use higgins_shared::PartitionName;
use higgins_shared::StreamName;
use higgins_shared::read_arrow;
use prost::Message as _;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;

pub async fn handle_produce(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) -> Result<(), HigginsError> {
    tracing::info!("[PRODUCE] Received produce request. Handling.");

    let ProduceRequest {
        stream_name,
        payload,
        ..
    } = message
        .produce_request
        .ok_or(HigginsError::MissingPayload)?;

    tracing::info!("[PRODUCE] Attempting to take the broker lock..");

    let broker_ref = broker.clone();

    let broker = broker.write().await;

    tracing::info!("[PRODUCE] Took the broker lock..");

    let stream_name = StreamName::from(stream_name);

    let (schema, _tx, _rx) = broker
        .get_stream(&stream_name)
        .ok_or(HigginsError::Arbitrary(
            "Stream not found for stream name.".to_string(),
        ))?;

    tracing::info!("[PRODUCE] Retrieved the schema.");

    let batch = read_arrow(&payload)
        .inspect_err(|err| {
            tracing::error!("[PRODUCE] Failed to read arrow: {:#?}", err);
        })?
        .next()
        .ok_or(HigginsError::Arbitrary(
            "No batch found in payload.".to_string(),
        ))??;

    tracing::info!("[PRODUCE] Successfully read the arrow.");

    let (_, stream_definition) = broker.get_topography_stream(&stream_name.clone())?;

    let key = &stream_definition.key;

    let key_type = schema.field_with_name(&key)?.data_type();

    let array = batch.column(batch.schema().index_of(&key)?);

    tracing::trace!("[PRODUCE] Array: {:#?}", array);

    let key = match key_type {
        DataType::Utf8 => arrow::array::as_string_array(array)
            .value(0)
            .as_bytes()
            .to_owned(),
        _ => unimplemented!(),
    };

    tracing::trace!("[PRODUCE] Key: {:#?}", key);

    tracing::trace!("[PRODUCE] Read the batch, producing..");

    drop(broker);

    let result = Broker::produce(
        &stream_name,
        &PartitionName::try_from(key.as_slice())?,
        batch,
        broker_ref,
    )
    .await;

    tracing::trace!("Result from producing to {}: {:#?}", stream_name, result);

    let mut result = BytesMut::new();

    let resp = ProduceResponse::default();

    Message {
        correlation_id: message.correlation_id,
        r#type: Type::Produceresponse as i32,
        produce_response: Some(resp),
        ..Default::default()
    }
    .encode(&mut result)?;

    writer_tx
        .send(result)
        .await
        .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

    Ok(())
}
