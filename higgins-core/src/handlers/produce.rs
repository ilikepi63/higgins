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
use zerocopy::IntoBytes;

pub async fn handle_produce(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) -> Result<(), HigginsError> {
    tracing::info!("[PRODUCE] Received produce request. Handling.");

    let ProduceRequest {
        stream_name,
        payload,
    } = message
        .produce_request
        .ok_or(HigginsError::MissingPayload)?;

    tracing::info!("[PRODUCE] Attempting to take the broker lock..");

    let broker_ref = broker.clone();

    let broker = broker.write().await;

    let stream_name = StreamName::from(stream_name);

    let (schema, _tx, _rx) = broker
        .get_stream(&stream_name)
        .ok_or(HigginsError::Arbitrary(
            "Stream not found for stream name.".to_string(),
        ))?;

    let batch = read_arrow(&payload)?
        .next()
        .ok_or(HigginsError::Arbitrary(
            "No batch found in payload.".to_string(),
        ))??;

    tracing::info!("[PRODUCE] Retrieved the broker lock.");

    let (_, stream_definition) = broker.get_topography_stream(&stream_name.clone())?;

    let key = &stream_definition.partition_key;

    tracing::trace!("[PRODUCE] Key for stream produce: {:#?}", key);

    #[allow(clippy::unwrap_used)]
    let key = key.to_string().unwrap();

    tracing::trace!("[PRODUCE] Key for stream produce: {}", key);

    let key_type = schema.field_with_name(&key)?.data_type();

    let array = batch.column(batch.schema().index_of(&key)?);

    tracing::trace!("[PRODUCE] Array: {:#?}", array);

    let key = match key_type {
        // DataType::Int8 => {
        //     let arr = as_primitive_array::<i8>(array); /* ... */
        // }
        // DataType::Int16 => {
        //     let arr = as_primitive_array::<i16>(array); /* ... */
        // }
        // DataType::Int32 => {
        //     let arr = as_primitive_array::<i32>(array); /* ... */
        // }
        // DataType::Int64 => {
        //     let arr = as_primitive_array::<i64>(array); /* ... */
        // }
        // DataType::UInt32 => {
        //     let arr = as_primitive_array::<u32>(array); /* ... */
        // }
        // DataType::Float32 => {
        //     let arr = as_primitive_array::<f32>(array); /* ... */
        // }
        // DataType::Float64 => {
        //     let arr = as_primitive_array::<f64>(array); /* ... */
        // }
        DataType::Utf8 => arrow::array::as_string_array(array)
            .value(0)
            .as_bytes()
            .to_owned(),
        DataType::Boolean => {
            let arr = arrow::array::as_boolean_array(array);
            let value = arr.value(0);
            value.as_bytes().to_owned()
        }
        _ => unimplemented!(),
    };

    tracing::trace!("[PRODUCE] Key: {:#?}", key);

    tracing::trace!("[PRODUCE] Read the batch, producing..");

    drop(broker);

    let result = Broker::produce(
        &stream_name,
        &PartitionName::try_from(key.as_bytes())?,
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
