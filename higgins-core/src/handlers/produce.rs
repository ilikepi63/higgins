use std::{io::Cursor, sync::Arc};

use crate::broker::Broker;
use crate::topography::Key;
use arrow_json::ReaderBuilder;
use arrow_schema::DataType;
use bytes::BytesMut;
use higgins_codec::{Message, ProduceRequest, ProduceResponse, message::Type};
use higgins_shared::PartitionName;
use prost::Message as _;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;
use zerocopy::IntoBytes;

pub async fn handle_produce(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) {
    tracing::info!("[PRODUCE] Received produce request. Handling.");

    let ProduceRequest {
        stream_name,
        payload,
    } = message.produce_request.unwrap();

    tracing::info!("[PRODUCE] Attempting to take the broker lock..");

    let mut broker = broker.write().await;

    let (schema, _tx, _rx) = broker
        .get_stream(&stream_name)
        .expect("Could not find stream for stream_name.");

    let cursor = Cursor::new(payload);
    let mut reader = ReaderBuilder::new(schema.clone()).build(cursor).unwrap();
    let batch = reader.next().unwrap().unwrap();

    tracing::info!("[PRODUCE] Retrieved the broker lock.");

    let (_, stream_definition) = broker
        .get_topography_stream(&Key::try_from(stream_name.as_bytes()).unwrap())
        .unwrap();

    let key = &stream_definition.partition_key;

    tracing::trace!("[PRODUCE] Key for stream produce: {:#?}", key);

    let key = String::from_utf8(key.as_bytes().to_vec()).unwrap();

    let key_type = schema.field_with_name(&key).unwrap().data_type();

    let array = batch.column(
        batch
            .schema()
            .index_of(&String::from_utf8(key.as_bytes().to_vec()).unwrap())
            .unwrap(),
    );

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

    if let Err(err) = broker
        .create_partition(
            &stream_name,
            &PartitionName::try_from(key.as_bytes()).unwrap(),
        )
        .await
    {
        tracing::error!("Failed to create partition inside of broker: {:#?}", err);
    };

    tracing::trace!("[PRODUCE] Read the batch, producing..");

    let result = broker
        .produce(
            &stream_name,
            &PartitionName::try_from(key.as_bytes()).unwrap(),
            batch,
        )
        .await;

    tracing::trace!(
        "Result from producing to {}: {:#?}",
        String::from_utf8(stream_name.to_vec()).unwrap(),
        result
    );

    drop(broker);

    let mut result = BytesMut::new();

    let resp = ProduceResponse::default();

    Message {
        r#type: Type::Produceresponse as i32,
        produce_response: Some(resp),
        ..Default::default()
    }
    .encode(&mut result)
    .unwrap();

    writer_tx.send(result).await.unwrap();
}
