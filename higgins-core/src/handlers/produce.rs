use std::{io::Cursor, sync::Arc};

use crate::broker::Broker;
use arrow_json::ReaderBuilder;
use bytes::BytesMut;
use higgins_codec::{Message, ProduceRequest, ProduceResponse, message::Type};
use higgins_shared::PartitionName;
use prost::Message as _;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;

pub async fn handle_produce(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) {
    tracing::info!("[PRODUCE] Received produce request. Handling.");

    let ProduceRequest {
        stream_name,
        partition_key,
        payload,
    } = message.produce_request.unwrap();

    tracing::info!("[PRODUCE] Attempting to take the broker lock..");

    let mut broker = broker.write().await;

    tracing::info!("[PRODUCE] Retrieved the broker lock.");

    if let Err(err) = broker.create_partition(&stream_name, &partition_key).await {
        tracing::error!("Failed to create partition inside of broker: {:#?}", err);
    };

    tracing::trace!("[PRODUCE] Successfully created the partition.");

    tracing::trace!("[PRODUCE] Streams: {:#?}", broker);

    let (schema, _tx, _rx) = broker
        .get_stream(&stream_name)
        .expect("Could not find stream for stream_name.");

    tracing::trace!("[PRODUCE] Retrieved the stream.");

    let cursor = Cursor::new(payload);
    let mut reader = ReaderBuilder::new(schema.clone()).build(cursor).unwrap();
    let batch = reader.next().unwrap().unwrap();

    tracing::trace!("[PRODUCE] Read the batch, producing..");

    let result = broker
        .produce(
            &stream_name,
            &PartitionName::try_from(&partition_key[..]).unwrap(),
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
