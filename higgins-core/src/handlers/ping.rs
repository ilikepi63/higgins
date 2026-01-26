use bytes::BytesMut;
use higgins_codec::{Message, Pong, message::Type};
use prost::Message as _;
use tokio::sync::mpsc::Sender;

pub async fn handle_ping(writer_tx: Sender<BytesMut>) {
    tracing::trace!("Received Ping, sending Pong.");

    let mut result = BytesMut::new();

    let pong = Pong::default();

    Message {
        r#type: Type::Pong as i32,
        pong: Some(pong),
        ..Default::default()
    }
    .encode(&mut result)
    .unwrap();

    tracing::info!("Responding with: {:#?}", result.clone().to_vec());

    writer_tx.send(result).await.unwrap();
}
