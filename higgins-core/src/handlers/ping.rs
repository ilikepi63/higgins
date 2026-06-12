use bytes::BytesMut;
use higgins_codec::{Message, Pong, message::Type};
use higgins_shared::HigginsError;
use prost::Message as _;
use tokio::sync::mpsc::Sender;

pub async fn handle_ping(
    message: Message,
    writer_tx: Sender<BytesMut>,
) -> Result<(), HigginsError> {
    tracing::trace!("Received Ping, sending Pong.");

    let mut result = BytesMut::new();

    let pong = Pong::default();

    Message {
        correlation_id: message.correlation_id,
        r#type: Type::Pong as i32,
        pong: Some(pong),
        ..Default::default()
    }
    .encode(&mut result)?;

    tracing::info!("Responding with: {:#?}", result.clone().to_vec());

    writer_tx
        .send(result)
        .await
        .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

    Ok(())
}
