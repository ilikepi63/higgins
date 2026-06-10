use bytes::BytesMut;
use higgins_codec::{Error, Message, message::Type};
use prost::Message as _;

use tokio::sync::mpsc::Sender;

pub async fn handle_incorrect_message_received(message: Message, writer_tx: Sender<BytesMut>) {
    let resp = Error { r#type: 1 };

    let mut result = BytesMut::new();

    Message {
        r#type: Type::Error as i32,
        error: Some(resp),
        correlation_id: message.correlation_id,
        ..Default::default()
    }
    .encode(&mut result)?;

    writer_tx.send(result).await?;
}
