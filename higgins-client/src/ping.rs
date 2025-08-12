use crate::error::HigginsClientError;
use bytes::BytesMut;
use higgins_codec::{Message, Ping, frame::Frame, message::Type};
use prost::Message as _;

#[allow(unused)]
pub async fn ping<S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin>(
    socket: &mut S,
) -> Result<(), HigginsClientError> {
    let mut write_buf = BytesMut::new();

    let ping = Ping::default();

    Message {
        r#type: Type::Ping as i32,
        ping: Some(ping),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;
    Ok(())
}

#[allow(unused)]
pub async fn ping_sync<
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    socket: &mut S,
) -> Result<(), HigginsClientError> {
    let mut read_buf = BytesMut::zeroed(20);

    ping(socket);

    let frame = Frame::try_read_async(socket).await.unwrap();

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Pong => Ok(()),
        _ => Err(HigginsClientError::IncorrectResponseReceived(
            Type::Pong.as_str_name().to_string(),
            message.r#type().as_str_name().to_string(),
        )),
    }
}
