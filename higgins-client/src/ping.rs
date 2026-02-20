use crate::error::HigginsClientError;
use bytes::BytesMut;
use higgins_codec::{Message, Ping, frame::Frame, message::Type};
use prost::Message as _;

#[allow(unused)]
pub async fn ping<S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin>(
    socket: &mut S,
    request_id: u64,
) -> Result<(), HigginsClientError> {
    let mut write_buf = BytesMut::new();

    let ping = Ping::default();

    Message {
        r#type: Type::Ping as i32,
        ping: Some(ping),
        correlation_id: Some(request_id),
        ..Default::default()
    }
    .encode(&mut write_buf)?;

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;
    Ok(())
}
