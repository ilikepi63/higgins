use higgins_codec::{Message, Ping, message::Type};
use prost::Message as _;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::BytesMut;

pub async fn ping<S: AsyncReadExt + AsyncWriteExt + std::marker::Unpin>(socket: &mut S) {
    let mut write_buf = BytesMut::new();

    let ping = Ping::default();

    Message {
        r#type: Type::Ping as i32,
        ping: Some(ping),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    tracing::info!("Writing: {:#?}", write_buf);

    socket.write_all(&write_buf).await.unwrap();
}

pub async fn ping_sync<S: AsyncReadExt + AsyncWriteExt + std::marker::Unpin>(socket: &mut S) {
    let mut read_buf = BytesMut::zeroed(20);

    ping(socket).await;

    let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut read_buf))
        .await
        .unwrap()
        .unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Pong => {}
        _ => panic!("Received incorrect response from server for ping request."),
    }
}
