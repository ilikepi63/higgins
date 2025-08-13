use bytes::BytesMut;
use higgins_codec::{Message, Ping, frame::Frame, message::Type};
use prost::Message as _;

#[allow(unused)]
pub fn ping<S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin>(
    socket: &mut S,
) {
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

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write(socket).unwrap();
}

#[allow(unused)]
pub fn ping_sync<S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin>(
    socket: &mut S,
) {
    let mut read_buf = BytesMut::zeroed(20);

    ping(socket);

    let frame = Frame::try_read(socket).unwrap();

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Pong => {}
        _ => panic!("Received incorrect response from server for ping request."),
    }
}
