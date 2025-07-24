use bytes::BytesMut;
use higgins_codec::{Message, Ping, message::Type};
use prost::Message as _;

#[allow(unused)]
pub fn ping<S: std::io::Read + std::io::Write>(socket: &mut S) {
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

    socket.write_all(&write_buf).unwrap();
}

#[allow(unused)]
pub fn ping_sync<S: std::io::Read + std::io::Write>(socket: &mut S) {
    let mut read_buf = BytesMut::zeroed(20);

    ping(socket);

    let n = socket.read(&mut read_buf).unwrap();
    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Pong => {}
        _ => panic!("Received incorrect response from server for ping request."),
    }
}
