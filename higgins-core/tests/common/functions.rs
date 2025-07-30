use std::io::{Write,Read};
use prost::Message as _;

use higgins_codec::{message::Type, Message, UploadModuleRequest};
use bytes::BytesMut;

pub fn upload_module(name: &str, wasm: &[u8], socket: &mut std::net::TcpStream) {
    let mut write_buf = BytesMut::new();

    let request = UploadModuleRequest {
        name:name.to_owned(),
        value: wasm.to_vec()
    };

    Message {
        r#type: Type::Uploadmodulerequest as i32,
        upload_module_request: Some(request),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    socket.write_all(&write_buf).unwrap();
}

#[allow(unused)]
pub fn upload_module_sync(name: &str, wasm: &[u8], socket: &mut std::net::TcpStream) {
    let mut read_buf = BytesMut::zeroed(20);

    upload_module(name, wasm, socket);

    let n = socket.read(&mut read_buf).unwrap();
    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Uploadmoduleresponse => {}
        _ => panic!("Received incorrect response from server for ping request."),
    }
}
