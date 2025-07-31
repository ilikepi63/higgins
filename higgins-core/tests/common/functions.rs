use std::io::{Write,Read};
use prost::Message as _;

use higgins_codec::{frame::Frame, message::Type, Message, UploadModuleRequest};
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

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write(socket).unwrap();

}

#[allow(unused)]
pub fn upload_module_sync(name: &str, wasm: &[u8], socket: &mut std::net::TcpStream) {
    let mut read_buf = BytesMut::zeroed(20);

    upload_module(name, wasm, socket);

    let frame = Frame::try_read(socket).unwrap();

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Uploadmoduleresponse => {}
        _ => panic!("Received incorrect response from server for ping request."),
    }
}
