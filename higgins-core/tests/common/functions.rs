use std::time::Duration;

use higgins_client::ResponseBody;
use prost::Message as _;

use bytes::BytesMut;
use higgins_codec::{Message, UploadModuleRequest, frame::Frame, message::Type};

pub fn upload_module(name: &str, wasm: &[u8], socket: &mut std::net::TcpStream) {
    let mut write_buf = BytesMut::new();

    let request = UploadModuleRequest {
        name: name.to_owned(),
        value: wasm.to_vec(),
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

pub fn upload_module_sync(
    name: &str,
    module: &[u8],
    client: &mut higgins_client::blocking::Client,
) {
    client.upload_module(name, module).unwrap();

    if !matches!(
        client.recv(Some(Duration::from_secs(60))).unwrap().body,
        ResponseBody::UploadModule(_)
    ) {
        panic!("Unexpected response. Expected Upload Module.");
    }
}
