use bytes::BytesMut;
use higgins_codec::{
    CreateConfigurationRequest, CreateConfigurationResponse, Message, message::Type,
};
use prost::Message as _;
use std::io::{Read, Write};

pub fn upload_configuration(
    config: &[u8],
    socket: &mut std::net::TcpStream,
) -> CreateConfigurationResponse {
    let mut read_buf = BytesMut::zeroed(1024);
    let mut write_buf = BytesMut::new();

    let create_config_req = CreateConfigurationRequest {
        data: config.to_vec(),
    };

    Message {
        r#type: Type::Createconfigurationrequest as i32,
        create_configuration_request: Some(create_config_req),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let _result = socket.write_all(&write_buf).unwrap();

    let n = socket.read(&mut read_buf).unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    let result = match Type::try_from(message.r#type).unwrap() {
        Type::Createconfigurationresponse => message.create_configuration_response,
        _ => panic!("Received incorrect response from server for create configuration request."),
    };

    result.unwrap()
}
