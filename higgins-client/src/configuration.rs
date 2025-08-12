use bytes::BytesMut;
use higgins_codec::{
    CreateConfigurationRequest, CreateConfigurationResponse, Message, frame::Frame, message::Type,
};
use prost::Message as _;

#[allow(unused)]
pub fn upload_configuration<
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    config: &[u8],
    socket: &mut S,
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

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write(socket).unwrap();

    let frame = Frame::try_read(socket).unwrap();

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    let result = match Type::try_from(message.r#type).unwrap() {
        Type::Createconfigurationresponse => message.create_configuration_response,
        _ => panic!("Received incorrect response from server for create configuration request."),
    };

    result.unwrap()
}
