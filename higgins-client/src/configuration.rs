use bytes::BytesMut;
use higgins_codec::{
    CreateConfigurationRequest, CreateConfigurationResponse, Message, frame::Frame, message::Type,
};
use prost::Message as _;

use crate::error::HigginsClientError;

pub async fn upload_configuration<
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    config: &[u8],
    socket: &mut S,
) -> Result<CreateConfigurationResponse, HigginsClientError> {
    let mut write_buf = BytesMut::new();

    let create_config_req = CreateConfigurationRequest {
        data: config.to_vec(),
    };

    Message {
        r#type: Type::Createconfigurationrequest as i32,
        create_configuration_request: Some(create_config_req),
        ..Default::default()
    }
    .encode(&mut write_buf)?;

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;

    let frame = Frame::try_read_async(socket).await?;

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Createconfigurationresponse => Ok(message
            .create_configuration_response
            .ok_or(HigginsClientError::MissingPayload)?),
        _ => Err(HigginsClientError::IncorrectResponseReceived(
            Type::Createconfigurationresponse.as_str_name().to_string(),
            message.r#type().as_str_name().to_string(),
        )),
    }
}
