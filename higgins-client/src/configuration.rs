use bytes::BytesMut;
use higgins_codec::{CreateConfigurationRequest, Message, frame::Frame, message::Type};
use prost::Message as _;

use crate::error::HigginsClientError;

pub async fn upload_configuration<
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    config: &[u8],
    socket: &mut S,
) -> Result<(), HigginsClientError> {
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

    Ok(())
}
