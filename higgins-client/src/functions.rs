use prost::Message as _;

use bytes::BytesMut;
use higgins_codec::{Message, UploadModuleRequest, frame::Frame, message::Type};

use crate::error::HigginsClientError;

pub async fn upload_module<
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    name: &str,
    wasm: &[u8],
    socket: &mut S,
) -> Result<(), HigginsClientError> {
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
    .encode(&mut write_buf)?;

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;

    Ok(())
}

#[allow(unused)]
pub async fn upload_module_sync<
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    name: &str,
    wasm: &[u8],
    socket: &mut S,
) -> Result<(), HigginsClientError> {
    upload_module(name, wasm, socket).await?;

    let frame = Frame::try_read_async(socket).await?;

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Uploadmoduleresponse => Ok(()),
        _ => Err(HigginsClientError::IncorrectResponseReceived(
            Type::Uploadmoduleresponse.as_str_name().to_string(),
            message.r#type().as_str_name().to_string(),
        )),
    }
}
