use prost::Message as _;

use bytes::BytesMut;
use higgins_codec::{Message, UploadModuleRequest, frame::Frame, message::Type};

use crate::error::HigginsClientError;

pub async fn upload_module<
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    name: &str,
    wasm: &[u8],
    request_id: u64,
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
        correlation_id: Some(request_id),
        ..Default::default()
    }
    .encode(&mut write_buf)?;

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;

    Ok(())
}
