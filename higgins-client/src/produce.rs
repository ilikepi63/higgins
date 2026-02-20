use bytes::BytesMut;
use higgins_codec::frame::Frame;
use higgins_codec::{Message, ProduceRequest, message::Type};
use prost::Message as _;

/// produce to a stream without waiting for the response.
///
/// This is helpful in scenarios where you may want to produce concurrently.
#[allow(dead_code)]
pub async fn produce<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + std::marker::Unpin>(
    stream: &[u8],
    payload: &[u8],
    request_id: u64,
    socket: &mut T,
) {
    let produce_request = ProduceRequest {
        payload: payload.to_vec(),
        stream_name: stream.to_vec(),
    };

    let mut write_buf = BytesMut::new();

    Message {
        r#type: Type::Producerequest as i32,
        produce_request: Some(produce_request),
        correlation_id: Some(request_id),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await.unwrap();
}
