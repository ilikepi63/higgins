use bytes::BytesMut;
use higgins_codec::frame::Frame;
use higgins_codec::{Message, ProduceRequest, message::Type};
use higgins_shared::PartitionName;
use prost::Message as _;

/// produce to a stream without waiting for the response.
///
/// This is helpful in scenarios where you may want to produce concurrently.
#[allow(dead_code)]
pub async fn produce<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + std::marker::Unpin>(
    stream: &[u8],
    partition: &PartitionName,
    payload: &[u8],
    socket: &mut T,
) {
    let produce_request = ProduceRequest {
        partition_key: partition.0.to_vec(),
        payload: payload.to_vec(),
        stream_name: stream.to_vec(),
    };

    let mut write_buf = BytesMut::new();

    Message {
        r#type: Type::Producerequest as i32,
        produce_request: Some(produce_request),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await.unwrap();
}
