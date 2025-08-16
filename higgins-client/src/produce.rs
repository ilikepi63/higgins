use crate::error::HigginsClientError;
use bytes::BytesMut;
use higgins_codec::ProduceResponse;
use higgins_codec::frame::Frame;
use higgins_codec::{Message, ProduceRequest, message::Type};
use prost::Message as _;

/// produce to a stream without waiting for the response.
///
/// This is helpful in scenarios where you may want to produce concurrently.
#[allow(dead_code)]
pub async fn produce<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + std::marker::Unpin>(
    stream: &[u8],
    partition: &[u8],
    payload: &[u8],
    socket: &mut T,
) {
    let produce_request = ProduceRequest {
        partition_key: partition.to_vec(),
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

/// Produce synchronously to a listener awaiting the response.
#[allow(unused)]
pub async fn produce_sync<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + std::marker::Unpin>(
    stream: &[u8],
    partition: &[u8],
    payload: &[u8],
    socket: &mut T,
) -> Result<ProduceResponse, HigginsClientError> {
    produce(stream, partition, payload, socket).await;

    let mut read_buf = BytesMut::zeroed(1024);

    let frame = Frame::try_read_async(socket).await.unwrap();

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    let result = match Type::try_from(message.r#type).unwrap() {
        Type::Produceresponse => {
            

            message.produce_response.unwrap()
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(result)
}
