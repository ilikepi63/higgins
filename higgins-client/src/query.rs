use bytes::BytesMut;
use higgins_codec::{GetIndexRequest, Index, Message, Record, frame::Frame, message::Type};
use prost::Message as _;

use crate::error::HigginsClientError;

#[allow(unused)]
pub async fn query_by_timestamp<
    T: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    stream: &[u8],
    partition: &[u8],
    socket: &mut T,
    timestamp: u64,
) -> Result<Vec<Record>, HigginsClientError> {
    let request = GetIndexRequest {
        indexes: vec![Index {
            r#type: higgins_codec::index::Type::Timestamp.into(),
            stream: stream.to_owned(),
            partition: partition.to_owned(),
            timestamp: Some(timestamp),
            index: None,
        }],
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(8048);

    Message {
        r#type: Type::Getindexrequest as i32,
        get_index_request: Some(request),
        ..Default::default()
    }
    .encode(&mut write_buf)?;
    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;

    let frame = Frame::try_read_async(socket).await?;

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    let result = match Type::try_from(message.r#type).unwrap() {
        Type::Getindexresponse => {
            let response = message.get_index_response.unwrap();

            response.records
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(result)
}

#[allow(unused)]
pub async fn query_latest<
    T: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    stream: &[u8],
    partition: &[u8],
    socket: &mut T,
) -> Result<Vec<Record>, HigginsClientError> {
    let request = GetIndexRequest {
        indexes: vec![Index {
            r#type: higgins_codec::index::Type::Latest.into(),
            stream: stream.to_owned(),
            partition: partition.to_owned(),
            timestamp: None,
            index: None,
        }],
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(8048);

    Message {
        r#type: Type::Getindexrequest as i32,
        get_index_request: Some(request),
        ..Default::default()
    }
    .encode(&mut write_buf)?;

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;

    let frame = Frame::try_read_async(socket).await?;

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();
    let result = match Type::try_from(message.r#type).unwrap() {
        Type::Getindexresponse => {
            let response = message.get_index_response.unwrap();

            response.records
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(result)
}
