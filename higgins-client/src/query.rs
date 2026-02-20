use bytes::BytesMut;
use higgins_codec::{GetIndexRequest, Index, Message, frame::Frame, message::Type};
use higgins_shared::PartitionName;
use prost::Message as _;

use crate::error::HigginsClientError;

#[allow(unused)]
pub async fn query_by_timestamp<
    T: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    stream: &[u8],
    partition: &PartitionName,
    request_id: u64,
    socket: &mut T,
    timestamp: u64,
) -> Result<(), HigginsClientError> {
    let request = GetIndexRequest {
        indexes: vec![Index {
            r#type: higgins_codec::index::Type::Timestamp.into(),
            stream: stream.to_owned(),
            partition: partition.0.to_vec(),
            timestamp: Some(timestamp),
            index: None,
        }],
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(8048);

    Message {
        r#type: Type::Getindexrequest as i32,
        get_index_request: Some(request),
        correlation_id: Some(request_id),
        ..Default::default()
    }
    .encode(&mut write_buf)?;
    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;

    Ok(())
}

#[allow(unused)]
pub async fn query_latest<
    T: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    stream: &[u8],
    partition: &PartitionName,
    request_id: u64,
    socket: &mut T,
) -> Result<(), HigginsClientError> {
    let request = GetIndexRequest {
        indexes: vec![Index {
            r#type: higgins_codec::index::Type::Latest.into(),
            stream: stream.to_owned(),
            partition: partition.0.to_vec(),
            timestamp: None,
            index: None,
        }],
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(8048);

    Message {
        r#type: Type::Getindexrequest as i32,
        get_index_request: Some(request),
        correlation_id: Some(request_id),
        ..Default::default()
    }
    .encode(&mut write_buf)?;

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;

    Ok(())
}
