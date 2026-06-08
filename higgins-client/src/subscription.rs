use crate::error::HigginsClientError;
use bytes::BytesMut;
use higgins_codec::frame::Frame;
use higgins_codec::{
    AcknowledgeSubscriptionOffsetsRequest, CreateSubscriptionRequest, GetSubscriptionRequest,
    Offset, Range, TakeRecordsRequest,
};
use higgins_codec::{Message, message::Type};
use higgins_shared::PartitionName;
use prost::Message as _;

pub async fn create_subscription<
    T: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    stream: &[u8],
    request_id: u64,
    socket: &mut T,
) -> Result<(), HigginsClientError> {
    let create_subscription = CreateSubscriptionRequest {
        offset: None,
        offset_type: 0,
        timestamp: None,
        stream_name: stream.to_vec(),
    };

    let mut write_buf = BytesMut::new();

    Message {
        r#type: Type::Createsubscriptionrequest as i32,
        correlation_id: Some(request_id),
        create_subscription_request: Some(create_subscription),
        ..Default::default()
    }
    .encode(&mut write_buf)?;

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await?;

    Ok(())
}

pub async fn take<T: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin>(
    sub_id: Vec<u8>,
    stream_name: &[u8],
    n: u64,
    request_id: u64,
    socket: &mut T,
) -> Result<(), HigginsClientError> {
    let take_request = TakeRecordsRequest {
        n,
        subscription_id: sub_id,
        stream_name: stream_name.to_vec(),
    };

    let mut write_buf = BytesMut::new();

    Message {
        r#type: Type::Takerecordsrequest as i32,
        take_records_request: Some(take_request),
        correlation_id: Some(request_id),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await.unwrap();

    Ok(())
}

pub async fn get_subscription<
    T: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    sub_id: &[u8],
    stream: &str,
    request_id: u64,
    socket: &mut T,
) -> Result<(), HigginsClientError> {
    let req = GetSubscriptionRequest {
        subscription_id: sub_id.to_owned(),
        stream: stream.to_owned(),
    };

    let mut write_buf = BytesMut::new();

    Message {
        r#type: Type::Getsubscriptionrequest as i32,
        get_subscription_request: Some(req),
        correlation_id: Some(request_id),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await.unwrap();

    Ok(())
}

pub async fn acknowledge<
    T: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + std::marker::Unpin,
>(
    sub_id: &[u8],
    stream: &str,
    offsets: Vec<(PartitionName, std::ops::Range<u64>)>,
    request_id: u64,
    socket: &mut T,
) -> Result<(), HigginsClientError> {
    let req = AcknowledgeSubscriptionOffsetsRequest {
        subscription_id: sub_id.to_owned(),
        stream: stream.to_owned(),
        offsets: offsets
            .iter()
            .map(|offset| Offset {
                key: offset.0.to_vec(),
                range: Some(Range {
                    start: offset.1.start,
                    end: offset.1.end,
                }),
            })
            .collect(),
    };

    let mut write_buf = BytesMut::new();

    Message {
        r#type: Type::Acknowledgerequest as i32,
        acknowledge_request: Some(req),
        correlation_id: Some(request_id),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write_async(socket).await.unwrap();

    Ok(())
}
