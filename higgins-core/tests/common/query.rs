use bytes::BytesMut;
use higgins_codec::{GetIndexRequest, Index, Message, Record, message::Type};
use prost::Message as _;

#[allow(unused)]
pub fn query_by_timestamp<T: std::io::Read + std::io::Write>(
    stream: &[u8],
    partition: &[u8],
    socket: &mut T,
    timestamp: u64,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
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
    .encode(&mut write_buf)
    .unwrap();

    socket.write_all(&write_buf).unwrap();

    let n = socket.read(&mut read_buf).unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

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
pub fn query_latest<T: std::io::Read + std::io::Write>(
    stream: &[u8],
    partition: &[u8],
    socket: &mut T,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
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
    .encode(&mut write_buf)
    .unwrap();

    socket.write_all(&write_buf).unwrap();

    let n = socket.read(&mut read_buf).unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

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
