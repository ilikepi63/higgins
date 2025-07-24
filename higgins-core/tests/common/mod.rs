
use bytes::BytesMut;
use higgins_codec::{Message, ProduceRequest, message::Type};
use higgins_codec::{ProduceResponse, TakeRecordsRequest};
use prost::Message as _;

pub mod configuration;
pub mod ping;
pub mod query;
pub mod subscription;

/// produce to a stream without waiting for the response.
///
/// This is helpful in scenarios where you may want to produce concurrently.
#[allow(dead_code)]
pub fn produce<T: std::io::Read + std::io::Write>(
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

    socket.write_all(&write_buf).unwrap();
}

/// Produce synchronously to a listener awaiting the response.
#[allow(unused)]
pub fn produce_sync<T: std::io::Read + std::io::Write>(
    stream: &[u8],
    partition: &[u8],
    payload: &[u8],
    socket: &mut T,
) -> Result<ProduceResponse, Box<dyn std::error::Error>> {
    produce(stream, partition, payload, socket);

    let mut read_buf = BytesMut::zeroed(1024);

    let n = socket.read(&mut read_buf).unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    let result = match Type::try_from(message.r#type).unwrap() {
        Type::Produceresponse => {
            

            message.produce_response.unwrap()
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(result)
}

#[allow(unused)]
pub fn consume<T: std::io::Read + std::io::Write>(
    sub_id: Vec<u8>,
    stream_name: &[u8],
    socket: &mut T,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let take_request = TakeRecordsRequest {
        n: 1,
        subscription_id: sub_id,
        stream_name: stream_name.to_vec(),
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(8048);

    Message {
        r#type: Type::Takerecordsrequest as i32,
        take_records_request: Some(take_request),
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
        Type::Takerecordsresponse => {
            tracing::info!("Receieved a take records response!");

            let take_records_response = message.take_records_response.unwrap();

            tracing::info!("Records_Response: {:#?}", take_records_response);

            let record = take_records_response.records.first().unwrap();

            record.data.clone()
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(result)
}
