use std::io::{Read, Write};
use std::time::Duration;

use bytes::BytesMut;
use higgins_codec::{CreateSubscriptionRequest, TakeRecordsRequest};
use higgins_codec::{Message, ProduceRequest, message::Type};
use prost::Message as _;
use std::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub fn produce<T: std::io::Read + std::io::Write>(stream: &[u8], partition: &[u8], payload: &[u8], socket: &mut T) {
    let produce_request = ProduceRequest {
        partition_key: partition.to_vec(),
        payload: payload.to_vec(),
        stream_name: stream.to_vec(),
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(1024);

    Message {
        r#type: Type::Producerequest as i32,
        produce_request: Some(produce_request),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let _result = socket.write_all(&write_buf).unwrap();

    let n = socket.read(&mut read_buf).unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Produceresponse => {
            let message = message.produce_response;

            tracing::info!("Received produce response: {:#?}", message);
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    }
}

pub fn create_subscription<T: std::io::Read + std::io::Write>(
    stream: &[u8],
    socket: &mut T,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let create_subscription = CreateSubscriptionRequest {
        offset: None,
        offset_type: 0,
        timestamp: None,
        stream_name: stream.to_vec(),
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(1024);

    Message {
        r#type: Type::Createsubscriptionrequest as i32,
        create_subscription_request: Some(create_subscription),
        ..Default::default()
    }
    .encode(&mut write_buf)?;

    let _result = socket.write_all(&write_buf)?;

    let n = socket.read(&mut read_buf).unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    let sub_id = match Type::try_from(message.r#type).unwrap() {
        Type::Createsubscriptionresponse => {
            let sub_id = message
                .create_subscription_response
                .unwrap()
                .subscription_id;

            tracing::info!("Got the sub_id: {:#?}", sub_id);

            sub_id.unwrap()
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(sub_id)
}

pub fn consume<T: std::io::Read + std::io::Write>(
    sub_id: Vec<u8>,
    socket: &mut T,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let take_request = TakeRecordsRequest {
        n: 1,
        subscription_id: sub_id,
        stream_name: "update_customer".as_bytes().to_vec(),
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

    let _result = socket.write_all(&write_buf).unwrap();


    let n = socket.read(&mut read_buf).unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    let result = match Type::try_from(message.r#type).unwrap() {
        Type::Takerecordsresponse => {
            tracing::info!("Receieved a take records response!");

            let take_records_response = message.take_records_response.unwrap();

            tracing::info!("Records_Response: {:#?}", take_records_response);

            let record = take_records_response.records.iter().nth(0).unwrap();

            record.data.clone()
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(result)
}
