use std::time::Duration;

use bytes::BytesMut;
use get_port::{Ops, Range, tcp::TcpPort};
use higgins::run_server;
use higgins_codec::{
    CreateConfigurationRequest, CreateSubscriptionRequest, Message, Ping, ProduceRequest,
    TakeRecordsRequest, message::Type,
};
use prost::Message as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing_test::traced_test;
#[tokio::test]
#[traced_test]
async fn can_achieve_basic_broker_functionality() {
    let port = TcpPort::in_range(
        "127.0.0.1",
        Range {
            min: 2000,
            max: 25000,
        },
    )
    .unwrap();

    println!("Running on port: {port}");

    let mut read_buf = BytesMut::zeroed(20);
    let mut write_buf = BytesMut::new();

    let handle = tokio::spawn(async move {
        let _ = run_server(port).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // 1. Do a basic Ping test.
    let ping = Ping::default();

    Message {
        r#type: Type::Ping as i32,
        ping: Some(ping),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    tracing::info!("Writing: {:#?}", write_buf);

    let _result = socket.write_all(&write_buf).await.unwrap();

    let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut read_buf))
        .await
        .unwrap()
        .unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Pong => {}
        _ => panic!("Received incorrect response from server for ping request."),
    }

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/basic_config.yaml").unwrap();

    let create_config_req = CreateConfigurationRequest {
        data: config.into_bytes(),
    };

    Message {
        r#type: Type::Createconfigurationrequest as i32,
        create_configuration_request: Some(create_config_req),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let _result = socket.write_all(&write_buf).await.unwrap();

    let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut read_buf))
        .await
        .unwrap()
        .unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Createconfigurationresponse => {
            tracing::info!("Received response from server for configuration request.")
        }
        _ => panic!("Received incorrect response from server for create configuration request."),
    }

    // Start a subscription on that stream.
    let create_subscription = CreateSubscriptionRequest {
        offset: None,
        offset_type: 0,
        timestamp: None,
        stream_name: "update_customer".as_bytes().to_vec(),
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(1024);

    Message {
        r#type: Type::Createsubscriptionrequest as i32,
        create_subscription_request: Some(create_subscription),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let _result = socket.write_all(&write_buf).await.unwrap();

    let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut read_buf))
        .await
        .unwrap()
        .unwrap();

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

    // Produce to the stream.

    let payload = std::fs::read_to_string("tests/customer.json").unwrap();

    let produce_request = ProduceRequest {
        partition_key: "test_partition".as_bytes().to_vec(),
        payload: payload.as_bytes().to_vec(),
        stream_name: "update_customer".as_bytes().to_vec(),
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

    let _result = socket.write_all(&write_buf).await.unwrap();

    let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut read_buf))
        .await
        .unwrap()
        .unwrap();

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

    // Consume from the stream.

    let take_request = TakeRecordsRequest {
        n: 1,
        subscription_id: sub_id,
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(1024);

    Message {
        r#type: Type::Takerecordsrequest as i32,
        take_records_request: Some(take_request),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    let _result = socket.write_all(&write_buf).await.unwrap();

    let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut read_buf))
        .await
        .unwrap()
        .unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Takerecordsresponse => {

            println!("Receieved a take records response!");

        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    }

    handle.abort();
}
