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

use crate::common::ping::ping_sync;

mod common;

#[tokio::test]
#[traced_test]
async fn can_implement_a_basic_stream_join() {
    let port = TcpPort::in_range(
        "127.0.0.1",
        Range {
            min: 2000,
            max: 25000,
        },
    )
    .unwrap();

    tracing::info!("Running on port: {port}");

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
    ping_sync(&mut socket).await;

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/join_config.yaml").unwrap();

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

    handle.abort();
}
