use std::time::Duration;

use bytes::BytesMut;
use get_port::{Ops, Range, tcp::TcpPort};
use higgins::run_server;
use higgins_codec::{CreateConfigurationRequest, Message, Ping, message::Type};
use prost::Message as _;
use std::io::{Read, Write};
use std::net::TcpStream;

use crate::common::{consume, produce, subscription::create_subscription};

mod common;

// #[tokio::test]
// #[traced_test]

#[allow(dead_code)]
async fn can_correctly_consume_and_produce_interleaving_requests() {
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

    let _handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(run_server(port));
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();

    // socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

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

    socket.write_all(&write_buf).unwrap();

    let n = socket.read(&mut read_buf).unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Pong => {}
        _ => panic!("Received incorrect response from server for ping request."),
    }

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/basic_config.yaml").unwrap();

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

    socket.write_all(&write_buf).unwrap();

    let n = socket.read(&mut read_buf).unwrap();

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
    let sub_id = create_subscription("update_customer".as_bytes(), &mut socket).unwrap();

    // Produce to the stream.
    let payload = std::fs::read_to_string("tests/customer.json").unwrap();

    loom::model(move || {
        let socket = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();

        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let socket = loom::sync::Arc::new(loom::sync::Mutex::new(socket));
        let result_collection = loom::sync::Arc::new(loom::sync::Mutex::new(vec![]));

        // this likely would be better in a constant.
        let message_count = 100;

        let produce_handles = (0..message_count).map(|_| {
            let payload = payload.clone();
            let socket = socket.clone(); // arc clone.

            let handle = loom::thread::spawn(move || {
                loom::future::block_on(async {
                    let mut socket = socket.lock().unwrap();

                    produce(
                        "update_customer".as_bytes(),
                        "test_partition".as_bytes(),
                        payload.as_bytes(),
                        &mut *socket,
                    )
                });
            });

            handle
        });

        let consume_handles = (0..message_count).map(|_| {
            let socket = socket.clone(); // arc clone.
            let sub_id = sub_id.clone();
            let result_collection = result_collection.clone();

            let handle = loom::thread::spawn(move || {
                loom::future::block_on(async {
                    let mut socket = socket.lock().unwrap();

                    // Consume from the stream.
                    let response = consume(sub_id, b"update_customer", &mut *socket).unwrap();

                    let mut collection_lock = result_collection.lock().unwrap();

                    collection_lock.push(response);
                });
            });

            handle
        });

        // Join the given handles.
        for handle in produce_handles.chain(consume_handles) {
            handle.join().unwrap();
        }

        let received_values = result_collection.lock().unwrap();
        assert_eq!(message_count, received_values.len());
    });
}
