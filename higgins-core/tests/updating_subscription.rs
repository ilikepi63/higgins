use std::{
    env::temp_dir,
    io::{Read, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::BytesMut;
use get_port::{Ops, Range, tcp::TcpPort};
use higgins::run_server;
use higgins_codec::{Message, TakeRecordsRequest, message::Type};
use prost::Message as _;
use tracing_test::traced_test;

use crate::common::{
    configuration::upload_configuration, produce, subscription::create_subscription,
};

mod common;

#[test]
#[traced_test]
fn can_update_subscription_after_created() {
    const NUMBER_OF_MESSAGES: u16 = 1;

    let port = TcpPort::in_range(
        "127.0.0.1",
        Range {
            min: 2000,
            max: 25000,
        },
    )
    .unwrap();

    tracing::trace!("Running on port: {port}");

    let dir = {
        let mut dir = temp_dir();
        dir.push(uuid::Uuid::new_v4().to_string());

        dir
    };

    let dir_remove = dir.clone();

    let _ = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(run_server(dir, port));
    });

    // This will make the above server more likely to be instantiated.
    std::thread::sleep(Duration::from_millis(100));

    let mut socket = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();

    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/basic_config.toml").unwrap();

    upload_configuration(config.as_bytes(), &mut socket);

    // Start a subscription on that stream.

    let sub_id = create_subscription("update_customer".as_bytes(), &mut socket).unwrap();

    // Split the socket.
    let mut socket_writer = socket.try_clone().unwrap();
    let mut socket_reader = socket;

    let result_vec = Arc::new(Mutex::new(vec![]));

    // Concurrently take from the socket.
    let handle_consume = std::thread::spawn(move || {
        let take_request = TakeRecordsRequest {
            n: 100,
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

        socket_reader.write_all(&write_buf).unwrap();

        let mut count = 0;

        loop {
            let n = socket_reader.read(&mut read_buf).unwrap();

            assert_ne!(n, 0);

            let slice = &read_buf[0..n];

            let message = Message::decode(slice).unwrap();

            tracing::trace!("Received: {:#?}", message);

            match Type::try_from(message.r#type).unwrap() {
                Type::Takerecordsresponse => {
                    let take_records_response = message.take_records_response.unwrap();

                    let mut result_vec = result_vec.lock().unwrap();

                    for record in take_records_response.records.iter() {
                        result_vec.push(String::from_utf8(record.data.clone()).unwrap());
                        count += 1;

                        if count >= NUMBER_OF_MESSAGES {
                            break;
                        }
                    }
                }
                Type::Produceresponse => {
                    let message = message.produce_response;

                    tracing::info!("Received produce response: {:#?}", message);
                }
                _ => {}
            }

            if count >= NUMBER_OF_MESSAGES {
                break;
            }
        }
    });

    // Produce to the stream.

    let payload = std::fs::read_to_string("tests/customer.json").unwrap();

    for _ in 0..NUMBER_OF_MESSAGES {
        produce(
            "update_customer".as_bytes(),
            "test_partition".as_bytes(),
            payload.as_bytes(),
            &mut socket_writer,
        );
    }

    handle_consume.join().unwrap();

    std::fs::remove_dir_all(dir_remove).unwrap();
}
