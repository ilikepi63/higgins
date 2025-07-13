use std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use bytes::BytesMut;
use get_port::{Ops, Range, tcp::TcpPort};
use higgins::run_server;
use higgins_codec::{Message, TakeRecordsRequest, message::Type};
use prost::Message as _;
use tracing_test::traced_test;

use crate::common::produce_sync;
use crate::common::{
    configuration::upload_configuration,
    produce,
    query::{query_by_timestamp, query_latest},
    subscription::create_subscription,
};

mod common;

#[test]
#[traced_test]
fn can_arbitrarily_query_for_time_based_values() {
    let port = TcpPort::in_range(
        "127.0.0.1",
        Range {
            min: 2000,
            max: 25000,
        },
    )
    .unwrap();

    tracing::trace!("Running on port: {port}");

    let _ = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(run_server(port));
    });

    // This will make the above server more likely to be instantiated.
    std::thread::sleep(Duration::from_millis(100));

    let mut socket = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();

    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/basic_config.yaml").unwrap();

    upload_configuration(config.as_bytes(), &mut socket);

    let first_payload = r#"
        {
            "id": "1",
            "first_name": "John",
            "last_name": "Doe",
            "age": 21
        }
    "#;

    let second_payload = r#"
        {
            "id": "2",
            "first_name": "Jane",
            "last_name": "Doesky",
            "age": 22
        }"#;

    let partition = "test_partition";
    let stream = "update_customer";

    produce_sync(
        stream.as_bytes(),
        partition.as_bytes(),
        first_payload.as_bytes(),
        &mut socket,
    );

    let latest_result = query_latest(stream.as_bytes(), partition.as_bytes(), &mut socket)
        .unwrap()
        .first()
        .map(|record| String::from_utf8(record.data.clone()).ok())
        .flatten()
        .unwrap();

    let result = query_by_timestamp(
        stream.as_bytes(),
        partition.as_bytes(),
        &mut socket,
        epoch(),
    )
    .unwrap()
    .first()
    .map(|record| String::from_utf8(record.data.clone()).ok())
    .flatten()
    .unwrap();

    assert_eq!(result, latest_result);

    let result: serde_json::Value = serde_json::from_slice(result.as_bytes()).unwrap();
    let first_payload: serde_json::Value =
        serde_json::from_slice(first_payload.as_bytes()).unwrap();

    assert_eq!(result, first_payload);

    produce_sync(
        stream.as_bytes(),
        partition.as_bytes(),
        second_payload.as_bytes(),
        &mut socket,
    );

    let latest_result = query_latest(stream.as_bytes(), partition.as_bytes(), &mut socket)
        .unwrap()
        .first()
        .map(|record| String::from_utf8(record.data.clone()).ok())
        .flatten()
        .unwrap();

    let result = query_latest(stream.as_bytes(), partition.as_bytes(), &mut socket)
        .unwrap()
        .first()
        .map(|record| String::from_utf8(record.data.clone()).ok())
        .flatten()
        .unwrap();

    assert_eq!(result, latest_result);

    let result: serde_json::Value = serde_json::from_slice(result.as_bytes()).unwrap();
    let second_payload: serde_json::Value =
        serde_json::from_slice(second_payload.as_bytes()).unwrap();

    assert_eq!(result, second_payload);
}

pub fn epoch() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("time should go forward");

    since_the_epoch.as_secs()
}
