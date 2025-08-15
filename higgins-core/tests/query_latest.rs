use std::env::temp_dir;
use std::time::Duration;

use get_port::{Ops, Range, tcp::TcpPort};
use higgins::run_server;
use tracing_test::traced_test;

use crate::common::produce_sync;
use crate::common::{
    configuration::upload_configuration,
    query::{query_by_timestamp, query_latest},
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
    )
    .unwrap();

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
    )
    .unwrap();

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
    std::fs::remove_dir_all(dir_remove).unwrap();
}

pub fn epoch() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("time should go forward");

    since_the_epoch.as_secs()
}
