use std::{net::TcpStream, time::Duration};

use get_port::{Ops, Range, tcp::TcpPort};
use higgins::run_server;
use serde_json::json;
use tracing_test::traced_test;

use crate::common::{
    configuration::upload_configuration, ping::ping_sync, produce_sync,
    query::query_latest
};

mod common;

#[test]
#[traced_test]
fn can_implement_a_basic_stream_join() {
    let port = TcpPort::in_range(
        "127.0.0.1",
        Range {
            min: 2000,
            max: 25000,
        },
    )
    .unwrap();

    tracing::info!("Running on port: {port}");

    let _ = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(run_server(port));
    });

    std::thread::sleep(Duration::from_millis(200)); // Sleep to allow 

    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();

    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Do a basic Ping test.
    ping_sync(&mut socket);

    // Upload a basic configuration with one stream.

    let config = std::fs::read_to_string("tests/configs/join_config.toml").unwrap();

    upload_configuration(config.as_bytes(), &mut socket);

    // let sub_id = create_subscription(b"customer_product", &mut socket).unwrap();

    // tracing::info!("Sub ID: {:#?}", Uuid::from_slice(&sub_id).unwrap());

    produce_sync(
        b"customer",
        b"1",
        r#"
        {
            "id": "1",
            "first_name": "TestFirstName",
            "last_name": "TestSurname",
            "age": 30
        }
    "#
        .as_bytes(),
        &mut socket,
    )
    .unwrap();

    produce_sync(
        b"address",
        b"1",
        r#"
        {
            "customer_id": "1",
            "address_line_1": "12 Tennatn Avenut",
            "address_line_2": "Bonteheuwel",
            "city": "Cape Town",
            "province": "Western Cape"
        }
    "#
        .as_bytes(),
        &mut socket,
    )
    .unwrap();

    // let result = consume(sub_id, b"customer_product", &mut socket).unwrap();

    let result = query_latest(b"customer_address", b"1", &mut socket).unwrap();

    let result: serde_json::Value = serde_json::from_slice(&result.first().unwrap().data).unwrap();
    let expected_result = json!(
        {"address_line_1":"12 Tennatn Avenut","address_line_2":"Bonteheuwel","age":30,"city":"Cape Town","customer_first_name":"TestFirstName","customer_id":"1","customer_last_name":"TestSurname","province":"Western Cape"}
    );

    assert_eq!(result, expected_result);
}
