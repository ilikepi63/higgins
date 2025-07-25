use std::{net::TcpStream, time::Duration};

use get_port::{Ops, Range, tcp::TcpPort};
use higgins::run_server;
use serde_json::json;
use tracing_test::traced_test;

use crate::common::{
    configuration::upload_configuration, ping::ping_sync, produce_sync, query::query_latest,
};

mod common;

#[test]
#[traced_test]
fn can_implement_basic_map() {
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

    let config = std::fs::read_to_string("tests/configs/map_config.yaml").unwrap();

    upload_configuration(config.as_bytes(), &mut socket);

    // schema:
    //   amount:
    //     id: string
    //     data: int32

    // streams:
    //   amount:
    //     schema: amount
    //     partition_key: id
    //   result:
    //     base: amount
    //     type: map
    //     partition_key: id
    //     schema: amount
    //     fn: map

    produce_sync(
        b"amount",
        b"1",
        r#"
        {   
            "id": "1",
            "amount": 1,
        }
    "#
        .as_bytes(),
        &mut socket,
    )
    .unwrap();

    // let result = consume(sub_id, b"customer_product", &mut socket).unwrap();

    let result = query_latest(b"result", b"1", &mut socket).unwrap();

    let result: serde_json::Value = serde_json::from_slice(&result.first().unwrap().data).unwrap();
    let expected_result = json!(
        {"id":"1","data":2,"age":30}
    );

    assert_eq!(result, expected_result);
}
