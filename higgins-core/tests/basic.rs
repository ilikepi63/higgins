use std::time::Duration;

use get_port::{Ops, Range, tcp::TcpPort};
use higgins::run_server;
use tracing_test::traced_test;

#[traced_test]
#[test]
fn can_achieve_basic_broker_functionality() {
    let port = TcpPort::in_range(
        "127.0.0.1",
        Range {
            min: 2000,
            max: 25000,
        },
    )
    .unwrap();

    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_server(port));
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    // 1. Do a basic Ping test.
    client.ping().unwrap();

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/basic_config.toml").unwrap();
    client.upload_configuration(config.as_bytes()).unwrap();

    // Start a subscription on that stream.
    let sub_id = client
        .create_subscription("update_customer".as_bytes())
        .unwrap();

    // Produce to the stream.
    let payload = std::fs::read_to_string("tests/customer.json").unwrap();

    client
        .produce(
            "update_customer",
            "test_partition".as_bytes(),
            payload.as_bytes(),
        )
        .unwrap();


    // Consume from the stream.
    client
        .take(sub_id, "update_customer".as_bytes(), 1)
        .unwrap();

    
}
