mod common;

use std::{env::temp_dir, path::PathBuf, time::Duration};

use higgins::run_server;
use tracing_test::traced_test;

use common::get_random_port;

fn get_dir() -> PathBuf {
    let mut dir = temp_dir();
    dir.push("basic");
    dir
}

static STREAM: &str = "update_customer";
static PARTITION: &[u8] = "test_partition".as_bytes();

#[traced_test]
#[test]
fn can_achieve_basic_broker_functionality() {
    let port = get_random_port();

    let dir = get_dir();

    let dir_remove = dir.clone();

    let _ = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(run_server(dir, port));
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    // 1. Do a basic Ping test.
    client.ping().unwrap();

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/basic_config.toml").unwrap();
    client.upload_configuration(config.as_bytes()).unwrap();

    // Produce to the stream.
    let payload = std::fs::read_to_string("tests/customer.json").unwrap();

    client
        .produce(STREAM, PARTITION, payload.as_bytes())
        .unwrap();

    // Consume from the stream.
    let result = client.query_latest(STREAM.as_bytes(), PARTITION);

    tracing::info!("{:#?}", result);

    panic!();

    std::fs::remove_dir_all(dir_remove).unwrap();
}
