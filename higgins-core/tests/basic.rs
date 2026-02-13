mod common;

use std::{path::PathBuf, time::Duration};

use higgins::{run_server, storage::arrow_ipc::read_arrow};
use higgins_client::Response;
use higgins_shared::PartitionName;

use common::get_random_port;

fn get_dir() -> PathBuf {
    // let mut dir = temp_dir();
    let mut dir = PathBuf::new();
    dir.push("basic");
    dir
}

static STREAM: &str = "update_customer";
static PARTITION: &[u8] = "test_partition".as_bytes();

#[test]
fn can_achieve_basic_broker_functionality() {
    tracing_subscriber::fmt::init();

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

    match client.recv(None).unwrap() {
        Response::Pong(_) => {
            println!("Retrieved Pong!");
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/basic_config.toml").unwrap();
    client.upload_configuration(config.as_bytes()).unwrap();

    match client.recv(None).unwrap() {
        Response::CreateConfiguration(_) => {
            println!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    // Produce to the stream.
    let payload = std::fs::read_to_string("tests/customer.json").unwrap();

    client
        .produce(
            STREAM,
            &PartitionName::try_from(PARTITION).unwrap(),
            payload.as_bytes(),
        )
        .unwrap();

    match client.recv(None).unwrap() {
        Response::Produce(_) => {
            println!("Retrieved Produce!");
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    // Consume from the stream.
    client
        .query_latest(
            STREAM.as_bytes(),
            &PartitionName::try_from(PARTITION).unwrap(),
        )
        .unwrap();

    let result = client.recv(None).map(|res| match res {
        Response::GetIndex(get_index_result) => get_index_result.records,
        _ => panic!("Got an unexpect result."),
    });

    let arrow_data = result.unwrap().into_iter().next().unwrap();

    let arrow = read_arrow(&arrow_data.data).next().unwrap().unwrap();

    tracing::trace!("Data: {:#?}", arrow);

    assert_eq!(
        arrow
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .unwrap(),
        21
    );

    assert_eq!(
        arrow
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .unwrap(),
        "John"
    );

    assert_eq!(
        arrow
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .unwrap(),
        "1"
    );

    assert_eq!(
        arrow
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .unwrap(),
        "Doe"
    );

    std::fs::remove_dir_all(dir_remove).unwrap();
}
