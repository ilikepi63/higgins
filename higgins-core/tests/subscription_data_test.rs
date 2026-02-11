use std::{
    env::temp_dir,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::common::get_random_port;
use higgins::run_server;
use higgins_client::Response;
use higgins_shared::PartitionName;

mod common;

#[test]
fn can_retrieve_data_from_subscription() {
    tracing_subscriber::fmt::init();

    let port = get_random_port();

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

    let mut client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/basic_config.toml").unwrap();

    client.upload_configuration(config.as_bytes()).unwrap();

    match client.recv().unwrap() {
        Response::CreateConfiguration(_) => {
            println!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    // Start a subscription on that stream.
    client
        .create_subscription("update_customer".as_bytes())
        .unwrap();

    let sub_id = match client.recv().unwrap() {
        Response::CreateSubscription(create_subscription_response) => {
            create_subscription_response.subscription_id.unwrap()
        }
        _ => panic!("Retrieved unexpected result."),
    };

    tracing::trace!("Successfully created subscription!");

    // Concurrently take from the socket.
    std::fs::remove_dir_all(dir_remove).unwrap();
}
