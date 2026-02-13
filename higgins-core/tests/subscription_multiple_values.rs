use std::{
    env::temp_dir,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::common::get_random_port;
use higgins::run_server;
use higgins_client::Response;
use higgins_codec::{GetSubscriptionResponse, Record};
use higgins_shared::PartitionName;

mod common;

static STREAM_NAME: &str = "update_customer";

#[test]
fn can_update_subscription_with_multiple_values() {
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

    let mut produce_client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/basic_config.toml").unwrap();

    produce_client
        .upload_configuration(config.as_bytes())
        .unwrap();

    match produce_client.recv(Some(Duration::from_secs(1))).unwrap() {
        Response::CreateConfiguration(_) => {
            println!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    // produce
    // await take response.
    // if no take response after some time, check subscription

    let mut consume_client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    let sub_id = create_subscription(&mut consume_client, STREAM_NAME);

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    println!("Subscription: {:#?}", subscription);

    let _ = consume_client.take(sub_id, STREAM_NAME.as_bytes(), 100);

    let payload = std::fs::read_to_string("tests/customer.json").unwrap();

    produce_client
        .produce(
            "update_customer",
            &PartitionName::try_from("1").unwrap(),
            payload.as_bytes(),
        )
        .unwrap();

    let records = recv_until_take(&mut consume_client);

    println!(
        "Records: {:#?}",
        records.iter().map(record_to_string).collect::<Vec<_>>()
    );

    // Produce to the stream.

    std::fs::remove_dir_all(dir_remove).unwrap();

    panic!();
}

/// Helper for receiving from a socket until it's taken.
pub fn recv_until_take(consume_client: &mut higgins_client::blocking::Client) -> Vec<Record> {
    loop {
        match consume_client.recv(None).unwrap() {
            Response::TakeRecords(response) => {
                return response.records;
            }
            Response::Produce(response) => {
                tracing::info!("Received produce response: {:#?}", response);
            }
            _ => {
                tracing::error!("Received unexpected response message.");
                panic!();
            }
        }
    }
}

pub fn record_to_string(record: &Record) -> String {
    String::from_utf8(record.data.to_owned()).unwrap()
}

pub fn get_subscription_data(
    stream: &str,
    sub_id: &[u8],
    client: &mut higgins_client::blocking::Client,
) -> GetSubscriptionResponse {
    client.get_subscription(stream, sub_id).unwrap();

    // Basically asserts that a Getsubscription request was returned
    match client.recv(None).unwrap() {
        Response::GetSubscription(response) => return response,
        _ => panic!("Retrieved unexpected result."),
    };
}

pub fn create_subscription(
    client: &mut higgins_client::blocking::Client,
    stream_name: &str,
) -> Vec<u8> {
    client.create_subscription(stream_name.as_bytes()).unwrap();

    let sub_id = match client.recv(None).unwrap() {
        Response::CreateSubscription(create_subscription_response) => {
            create_subscription_response.subscription_id.unwrap()
        }
        _ => panic!("Retrieved unexpected result."),
    };

    sub_id
}
