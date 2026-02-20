use std::{
    env::temp_dir,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::common::get_random_port;
use higgins::run_server;
use higgins_client::Response;

mod common;

#[test]
fn can_update_subscription_after_created() {
    tracing_subscriber::fmt::init();

    const NUMBER_OF_MESSAGES: u16 = 1;

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

    match produce_client.recv(None).unwrap() {
        Response::CreateConfiguration(_) => {
            println!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    // Start a subscription on that stream.
    produce_client
        .create_subscription("update_customer".as_bytes())
        .unwrap();

    let sub_id = match produce_client.recv(None).unwrap() {
        Response::CreateSubscription(create_subscription_response) => {
            create_subscription_response.subscription_id.unwrap()
        }
        _ => panic!("Retrieved unexpected result."),
    };

    tracing::trace!("Successfully created subscription!");

    let result_vec = Arc::new(Mutex::new(vec![]));

    // Concurrently take from the socket.
    let handle_consume = std::thread::spawn(move || {
        let mut consume_client =
            higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

        let result = consume_client.take(sub_id, "update_customer".as_bytes(), 100);

        assert!(matches!(result, Ok(_)));

        let mut count = 0;

        loop {
            tracing::trace!("Consuming from stream..");

            let response = consume_client.recv(None).unwrap();

            match response {
                Response::TakeRecords(response) => {
                    let mut result_vec = result_vec.lock().unwrap();

                    for record in response.records.iter() {
                        result_vec.push(String::from_utf8(record.data.clone()).unwrap());
                        count += 1;

                        if count >= NUMBER_OF_MESSAGES {
                            break;
                        }
                    }
                }
                Response::Produce(response) => {
                    tracing::info!("Received produce response: {:#?}", response);
                }
                _ => {
                    tracing::error!("Received unexpected response message.");
                }
            }

            if count >= NUMBER_OF_MESSAGES {
                break;
            }
        }
    });

    // Produce to the stream.
    // tracing::trace!("Producing to stream..");
    let payload = std::fs::read_to_string("tests/customer.json").unwrap();

    for _ in 0..NUMBER_OF_MESSAGES {
        produce_client
            .produce("update_customer", payload.as_bytes())
            .unwrap();
    }

    handle_consume.join().unwrap();

    std::fs::remove_dir_all(dir_remove).unwrap();
}
