use std::{
    env::temp_dir,
    io::Read,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::common::get_random_port;
use higgins::run_server;
use higgins_codec::{Message, TakeRecordsRequest, message::Type};
use higgins_shared::PartitionName;
use prost::Message as _;

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
    // Start a subscription on that stream.
    let sub_id = produce_client
        .create_subscription("update_customer".as_bytes())
        .unwrap();

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

            let response = consume_client.recv().unwrap();

            match response {}

            match Type::try_from(message.r#type).unwrap() {
                Type::Takerecordsresponse => {
                    let take_records_response = message.take_records_response.unwrap();

                    let mut result_vec = result_vec.lock().unwrap();

                    for record in take_records_response.records.iter() {
                        result_vec.push(String::from_utf8(record.data.clone()).unwrap());
                        count += 1;

                        if count >= NUMBER_OF_MESSAGES {
                            break;
                        }
                    }
                }
                Type::Produceresponse => {
                    let message = message.produce_response;

                    tracing::info!("Received produce response: {:#?}", message);
                }
                _ => {}
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
            .produce(
                STREAM,
                &PartitionName::try_from(PARTITION).unwrap(),
                payload.as_bytes(),
            )
            .unwrap();
    }

    handle_consume.join().unwrap();

    std::fs::remove_dir_all(dir_remove).unwrap();
}
