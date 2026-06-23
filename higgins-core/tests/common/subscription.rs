use super::client_utils::*;
use bytes::BytesMut;
use higgins_codec::CreateSubscriptionRequest;
use higgins_codec::frame::Frame;
use higgins_codec::{Message, message::Type};
use prost::Message as _;

pub fn can_retrieve_data_from_subscription() {
    static STREAM_NAME: &str = "update_customer";

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

    match client.recv(None).unwrap().body {
        ResponseBody::CreateConfiguration(_) => {
            println!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id?,
        _ => panic!("Retrieved unexpected result."),
    };

    // Start a subscription on that stream.
    client.create_subscription(STREAM_NAME.as_bytes()).unwrap();

    let sub_id = match client.recv(None).unwrap().body {
        ResponseBody::CreateSubscription(create_subscription_response) => {
            create_subscription_response.subscription_id.unwrap()
        }
        _ => panic!("Retrieved unexpected result."),
    };

    client.get_subscription(STREAM_NAME, &sub_id).unwrap();

    // Basically asserts that a Getsubscription request was returned
    match client.recv(None).unwrap().body {
        ResponseBody::GetSubscription(_) => {}
        _ => panic!("Retrieved unexpected result."),
    };

    // Concurrently take from the socket.
    std::fs::remove_dir_all(dir_remove).unwrap();
}

use super::data::assert_customer_data;
use super::schema::customer_schema;
use higgins_codec::ProduceResponse;
use higgins_shared::read_arrow;

// #[test]
pub fn subscription_works_with_multiple_clients() {
    static STREAM_NAME: &str = "update_customer";

    static PAYLOAD: &str = include_str!("../customer.json");

    static CONSUME_CLIENT_ONE_COUNT: u64 = 1;
    static CONSUME_CLIENT_TWO_COUNT: u64 = 1;

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

    match produce_client
        .recv(Some(Duration::from_secs(1)))
        .unwrap()
        .body
    {
        ResponseBody::CreateConfiguration(_) => {
            tracing::info!("Retrieved create configuration!");
        }
        _ => panic!("Retrieved unexpected result."),
    };

    let mut consume_client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    let mut second_consume_client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    let sub_id = create_subscription(&mut consume_client, STREAM_NAME);

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    assert_eq!(
        subscription,
        GetSubscriptionResponse {
            errors: vec![],
            stream: Some(STREAM_NAME.to_string()),
            subscription_id: Some(sub_id.clone()),
            offsets: vec! {},
            client_counts: vec![]
        }
    );

    let _ = consume_client.take(
        sub_id.clone(),
        STREAM_NAME.as_bytes(),
        CONSUME_CLIENT_ONE_COUNT,
    );
    let _ = second_consume_client.take(
        sub_id.clone(),
        STREAM_NAME.as_bytes(),
        CONSUME_CLIENT_TWO_COUNT,
    );

    std::thread::sleep(Duration::from_millis(200)); // Await a rough amount of time for subscription to propagate.

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    assert_eq!(
        subscription,
        GetSubscriptionResponse {
            errors: vec![],
            stream: Some(STREAM_NAME.to_string()),
            subscription_id: Some(sub_id.clone()),
            offsets: vec! {},
            client_counts: vec![
                ClientCount {
                    client_id: 2,
                    count: CONSUME_CLIENT_TWO_COUNT
                },
                ClientCount {
                    client_id: 1,
                    count: CONSUME_CLIENT_ONE_COUNT
                }
            ]
        }
    );

    let produce_response = produce(
        &mut produce_client,
        STREAM_NAME,
        PAYLOAD.as_bytes(),
        std::sync::Arc::new(customer_schema()),
    );

    assert_eq!(produce_response, ProduceResponse { errors: vec![] });

    let response = recv_until_take(&mut consume_client);

    let data = &response.records.first().unwrap().data;

    let data = read_arrow(data).unwrap().next().unwrap();

    let record = response.records.first().unwrap();

    assert_eq!(record.offset, 0);
    assert_eq!(record.stream, STREAM_NAME.as_bytes().to_owned());
    assert_eq!(
        record.partition,
        vec![
            49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0
        ],
    );
    assert_customer_data(data.unwrap());

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    assert_eq!(
        subscription,
        GetSubscriptionResponse {
            errors: vec![],
            stream: Some(STREAM_NAME.to_string()),
            subscription_id: Some(sub_id.clone()),
            offsets: vec![KeyOffset {
                key: vec![
                    49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0
                ],
                last_completed_offset: 1,
                max_offset: 0,
            }],
            client_counts: vec![
                ClientCount {
                    client_id: 2,
                    count: CONSUME_CLIENT_TWO_COUNT
                },
                ClientCount {
                    client_id: 1,
                    count: CONSUME_CLIENT_ONE_COUNT - 1
                }
            ]
        }
    );

    let produce_response = produce(
        &mut produce_client,
        STREAM_NAME,
        PAYLOAD.as_bytes(),
        std::sync::Arc::new(customer_schema()),
    );

    assert_eq!(produce_response, ProduceResponse { errors: vec![] });

    let response = recv_until_take(&mut second_consume_client);

    let record = response.records.first().unwrap();

    assert_eq!(record.offset, 1);
    assert_eq!(record.stream, STREAM_NAME.as_bytes().to_owned());
    assert_eq!(
        record.partition,
        vec![
            49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0
        ],
    );

    let data = read_arrow(&record.data).unwrap().next().unwrap().unwrap();

    assert_customer_data(data);

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    dbg!(&subscription);

    let expected = GetSubscriptionResponse {
        errors: vec![],
        stream: Some(STREAM_NAME.to_owned()),
        subscription_id: Some(sub_id.clone()),
        offsets: vec![KeyOffset {
            key: vec![
                49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
            ],
            last_completed_offset: 2,
            max_offset: 1,
        }],
        client_counts: vec![
            ClientCount {
                client_id: 2,
                count: CONSUME_CLIENT_TWO_COUNT - 1,
            },
            ClientCount {
                client_id: 1,
                count: CONSUME_CLIENT_ONE_COUNT - 1,
            },
        ],
    };

    dbg!(&expected);

    assert_eq!(subscription, expected);

    std::fs::remove_dir_all(dir_remove).unwrap();
}

use colored::Colorize;
use higgins_codec::{
    AcknowledgeSubscriptionOffsetsResponse, ClientCount, GetSubscriptionResponse, KeyOffset,
};

use higgins_shared::PartitionName;
use zerocopy::IntoBytes;

pub fn can_update_subscription_with_multiple_values() {
    static STREAM_NAME: &str = "update_customer";

    static PAYLOAD: &str = include_str!("../customer.json");

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

    match produce_client
        .recv(Some(Duration::from_secs(1)))
        .unwrap()
        .body
    {
        ResponseBody::CreateConfiguration(_) => {
            tracing::info!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id?,
        _ => panic!("Retrieved unexpected result."),
    };

    let mut consume_client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    tracing::debug!("{}", "CREATE SUBSCRIPTION".blue());

    let sub_id = create_subscription(&mut consume_client, STREAM_NAME);

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    assert_eq!(
        subscription,
        GetSubscriptionResponse {
            errors: vec![],
            stream: Some(STREAM_NAME.to_string()),
            subscription_id: Some(sub_id.clone()),
            offsets: vec! {},
            client_counts: vec![]
        }
    );

    tracing::debug!("{}", "######## TAKE ########");

    let _ = consume_client.take(sub_id.clone(), STREAM_NAME.as_bytes(), 100);

    // Enough time for this to propagate.
    std::thread::sleep(Duration::from_millis(200));

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    assert_eq!(
        subscription,
        GetSubscriptionResponse {
            errors: vec![],
            stream: Some(STREAM_NAME.to_string()),
            subscription_id: Some(sub_id.clone()),
            offsets: vec! {},
            client_counts: vec![ClientCount {
                client_id: 1,
                count: 100
            }]
        }
    );

    tracing::debug!("##### {} #####", "FIRST PRODUCE");

    produce(
        &mut produce_client,
        STREAM_NAME,
        PAYLOAD.as_bytes(),
        std::sync::Arc::new(customer_schema()),
    );

    let response = recv_until_take(&mut consume_client);

    let record = response.records.first().unwrap();

    assert_eq!(record.offset, 0);
    assert_eq!(record.stream, STREAM_NAME.as_bytes().to_owned());
    assert_eq!(
        record.partition,
        vec![
            49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0
        ],
    );

    let data = higgins_shared::read_arrow(&record.data)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    assert_customer_data(data);

    // assert_eq!(
    //     response,
    //     TakeRecordsResponse {
    //         records: vec![Record {
    //             data: vec![
    //                 123, 34, 97, 103, 101, 34, 58, 50, 49, 44, 34, 102, 105, 114, 115, 116, 95,
    //                 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 34, 44, 34, 105, 100, 34, 58,
    //                 34, 49, 34, 44, 34, 108, 97, 115, 116, 95, 110, 97, 109, 101, 34, 58, 34, 68,
    //                 111, 101, 34, 125, 10,
    //             ],
    //             stream: STREAM_NAME.as_bytes().to_owned(),
    //             partition: vec![
    //                 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    //                 0, 0, 0, 0, 0, 0
    //             ],
    //             offset: 0
    //         }]
    //     }
    // );

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    tracing::debug!("{:#?}", &subscription);

    tracing::debug!("{}", "##### FIRST TAKE #####");

    let acknowledge_response = acknowledge(
        STREAM_NAME,
        &sub_id,
        response
            .records
            .iter()
            .map(|record| {
                (
                    PartitionName::try_from(record.partition.as_bytes()).unwrap(),
                    std::ops::Range {
                        start: record.offset,
                        end: record.offset,
                    },
                )
            })
            .collect(),
        &mut consume_client,
    );

    assert_eq!(
        acknowledge_response,
        AcknowledgeSubscriptionOffsetsResponse {
            stream: STREAM_NAME.to_string(),
            subscription_id: sub_id.clone(),
            failed_offsets: vec![],
            error: "".to_string(),
        }
    );

    tracing::debug!("{}", "###### FIRST ACKNOWLEDGE #######");

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    tracing::debug!("{:#?}", &subscription);

    assert_eq!(
        subscription,
        GetSubscriptionResponse {
            errors: vec![],
            stream: Some(STREAM_NAME.to_owned()),
            subscription_id: Some(sub_id.clone()),
            offsets: vec![KeyOffset {
                key: vec![
                    49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0,
                ],
                last_completed_offset: 1,
                max_offset: 0,
            },],
            client_counts: vec![ClientCount {
                client_id: 1,
                count: 99,
            },],
        }
    );

    tracing::debug!("{}", "SECOND PRODUCE".red());

    let _ = produce(
        &mut produce_client,
        STREAM_NAME,
        PAYLOAD.as_bytes(),
        std::sync::Arc::new(customer_schema()),
    );

    let response = recv_until_take(&mut consume_client);

    assert_eq!(record.offset, 0);
    assert_eq!(record.stream, STREAM_NAME.as_bytes().to_owned());
    assert_eq!(
        record.partition,
        vec![
            49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0
        ],
    );

    let data = higgins_shared::read_arrow(&record.data)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    assert_customer_data(data);

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    tracing::debug!("{:#?}", &subscription);

    assert_eq!(
        subscription,
        GetSubscriptionResponse {
            errors: vec![],
            stream: Some(STREAM_NAME.to_owned()),
            subscription_id: Some(sub_id.clone()),
            offsets: vec![KeyOffset {
                key: vec![
                    49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0,
                ],
                last_completed_offset: 2,
                max_offset: 1,
            },],
            client_counts: vec![ClientCount {
                client_id: 1,
                count: 98,
            },],
        }
    );

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    tracing::debug!("{:#?}", &subscription);

    let acknowledge_response = acknowledge(
        STREAM_NAME,
        &sub_id,
        response
            .records
            .iter()
            .map(|record| {
                (
                    PartitionName::try_from(record.partition.as_bytes()).unwrap(),
                    std::ops::Range {
                        start: record.offset,
                        end: record.offset + 1,
                    },
                )
            })
            .collect(),
        &mut consume_client,
    );

    assert_eq!(
        acknowledge_response,
        AcknowledgeSubscriptionOffsetsResponse {
            stream: STREAM_NAME.to_string(),
            subscription_id: sub_id.clone(),
            failed_offsets: vec![],
            error: "".to_string(),
        }
    );

    std::fs::remove_dir_all(dir_remove).unwrap();
}

use std::{env::temp_dir, time::Duration};

use crate::common::get_random_port;
use higgins::run_server;
use higgins_client::ResponseBody;

pub fn can_update_subscription_after_created() {
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

    match produce_client.recv(None).unwrap().body {
        ResponseBody::CreateConfiguration(_) => {
            println!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id?,
        _ => panic!("Retrieved unexpected result."),
    };

    // Start a subscription on that stream.
    produce_client
        .create_subscription("update_customer".as_bytes())
        .unwrap();

    let sub_id = match produce_client.recv(None).unwrap().body {
        ResponseBody::CreateSubscription(create_subscription_response) => {
            create_subscription_response.subscription_id.unwrap()
        }
        _ => panic!("Retrieved unexpected result."),
    };

    tracing::trace!("Successfully created subscription!");

    //let result_vec = Arc::new(Mutex::new(vec![]));

    // Concurrently take from the socket.
    let handle_consume = std::thread::spawn(move || {
        let mut consume_client =
            higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

        let result = consume_client.take(sub_id, "update_customer".as_bytes(), 100);

        assert!(result.is_ok());

        let mut count = 0;

        loop {
            tracing::trace!("Consuming from stream..");

            let response = consume_client.recv(None).unwrap();

            match response.body {
                ResponseBody::TakeRecords(response) => {
                    //let mut result_vec = result_vec.lock().unwrap();

                    for _ in response.records.iter() {
                        //result_vec.push(String::from_utf8(record.data.clone()).unwrap());
                        count += 1;

                        if count >= NUMBER_OF_MESSAGES {
                            break;
                        }
                    }
                }
                ResponseBody::Produce(response) => {
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
            .produce_json(
                "update_customer",
                payload.as_bytes(),
                std::sync::Arc::new(customer_schema()),
            )
            .unwrap();
    }

    handle_consume.join().unwrap();

    std::fs::remove_dir_all(dir_remove).unwrap();
}
