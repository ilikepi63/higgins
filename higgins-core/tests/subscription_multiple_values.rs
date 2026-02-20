use std::{env::temp_dir, time::Duration};

use crate::common::get_random_port;
use colored::Colorize;
use higgins::run_server;
use higgins_client::ResponseBody;
use higgins_codec::{
    AcknowledgeSubscriptionOffsetsResponse, ClientCount, GetSubscriptionResponse, KeyOffset,
    Record, TakeRecordsResponse,
};
use higgins_shared::PartitionName;
use zerocopy::IntoBytes;

mod common;

use common::client_utils::*;

static STREAM_NAME: &str = "update_customer";

static PAYLOAD: &str = include_str!("customer.json");

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

    match produce_client
        .recv(Some(Duration::from_secs(1)))
        .unwrap()
        .body
    {
        ResponseBody::CreateConfiguration(_) => {
            tracing::info!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id.unwrap(),
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

    tracing::debug!("{}", "TAKE".red());

    let _ = consume_client.take(sub_id.clone(), STREAM_NAME.as_bytes(), 100);

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

    tracing::debug!("{}", "FIRST PRODUCE".red());

    produce(&mut produce_client, STREAM_NAME, PAYLOAD.as_bytes());

    let response = recv_until_take(&mut consume_client);

    assert_eq!(
        response,
        TakeRecordsResponse {
            records: vec![Record {
                data: vec![
                    123, 34, 97, 103, 101, 34, 58, 50, 49, 44, 34, 102, 105, 114, 115, 116, 95,
                    110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 34, 44, 34, 105, 100, 34, 58,
                    34, 49, 34, 44, 34, 108, 97, 115, 116, 95, 110, 97, 109, 101, 34, 58, 34, 68,
                    111, 101, 34, 125, 10,
                ],
                stream: STREAM_NAME.as_bytes().to_owned(),
                partition: vec![
                    49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0
                ],
                offset: 0
            }]
        }
    );

    tracing::debug!("{}", "FIRST TAKE".red());

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

    tracing::debug!("{}", "FIRST ACKNOWLEDGE".red());

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

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
                max_offset: 1,
                amount_to_take: 0,
            },],
            client_counts: vec![ClientCount {
                client_id: 1,
                count: 99,
            },],
        }
    );

    tracing::debug!("{}", "SECOND PRODUCE".red());

    let produce_response = produce(&mut produce_client, STREAM_NAME, PAYLOAD.as_bytes());

    println!("Produce Response: {:#?}", produce_response);

    let response = recv_until_take(&mut consume_client);

    assert_eq!(
        response,
        TakeRecordsResponse {
            records: vec![Record {
                data: vec![
                    123, 34, 97, 103, 101, 34, 58, 50, 49, 44, 34, 102, 105, 114, 115, 116, 95,
                    110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 34, 44, 34, 105, 100, 34, 58,
                    34, 49, 34, 44, 34, 108, 97, 115, 116, 95, 110, 97, 109, 101, 34, 58, 34, 68,
                    111, 101, 34, 125, 10,
                ],
                stream: STREAM_NAME.as_bytes().to_owned(),
                partition: vec![
                    49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0
                ],
                offset: 1
            }]
        }
    );

    let subscription = get_subscription_data(STREAM_NAME, &sub_id, &mut consume_client);

    dbg!(&subscription);

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
                max_offset: 2,
                amount_to_take: 0
            },],
            client_counts: vec![ClientCount {
                client_id: 1,
                count: 98,
            },],
        }
    );

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
