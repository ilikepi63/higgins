use std::{env::temp_dir, time::Duration};

use crate::common::{get_random_port, schema::customer_schema};
use higgins::run_server;
use higgins_client::ResponseBody;
use higgins_codec::{ClientCount, GetSubscriptionResponse, KeyOffset, ProduceResponse};

mod common;

use common::client_utils::*;
use higgins_shared::read_arrow;

static STREAM_NAME: &str = "update_customer";

static PAYLOAD: &str = include_str!("customer.json");

static CONSUME_CLIENT_ONE_COUNT: u64 = 1;
static CONSUME_CLIENT_TWO_COUNT: u64 = 1;

#[test]
fn subscription_works_with_multiple_clients() {
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

    let data = &response.records.get(0).unwrap().data;

    let data = read_arrow(data).next().unwrap();

    let record = response.records.get(0).unwrap();

    assert_eq!(record.offset, 0);
    assert_eq!(record.stream, STREAM_NAME.as_bytes().to_owned());
    assert_eq!(
        record.partition,
        vec![
            49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0
        ],
    );
    common::data::assert_customer_data(data.unwrap());

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

    let record = response.records.get(0).unwrap();

    assert_eq!(record.offset, 1);
    assert_eq!(record.stream, STREAM_NAME.as_bytes().to_owned());
    assert_eq!(
        record.partition,
        vec![
            49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0
        ],
    );

    let data = read_arrow(&record.data).next().unwrap().unwrap();

    common::data::assert_customer_data(data);

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
