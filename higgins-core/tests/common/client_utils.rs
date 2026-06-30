#![allow(unused)]

use std::time::{self, Duration};

use arrow_schema::SchemaRef;
use higgins_client::{Response, ResponseBody};
use higgins_codec::{GetSubscriptionResponse, Record, TakeRecordsResponse};
use higgins_shared::{HigginsError, PartitionName};

/// Helper for receiving from a socket until it's taken.
pub fn recv_until_take(
    consume_client: &mut higgins_client::blocking::Client,
) -> Result<TakeRecordsResponse, Box<dyn std::error::Error>> {
    loop {
        match consume_client.recv(Some(Duration::from_secs(10)))?.body {
            ResponseBody::TakeRecords(response) => {
                return Ok(response);
            }
            ResponseBody::Produce(response) => {
                tracing::info!("Received produce response: {:#?}", response);
            }
            _ => {
                tracing::error!("Received unexpected response message.");
                return Err(Box::new(HigginsError::Arbitrary(
                    "Unexpected response message".to_string(),
                )));
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
    let result = client.recv(None).unwrap();
    match result.body {
        ResponseBody::GetSubscription(response) => response,
        _ => panic!("Retrieved unexpected result: {:#?}", result),
    }
}

pub fn create_subscription(
    client: &mut higgins_client::blocking::Client,
    stream_name: &str,
    timeout: Option<u64>,
) -> Vec<u8> {
    client
        .create_subscription(stream_name.as_bytes(), timeout)
        .unwrap();

    match client.recv(None).unwrap().body {
        ResponseBody::CreateSubscription(create_subscription_response) => {
            create_subscription_response.subscription_id.unwrap()
        }
        _ => panic!("Retrieved unexpected result."),
    }
}

pub fn produce(
    client: &mut higgins_client::blocking::Client,
    stream: &str,
    payload: &[u8],
    schema: SchemaRef,
) -> higgins_codec::ProduceResponse {
    println!("Producing! {:#?}", payload);

    client.produce_json(stream, payload, schema).unwrap();

    match client.recv(None).unwrap().body {
        ResponseBody::Produce(response) => response,
        _ => {
            tracing::error!("Received unexpected response message.");
            panic!();
        }
    }
}

pub fn acknowledge(
    stream: &str,
    sub_id: &[u8],
    offsets: Vec<(PartitionName, std::ops::Range<u64>)>,
    client: &mut higgins_client::blocking::Client,
) -> higgins_codec::AcknowledgeSubscriptionOffsetsResponse {
    client.acknowledge(stream, sub_id, offsets).unwrap();

    match client.recv(None).unwrap().body {
        ResponseBody::Acknowledge(response) => response,
        _ => {
            tracing::error!("Received unexpected response message.");
            panic!();
        }
    }
}
