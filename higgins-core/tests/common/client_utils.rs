#![allow(unused)]

use higgins_client::Response;
use higgins_codec::{GetSubscriptionResponse, Record, TakeRecordsResponse};
use higgins_shared::PartitionName;

/// Helper for receiving from a socket until it's taken.
pub fn recv_until_take(
    consume_client: &mut higgins_client::blocking::Client,
) -> TakeRecordsResponse {
    loop {
        match consume_client.recv(None).unwrap() {
            Response::TakeRecords(response) => {
                return response;
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
    let result = client.recv(None).unwrap();
    match result {
        Response::GetSubscription(response) => return response,
        _ => panic!("Retrieved unexpected result: {:#?}", result),
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

pub fn produce(
    client: &mut higgins_client::blocking::Client,
    stream: &str,
    payload: &[u8],
) -> higgins_codec::ProduceResponse {
    client.produce(stream, payload).unwrap();

    match client.recv(None).unwrap() {
        Response::Produce(response) => {
            return response;
        }
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
    client.acknowledge(stream, &sub_id, offsets).unwrap();

    match client.recv(None).unwrap() {
        Response::Acknowledge(response) => {
            return response;
        }
        _ => {
            tracing::error!("Received unexpected response message.");
            panic!();
        }
    }
}
