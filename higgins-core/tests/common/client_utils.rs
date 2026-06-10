#![allow(unused)]

use arrow_schema::SchemaRef;
use higgins_client::{Response, ResponseBody};
use higgins_codec::{GetSubscriptionResponse, Record, TakeRecordsResponse};
use higgins_shared::PartitionName;

/// Helper for receiving from a socket until it's taken.
pub fn recv_until_take(
    consume_client: &mut higgins_client::blocking::Client,
) -> TakeRecordsResponse {
    loop {
        match consume_client.recv(None)?.body {
            ResponseBody::TakeRecords(response) => {
                return response;
            }
            ResponseBody::Produce(response) => {
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
    String::from_utf8(record.data.to_owned())?
}

pub fn get_subscription_data(
    stream: &str,
    sub_id: &[u8],
    client: &mut higgins_client::blocking::Client,
) -> GetSubscriptionResponse {
    client.get_subscription(stream, sub_id)?;

    // Basically asserts that a Getsubscription request was returned
    let result = client.recv(None)?;
    match result.body {
        ResponseBody::GetSubscription(response) => return response,
        _ => panic!("Retrieved unexpected result: {:#?}", result),
    };
}

pub fn create_subscription(
    client: &mut higgins_client::blocking::Client,
    stream_name: &str,
) -> Vec<u8> {
    client.create_subscription(stream_name.as_bytes())?;

    let sub_id = match client.recv(None)?.body {
        ResponseBody::CreateSubscription(create_subscription_response) => {
            create_subscription_response.subscription_id?
        }
        _ => panic!("Retrieved unexpected result."),
    };

    sub_id
}

pub fn produce(
    client: &mut higgins_client::blocking::Client,
    stream: &str,
    payload: &[u8],
    schema: SchemaRef,
) -> higgins_codec::ProduceResponse {
    println!("Producing! {:#?}", payload);

    client.produce_json(stream, payload, schema)?;

    match client.recv(None)?.body {
        ResponseBody::Produce(response) => {
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
    client.acknowledge(stream, &sub_id, offsets)?;

    match client.recv(None)?.body {
        ResponseBody::Acknowledge(response) => {
            return response;
        }
        _ => {
            tracing::error!("Received unexpected response message.");
            panic!();
        }
    }
}
