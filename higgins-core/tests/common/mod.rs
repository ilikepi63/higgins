#![allow(unused)]

use std::path::PathBuf;
use std::time::Duration;

use arrow_schema::SchemaRef;
use bytes::BytesMut;
use higgins::{ServerHandle, run_server_returning};
use higgins_client::ResponseBody;
use higgins_client::blocking::Client;
use higgins_codec::frame::Frame;
use higgins_codec::{Message, ProduceRequest, message::Type};
use higgins_codec::{ProduceResponse, TakeRecordsRequest};
use prost::Message as _;

pub mod client_utils;
pub mod concurrency_tests;
pub mod configuration;
pub mod data;
pub mod functions;
pub mod invariant_tests;
pub mod join;
pub mod ping;
mod port;
pub mod query;
pub mod schema;
pub mod subscription;
pub use port::get_random_port;

pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();
}

pub mod basic;
pub mod map;
pub mod reduce;
pub mod topography;
pub mod windowing;

/// Produce synchronously to a listener awaiting the response.
pub fn produce_sync(client: &mut Client, stream: &str, json: &str, schema: SchemaRef) {
    client
        .produce_json(stream, json.as_bytes(), schema)
        .unwrap();
    match client.recv(Some(Duration::from_secs(5))).unwrap().body {
        ResponseBody::Produce(_) => {}
        other => panic!("expected Produce response, got {:?}", other),
    }
}

#[allow(unused)]
pub fn consume<T: std::io::Read + std::io::Write>(
    sub_id: Vec<u8>,
    stream_name: &[u8],
    socket: &mut T,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let take_request = TakeRecordsRequest {
        n: 1,
        subscription_id: sub_id,
        stream_name: stream_name.to_vec(),
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(8048);

    Message {
        r#type: Type::Takerecordsrequest as i32,
        take_records_request: Some(take_request),
        ..Default::default()
    }
    .encode(&mut write_buf)?;

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write(socket).unwrap();

    let frame = Frame::try_read(socket).unwrap();

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    let result = match Type::try_from(message.r#type).unwrap() {
        Type::Takerecordsresponse => {
            tracing::info!("Receieved a take records response!");

            let take_records_response = message.take_records_response.unwrap();

            tracing::info!("Records_Response: {:#?}", take_records_response);

            let record = take_records_response.records.first().unwrap();

            record.data.clone()
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(result)
}

// Utilities.
pub fn setup_server(dir: PathBuf, port: u16) -> (ServerHandle, Client) {
    let handle = run_server_returning(dir, port).unwrap();
    std::thread::sleep(Duration::from_millis(150));
    let client =
        Client::connect(format!("127.0.0.1:{port}"), Some(Duration::from_secs(5))).unwrap();
    (handle, client)
}

pub fn unique_dir() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    dir.push(format!("higgins-it-{}", uuid::Uuid::new_v4()));
    Some(dir)
}
