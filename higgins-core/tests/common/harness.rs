//! Shared helpers for the invariant integration tests.
//!
//! These helpers wrap the common boilerplate of spinning up an in-process
//! Higgins server, connecting a blocking client, producing/decoding Arrow data
//! and managing temporary directories.

#![allow(unused)]

use std::net::TcpListener;
use std::sync::atomic::{AtomicU16, Ordering};
use std::{path::PathBuf, time::Duration};

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use higgins::{ServerHandle, run_server_returning, run_server_returning_with_visibility_timeout};
use higgins_client::{ResponseBody, blocking::Client};
use higgins_shared::{PartitionName, read_arrow};

/// A minimal single-stream configuration (`update_customer`) backed by
/// in-memory storage. The schema matches [`crate::common::schema::customer_schema`].
pub const BASIC_CONFIG: &str = r#"
[storage.memory]
type="memory"

[schema.update_customer_event]
id = "string"
first_name = "string"
last_name = "string"
age = "int32"

[streams.update_customer]
schema = "update_customer_event"
partition_key = "id"
"#;

/// Creates a unique, isolated on-disk directory for a server instance.

/// A `PartitionName` from a string key, panicking on overflow.
pub fn partition(key: &str) -> PartitionName {
    PartitionName::try_from(key).unwrap()
}

/// Boots a server on `port` rooted at `dir` and returns a connected client.

/// Boots a server with an explicit subscription visibility timeout.
pub fn setup_server_with_visibility(
    dir: PathBuf,
    port: u16,
    visibility_timeout: Duration,
) -> (ServerHandle, Client) {
    let handle =
        run_server_returning_with_visibility_timeout(dir, port, visibility_timeout).unwrap();
    std::thread::sleep(Duration::from_millis(150));
    let client =
        Client::connect(format!("127.0.0.1:{port}"), Some(Duration::from_secs(5))).unwrap();
    (handle, client)
}

/// Uploads a configuration and asserts the server acknowledged it without error.
pub fn upload_config(client: &mut Client, config: &str) {
    client.upload_configuration(config.as_bytes()).unwrap();
    match client.recv(Some(Duration::from_secs(5))).unwrap().body {
        ResponseBody::CreateConfiguration(resp) => {
            assert!(
                resp.errors.is_empty(),
                "configuration upload returned errors: {:?}",
                resp.errors
            );
        }
        other => panic!("expected CreateConfiguration response, got {:?}", other),
    }
}

/// Produces a JSON payload and waits for (and asserts) the `ProduceResponse`.
pub fn produce_await(client: &mut Client, stream: &str, json: &str, schema: SchemaRef) {
    client
        .produce_json(stream, json.as_bytes(), schema)
        .unwrap();
    match client.recv(Some(Duration::from_secs(5))).unwrap().body {
        ResponseBody::Produce(_) => {}
        other => panic!("expected Produce response, got {:?}", other),
    }
}

/// Reads a Utf8 column of the first row.
pub fn string_of(batch: &RecordBatch, column: &str) -> String {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string()
}
