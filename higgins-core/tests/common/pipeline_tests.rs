//! End-to-end pipeline test: every stream type in one dependency graph, each
//! with its own subscription.
//!
//! The topography wires all five kinds of stream so that each derivation feeds
//! off another:
//!
//! ```text
//!   readings (base)
//!     ├── doubled        = map(readings)          // ×2 on `data`
//!     │     └── running_total = reduce(doubled)   // running sum
//!     │            └── recent = window(running_total)  // last 5
//!     └── enriched       = join(readings, doubled)
//! ```
//!
//! A subscription is registered on every stream, then a single record is
//! produced to the base. The derivation must ripple through the whole graph and
//! each subscription must be answered with a delivery.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use higgins_client::ResponseBody;

use crate::common::{
    client_utils::{create_subscription, recv_until_take},
    configuration::upload_configuration_sync,
    functions::upload_module_sync,
    get_random_port,
    schema::amount_schema,
    setup_server, unique_dir,
};

const CONFIG: &str = r#"
[storage.memory]
type = "memory"

[schema.reading]
id = "string"
data = "int32"

[schema.enriched]
id = "string"
data = "int32"
doubled = "int32"

[streams.readings]
schema = "reading"
partition_key = "id"

[streams.doubled]
base = "readings"
type = "map"
schema = "reading"
partition_key = "id"
fn = "map"

[streams.running_total]
base = "doubled"
type = "reduce"
schema = "reading"
partition_key = "id"
fn = "reduce"

[streams.recent]
base = "running_total"
type = "window"
schema = "reading"
partition_key = "id"
window = { type = "count", interval = "5" }

[streams.enriched]
base = "readings"
type = "join"
schema = "enriched"
partition_key = "id"
join = { type = "inner", stream = "doubled" }
map = { id = "readings.id", data = "readings.data", doubled = "doubled.data" }
"#;

const STREAMS: [&str; 5] = ["readings", "doubled", "running_total", "recent", "enriched"];

pub fn every_stream_type_answers_its_subscription() {
    let port = get_random_port();
    let dir = unique_dir().unwrap();

    let result = std::panic::catch_unwind(|| {
        let (handle, mut client) = setup_server(dir.clone(), port);

        // Upload configuration.
        upload_configuration_sync(CONFIG, &mut client);

        // Upload map + reduce modules.
        upload_module_sync(
            "map",
            &std::fs::read("tests/functions/basic_map.wasm").unwrap(),
            &mut client,
        );
        upload_module_sync(
            "reduce",
            &std::fs::read("tests/functions/basic_reduce.wasm").unwrap(),
            &mut client,
        );

        // Setup all subscriptions for these different streams.
        let mut subscriptions = Vec::new();
        for stream in STREAMS {
            let sub_id = create_subscription(&mut client, stream, None);
            client.take(sub_id.clone(), stream.as_bytes(), 1).unwrap();
            subscriptions.push((stream, sub_id));
        }

        // produce to base stream.
        client
            .produce_json(
                "readings",
                r#"{"id":"1","data":21}"#.as_bytes(),
                Arc::new(amount_schema()),
            )
            .unwrap();

        if let Ok(produce_response) = client.recv(Some(Duration::from_secs(60))) {
            tracing::debug!("Successfully produced the json!");
            tracing::debug!("{:#?}", produce_response);
        }

        let mut answered: HashMap<String, bool> =
            HashMap::from(STREAMS.map(|name| (name.to_owned(), false)));

        while answered.len() < STREAMS.len() {
            let take = match recv_until_take(&mut client) {
                Ok(t) => t,
                _ => {
                    break;
                }
            };

            tracing::debug!("We did return a take response!");

            let record = match take.records.first() {
                Some(r) => r,
                _ => {
                    break;
                }
            };

            let stream = String::from_utf8_lossy(&record.stream).to_string();
            if let Some(stream) = answered.get_mut(&stream) {
                *stream = true;
            };
        }

        tracing::info!("Result: {:#?}", answered);

        if answered.values().any(|b| !b.clone()) {
            panic!("Some values weren't answered :<");
        }

        handle.close();
    });

    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}
