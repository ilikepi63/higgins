# higgins

higgins is an experimental streaming platform that offers some quality of life changes to existing offerings. You declare *streams* with typed schemas, extend them with derived streams, and publish, subscribe, and query them. Higgins keeps the derived streams continuously in sync as new records arrive, and lets you query any stream by partition key and offset or consume it reactively via subscriptions.

## Features

#### Apache Arrow
Every stream is schema'd and records are stored as Arrow, initially this was simply for schema, but wil be extended to make use of Arrow's columnar format for streams.

#### Derived Stream Operators
Maps, reduces, joins, and windows can be applied to other streams, data is copied and debuggable.

#### User Defined Functions
Map and reduce transformations run as UDF's.

#### Push and Pull semantics
Consumers can subscribe to streams, or directly poll them.

#### Configurable storage
Backing storage is configurable, initially only for object storage.

The indexes and the storage are naturally detached logically, although it is fully possible 
to build file or memory based storage mechanisms. 

## Quick start

### 1. Start the server

```bash
./higgins --port=4932 --dir=data
```

- `--port` — TCP port to listen on (default `4932`).
- `--dir` — root directory for the index / topography log (default `data`).

### 2. Write a topography
```bash
touch config.toml:
```

```toml
[storage.memory]
type = "memory"

[schema.amount]
id   = "string"
data = "int32"

[streams.amount]
schema        = "amount"
partition_key = "id"
```

### 3. Upload it and produce

Using the CLI:

```bash
# upload the topography
cargo run -p higgins-cli -- --port 4932 create-configuration --file=config.toml

# health check
cargo run -p higgins-cli -- --port 4932 ping
```

Or via the Rust client library:

```rust
use std::{sync::Arc, time::Duration};

use higgins_client::{Client, ResponseBody};
use higgins_shared::{PartitionName, read_arrow};
use arrow_schema::{DataType, Field, Schema};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("127.0.0.1:4932", Some(Duration::from_secs(5))).await?;

    // Upload the configuration.
    client.upload_configuration(include_bytes!("config.toml")).await?;
    client.recv(None).await?;

    // Produce a record.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id",   DataType::Utf8,  false),
        Field::new("data", DataType::Int32, false),
    ]));
    client.produce_json("amount", br#"{"id":"key","data":21}"#, schema).await?;
    client.recv(None).await?; 

    // Read the latest record for the key..
    client.query_latest(b"amount", &PartitionName::try_from("key")?).await?;
    if let ResponseBody::GetIndex(resp) = client.recv(Some(Duration::from_secs(5))).await?.body {
        if let Some(batch) = read_arrow(&resp.records[0].data)?.next()?{
            println!("{batch:?}");
        };
    }
    Ok(())
}
```

---

## Core concepts

### Topography

A `Configuration` is the building block that states what should be configured on the broker. Once a configuration is uploaded, it is merged into the broker's topography, which can be queried and shared amongst different brokers. It has three kinds of blocks:

```toml
[storage.<name>]   # a configurable storage mechanism to hold the records that are produced.
[schema.<name>]    # a schema that is keyed by different streams.
[streams.<name>]   # a declared stream.
```

### Storage backends

```toml
[storage.memory]
type = "memory"         
```

Object storage (specifically S3-compatible) is also supported via:

```toml
[storage.s3]
type            = "s3"
aws_access_key_id   = ""
aws_secret_access_key = ""
aws_region          = ""
aws_endpoint        = ""
```

### Schemas & data types

A schema maps field names to Arrow types:

```toml
[schema.customer]
id         = "string"
first_name = "string"
age        = "int32"
```

Supported type tags:

`string`, `large_string`, `bytes`, `large_bytes`, `bool`,
`int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`,
`float16`, `float32`, `float64`, `date32`, `date64`, `decimal`.

These map one-for-one to their respective Arrow types.

### Streams, partitions & offsets

Every stream declares a `partition_key` — a column whose value shards records. Within each `(stream, partition)` records get a offset which represents the log for that stream/key pair. You can query these streams in one of two ways:

- `query_at(stream, partition, offset)` — the record at an exact offset.
- `query_latest(stream, partition)` — the highest committed offset.

but you can also subscribe to these streams via the subscription mechanism and retrieve them asynchronously.

### Derived streams

A stream can be derived from other streams: 

| `type` | Purpose | Extra keys |
|---|---|---|
| `map` | maps values from one stream to another with a UDF | `fn` |
| `reduce` | folds n into n + 1 with a UDF | `fn` |
| `join` | join two streams by key | `join`, `map` |
| `window` | aggregate a group of records by count | `window` |


## Map

A map stream applies a UDF to each value inside of a derivative stream. The result is written to the derived stream in the same stream/key as the base stream.

Example: 

```toml
[storage.memory]
type = "memory"

[schema.amount]
id   = "string"
data = "int32"

[streams.amount]
schema        = "amount"
partition_key = "id"

[streams.result]
base          = "amount"
type          = "map"
partition_key = "id"
schema        = "amount"
fn            = "double"        # name of the UDF, should be uploaded before this configuration is uploaded.
```

if  the `double` UDF doubles the `data` column, this would be the mappings: 

```json
{"id":"1","data":1} -> {"id":"1","data":2}
{"id":"2","data":2} -> {"id":"2","data":4}
{"id":"3","data":3} -> {"id":"3","data":6} 
```

```rust
// upload the compiled module under the name referenced by `fn = "map"`
client.upload_module("map", &std::fs::read("basic_map.wasm")?).await?;
client.recv(None).await?;

client.produce_json("amount", br#"{"id":"1","data":1}"#, amount_schema).await?;
client.recv(None).await?;

client.query_at(b"result", &PartitionName::try_from("1")?, 0).await?; // -> data = 2
```


## Reduce

A `reduce` folds each value into the previous using a user-defined function, accumulating values over time. So `n` in the derived stream represents `fn(n-1, n)` for all `0..n` offsets.

```toml
[streams.result]
base          = "amount"
type          = "reduce"
partition_key = "id"
schema        = "amount"
fn            = "sum" # UDF to be used.
```

The example `reduce` function keeps a running sum. Producing `data` values `1, 2, 3` yields derived results `1, 3, 6`:

| base offset | input `data` | reduce result |
|---|---|---|
| 0 | 1 | 1  (no previous — first record) |
| 1 | 2 | 3  (`2 + 1`) |
| 2 | 3 | 6  (`3 + 3`) |


## Joins

A `join` zips two streams into one, using the key to match records from each stream. Higgins carries forward the most recent contribution from each side, so a joined record reflects the latest known state of both streams for that key. Joins can join multiple streams.

```toml
[schema.customer]
id         = "string"
first_name = "string"
last_name  = "string"
age        = "int32"

[schema.address]
customer_id    = "string"
address_line_1 = "string"
city           = "string"
province       = "string"

[schema.customer_address]
customer_id         = "string"
customer_first_name = "string"
age                 = "int32"
address_line_1      = "string"
city                = "string"

[streams.customer]
schema        = "customer"
partition_key = "id"

[streams.address]
schema        = "address"
partition_key = "customer_id"

[streams.customer_address]
type          = "join"
schema        = "customer_address"
partition_key = "customer_id"
base          = "customer"
join          = { type = "inner", stream = "address" }
# How the derivative stream is constructed.
map = {
    customer_id         = "customer.id",
    customer_first_name = "customer.first_name",
    age                 = "customer.age",
    address_line_1      = "address.address_line_1",
    city                = "address.city",
}
```

### Event Sequence

1. Produce to customer topic
```json
{"id": "1", "first_name": "John", "age": 30, "last_name": "Doe"}
```

2. Customer_address after update
```json
{"customer_id": "1", "customer_first_name": "John", "age": 30, "address_line_1": null, "city": null}
```

3. Produce to address topic (key: 1)  
```json
{"customer_id": "1", "address_line_1": "1 John Road", "city": "John City", "province": "Province"}
```

4. customer_address after update
```json
{"customer_id": "1", "customer_first_name": "John", "age": 30, "address_line_1": "1 John Road", "city": "John City"}
```

## Windowing

A window groups the most recent n records of the base stream into each output record.

```toml
[schema.value]
id         = "string"
some_data  = "string"
other_data = "string"
i          = "int32"

[streams.value]
schema        = "value"
partition_key = "id"

[streams.value_windowed]
base          = "value"
type          = "window"
partition_key = "id"
schema        = "value"
window        = { type = "count", interval = "5" }   # grouped into 5 records each
```

Base stream records:

```json
[
  { "id": "1", "some_data": "" },
  { "id": "2", "some_data": "" },
  { "id": "3", "some_data": "" },
  { "id": "4", "some_data": "" },
  { "id": "5", "some_data": "" },
  { "id": "6", "some_data": "" },
  { "id": "7", "some_data": "" },
  { "id": "8", "some_data": "" },
  { "id": "9", "some_data": "" },
]
```

Derived stream records: 

```json
[
  [
    { "id": "1", "some_data": "" },
    { "id": "2", "some_data": "" },
    { "id": "3", "some_data": "" },
    { "id": "4", "some_data": "" },
    { "id": "5", "some_data": "" },
  ],
  [
    { "id": "6", "some_data": "" },
    { "id": "7", "some_data": "" },
    { "id": "8", "some_data": "" },
    { "id": "9", "some_data": "" },
  ],
]
```


## Writing User defined functions

Map and reduce transformations are done through UDFs that are compiled to wasm.

### Rust example:

The wasm exposes an entry point `run` function and a `_malloc` function for allocating data.

The core of the example `double` looks like:

```rust
use arrow::array::{AsArray, Int32Array};
use arrow::datatypes::Int32Type;
use higgins_functions::{record_batch_from_ffi, record_batch_to_ffi};

#[unsafe(no_mangle)]
pub unsafe fn run(rb_ptr: *const u8) -> *const u8 {
    let batch = record_batch_from_ffi(rb_ptr);

    let col = batch.column_by_name("data").unwrap().as_primitive::<Int32Type>();
    let doubled: Int32Array = col.iter().map(|v| v.map(|v| v * 2)).collect();

    // rebuild the batch with the transformed column, then hand it back:
    record_batch_to_ffi(/* new batch */)
}
```

Upload the compiled module with the client library: 

```rust
client.upload_module("map", &std::fs::read("my_function.wasm")?).await?;
```

The first argument you pass to `upload_module` is what a stream references via `fn = "..."`. Example modules can be found in `higgins-core/tests/functions/` (`basic_map.wasm`, `basic_reduce.wasm`) with their sources in `basic-map/` and `basic-reduce/`. 

## Subscriptions

Beyond point queries, consumers can subscribe and be pushed records reactively:

```rust
let _ = client.create_subscription(b"amount").await?;

let subscription_id = match client.recv().await?.body {
    ResponseBody::SubscriptionId(id) => id,
    _ => unreachable!(),
};


/// Express a numbered interest in the subscription.
client.take(subscription_id.clone(), b"amount", 1).await?;     

/// Data is pushed asynchronously as it is produced to the stream.
let data = match client.recv().await?.body {
    ResponseBody::TakeRecords(data) => data,
    _ => unreachable!(),
};


/// Acknowledge a range of records, durably advancing the subscription watermark.
/// If this doesn't get called, the mark times out after some configurable 
/// time and makes the records rediscoverable.
client.acknowledge("amount", &subscription_id, vec![(partition, 0..0)]).await?; 
```

## Development

```bash
cargo build                                  # build the workspace
cargo test -p higgins                        # broker unit + integration tests
cargo test -p higgins --test basic           # end-to-end feature/invariant suite
RUST_LOG=higgins=debug cargo test --test basic <name> -- --nocapture   # with logs
cargo clippy --workspace
```

The integration tests in `higgins-core/tests/` can also be used to discover the API for now. 

## Status

Higgins is still very much a work in progress, but it is already usable for some simple stream processing tasks. If you have any feedback or suggestions, please open an issue or submit a pull request. Please contact me directly if you'd like to use higgins inside of your project or organisation, more than happy to help out!

## License

Licensed under the Apache License, Version 2.0.
