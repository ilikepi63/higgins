use common::get_random_port;
use higgins::run_server;
use higgins::storage::arrow_ipc::read_arrow;
use higgins_client::ResponseBody;
use higgins_client::blocking::Client;
use higgins_shared::PartitionName;
use std::panic::catch_unwind;
use std::time::Duration;

mod common;

static CONFIG: &str = r#"
[storage.memory]
type="memory"

[schema.customer]
id = "string"
first_name = "string"
last_name = "string"
age = "int32"

[schema.address]
customer_id = "string"
address_line_1 = "string"
address_line_2 = "string"
city = "string"
province = "string"

[schema.customer_address]
customer_id = "string"
customer_first_name = "string"
customer_last_name = "string"
age = "int32"
address_line_1 = "string"
address_line_2 = "string"
city = "string"
province = "string"

[streams.customer]
schema = "customer"
partition_key = "id"

[streams.address]
schema = "address"
partition_key = "customer_id"

[streams.customer_address]
type = "join"
schema = "customer_address"
partition_key = "customer_id"
base = "customer"
join = [
    "customer", "address"
]

[streams.customer_address.map]
customer_id = "customer.id"
customer_first_name = "customer.first_name"
customer_last_name = "customer.last_name"
age = "customer.age"
address_line_1 = "address.address_line_1"
address_line_2 = "address.address_line_2"
city = "address.city"
province = "address.province"
"#;

#[test]
fn can_implement_a_basic_stream_join() {
    let port = get_random_port();
    tracing_subscriber::fmt::init();

    tracing::info!("Running on port: {port}");

    let dir = {
        let mut dir = std::path::PathBuf::new();

        dir.push(uuid::Uuid::new_v4().to_string());

        dir
    };

    if dir.exists() {
        std::fs::remove_dir_all(dir.clone()).unwrap();
    }

    let dir_remove = dir.clone();

    let _ = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(run_server(dir, port));
    });

    std::thread::sleep(Duration::from_millis(200)); // Sleep to allow

    let result = catch_unwind(|| {
        let mut client = Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

        client.ping().unwrap();

        client.recv(None).unwrap();

        client.upload_configuration(CONFIG.as_bytes()).unwrap();

        client.recv(None).unwrap();

        client
            .produce(
                "customer",
                r#"
            {
                "id": "1",
                "first_name": "TestFirstName",
                "last_name": "TestSurname",
                "age": 30
            }
        "#
                .as_bytes(),
            )
            .unwrap();

        client.recv(None).unwrap();

        std::thread::sleep(Duration::from_secs(1));

        let _ = client
            .query_latest(b"customer_address", &PartitionName::try_from("1").unwrap())
            .unwrap();

        let bytes = match client.recv(Some(Duration::from_secs(5))).unwrap().body {
            ResponseBody::GetIndex(get_index) => get_index.records.get(0).unwrap().data.clone(),
            _ => panic!("Incorrect response received"),
        };

        let record_batch = read_arrow(&bytes).nth(0).unwrap().unwrap();

        // dbg!(record_batch);

        assert_eq!(record_batch, create_batch_with_nulled_values_in_address());

        client
            .produce(
                "address",
                r#"
            {
                "customer_id": "1",
                "address_line_1": "12 Tennatn Avenut",
                "address_line_2": "Bonteheuwel",
                "city": "Cape Town",
                "province": "Western Cape"
            }
        "#
                .as_bytes(),
            )
            .unwrap();

        std::thread::sleep(Duration::from_secs(1));

        client.recv(None).unwrap();

        client
            .query_latest(b"customer_address", &PartitionName::try_from("1").unwrap())
            .unwrap();

        let bytes = match client.recv(Some(Duration::from_secs(5))).unwrap().body {
            ResponseBody::GetIndex(get_index) => get_index.records.get(0).unwrap().data.clone(),
            _ => panic!("Incorrect response received"),
        };

        let record_batch = read_arrow(&bytes).nth(0).unwrap().unwrap();

        assert_eq!(record_batch, create_test_customer_address_data());
    });

    std::fs::remove_dir_all(dir_remove).unwrap();

    result.unwrap()
}

use arrow::array::RecordBatch;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub fn create_test_customer_address_data() -> RecordBatch {
    // Define the schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("address_line_1", DataType::Utf8, true),
        Field::new("address_line_2", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("customer_first_name", DataType::Utf8, true),
        Field::new("customer_id", DataType::Utf8, true),
        Field::new("customer_last_name", DataType::Utf8, true),
        Field::new("province", DataType::Utf8, true),
    ]));

    // Create the arrays with the provided data (one row, non-null values)
    let address_line_1 = Arc::new(StringArray::from(vec!["12 Tennatn Avenut"]));
    let address_line_2 = Arc::new(StringArray::from(vec!["Bonteheuwel"]));
    let age = Arc::new(Int32Array::from(vec![30]));
    let city = Arc::new(StringArray::from(vec!["Cape Town"]));
    let customer_first_name = Arc::new(StringArray::from(vec!["TestFirstName"]));
    let customer_id = Arc::new(StringArray::from(vec!["1"]));
    let customer_last_name = Arc::new(StringArray::from(vec!["TestSurname"]));
    let province = Arc::new(StringArray::from(vec!["Western Cape"]));

    // Create the RecordBatch

    RecordBatch::try_new(
        schema,
        vec![
            address_line_1,
            address_line_2,
            age,
            city,
            customer_first_name,
            customer_id,
            customer_last_name,
            province,
        ],
    )
    .unwrap()
}

pub fn create_batch_with_nulled_values_in_address() -> RecordBatch {
    // Define the schema (same as before)
    let schema = Arc::new(Schema::new(vec![
        Field::new("address_line_1", DataType::Utf8, true),
        Field::new("address_line_2", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("customer_first_name", DataType::Utf8, true),
        Field::new("customer_id", DataType::Utf8, true),
        Field::new("customer_last_name", DataType::Utf8, true),
        Field::new("province", DataType::Utf8, true),
    ]));

    // Create the arrays with the provided data (one row, with nulls where specified)
    let address_line_1 = Arc::new(StringArray::from(vec![None::<&str>]));
    let address_line_2 = Arc::new(StringArray::from(vec![None::<&str>]));
    let age = Arc::new(Int32Array::from(vec![Some(30i32)]));
    let city = Arc::new(StringArray::from(vec![None::<&str>]));
    let customer_first_name = Arc::new(StringArray::from(vec!["TestFirstName"]));
    let customer_id = Arc::new(StringArray::from(vec!["1"]));
    let customer_last_name = Arc::new(StringArray::from(vec!["TestSurname"]));
    let province = Arc::new(StringArray::from(vec![None::<&str>]));

    // Create the RecordBatch

    RecordBatch::try_new(
        schema,
        vec![
            address_line_1,
            address_line_2,
            age,
            city,
            customer_first_name,
            customer_id,
            customer_last_name,
            province,
        ],
    )
    .unwrap()
}
