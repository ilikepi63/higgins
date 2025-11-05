use crate::common::{
    configuration::upload_configuration, ping::ping_sync, produce_sync, query::query_latest,
    query_latest_arrow,
};
use common::get_random_port;
use higgins::run_server;
use higgins::storage::arrow_ipc::read_arrow;
use serde_json::json;
use std::{env::temp_dir, net::TcpStream, time::Duration};
use tracing_test::traced_test;

mod common;

static CONFIG: &str = r#"[schema.customer]
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
#[traced_test]
fn can_implement_a_basic_stream_join() {
    let port = get_random_port();

    tracing::info!("Running on port: {port}");

    let dir = {
        // let mut dir = temp_dir();
        // dir.push(uuid::Uuid::new_v4().to_string());

        let mut dir = std::path::PathBuf::new();

        dir.push("test_base");

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

    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();

    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    ping_sync(&mut socket);

    upload_configuration(CONFIG.as_bytes(), &mut socket);

    produce_sync(
        b"customer",
        b"1",
        r#"
        {
            "id": "1",
            "first_name": "TestFirstName",
            "last_name": "TestSurname",
            "age": 30
        }
    "#
        .as_bytes(),
        &mut socket,
    )
    .unwrap();

    // let result = query_latest_arrow(b"customer", b"1", &mut socket).unwrap();

    // println!("Customer Result: {:#?}", result);

    std::thread::sleep(Duration::from_secs(1));

    // let result = query_latest_arrow(b"customer_address", b"1", &mut socket).unwrap();

    panic!();

    // produce_sync(
    //     b"address",
    //     b"1",
    //     r#"
    //     {
    //         "customer_id": "1",
    //         "address_line_1": "12 Tennatn Avenut",
    //         "address_line_2": "Bonteheuwel",
    //         "city": "Cape Town",
    //         "province": "Western Cape"
    //     }
    // "#
    //     .as_bytes(),
    //     &mut socket,
    // )
    // .unwrap();

    // let result = query_latest(b"customer_address", b"1", &mut socket).unwrap();

    // let result: serde_json::Value = serde_json::from_slice(&result.first().unwrap().data).unwrap();
    // let expected_result = json!(
    //     {"address_line_1":"12 Tennatn Avenut","address_line_2":"Bonteheuwel","age":30,"city":"Cape Town","customer_first_name":"TestFirstName","customer_id":"1","customer_last_name":"TestSurname","province":"Western Cape"}
    // );

    // assert_eq!(result, expected_result);

    // std::fs::remove_dir_all(dir_remove).unwrap();
}
