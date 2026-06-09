use super::get_random_port;
use arrow::{array::AsArray, datatypes::Int32Type};
use higgins::run_server;
use higgins_client::{ResponseBody, blocking::Client};
use higgins_shared::{PartitionName, read_arrow};
use std::{env::temp_dir, time::Duration};

use crate::common::schema::amount_schema;

pub fn can_implement_basic_map() {
    let port = get_random_port();
    tracing::info!("Running on port: {port}");

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

    std::thread::sleep(Duration::from_millis(200)); // Sleep to allow

    let mut client =
        Client::connect(format!("127.0.0.1:{}", port), Some(Duration::from_secs(5))).unwrap();

    client.ping().unwrap();
    client.recv(Some(Duration::from_secs(5))).unwrap();

    let config = std::fs::read_to_string("tests/configs/map_config.toml").unwrap();

    client.upload_configuration(config.as_bytes()).unwrap();
    client.recv(Some(Duration::from_secs(5))).unwrap();

    client
        .upload_module(
            "map",
            &std::fs::read("tests/functions/basic_map.wasm").unwrap(),
        )
        .unwrap();

    client.recv(Some(Duration::from_secs(60))).unwrap();

    client
        .produce_json(
            "amount",
            r#"{"id": "1","data": 1,}"#.as_bytes(),
            std::sync::Arc::new(amount_schema()),
        )
        .unwrap();

    std::thread::sleep(Duration::from_secs(5));

    client.recv(Some(Duration::from_secs(30))).unwrap();

    client
        .query_latest(b"result", &PartitionName::try_from("1").unwrap())
        .unwrap();

    let value = client.recv(Some(Duration::from_secs(10))).unwrap().body;

    let result = match value {
        ResponseBody::GetIndex(response) => response,
        _ => panic!(),
    };

    let arrow = read_arrow(&result.records.first().unwrap().data.clone())
        .next()
        .unwrap()
        .unwrap();

    assert_eq!(
        arrow
            .column_by_name("id")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "1"
    );

    assert_eq!(
        arrow
            .column_by_name("data")
            .unwrap()
            .as_primitive::<Int32Type>()
            .value(0),
        2
    );

    std::fs::remove_dir_all(dir_remove).unwrap();
}
