use super::get_random_port;
use super::schema::amount_schema;
use arrow::array::AsArray;
use arrow::datatypes::Int32Type;
use higgins::run_server;
use higgins_client::ResponseBody;
use higgins_client::blocking::Client;
use higgins_shared::{PartitionName, read_arrow};
use std::{env::temp_dir, time::Duration};

pub fn can_implement_basic_reduce() {
    {
        // Delete the current files for this..
        let _ = std::fs::remove_dir("result");
        let _ = std::fs::remove_dir("amount");
    }

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

    let config = std::fs::read_to_string("tests/configs/reduce_config.toml").unwrap();

    client.upload_configuration(config.as_bytes()).unwrap();
    client.recv(Some(Duration::from_secs(5))).unwrap();

    client
        .upload_module(
            "reduce",
            &std::fs::read("tests/functions/basic_reduce.wasm").unwrap(),
        )
        .unwrap();

    client.recv(Some(Duration::from_mins(1))).unwrap();

    client
        .produce_json(
            "amount",
            r#"
                {
                    "id": "1",
                    "data": 1,
                }
            "#
            .as_bytes(),
            std::sync::Arc::new(amount_schema()),
        )
        .unwrap();

    std::thread::sleep(Duration::from_secs(1));

    client.recv(Some(Duration::from_secs(5))).unwrap(); // await initial produce.

    client
        .query_at(b"result", &PartitionName::try_from("1").unwrap(), 0)
        .unwrap();

    let result = match client.recv(Some(Duration::from_secs(5))).unwrap().body {
        ResponseBody::GetIndex(response) => response,
        _ => panic!("Unexpected response returned."),
    }; // Get the result.

    let arrow = read_arrow(&result.records.first().unwrap().data.clone())
        .unwrap()
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
        1
    );

    client
        .produce_json(
            "amount",
            r#"
                {
                    "id": "1",
                    "data": 1,
                }
            "#
            .as_bytes(),
            std::sync::Arc::new(amount_schema()),
        )
        .unwrap();

    client.recv(Some(Duration::from_secs(5))).unwrap();

    std::thread::sleep(Duration::from_secs(1));

    client
        .query_at(b"result", &PartitionName::try_from("1").unwrap(), 1)
        .unwrap();

    let result = match client.recv(Some(Duration::from_secs(10))).unwrap().body {
        ResponseBody::GetIndex(response) => response,
        _ => panic!("Unexpected response returned."),
    };

    let arrow = read_arrow(&result.records.first().unwrap().data.clone())
        .unwrap()
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

    // client
    //     .produce_json(
    //         "amount",
    //         r#"
    //             {
    //                 "id": "1",
    //                 "data": 1,
    //             }
    //         "#
    //         .as_bytes(),
    //     )
    //     ?;

    // client.recv(Some(Duration::from_secs(5))).unwrap();

    // std::thread::sleep(Duration::from_secs(1));

    // client
    //     .query_latest(b"result", &PartitionName::try_from("1").unwrap())
    //     ?;

    // let result = match client.recv(Some(Duration::from_secs(5))).unwrap().body {
    //     ResponseBody::GetIndex(response) => response,
    //     _ => panic!("Unexpected response returned."),
    // };

    // let arrow = read_arrow(&result.records.first().unwrap().data.clone())
    //     .next()
    //     ?
    //     ?;

    // assert_eq!(
    //     arrow
    //         .column_by_name("id")
    //         ?
    //         .as_string::<i32>()
    //         .value(0),
    //     "1"
    // );

    // assert_eq!(
    //     arrow
    //         .column_by_name("data")
    //         ?
    //         .as_primitive::<Int32Type>()
    //         .value(0),
    //     3
    // );

    // produce_sync(
    //     b"amount",
    //     b"1",
    //     r#"
    //     {
    //         "id": "1",
    //         "amount": 1,
    //     }
    // "#
    //     .as_bytes(),
    //     &mut socket,
    // )
    // ?;

    // let result = query_latest(b"result", b"1", &mut socket).unwrap();

    // let result: serde_json::Value = serde_json::from_slice(&result.first().unwrap().data).unwrap();
    // let expected_result = json!(
    //     {"id":"1","data":3}
    // );

    // assert_eq!(result, expected_result);
    std::fs::remove_dir_all(dir_remove).unwrap();
}
