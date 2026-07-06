use super::{configuration::upload_configuration_sync, ping::client_sync_ping_test};
use higgins::run_server;
use higgins_client::ResponseBody;
use higgins_shared::{PartitionName, read_arrow};
use std::{panic::catch_unwind, path::PathBuf, time::Duration};

use super::get_random_port;

fn get_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(uuid::Uuid::new_v4().to_string());
    dir
}

static STREAM: &str = "value";
static WINDOWED_STREAM: &str = "value_windowed";

static PAYLOAD: &str = r#"{
    "id": "1",
    "some_data": "1",
    "other_data": "1"
    "i": 123
}"#;

pub fn basic_windowing() {
    let port = get_random_port();

    let dir = get_dir();

    let dir_remove = dir.clone();

    let _ = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(run_server(dir, port));
    });

    std::thread::sleep(Duration::from_millis(100));

    let result = catch_unwind(|| {
        let mut client =
            higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

        client_sync_ping_test(&mut client);

        tracing::info!("Uploading the config..");

        upload_configuration_sync(
            &std::fs::read_to_string("tests/configs/basic_window.toml").unwrap(),
            &mut client,
        );
        tracing::info!("Uploaded the config..");

        // first produce
        client
            .produce_json(
                STREAM,
                PAYLOAD.as_bytes(),
                std::sync::Arc::new(value_schema()),
            )
            .unwrap();

        let produce_result = client.recv(Some(Duration::from_secs(5)));

        assert!(matches!(
            produce_result.unwrap().body,
            ResponseBody::Produce(_)
        ));

        std::thread::sleep(Duration::from_secs(2));

        client
            .query_at(
                WINDOWED_STREAM.as_bytes(),
                &PartitionName::try_from("1").unwrap(),
                0,
            )
            .unwrap();

        let response = client.recv(Some(Duration::from_secs(5))).unwrap();

        match response.body {
            ResponseBody::GetIndex(index_data) => {
                let record = index_data.records.first().unwrap();

                // assert_eq!(record.offset, 0);
                // assert_eq!(record.partition, "1".as_bytes());
                // assert_eq!(record.stream, WINDOWED_STREAM.as_bytes());

                let arrow = read_arrow(&record.data)
                    .unwrap()
                    .next()
                    .unwrap()
                    .inspect_err(|err| {
                        dbg!(err);
                    })
                    .unwrap();

                tracing::debug!("{:#?}", arrow);
            }
            _ => panic!("Retreved incorrect response for produce query."),
        }

        // second produce
        client
            .produce_json(
                STREAM,
                PAYLOAD.as_bytes(),
                std::sync::Arc::new(value_schema()),
            )
            .unwrap();

        let produce_result = client.recv(Some(Duration::from_secs(5)));

        assert!(matches!(
            produce_result.unwrap().body,
            ResponseBody::Produce(_)
        ));

        std::thread::sleep(Duration::from_secs(2));

        client
            .query_at(
                WINDOWED_STREAM.as_bytes(),
                &PartitionName::try_from("1").unwrap(),
                0,
            )
            .unwrap();

        let response = client.recv(Some(Duration::from_secs(5))).unwrap();

        match response.body {
            ResponseBody::GetIndex(index_data) => {
                let record = index_data.records.first().unwrap();

                // assert_eq!(record.offset, 0);
                // assert_eq!(record.partition, "1".as_bytes());
                // assert_eq!(record.stream, WINDOWED_STREAM.as_bytes());

                let arrow = read_arrow(&record.data)
                    .unwrap()
                    .next()
                    .unwrap()
                    .inspect_err(|err| {
                        dbg!(err);
                    })
                    .unwrap();

                assert_eq!(arrow.num_rows(), 2);
            }
            _ => panic!("Retreved incorrect response for produce query."),
        }
    });

    std::fs::remove_dir_all(dir_remove).unwrap();

    result.unwrap();
}

use arrow_schema::{DataType, Field, Schema};

pub fn value_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", arrow_schema::DataType::Utf8, false),
        Field::new("some_data", arrow_schema::DataType::Utf8, false),
        Field::new("other_data", DataType::Utf8, false),
        Field::new("i", DataType::Int32, false),
    ])
}
