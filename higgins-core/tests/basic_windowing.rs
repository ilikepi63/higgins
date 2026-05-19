mod common;

use common::{
    configuration::upload_configuration_sync, ping::client_sync_ping_test, schema::customer_schema,
};
use higgins::run_server;
use higgins_client::ResponseBody;
use higgins_shared::{PartitionName, read_arrow};
use std::{panic::catch_unwind, path::PathBuf, time::Duration};

use common::get_random_port;

fn get_dir() -> PathBuf {
    // let mut dir = temp_dir();
    let mut dir = PathBuf::new();
    dir.push("basic");
    dir
}

static STREAM: &str = "update_customer";
static PARTITION: &[u8] = "1".as_bytes();

#[test]
fn basic_windowing() {
    tracing_subscriber::fmt::init();

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
    });

    std::fs::remove_dir_all(dir_remove).unwrap();

    result.unwrap();
}
