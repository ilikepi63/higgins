mod common;

use std::{path::PathBuf, time::Duration};

use higgins::run_server;
use higgins_client::Response;

use common::get_random_port;

fn get_dir() -> PathBuf {
    // let mut dir = temp_dir();
    let mut dir = PathBuf::new();
    dir.push("basic_topography");
    dir
}

#[test]
fn can_achieve_basic_topography_retrieval() {
    tracing_subscriber::fmt::init();

    let port = get_random_port();

    let dir = get_dir();

    let dir_remove = dir.clone();

    let _ = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(run_server(dir, port));
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    // 1. Do a basic Ping test.
    client.ping().unwrap();

    match client.recv().unwrap() {
        Response::Pong(_) => {
            println!("Retrieved Pong!");
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/basic_config.toml").unwrap();
    client.upload_configuration(config.as_bytes()).unwrap();

    match client.recv().unwrap() {
        Response::CreateConfiguration(_) => {
            println!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    client.get_current_topography().unwrap();

    match client.recv().unwrap() {
        Response::GetCurrentTopography(topography) => {
            let value: toml::Value = toml::from_slice(&topography.data).unwrap();
            println!(
                "Retrieved Topography: {}",
                toml::to_string_pretty(&value).unwrap()
            );
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    std::fs::remove_dir_all(dir_remove).unwrap();

    panic!();
}
