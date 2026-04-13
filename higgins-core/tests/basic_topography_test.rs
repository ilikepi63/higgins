mod common;

use higgins::run_server_returning;
use higgins_client::ResponseBody;
use std::{path::PathBuf, time::Duration};

use common::get_random_port;

fn get_dir() -> PathBuf {
    // let mut dir = temp_dir();
    let mut dir = PathBuf::new();
    dir.push("basic_topography");
    dir
}

pub fn setup_server(
    dir: PathBuf,
    port: u16,
) -> (higgins::ServerHandle, higgins_client::blocking::Client) {
    let server_handle = run_server_returning(dir, port);

    std::thread::sleep(Duration::from_millis(100));

    let client =
        higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    (server_handle, client)
}

// #[test]
fn can_achieve_basic_topography_retrieval() {
    tracing_subscriber::fmt::init();

    let port = get_random_port();

    let dir = get_dir();
    let dir_clone = dir.clone();

    let dir_remove = dir.clone();

    let (server_handle, mut client) = setup_server(dir, port);

    // Upload a basic configuration with one stream.
    let config = std::fs::read_to_string("tests/configs/basic_config.toml").unwrap();
    client.upload_configuration(config.as_bytes()).unwrap();

    match client.recv(None).unwrap().body {
        ResponseBody::CreateConfiguration(_) => {
            println!("Retrieved create configuration!");
        } //create_subscription_response.subscription_id.unwrap(),
        _ => panic!("Retrieved unexpected result."),
    };

    client.get_current_topography().unwrap();

    let first_response = match client.recv(None).unwrap().body {
        ResponseBody::GetCurrentTopography(topography) => {
            let value: toml::Value = toml::from_slice(&topography.data).unwrap();
            value
        }
        _ => panic!("Retrieved unexpected result."),
    };

    server_handle.close();

    let (server_handle, mut client) = setup_server(dir_clone, port);

    client.get_current_topography().unwrap();

    let second_response = match client.recv(None).unwrap().body {
        ResponseBody::GetCurrentTopography(topography) => {
            let value: toml::Value = toml::from_slice(&topography.data).unwrap();
            value
        }
        _ => panic!("Retrieved unexpected result."),
    };

    server_handle.close();

    std::fs::remove_dir_all(dir_remove).unwrap();

    assert_eq!(first_response, second_response);
}
