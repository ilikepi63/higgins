mod common;

use std::path::PathBuf;

use crate::common::{
    basic::can_achieve_basic_broker_functionality, join::run_basic_join_test,
    map::can_implement_basic_map, reduce::can_implement_basic_reduce, subscription::*,
    topography::can_achieve_basic_topography_retrieval, windowing::basic_windowing,
};

#[test]
fn basic_test() {
    tracing_subscriber::fmt::init();

    can_achieve_basic_broker_functionality();
    // let port = get_random_port();

    // let dir = get_dir();

    // let dir_remove = dir.clone();

    // let _ = std::thread::spawn(move || {
    //     let rt = tokio::runtime::Runtime::new().unwrap();

    //     rt.block_on(run_server(dir, port));
    // });

    // std::thread::sleep(Duration::from_millis(100));

    // let result = catch_unwind(|| {
    //     let mut client =
    //         higgins_client::blocking::Client::connect(format!("127.0.0.1:{port}"), None).unwrap();

    //     // 1. Do a basic Ping test.
    //     client.ping().unwrap();

    //     match client.recv(None).unwrap().body {
    //         ResponseBody::Pong(_) => {
    //             println!("Retrieved Pong!");
    //         } //create_subscription_response.subscription_id.unwrap(),
    //         _ => panic!("Retrieved unexpected result."),
    //     };

    //     // Upload a basic configuration with one stream.
    //     let config = std::fs::read_to_string("tests/configs/basic_config.toml").unwrap();
    //     client.upload_configuration(config.as_bytes()).unwrap();

    //     match client.recv(None).unwrap().body {
    //         ResponseBody::CreateConfiguration(_) => {
    //             println!("Retrieved create configuration!");
    //         } //create_subscription_response.subscription_id.unwrap(),
    //         _ => panic!("Retrieved unexpected result."),
    //     };

    //     // Produce to the stream.
    //     let payload = std::fs::read_to_string("tests/customer.json").unwrap();

    //     client
    //         .produce_json(
    //             STREAM,
    //             payload.as_bytes(),
    //             std::sync::Arc::new(customer_schema()),
    //         )
    //         .unwrap();

    //     match client.recv(Some(Duration::from_secs(1))).unwrap().body {
    //         ResponseBody::Produce(_) => {
    //             println!("Retrieved Produce!");
    //         } //create_subscription_response.subscription_id.unwrap(),
    //         _ => panic!("Retrieved unexpected result."),
    //     };

    //     // Consume from the stream.
    //     client
    //         .query_latest(
    //             STREAM.as_bytes(),
    //             &PartitionName::try_from(PARTITION).unwrap(),
    //         )
    //         .unwrap();

    //     let result = client
    //         .recv(Some(Duration::from_secs(1)))
    //         .map(|res| match res.body {
    //             ResponseBody::GetIndex(get_index_result) => get_index_result.records,
    //             _ => panic!("Got an unexpect result."),
    //         });

    //     let arrow_data = result.unwrap().into_iter().next().unwrap();

    //     let arrow = read_arrow(&arrow_data.data)
    //         .next()
    //         .unwrap()
    //         .inspect_err(|err| {
    //             dbg!(err);
    //         })
    //         .unwrap();

    //     tracing::trace!("Data: {:#?}", arrow);

    //     assert_eq!(
    //         arrow
    //             .column_by_name("age")
    //             .unwrap()
    //             .as_any()
    //             .downcast_ref::<arrow::array::Int32Array>()
    //             .unwrap()
    //             .iter()
    //             .next()
    //             .unwrap()
    //             .unwrap(),
    //         21
    //     );

    //     assert_eq!(
    //         arrow
    //             .column_by_name("first_name")
    //             .unwrap()
    //             .as_any()
    //             .downcast_ref::<arrow::array::StringArray>()
    //             .unwrap()
    //             .iter()
    //             .next()
    //             .unwrap()
    //             .unwrap(),
    //         "John"
    //     );

    //     assert_eq!(
    //         arrow
    //             .column_by_name("id")
    //             .unwrap()
    //             .as_any()
    //             .downcast_ref::<arrow::array::StringArray>()
    //             .unwrap()
    //             .iter()
    //             .next()
    //             .unwrap()
    //             .unwrap(),
    //         "1"
    //     );

    //     assert_eq!(
    //         arrow
    //             .column_by_name("last_name")
    //             .unwrap()
    //             .as_any()
    //             .downcast_ref::<arrow::array::StringArray>()
    //             .unwrap()
    //             .iter()
    //             .next()
    //             .unwrap()
    //             .unwrap(),
    //         "Doe"
    //     );
    // });

    // std::fs::remove_dir_all(dir_remove).unwrap();

    // result.unwrap();

    run_basic_join_test();
    can_implement_basic_map();
    can_implement_basic_reduce();
    can_achieve_basic_topography_retrieval();
    basic_windowing();
    can_retrieve_data_from_subscription();
    subscription_works_with_multiple_clients();
    can_update_subscription_with_multiple_values();
    can_update_subscription_after_created();
}
