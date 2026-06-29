use std::{panic::catch_unwind, sync::Arc, time::Duration};

use arrow::array::Int32Array;
use higgins::run_server;
use higgins_client::ResponseBody;
use higgins_shared::{PartitionName, read_arrow};

use crate::common::{
    data::customer_json_with_id_and_age,
    get_random_port,
    harness::{BASIC_CONFIG, produce_await, upload_config},
    schema::customer_schema,
    setup_server, unique_dir,
};

pub fn offsets_are_monotonically_increasing() {
    let dir = unique_dir().unwrap();

    let close_dir_handle = dir.clone();

    let result = catch_unwind(|| {
        let (handle, mut client) = setup_server(dir, get_random_port());

        upload_config(&mut client, BASIC_CONFIG);

        for age in 0..5 {
            client
                .produce_json(
                    "update_customer",
                    customer_json_with_id_and_age("1", 1).as_bytes(),
                    Arc::new(customer_schema()),
                )
                .unwrap();
            match client.recv(Some(Duration::from_secs(5))).unwrap().body {
                ResponseBody::Produce(_) => {}
                other => panic!("expected Produce response, got {:?}", other),
            }
        }

        for offset in 0u64..5 {
            client
                .query_at(
                    b"update_customer",
                    &PartitionName::try_from("1").unwrap(),
                    offset,
                )
                .unwrap();

            let response = client.recv(Some(Duration::from_secs(1))).unwrap();

            let batch = match response.body {
                ResponseBody::GetIndex(resp) => {
                    let record = resp
                        .records
                        .first()
                        .expect("GetIndex response had no records");
                    read_arrow(&record.data).unwrap().next().unwrap().unwrap()
                }
                other => panic!("expected GetIndex response, got {:?}", other),
            };

            assert_eq!(
                batch
                    .column_by_name("age")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                offset as i32,
                "offset {offset} did not contain the expected record"
            );
        }

        handle.close();
    });

    let _ = std::fs::remove_dir_all(close_dir_handle);
}
