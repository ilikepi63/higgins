use std::{panic::catch_unwind, sync::Arc, time::Duration};

use arrow::{
    array::{Int32Array, StringArray},
    ipc::RecordBatch,
};
use higgins::run_server;
use higgins_client::ResponseBody;
use higgins_shared::{PartitionName, read_arrow};

use crate::common::{
    client_utils::{acknowledge, create_subscription, recv_until_take},
    configuration::upload_configuration_sync,
    data::customer_json_with_id_and_age,
    get_random_port, produce_sync,
    schema::customer_schema,
    setup_server, unique_dir,
};

pub const BASIC_CONFIG: &str = r#"
[storage.memory]
type="memory"

[schema.update_customer_event]
id = "string"
first_name = "string"
last_name = "string"
age = "int32"

[streams.update_customer]
schema = "update_customer_event"
partition_key = "id"
"#;

pub fn offsets_are_monotonically_increasing() {
    let dir = unique_dir().unwrap();

    let close_dir_handle = dir.clone();

    let result = catch_unwind(|| {
        let (handle, mut client) = setup_server(dir, get_random_port());

        upload_configuration_sync(BASIC_CONFIG, &mut client);

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

pub fn records_in_different_partitions_do_not_cross_contaminate() {
    let port = get_random_port();
    let dir = unique_dir().unwrap();

    let result = catch_unwind(|| {
        let (handle, mut client) = setup_server(dir.clone(), port);

        upload_configuration_sync(BASIC_CONFIG, &mut client);

        produce_sync(
            &mut client,
            "update_customer",
            r#"{"id":"1","first_name":"Alice","last_name":"A","age":1}"#,
            Arc::new(customer_schema()),
        );
        produce_sync(
            &mut client,
            "update_customer",
            r#"{"id":"2","first_name":"Bob","last_name":"B","age":2}"#,
            Arc::new(customer_schema()),
        );

        client
            .query_at(
                b"update_customer",
                &PartitionName::try_from("1").unwrap(),
                0,
            )
            .unwrap();

        let response = client.recv(Some(Duration::from_secs(1))).unwrap();

        let b1 = match response.body {
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
            b1.column_by_name("first_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string(),
            "Alice"
        );

        client
            .query_at(
                b"update_customer",
                &PartitionName::try_from("2").unwrap(),
                0,
            )
            .unwrap();

        let response = client.recv(Some(Duration::from_secs(1))).unwrap();

        let b2 = match response.body {
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
            b2.column_by_name("first_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string(),
            "Bob"
        );

        handle.close();
    });
    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}

pub fn partition_offsets_are_independent() {
    let port = get_random_port();
    let dir = unique_dir().unwrap();

    let result = catch_unwind(|| {
        let (handle, mut client) = setup_server(dir.clone(), port);

        upload_configuration_sync(BASIC_CONFIG, &mut client);

        for age in 0..3 {
            produce_sync(
                &mut client,
                "update_customer",
                &customer_json_with_id_and_age("A", age),
                Arc::new(customer_schema()),
            );
        }
        produce_sync(
            &mut client,
            "update_customer",
            &customer_json_with_id_and_age("B", 99),
            Arc::new(customer_schema()),
        );

        client
            .query_at(
                b"update_customer",
                &PartitionName::try_from("A").unwrap(),
                2,
            )
            .unwrap();
        let a2 = await_get_index(&mut client);
        assert_eq!(
            a2.column_by_name("age")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2,
        );

        client
            .query_at(
                b"update_customer",
                &PartitionName::try_from("B").unwrap(),
                0,
            )
            .unwrap();
        let b0 = await_get_index(&mut client);

        assert_eq!(
            b0.column_by_name("age")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            99,
        );

        client
            .query_at(
                b"update_customer",
                &PartitionName::try_from("B").unwrap(),
                1,
            )
            .unwrap();
        assert!(
            client.recv(Some(Duration::from_millis(1200))).is_err(),
            "partition B must not have a record at offset 1"
        );

        handle.close();
    });

    let _ = std::fs::remove_dir_all(dir);

    result.unwrap();
}

fn await_get_index(client: &mut higgins_client::blocking::Client) -> arrow::array::RecordBatch {
    let response = client.recv(Some(Duration::from_secs(1))).unwrap();

    match response.body {
        ResponseBody::GetIndex(resp) => {
            let record = resp
                .records
                .first()
                .expect("GetIndex response had no records");
            read_arrow(&record.data).unwrap().next().unwrap().unwrap()
        }
        other => panic!("expected GetIndex response, got {:?}", other),
    }
}

pub fn acknowledge_out_of_order_is_rejected() {
    let port = get_random_port();
    let dir = unique_dir().unwrap();

    let result = catch_unwind(|| {
        let (handle, mut client) = setup_server(dir.clone(), port);

        upload_configuration_sync(BASIC_CONFIG, &mut client);

        let sub_id = create_subscription(&mut client, "update_customer", None);

        produce_sync(
            &mut client,
            "update_customer",
            &customer_json_with_id_and_age("1", 0),
            Arc::new(customer_schema()),
        );
        produce_sync(
            &mut client,
            "update_customer",
            &customer_json_with_id_and_age("1", 1),
            Arc::new(customer_schema()),
        );

        client.take(sub_id.clone(), b"update_customer", 1).unwrap();
        let _ = recv_until_take(&mut client).unwrap();

        // Acknowledge a far-future offset (5) while earlier offsets remain
        // unacknowledged — this skips offsets and must be rejected.
        let response = acknowledge(
            "update_customer",
            &sub_id,
            vec![(PartitionName::try_from("1").unwrap(), 5u64..5u64)],
            &mut client,
        );

        assert!(
            !response.error.is_empty(),
            "out-of-order acknowledgement should be rejected with an error"
        );

        handle.close();
    });
    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}

pub fn topography_is_idempotent_across_multiple_restarts() {
    let dir = unique_dir().unwrap();

    let result = catch_unwind(|| {
        let mut snapshots: Vec<Vec<u8>> = Vec::new();

        for (i, _) in (0..3).enumerate() {
            let port = get_random_port();
            let (handle, mut client) = setup_server(dir.clone(), port);

            if i == 0 {
                upload_configuration_sync(BASIC_CONFIG, &mut client);
            }

            client.get_current_topography().unwrap();
            let data = match client.recv(Some(Duration::from_secs(5))).unwrap().body {
                ResponseBody::GetCurrentTopography(t) => t.data,
                other => panic!("expected GetCurrentTopography response, got {:?}", other),
            };
            snapshots.push(data);

            handle.close();
            std::thread::sleep(Duration::from_millis(150));
        }

        assert_eq!(
            snapshots[0], snapshots[1],
            "topography changed after 1 restart"
        );
        assert_eq!(
            snapshots[1], snapshots[2],
            "topography changed after 2 restarts"
        );
    });
    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}

pub fn produce_to_nonexistent_stream_does_not_crash_server() {
    let port = get_random_port();
    let dir = unique_dir().unwrap();

    let result = catch_unwind(|| {
        let (handle, mut client) = setup_server(dir.clone(), port);

        upload_configuration_sync(BASIC_CONFIG, &mut client);

        // Produce to a stream that was never configured.
        client
            .produce_json(
                "ghost_stream",
                customer_json_with_id_and_age("1", 1).as_bytes(),
                Arc::new(customer_schema()),
            )
            .unwrap();

        // The server does not respond to an invalid produce.
        assert!(
            client.recv(Some(Duration::from_millis(1000))).is_err(),
            "server should not send a produce response for an unknown stream"
        );

        // The server is still alive and responsive.
        client.ping().unwrap();
        assert!(matches!(
            client.recv(Some(Duration::from_secs(5))).unwrap().body,
            ResponseBody::Pong(_)
        ));

        handle.close();
    });

    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}

pub fn query_nonexistent_offset_does_not_crash_server() {
    let port = get_random_port();
    let dir = unique_dir().unwrap();
    let result = catch_unwind(|| {
        let (handle, mut client) = setup_server(dir.clone(), port);

        upload_configuration_sync(BASIC_CONFIG, &mut client);

        // No records have been produced yet.
        client
            .query_at(
                b"update_customer",
                &PartitionName::try_from("1").unwrap(),
                0,
            )
            .unwrap();
        assert!(
            client.recv(Some(Duration::from_millis(1000))).is_err(),
            "querying an empty stream should not return a record"
        );

        // The server is still alive and responsive.
        client.ping().unwrap();
        assert!(matches!(
            client.recv(Some(Duration::from_secs(5))).unwrap().body,
            ResponseBody::Pong(_)
        ));

        handle.close();
    });
    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}

pub fn subscription_redelivers_after_visibility_timeout() {
    let port = get_random_port();
    let dir = unique_dir().unwrap();

    let result = catch_unwind(|| {
        let (handle, mut client) = setup_server(dir.clone(), port);

        upload_configuration_sync(BASIC_CONFIG, &mut client);

        let sub_id = create_subscription(&mut client, "update_customer", Some(700));

        // Express standing demand before any data exists.
        client.take(sub_id.clone(), b"update_customer", 1).unwrap();

        // Produce a single record; it will be pushed to the waiting consumer.
        client
            .produce_json(
                "update_customer",
                customer_json_with_id_and_age("1", 42).as_bytes(),
                Arc::new(customer_schema()),
            )
            .unwrap();

        // First delivery.
        let first = recv_until_take(&mut client).unwrap();
        assert_eq!(
            first.records.first().unwrap().offset,
            0,
            "first delivery should be offset 0"
        );

        // Deliberately do NOT acknowledge. After the visibility timeout the offset
        // must be reset and redelivered automatically.
        let redelivered = recv_until_take(&mut client).unwrap();
        assert_eq!(
            redelivered.records.first().unwrap().offset,
            0,
            "offset 0 should be redelivered after the visibility timeout"
        );

        handle.close();
    });

    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}

pub fn subscription_does_not_redeliver_after_acknowledge() {
    let port = get_random_port();
    let dir = unique_dir().unwrap();
    let result = catch_unwind(|| {
        let (handle, mut client) = setup_server(dir.clone(), port);

        upload_configuration_sync(BASIC_CONFIG, &mut client);

        let sub_id = create_subscription(&mut client, "update_customer", Some(700));

        client.take(sub_id.clone(), b"update_customer", 1).unwrap();

        client
            .produce_json(
                "update_customer",
                customer_json_with_id_and_age("1", 42).as_bytes(),
                Arc::new(customer_schema()),
            )
            .unwrap();

        let first = recv_until_take(&mut client).unwrap();
        assert_eq!(first.records.first().unwrap().offset, 0);

        // Acknowledge offset 0. A `ProduceResponse` from the earlier produce may
        // still be in flight, so drain until we observe the AcknowledgeResponse.
        client
            .acknowledge(
                "update_customer",
                &sub_id,
                vec![(PartitionName::try_from("1").unwrap(), 0u64..0u64)],
            )
            .unwrap();
        let ack = loop {
            match client.recv(Some(Duration::from_secs(5))).unwrap().body {
                ResponseBody::Acknowledge(a) => break a,
                ResponseBody::Produce(_) | ResponseBody::TakeRecords(_) => continue,
                other => panic!("expected Acknowledge response, got {:?}", other),
            }
        };
        assert!(
            ack.error.is_empty(),
            "acknowledging offset 0 should succeed: {}",
            ack.error
        );

        // Wait well past the visibility timeout — nothing should be redelivered.
        std::thread::sleep(Duration::from_millis(1000));
        assert!(
            client.recv(Some(Duration::from_millis(500))).is_err(),
            "an acknowledged offset must not be redelivered"
        );

        handle.close();
    });
    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}
