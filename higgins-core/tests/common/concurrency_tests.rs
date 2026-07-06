use std::{
    panic::catch_unwind,
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use arrow::array::{Int32Array, StringArray};
use higgins_client::{ResponseBody, blocking::Client};
use higgins_codec::GetIndexResponse;
use higgins_shared::{PartitionName, read_arrow};

use crate::common::{
    configuration::upload_configuration_sync, data::customer_json_with_id_and_age, get_random_port,
    invariant_tests::BASIC_CONFIG, schema::amount_schema, schema::customer_schema, setup_server,
    unique_dir,
};

fn query_at(
    client: &mut Client,
    stream: &str,
    partition: &PartitionName,
    offset: u64,
) -> Option<GetIndexResponse> {
    client
        .query_at(stream.as_bytes(), partition, offset)
        .unwrap();

    match client.recv(Some(Duration::from_secs(2))) {
        Ok(response) => match response.body {
            ResponseBody::GetIndex(resp) => Some(resp),
            other => panic!("expected GetIndex response, got {:?}", other),
        },
        Err(_) => None,
    }
}

fn record_count_at(
    client: &mut Client,
    stream: &str,
    partition: &PartitionName,
    offset: u64,
) -> usize {
    query_at(client, stream, partition, offset)
        .map(|resp| resp.records.len())
        .unwrap_or(0)
}

fn id_at(
    client: &mut Client,
    stream: &str,
    partition: &PartitionName,
    offset: u64,
) -> Option<String> {
    let resp = query_at(client, stream, partition, offset)?;
    resp.records.first().map(|record| {
        let batch = read_arrow(&record.data).unwrap().next().unwrap().unwrap();
        batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    })
}

pub fn concurrent_produces_to_same_partition_are_serialised() {
    const PRODUCERS: usize = 4;
    const RECORDS_EACH: usize = 5;
    const TOTAL: u64 = (PRODUCERS * RECORDS_EACH) as u64;

    let port = get_random_port();
    let dir = unique_dir().unwrap();

    let result = catch_unwind(|| {
        let (handle, mut reader) = setup_server(dir.clone(), port);

        upload_configuration_sync(BASIC_CONFIG, &mut reader);

        // Every producer connects, then waits on the barrier so all writes race.
        let barrier = Arc::new(Barrier::new(PRODUCERS));
        let mut producers = Vec::new();
        for produce_i in 0..PRODUCERS {
            let barrier = barrier.clone();
            producers.push(thread::spawn(move || {
                let mut client = Client::connect(format!("127.0.0.1:{}", port), None).unwrap();
                barrier.wait();
                for record_i in 0..RECORDS_EACH {
                    println!("PRODUCING TO");
                    let age = (produce_i * RECORDS_EACH + record_i) as i32;
                    client
                        .produce_json(
                            "update_customer",
                            customer_json_with_id_and_age("1", age).as_bytes(),
                            Arc::new(customer_schema()),
                        )
                        .unwrap();
                    match client.recv(Some(Duration::from_secs(5))).unwrap().body {
                        ResponseBody::Produce(_response) => {}
                        other => panic!("expected Produce response, got {:?}", other),
                    }
                }
            }));
        }

        for producer in producers {
            producer.join().unwrap();
        }

        // The index must contain exactly TOTAL dense records.
        for offset in 0..TOTAL {
            assert_eq!(
                record_count_at(
                    &mut reader,
                    "update_customer",
                    &PartitionName::try_from("1").unwrap(),
                    offset
                ),
                1,
                "offset {offset} must hold exactly one record after concurrent produces",
            );
        }
        assert_eq!(
            record_count_at(
                &mut reader,
                "update_customer",
                &PartitionName::try_from("1").unwrap(),
                TOTAL
            ),
            0,
            "no record may exist beyond the {TOTAL} concurrently produced records",
        );

        handle.close();
    });

    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}

pub fn concurrent_produces_to_different_partitions_do_not_interfere() {
    const RECORDS_EACH: u64 = 3;
    const KEYS: [&str; 2] = ["1", "2"];

    let port = get_random_port();
    let dir = unique_dir().unwrap();

    let result = catch_unwind(|| {
        let (handle, mut reader) = setup_server(dir.clone(), port);

        upload_configuration_sync(BASIC_CONFIG, &mut reader);

        let barrier = Arc::new(Barrier::new(KEYS.len()));
        let mut producers = Vec::new();
        for key in KEYS {
            let barrier = barrier.clone();
            producers.push(thread::spawn(move || {
                let mut client = Client::connect(format!("127.0.0.1:{}", port), None).unwrap();
                barrier.wait();
                for age in 0..RECORDS_EACH {
                    client
                        .produce_json(
                            "update_customer",
                            customer_json_with_id_and_age(key, age as i32).as_bytes(),
                            Arc::new(customer_schema()),
                        )
                        .unwrap();
                    match client.recv(Some(Duration::from_secs(5))).unwrap().body {
                        ResponseBody::Produce(response) => {
                            println!("Retrieved produce response for {key} {age} {:#?}", response);
                        }
                        other => panic!("expected Produce response, got {:?}", other),
                    }
                }
            }));
        }

        for producer in producers {
            producer.join().unwrap();
        }

        // Each partition must hold exactly its own records and nothing else.
        for key in KEYS {
            for offset in 0..RECORDS_EACH {
                let id = id_at(
                    &mut reader,
                    "update_customer",
                    &PartitionName::try_from(key).unwrap(),
                    offset,
                )
                .unwrap_or_else(|| panic!("partition {key} is missing offset {offset}"));
                assert_eq!(
                    id, key,
                    "partition {key} offset {offset} contained another partition's record",
                );
            }
            assert_eq!(
                record_count_at(
                    &mut reader,
                    "update_customer",
                    &PartitionName::try_from(key).unwrap(),
                    RECORDS_EACH
                ),
                0,
                "partition {key} must hold exactly {RECORDS_EACH} records",
            );
        }

        handle.close();
    });

    let _ = std::fs::remove_dir_all(dir);
    result.unwrap();
}
