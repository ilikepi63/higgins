//! Algorithms related to joining two streams.
//!
//! There exists a  set of types of stream joins, akin to SQL joins:
//! - Inner Join        -> emits a value for each corresponding index value for both underlying streams.
//! - Left Outer Join   -> emits a value for every value on the left side of the join, regardless of whether they have a matching key on the alternate stream.
//! - Right Outer Join  -> Similar to Left Outer Join, except on the right side of the join.
//! - Full Join         -> Similar to Right or Left Outer, except will emit for all values.

// TODO: How do we chain multiple streams together?.

use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{ArrayRef, AsArray, RecordBatch, StringArrayType},
    compute::cast,
    datatypes::{Field, Schema, Utf8Type},
    util::display::array_value_to_string,
};
use tokio::sync::RwLock;

use crate::{
    broker::Broker,
    client::ClientRef,
    error::HigginsError,
    storage::arrow_ipc::read_arrow,
    topography::{Join, Key, StreamDefinition},
    utils::epoch,
};

pub async fn create_derived_stream_from_definition(
    stream_name: Key,
    stream_def: StreamDefinition,
    left: (Key, StreamDefinition),
    right: (Key, StreamDefinition),
    join_type: Join,
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    let client_id = broker.clients.insert(ClientRef::NoOp);

    // Subscribe to both streams.
    let left_subscription = broker.create_subscription(left.0.inner());
    let _right_subscription = broker.create_subscription(right.0.inner());

    let (left_notify, left_subscription_ref) = broker
        .get_subscription_by_key(left.0.inner(), &left_subscription)
        .unwrap();
    let (_right_notify, _right_subscription_ref) = broker
        .get_subscription_by_key(left.0.inner(), &left_subscription)
        .unwrap();

    let left_broker = broker_ref.clone();
    let left_stream_name = left.0.inner().to_owned();
    let left_stream_partition_key = left.1.partition_key;
    let right_stream_name = right.0.inner().to_owned();

    // Left join runner for this subscription.
    tokio::task::spawn(async move {
        tracing::trace!("[DERIVED TAKE] We are being initiated");

        loop {
            let mut lock = left_subscription_ref.write().await;

            let n = 10; // Generally, there is a set amount of n that we are interested in at a point.

            let offsets_result = lock.take(client_id, n);

            drop(lock);

            if let Ok(mut offsets) = offsets_result {
                // If there are no given offsts, await the wakener then.
                if offsets.len() < 1 {
                    tracing::trace!("[DERIVED TAKE] Awaiting to be notified for produce..");
                    left_notify.notified().await;
                    tracing::trace!("[DERIVED TAKE] We've been notified!");

                    offsets = {
                        let mut lock = left_subscription_ref.write().await;
                        lock.take(client_id, n).unwrap()
                    };
                }

                tracing::trace!(
                    "[DERIVED TAKE] Received offsets {:#?}. Initiating join.",
                    offsets
                );

                //Get payloads from offsets.
                for (partition, offset) in offsets {
                    let broker_lock = left_broker.read().await;

                    let mut consumption = broker_lock
                        .consume(&left_stream_name, &partition, offset, 50_000)
                        .await;

                    drop(broker_lock);

                    while let Some(val) = consumption.recv().await {
                        tracing::trace!("[DERIVED TAKE] Received consume Response {:#?}.", val);

                        for consume_batch in val.batches.iter() {
                            let stream_reader = read_arrow(&consume_batch.data);

                            let batches =
                                stream_reader.filter_map(|val| val.ok()).collect::<Vec<_>>();

                            for record_batch in batches {
                                let mut broker_lock = left_broker.write().await;

                                let epoch_val = epoch();

                                for index in 0..record_batch.num_rows() {
                                    let partition_val = get_partition_key_from_record_batch(
                                        &record_batch,
                                        index,
                                        String::from_utf8_lossy(left_stream_partition_key.inner())
                                            .to_string()
                                            .as_str(),
                                    );

                                    let right_record = broker_lock
                                        .get_by_timestamp(
                                            &right_stream_name,
                                            &partition_val,
                                            epoch_val,
                                        )
                                        .await
                                        .map(|consume| {
                                            consume.batches.first().map(|batch| {
                                                let stream_reader = read_arrow(&batch.data);

                                                let batches = stream_reader
                                                    .filter_map(|val| val.ok())
                                                    .collect::<Vec<_>>();
                                                batches.into_iter().nth(0)
                                            })
                                        })
                                        .flatten()
                                        .flatten();

                                    tracing::trace!(
                                        "[DERIVED TAKE] Right record: {:#?}",
                                        right_record
                                    );

                                    let right_key =
                                        String::from_utf8(join_type.key().to_vec()).unwrap();

                                    let new_record_batch = values_to_batches(
                                        &join_type,
                                        Some(record_batch.clone()),
                                        right_record,
                                        String::from_utf8(
                                            stream_def.base.clone().unwrap().inner().to_vec(),
                                        )
                                        .unwrap(),
                                        right_key,
                                        stream_def.map.clone().unwrap(),
                                    )
                                    .unwrap();

                                    tracing::trace!(
                                        "Managed to write to partition: {:#?}",
                                        stream_def.partition_key
                                    );

                                    let result = broker_lock
                                        .produce(
                                            stream_name.inner(),
                                            &partition_val,
                                            new_record_batch,
                                        )
                                        .await;

                                    tracing::trace!(
                                        "Result from producing with a join: {:#?}",
                                        result
                                    );
                                }

                                drop(broker_lock);
                            }
                        }
                    }

                    let lock = left_subscription_ref.write().await;

                    lock.acknowledge(&partition, offset).unwrap();

                    drop(lock);
                }
            } else {
                tracing::info!("Nothing to take, will just continue..");
            };
        }
    });

    Ok(())
}

/// Function to effectively take the Join, the right and left values
/// and return the resultant record batch that needs to be written to the new
/// stream.
fn values_to_batches(
    join: &Join,
    left: Option<RecordBatch>,
    right: Option<RecordBatch>,
    left_key: String,
    right_key: String,
    map: BTreeMap<String, String>,
) -> Option<RecordBatch> {
    let mut fields = vec![];
    let mut columns = vec![];

    for (resultant_name, origin) in map.iter() {
        tracing::info!("Resultant Name: {resultant_name}, origin: {origin} ");

        let mut split_origin = origin.split(".");

        if let (Some(origin), Some(origin_key)) = (split_origin.nth(0), split_origin.nth(0)) {
            match origin {
                origin if origin == left_key => match join {
                    Join::Inner(_) => {
                        let left = left.as_ref().unwrap();

                        let (col, field) = col_name_to_field_and_col(&left, origin_key);
                        let field = field.with_name(resultant_name);

                        columns.push(col);
                        fields.push(field);
                    }
                    Join::LeftOuter(_) => todo!(),
                    Join::RightOuter(_) => todo!(),
                    Join::Full(_) => todo!(),
                },
                origin if origin == right_key => match join {
                    Join::Inner(_) => {
                        let right = right.as_ref().unwrap();

                        let (col, field) = col_name_to_field_and_col(&right, origin_key);

                        columns.push(col);
                        fields.push(field);
                    }
                    Join::LeftOuter(_) => todo!(),
                    Join::RightOuter(_) => todo!(),
                    Join::Full(_) => todo!(),
                },
                _ => {
                    tracing::error!("Origin does not match left of right. Continuing.");
                }
            }
        } else {
            tracing::error!("Origin/Origin Key pairing non-existent. Continuing.");
        }
    }

    let schema = Schema::new(fields);

    let result = RecordBatch::try_new(Arc::new(schema), columns)
        .inspect_err(|err| {
            tracing::error!(
                "Failed to create RecordBatch from Schema and Columsn: {:#?}",
                err
            );
        })
        .ok();

    result
}

fn col_name_to_field_and_col(batch: &RecordBatch, col_name: &str) -> (ArrayRef, Field) {
    tracing::info!("Attempting to retrieve data from RecordBatch: {:#?}", batch);

    let schema = batch.schema();

    let schema_index = schema
        .index_of(col_name)
        .inspect(|err| {
            tracing::error!(
                "Unexpected error not being able to retrieve partition key by name: {:#?}",
                err
            );
        })
        .unwrap();

    let col = batch.column(schema_index);
    let field = schema.field(schema_index);

    (col.clone(), field.clone())
}

fn get_partition_key_from_record_batch(
    batch: &RecordBatch,
    index: usize,
    col_name: &str,
) -> Vec<u8> {
    let schema_index = batch
        .schema()
        .index_of(col_name)
        .inspect_err(|err| {
            tracing::error!(
                "Unexpected error not being able to retrieve partition key by name: {:#?}",
                err
            );
        })
        .unwrap();

    let col = batch.column(schema_index);

    tracing::info!("Col: {:#?}", col);

    let value = array_value_to_string(col, 0);

    // let col = cast(col, &arrow::datatypes::DataType::Utf8).inspect_err(|err| {
    //     tracing::error!("Received Error when trying to cast StringArray: {:#?}", err);
    // }).unwrap();

    // let value = col.as_string_opt::<i64>(  );

    tracing::info!("Result: {:#?}", value);

    value.unwrap().as_bytes().to_vec()
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow::{
        array::{Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use tracing_test::traced_test;

    use crate::{derive::joining::values_to_batches, topography::Join};

    #[test]
    #[traced_test]
    fn can_query_record_batches() {
        let map = BTreeMap::from([
            ("customer_id".to_string(), "customer.id".to_string()),
            (
                "customer_first_name".to_string(),
                "customer.first_name".to_string(),
            ),
            (
                "customer_last_name".to_string(),
                "customer.last_name".to_string(),
            ),
            ("age".to_string(), "customer.age".to_string()),
            (
                "address_line_1".to_string(),
                "address.address_line_1".to_string(),
            ),
            (
                "address_line_2".to_string(),
                "address.address_line_2".to_string(),
            ),
            ("city".to_string(), "address.city".to_string()),
            ("province".to_string(), "address.province".to_string()),
        ]);

        let join = Join::Inner("customer_id".into());

        let left_schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]);

        let left = RecordBatch::try_new(
            Arc::new(left_schema),
            vec![
                Arc::new(StringArray::from(vec!["ID"])),
                Arc::new(StringArray::from(vec!["TestFirstName"])),
                Arc::new(StringArray::from(vec!["TestSurname"])),
                Arc::new(Int32Array::from(vec![30])),
            ],
        )
        .unwrap();

        let right_schema: Schema = Schema::new(vec![
            Field::new("address_line_1", DataType::Utf8, false),
            Field::new("address_line_2", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("province", DataType::Utf8, false),
        ]);

        let right = RecordBatch::try_new(
            Arc::new(right_schema),
            vec![
                Arc::new(StringArray::from(vec!["12 Tennatn Avenut"])),
                Arc::new(StringArray::from(vec!["Bonteheuwel"])),
                Arc::new(StringArray::from(vec!["Cape Town"])),
                Arc::new(StringArray::from(vec!["Western Cape"])),
            ],
        )
        .unwrap();

        let result = values_to_batches(
            &join,
            Some(left),
            Some(right),
            "customer".to_string(),
            "address".to_string(),
            map,
        );

        assert!(result.is_some());

        let result = result.unwrap();

        let result_schema = Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("customer_first_name", DataType::Utf8, false),
            Field::new("customer_last_name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("address_line_1", DataType::Utf8, false),
            Field::new("address_line_2", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("province", DataType::Utf8, false),
        ]);

        let expected_result = RecordBatch::try_new(
            Arc::new(result_schema),
            vec![
                Arc::new(StringArray::from(vec!["ID"])),
                Arc::new(StringArray::from(vec!["TestFirstName"])),
                Arc::new(StringArray::from(vec!["TestSurname"])),
                Arc::new(Int32Array::from(vec![30])),
                Arc::new(StringArray::from(vec!["12 Tennatn Avenut"])),
                Arc::new(StringArray::from(vec!["Bonteheuwel"])),
                Arc::new(StringArray::from(vec!["Cape Town"])),
                Arc::new(StringArray::from(vec!["Western Cape"])),
            ],
        )
        .unwrap();

        for field in result.schema().fields() {
            let schema = result.schema();

            let result_field = schema.field_with_name(field.name()).unwrap();

            // field equality.
            assert_eq!(*result_field, **field);

            let expected_schema = expected_result.schema();

            let (index, _) = schema.column_with_name(field.name()).unwrap();
            let (expect_index, _) = expected_schema.column_with_name(field.name()).unwrap();

            let expected_value = expected_result.column(expect_index);
            let value = result.column(index);

            assert_eq!(expected_value, value);
        }
    }
}
