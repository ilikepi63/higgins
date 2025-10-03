//! Algorithms related to joining two streams.
//!
//! There exists a  set of types of stream joins, akin to SQL joins:
//! - Inner Join        -> emits a value for each corresponding index value for both underlying streams.
//! - Left Outer Join   -> emits a value for every value on the left side of the join, regardless of whether they have a matching key on the alternate stream.
//! - Right Outer Join  -> Similar to Left Outer Join, except on the right side of the join.
//! - Full Join         -> Similar to Right or Left Outer, except will emit for all values.

// TODO: How do we chain multiple streams together?.

use std::{collections::BTreeMap, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema};
use tokio::sync::RwLock;

mod full_join;
mod inner_join;
pub mod join;
mod opts;
mod outer_join;

use crate::{
    broker::Broker,
    client::ClientRef,
    derive::{
        joining::{join::JoinDefinition, opts::create_join_operator, outer_join::OuterSide},
        utils::col_name_to_field_and_col,
    },
    error::HigginsError,
    topography::Join,
};

pub async fn create_joined_stream_from_definition(
    definition: JoinDefinition,
    _broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    // Instantiate Operator on this definition.
    let operator = create_join_operator(definition, broker_ref);

    // Add the operator to a referencable struct.
    // broker.add_operator();

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

        if let (Some(origin), Some(origin_key)) = (split_origin.next(), split_origin.next()) {
            match origin {
                origin if origin == left_key => match join {
                    Join::Inner(_) => {
                        let left = left.as_ref().unwrap();

                        let (col, field) = col_name_to_field_and_col(left, origin_key);
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
                        if let Some(right) = right.as_ref() {
                            let (col, field) = col_name_to_field_and_col(right, origin_key);
                            let field = field.with_name(resultant_name);

                            columns.push(col);
                            fields.push(field);
                        }
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

    RecordBatch::try_new(Arc::new(schema), columns)
        .inspect_err(|err| {
            tracing::error!(
                "Failed to create RecordBatch from Schema and Columsn: {:#?}",
                err
            );
        })
        .ok()
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
