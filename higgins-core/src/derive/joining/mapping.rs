//! The utilities surrounding mapping of joined properties to their ultime representation inside of the
//! joined dataset.

use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;

/// JoinMapping is the mapping metadata between a joined data structs properties
/// and its derivative properties.
///
/// For example:
///
/// Customer {
///  id,
///  first_name,
///  last_name
///}
///
/// Address {
///  customer_id,
///  address
///  etc...
///}
///
/// JoinedCustomerAddress {
///    customer_first_name: customer.first_name,
///    customer_last_name: customer.last_name,
///    customer_address: address.address
/// }
#[derive(Clone)]
pub struct JoinMapping(
    std::sync::Arc<arrow::datatypes::Schema>,
    Vec<JoinMappingDerivativeToProperty>,
);

type JoinMappingDerivativeToProperty = (JoinedStreamPropertyKey, (StreamName, StreamPropertyKey));

/// The original stream that this derived value comes from.
type StreamName = String;

/// The name of the key that this value originates from.
type StreamPropertyKey = String;

/// The renamed property where this value will be present in the resultant joined data structure.
type JoinedStreamPropertyKey = String;

impl JoinMapping {
    /// Given a list of record batches with their stream names,
    /// return a batch that represents the amalgamated result using this mapping.
    pub fn map_arrow(
        &self,
        batches: Vec<Option<(String, RecordBatch)>>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let mut columns = vec![];

        // We need to order the resultant columns by the given schema.
        for field in self.0.fields.iter() {
            let field_name = field.name();

            let (_, (stream_name, stream_propery_key)) = self
                .1
                .iter()
                .find(|(prop_name, _)| prop_name == field_name)
                .unwrap();

            let (_, batch) = batches.iter().find(|val| name == stream_name).unwrap();

            let column = batch.column_by_name(stream_propery_key).unwrap();

            columns.push(column.clone());
        }

        Ok(RecordBatch::try_new(self.0.clone(), columns)?)
    }
}

/// Conversion from a BTreeMap representing the Property Mapping here.
///
/// See 'ConfigurationStreamDefinition' for more information.
impl
    From<(
        std::sync::Arc<arrow::datatypes::Schema>,
        BTreeMap<String, String>,
    )> for JoinMapping
{
    fn from(
        (schema, value): (
            std::sync::Arc<arrow::datatypes::Schema>,
            BTreeMap<String, String>,
        ),
    ) -> Self {
        JoinMapping(
            schema,
            value
                .iter()
                .filter_map(|(resultant_name, origin)| {
                    let mut split_origin = origin.split(".");

                    if let (Some(origin), Some(origin_key)) =
                        (split_origin.next(), split_origin.next())
                    {
                        Some((resultant_name, origin, origin_key))
                    } else {
                        None
                    }
                })
                .fold(
                    Vec::<JoinMappingDerivativeToProperty>::new(),
                    |mut acc, (resultant_name, origin, origin_key)| {
                        acc.push((
                            resultant_name.to_owned(),
                            (origin.to_owned(), origin_key.to_owned()),
                        ));

                        acc
                    },
                ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    // Helper function to create a simple schema
    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("customer_first_name", DataType::Utf8, false),
            Field::new("customer_last_name", DataType::Utf8, false),
            Field::new("customer_address", DataType::Utf8, false),
        ]))
    }

    // Helper function to create a customer record batch
    fn create_customer_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
        ]));

        let id = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let first_name = Arc::new(StringArray::from(vec!["John", "Jane", "Bob"]));
        let last_name = Arc::new(StringArray::from(vec!["Doe", "Smith", "Johnson"]));

        RecordBatch::try_new(schema, vec![id, first_name, last_name]).unwrap()
    }

    // Helper function to create an address record batch
    fn create_address_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, false),
            Field::new("address", DataType::Utf8, false),
        ]));

        let customer_id = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let address = Arc::new(StringArray::from(vec![
            "123 Main St",
            "456 Oak Ave",
            "789 Pine Rd",
        ]));

        RecordBatch::try_new(schema, vec![customer_id, address]).unwrap()
    }

    #[test]
    fn test_join_mapping_from_btreemap_basic() {
        let schema = create_test_schema();
        let mut mapping = BTreeMap::new();
        mapping.insert(
            "customer_first_name".to_string(),
            "customer.first_name".to_string(),
        );
        mapping.insert(
            "customer_last_name".to_string(),
            "customer.last_name".to_string(),
        );
        mapping.insert(
            "customer_address".to_string(),
            "address.address".to_string(),
        );

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        // Verify the schema is stored correctly
        assert_eq!(join_mapping.0.fields().len(), 3);

        // Verify the mapping has 3 entries
        assert_eq!(join_mapping.1.len(), 3);
    }

    #[test]
    fn test_join_mapping_from_btreemap_with_invalid_entries() {
        let schema = create_test_schema();
        let mut mapping = BTreeMap::new();
        mapping.insert(
            "customer_first_name".to_string(),
            "customer.first_name".to_string(),
        );
        // Invalid entry without a dot separator
        mapping.insert("invalid_entry".to_string(), "nodot".to_string());
        mapping.insert(
            "customer_address".to_string(),
            "address.address".to_string(),
        );

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        // Should filter out invalid entries
        assert_eq!(join_mapping.1.len(), 2);
    }

    #[test]
    fn test_join_mapping_from_btreemap_empty() {
        let schema = Arc::new(Schema::empty());
        let mapping = BTreeMap::new();

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        assert_eq!(join_mapping.1.len(), 0);
    }

    #[test]
    fn test_join_mapping_from_btreemap_multiple_dots() {
        let schema = create_test_schema();
        let mut mapping = BTreeMap::new();
        // Entry with multiple dots - should only use first two parts
        mapping.insert(
            "customer_first_name".to_string(),
            "customer.first_name.extra".to_string(),
        );

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        assert_eq!(join_mapping.1.len(), 1);
        let (resultant, (stream, key)) = &join_mapping.1[0];
        assert_eq!(resultant, "customer_first_name");
        assert_eq!(stream, "customer");
        assert_eq!(key, "first_name");
    }

    #[test]
    fn test_map_arrow_basic() {
        let schema = create_test_schema();
        let mut mapping = BTreeMap::new();
        mapping.insert(
            "customer_first_name".to_string(),
            "customer.first_name".to_string(),
        );
        mapping.insert(
            "customer_last_name".to_string(),
            "customer.last_name".to_string(),
        );
        mapping.insert(
            "customer_address".to_string(),
            "address.address".to_string(),
        );

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        let customer_batch = create_customer_batch();
        let address_batch = create_address_batch();

        let batches = vec![
            ("customer".to_string(), customer_batch),
            ("address".to_string(), address_batch),
        ];

        let result = join_mapping.map_arrow(batches).unwrap();

        // Verify the result has the correct schema
        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 3);

        // Verify column names
        assert!(result.column_by_name("customer_first_name").is_some());
        assert!(result.column_by_name("customer_last_name").is_some());
        assert!(result.column_by_name("customer_address").is_some());
    }

    #[test]
    fn test_map_arrow_column_order_matches_schema() {
        let schema = create_test_schema();
        let mut mapping = BTreeMap::new();
        mapping.insert(
            "customer_first_name".to_string(),
            "customer.first_name".to_string(),
        );
        mapping.insert(
            "customer_last_name".to_string(),
            "customer.last_name".to_string(),
        );
        mapping.insert(
            "customer_address".to_string(),
            "address.address".to_string(),
        );

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        let customer_batch = create_customer_batch();
        let address_batch = create_address_batch();

        let batches = vec![
            ("customer".to_string(), customer_batch),
            ("address".to_string(), address_batch),
        ];

        let result = join_mapping.map_arrow(batches).unwrap();

        let schema = result.schema();

        // Verify columns are in schema order
        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert_eq!(
            column_names,
            vec![
                "customer_first_name",
                "customer_last_name",
                "customer_address"
            ]
        );
    }

    #[test]
    fn test_map_arrow_single_stream() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("renamed_id", DataType::Int32, false),
            Field::new("renamed_first_name", DataType::Utf8, false),
        ]));

        let mut mapping = BTreeMap::new();
        mapping.insert("renamed_id".to_string(), "customer.id".to_string());
        mapping.insert(
            "renamed_first_name".to_string(),
            "customer.first_name".to_string(),
        );

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        let customer_batch = create_customer_batch();
        let batches = vec![("customer".to_string(), customer_batch)];

        let result = join_mapping.map_arrow(batches).unwrap();

        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    #[should_panic]
    fn test_map_arrow_missing_stream() {
        let schema = create_test_schema();
        let mut mapping = BTreeMap::new();
        mapping.insert(
            "customer_first_name".to_string(),
            "customer.first_name".to_string(),
        );

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        // Only provide address batch, missing customer batch
        let address_batch = create_address_batch();
        let batches = vec![("address".to_string(), address_batch)];

        // This should panic due to unwrap() on missing stream
        let _ = join_mapping.map_arrow(batches);
    }

    #[test]
    #[should_panic]
    fn test_map_arrow_missing_column() {
        let schema = create_test_schema();
        let mut mapping = BTreeMap::new();
        mapping.insert(
            "customer_first_name".to_string(),
            "customer.nonexistent_column".to_string(),
        );

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        let customer_batch = create_customer_batch();
        let batches = vec![("customer".to_string(), customer_batch)];

        // This should panic due to unwrap() on missing column
        let _ = join_mapping.map_arrow(batches);
    }

    #[test]
    fn test_join_mapping_clone() {
        let schema = create_test_schema();
        let mut mapping = BTreeMap::new();
        mapping.insert(
            "customer_first_name".to_string(),
            "customer.first_name".to_string(),
        );

        let join_mapping = JoinMapping::from((schema.clone(), mapping));
        let cloned_mapping = join_mapping.clone();

        // Verify clone has same number of mappings
        assert_eq!(join_mapping.1.len(), cloned_mapping.1.len());
    }

    #[test]
    fn test_btreemap_ordering_preserved() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a_field", DataType::Utf8, false),
            Field::new("b_field", DataType::Utf8, false),
            Field::new("c_field", DataType::Utf8, false),
        ]));

        let mut mapping = BTreeMap::new();
        // Insert in non-alphabetical order
        mapping.insert("c_field".to_string(), "stream.c".to_string());
        mapping.insert("a_field".to_string(), "stream.a".to_string());
        mapping.insert("b_field".to_string(), "stream.b".to_string());

        let join_mapping = JoinMapping::from((schema.clone(), mapping));

        // BTreeMap should sort keys, so verify we have all entries
        assert_eq!(join_mapping.1.len(), 3);
    }
}
