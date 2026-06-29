use arrow::array::RecordBatch;
use higgins_shared::{PartitionName, read_arrow};

pub fn customer_json_with_id_and_age(id: &str, age: i32) -> String {
    format!(r#"{{"id":"{id}","first_name":"John","last_name":"Doe","age":{age}}}"#)
}

pub fn assert_customer_data(arrow: RecordBatch) {
    assert_eq!(
        arrow
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .unwrap(),
        21
    );

    assert_eq!(
        arrow
            .column_by_name("first_name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .unwrap(),
        "John"
    );

    assert_eq!(
        arrow
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .unwrap(),
        "1"
    );

    assert_eq!(
        arrow
            .column_by_name("last_name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .unwrap(),
        "Doe"
    );
}
