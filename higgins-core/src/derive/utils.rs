use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::{Field, Schema},
    util::display::array_value_to_string,
};

pub fn col_name_to_field_and_col(batch: &RecordBatch, col_name: &str) -> (ArrayRef, Field) {
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

pub fn get_partition_key_from_record_batch(
    batch: &RecordBatch,
    _index: usize,
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

    let value = array_value_to_string(col, 0);

    value.unwrap().as_bytes().to_vec()
}
