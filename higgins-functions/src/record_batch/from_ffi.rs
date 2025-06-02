use std::sync::Arc;

use arrow::{
    array::{RecordBatch, make_array},
    datatypes::{Field, Schema},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
};

use super::FFIRecordBatch;

pub fn record_batch_from_ffi(rb: FFIRecordBatch) -> RecordBatch {
    let mut arrays = Vec::new();
    let mut fields = Vec::new();

    for columns in 0..rb.n_columns {
        let array = unsafe {
            FFI_ArrowArray::from_raw((*rb.columns).add(columns as usize) as *mut FFI_ArrowArray)
        };
        let schema = unsafe {
            FFI_ArrowSchema::from_raw((*rb.schema).add(columns as usize) as *mut FFI_ArrowSchema)
        };

        let data = unsafe { from_ffi(array, &schema) }.unwrap();

        // Push the field.
        fields.push(Field::new(
            schema.name().unwrap_or("_"),
            data.data_type().clone(),
            schema.nullable(),
        ));

        // Push the array.
        arrays.push(make_array(data));
    }

    let schema = Schema::new(fields);

    RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
}
