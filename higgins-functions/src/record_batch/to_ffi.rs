
use std::sync::Arc;

use arrow::{
    array::{RecordBatch, make_array},
    datatypes::{DataType, Field, Schema},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi},
};

use super::{ArrowArray, ArrowSchema, FFIRecordBatch};

pub fn record_batch_to_ffi(record_batch: RecordBatch) -> FFIRecordBatch {

    let mut arrays_vec = vec![];
    let mut schema_vec = vec![];

    for columns in record_batch.columns().into_iter() {
        let data = columns.to_data();

        let (array, schema) = to_ffi(&data).unwrap();

        let array = unsafe { std::mem::transmute::<FFI_ArrowArray, ArrowArray>(array) };
        let schema = unsafe { std::mem::transmute::<FFI_ArrowSchema, ArrowSchema>(schema) };

        arrays_vec.push(array);
        schema_vec.push(schema);
    }

    let mut arrays = arrays_vec
        .into_iter()
        .map(Box::new)
        .map(Box::into_raw)
        .collect::<Box<_>>();
    let mut schema = schema_vec
        .into_iter()
        .map(Box::new)
        .map(Box::into_raw)
        .collect::<Box<_>>();

    FFIRecordBatch {
        n_columns: record_batch.num_columns() as i64,
        schema: schema.as_mut_ptr(),
        columns: arrays.as_mut_ptr(),
    }
}