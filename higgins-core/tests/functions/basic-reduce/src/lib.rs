use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::datatypes::{Field, Int32Type};
use arrow::record_batch::RecordBatch;
use higgins_functions::{FFIRecordBatch, record_batch_from_ffi, record_batch_to_ffi};
use std::sync::Arc;

#[unsafe(no_mangle)]
pub unsafe fn _malloc(len: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(len.try_into().unwrap());
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

#[unsafe(no_mangle)]
pub unsafe fn run(
    prev_rb_ptr: *const FFIRecordBatch,
    rb_ptr: *const FFIRecordBatch,
) -> *const FFIRecordBatch {
    // Retrieve record batch from FFI ptr.
    let record_batch = record_batch_from_ffi(unsafe { *rb_ptr });
    let prev_record_batch = record_batch_from_ffi(unsafe { *rb_ptr });

        // Retrieve the data col name.
        let col = col_name_to_field_and_col(&record_batch, "data");
        let prev_col = col_name_to_field_and_col(&prev_record_batch, "data");

        // Cast to primitive type.
        let curr_col = col.0.as_primitive::<Int32Type>();
        let prev_col = prev_col.0.as_primitive::<Int32Type>();

    let arr = {
        let mut result = vec![];

        for index in 0..curr_col.len() {
            let curr_val = curr_col.value(index);
            let prev_val = prev_col.value(index);

            result.push(curr_val + prev_val);
        }

        Int32Array::from(result)
    };

    let batch = RecordBatch::try_new(
        record_batch.schema(),
        vec![
            col_name_to_field_and_col(&record_batch, "id").0,
            Arc::new(arr),
        ],
    )
    .unwrap();

    let result = record_batch_to_ffi(batch);

    let heaped = Box::new(result);

    let ptr = Box::leak(heaped) as *const FFIRecordBatch;

    ptr
}

pub fn col_name_to_field_and_col(batch: &RecordBatch, col_name: &str) -> (ArrayRef, Field) {
    let schema = batch.schema();

    let schema_index = schema.index_of(col_name).unwrap();

    let col = batch.column(schema_index);
    let field = schema.field(schema_index);

    (col.clone(), field.clone())
}
