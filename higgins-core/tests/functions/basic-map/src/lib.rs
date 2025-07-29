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
pub unsafe fn run(rb_ptr: *const FFIRecordBatch) -> *const FFIRecordBatch {
    // Retrieve record batch from FFI ptr.
    let record_batch = record_batch_from_ffi(unsafe { *rb_ptr });

    // Retrieve the data col name.
    let col = col_name_to_field_and_col(&record_batch, "data");

    // Cast to primitive type.
    let col = col.0.as_primitive::<Int32Type>();

    let arr = {
        let mut result = vec![];

        for val in col.iter() {
            result.push(val.map(|val| val * 2));
        }

        Int32Array::from(vec![Some(1), None, Some(2)])
    };

    let batch = RecordBatch::try_new(
        record_batch.schema(),
        vec![col_name_to_field_and_col(&record_batch, "id").0, Arc::new(arr)],
    ).unwrap();

    // record_batch!(
    //     ("id", Utf8, col_name_to_field_and_col(&record_batch, "id").0),
    //     ("data", Int32, arr),
    // );

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
