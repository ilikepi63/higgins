#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::sync::Arc;

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
};
use wasmtime::component::types::Record;

use crate::{
    copy_array, copy_schema,
    types::{WasmPtr, WasmRecordBatch},
    utils::{WasmAllocator, u32_to_u8},
};

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

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

        fields.push(Field::new(
            schema.name().unwrap_or("_"),
            data.data_type().clone(),
            schema.nullable(),
        ));

        arrays
    }

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("b", DataType::Float64, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(id_array), Arc::new(float_array)],
    )
    .unwrap();

    // let record_batch = RecordBatch::try_new(schema, columns);

    todo!()
}

pub fn record_batch_to_wasm(rb: RecordBatch, allocator: &mut WasmAllocator) -> WasmRecordBatch {
    let len = rb.num_columns();

    let data = rb.columns().iter().map(|array| {
        let data = array.to_data();

        data
    });

    let arrays = data
        .clone() // hoping this clone is cheap somehow.
        .map(|data| {
            let array = copy_array(&data, allocator);

            array.inner()
        })
        .collect::<Box<[_]>>();

    let schema = data
        .map(|data| {
            let schema = copy_schema(data.data_type(), allocator).unwrap();

            schema.inner()
        })
        .collect::<Box<[_]>>();

    let arrays_ptr = allocator.copy(u32_to_u8(&arrays));
    let schema_ptr = allocator.copy(u32_to_u8(&schema));

    WasmRecordBatch {
        n_columns: len as i64,
        schema: WasmPtr::new(schema_ptr),
        columns: WasmPtr::new(arrays_ptr),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Array, Float64Array, Int32Array, RecordBatch, record_batch};
    use arrow::datatypes::{DataType, Field, Float64Type, Schema};
    use arrow::ffi::to_ffi;
    #[test]
    fn can_test_record_batch() {
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let float_array = Float64Array::from(vec![Some(1.2), None, Some(1.4)]);

        println!("data type: {:#?}", schema.field(0).data_type());

        let (ffi_array, ffi_schema) = to_ffi(&id_array.to_data()).unwrap();

        println!("Schema: {:#?}", ffi_schema);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("b", DataType::Float64, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(float_array)],
        )
        .unwrap();

        println!("Schema: {:#?}", batch.schema());

        println!("{:#?}", batch);

        panic!();
    }
}
