use arrow::array::RecordBatch;

use crate::{
    copy_array, copy_schema,
    types::{WasmPtr, WasmRecordBatch},
    utils::{WasmAllocator, u32_to_u8},
};

pub fn record_batch_to_wasm(rb: RecordBatch, allocator: &mut WasmAllocator) -> WasmRecordBatch {
    let len = rb.num_columns();

    let data = rb.columns().iter().map(|array| array.to_data());

    let schema = rb.schema();

    let schema_data = data.clone().zip(schema.fields());

    let arrays = data
        .clone() // hoping this clone is cheap somehow.
        .map(|data| {
            let array = copy_array(&data, allocator);

            array.inner()
        })
        .collect::<Box<[_]>>();

    let schema = schema_data
        .map(|(data, field)| {
            let schema = copy_schema(data.data_type(), field.clone(), allocator).unwrap();

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

pub fn clone_record_batch(array: WasmRecordBatch, allocator: &mut WasmAllocator) -> u32 {
    let buffer: &[u8] = unsafe {
        &std::mem::transmute::<WasmRecordBatch, [u8; std::mem::size_of::<WasmRecordBatch>()]>(array)
    };

    allocator.copy(buffer)
}
