use arrow::array::RecordBatch;
use higgins_functions::{
    ArbitraryLengthBuffer,
    utils::WasmAllocator,
    wasmtime::{Engine, Linker, Module, Store},
};

use higgins_shared::{HigginsError, read_arrow, write_arrow};

pub fn run_reduce_function(
    curr: &RecordBatch,
    prev: &RecordBatch,
    engine: &Engine,
    module: &Module,
) -> Result<RecordBatch, HigginsError> {
    let linker = Linker::new(&engine);

    let mut store: Store<u32> = Store::new(&engine, 4);

    let instance = linker.instantiate(&mut store, &module)?;

    let mut wasm_malloc_fn = instance.get_typed_func::<u32, u32>(&mut store, "_malloc")?;

    let mut memory = instance.get_memory(&mut store, "memory")?;

    let mut allocator = WasmAllocator::from(&mut store, &mut wasm_malloc_fn, &mut memory);

    let curr_ptr = {
        let current_record_batch_bytes = write_arrow(curr);
        tracing::debug!("Current Record Batch: {:#?}", current_record_batch_bytes);
        let data = ArbitraryLengthBuffer::from(write_arrow(curr).as_ref()).into_inner();

        allocator.copy(&data)?
    };

    let prev_ptr = {
        let previous_record_batch_bytes = write_arrow(prev);
        tracing::debug!("Previous Record Batch: {:#?}", previous_record_batch_bytes);

        let data = ArbitraryLengthBuffer::from(write_arrow(prev).as_ref()).into_inner();

        allocator.copy(&data)?
    };

    let wasm_run_fn = instance.get_typed_func::<(u32, u32), u32>(&mut store, "run")?;

    let record_batch_ptr = wasm_run_fn.call(&mut store, (prev_ptr, curr_ptr));

    tracing::debug!("{:#?}", record_batch_ptr);

    {
        let wasm_error_fn = instance.get_typed_func::<(), u32>(&mut store, "get_errors")?;

        let errors = wasm_error_fn.call(&mut store, ())?;

        let mut bytes = vec![0; 1000 * 10];

        memory.read(&mut store, errors.try_into()?, &mut bytes)?;

        for chunk in bytes.chunks(100) {
            let s = String::from_utf8_lossy(chunk);

            tracing::info!("{:#?}", s);
        }
    }

    let record_batch_ptr = record_batch_ptr?;

    tracing::trace!("Received Record batch PTR: {record_batch_ptr}");

    let result = {
        let mut buf = [0_u8; 8];

        memory.read(&store, record_batch_ptr.try_into()?, &mut buf)?;

        let length = u64::from_be_bytes(buf);

        let mut buf = vec![0_u8; length as usize + 8];

        memory.read(&store, record_batch_ptr.try_into()?, &mut buf)?;

        let array = ArbitraryLengthBuffer::new(buf);

        read_arrow(array.data()).next()??
    };

    result
}
