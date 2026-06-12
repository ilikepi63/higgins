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
    let linker = Linker::new(engine);

    let mut store: Store<u32> = Store::new(engine, 4);

    let instance = linker
        .instantiate(&mut store, module)
        .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

    let mut wasm_malloc_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "_malloc")
        .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

    let mut memory = instance
        .get_memory(&mut store, "memory")
        .ok_or(HigginsError::Arbitrary(
            "Failed to retrieve WASM Memory.".to_string(),
        ))?;

    let mut allocator = WasmAllocator::from(&mut store, &mut wasm_malloc_fn, &mut memory);

    let curr_ptr = {
        let current_record_batch_bytes = write_arrow(curr);
        tracing::debug!("Current Record Batch: {:#?}", current_record_batch_bytes);
        let data = ArbitraryLengthBuffer::from(write_arrow(curr)?.as_ref()).into_inner();

        allocator.copy(&data)?
    };

    let prev_ptr = {
        let previous_record_batch_bytes = write_arrow(prev);
        tracing::debug!("Previous Record Batch: {:#?}", previous_record_batch_bytes);

        let data = ArbitraryLengthBuffer::from(write_arrow(prev)?.as_ref()).into_inner();

        allocator.copy(&data)?
    };

    let wasm_run_fn = instance
        .get_typed_func::<(u32, u32), u32>(&mut store, "run")
        .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

    let record_batch_ptr = wasm_run_fn
        .call(&mut store, (prev_ptr, curr_ptr))
        .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

    tracing::debug!("{:#?}", record_batch_ptr);

    {
        let wasm_error_fn = instance
            .get_typed_func::<(), u32>(&mut store, "get_errors")
            .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

        let errors = wasm_error_fn
            .call(&mut store, ())
            .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

        let mut bytes = vec![0; 1000 * 10];

        memory
            .read(&mut store, errors.try_into()?, &mut bytes)
            .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

        for chunk in bytes.chunks(100) {
            let s = String::from_utf8_lossy(chunk);

            tracing::info!("{:#?}", s);
        }
    }

    tracing::trace!("Received Record batch PTR: {record_batch_ptr}");

    let result = {
        let mut buf = [0_u8; 8];

        memory
            .read(&store, record_batch_ptr.try_into()?, &mut buf)
            .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

        let length = u64::from_be_bytes(buf);

        let mut buf = vec![0_u8; length as usize + 8];

        memory
            .read(&store, record_batch_ptr.try_into()?, &mut buf)
            .map_err(|err| HigginsError::Arbitrary(err.to_string()))?;

        let array = ArbitraryLengthBuffer::new(buf);

        read_arrow(array.data())?
            .next()
            .ok_or(HigginsError::Arbitrary(
                "Reduction function did not return a record.".to_string(),
            ))??
    };

    Ok(result)
}
