use arrow::array::RecordBatch;
use higgins_functions::{
    types::ArbitraryLengthBuffer,
    utils::WasmAllocator,
    wasmtime::{Engine, Linker, Module, Store},
};

use higgins_shared::{HigginsError, read_arrow, write_arrow};

/// Wrapper around the mapping functions.
pub fn run_map_function(
    batch: &RecordBatch,
    engine: &Engine,
    module: &Module,
) -> Result<RecordBatch, HigginsError> {
    let linker = Linker::new(&engine);

    let mut store: Store<u32> = Store::new(&engine, 4);

    let instance = linker.instantiate(&mut store, &module)?;

    let mut wasm_malloc_fn = instance.get_typed_func::<u32, u32>(&mut store, "_malloc")?;

    let mut memory = instance.get_memory(&mut store, "memory")?;

    let mut allocator = WasmAllocator::from(&mut store, &mut wasm_malloc_fn, &mut memory);

    tracing::info!("Copying batch {:#?} to Wasm", batch);

    let data = ArbitraryLengthBuffer::from(write_arrow(batch).as_ref()).into_inner();

    let record_batch_ptr = allocator.copy(&data)?;

    let wasm_run_fn = instance.get_typed_func::<u32, u32>(&mut store, "run")?;

    let result = wasm_run_fn.call(&mut store, record_batch_ptr);

    // Get errors.

    let wasm_error_fn = instance.get_typed_func::<(), u32>(&mut store, "get_errors")?;

    let errors = wasm_error_fn.call(&mut store, ())?;

    let mut bytes = vec![0; 1000 * 10];

    memory.read(&mut store, errors.try_into()?, &mut bytes)?;

    for chunk in bytes.chunks(100) {
        let s = String::from_utf8_lossy(chunk);

        tracing::info!("{:#?}", s);
    }

    let record_batch_ptr = result?;

    tracing::trace!("Received Record batch PTR: {record_batch_ptr}");

    {
        let mut buf = [0_u8; 8];

        memory.read(&store, record_batch_ptr.try_into()?, &mut buf)?;

        let length = u64::from_be_bytes(buf);

        let mut buf = vec![0_u8; length as usize + 8];

        memory.read(&store, record_batch_ptr.try_into()?, &mut buf)?;

        let array = ArbitraryLengthBuffer::new(buf);

        read_arrow(array.data()).next()??
    }
}
