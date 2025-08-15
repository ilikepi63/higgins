use arrow::array::RecordBatch;
use higgins_functions::{
    ArbitraryLengthBuffer,
    utils::WasmAllocator,
    wasmtime::{Config, Engine, Linker, Module, OptLevel, Store},
};

use crate::storage::arrow_ipc::{read_arrow, write_arrow};

/// Wrapper around the reduce functions.
pub fn run_reduce_function(curr: &RecordBatch, prev: &RecordBatch, module: Vec<u8>) -> RecordBatch {
    let engine = Engine::new(
        Config::new()
            .debug_info(true)
            .coredump_on_trap(true)
            .cranelift_opt_level(OptLevel::None),
    )
    .unwrap();

    let module = Module::new(&engine, module).unwrap();

    let linker = Linker::new(&engine);

    let mut store: Store<u32> = Store::new(&engine, 4);

    let instance = linker.instantiate(&mut store, &module).unwrap();

    let mut wasm_malloc_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "_malloc")
        .unwrap();

    let mut memory = instance.get_memory(&mut store, "memory").unwrap();

    let mut allocator = WasmAllocator::from(&mut store, &mut wasm_malloc_fn, &mut memory);

    let curr_ptr = {
        let data = ArbitraryLengthBuffer::from(write_arrow(curr).as_ref()).into_inner();
        let record_batch_ptr = allocator.copy(&data);
        record_batch_ptr
    };

    let prev_ptr = {
        let data = ArbitraryLengthBuffer::from(write_arrow(prev).as_ref()).into_inner();
        let record_batch_ptr = allocator.copy(&data);
        record_batch_ptr
    };

    let wasm_run_fn = instance
        .get_typed_func::<(u32, u32), u32>(&mut store, "run")
        .unwrap();

    let record_batch_ptr = wasm_run_fn.call(&mut store, (prev_ptr, curr_ptr)).unwrap();

    tracing::trace!("Received Record batch PTR: {record_batch_ptr}");

    let record_batch = {
        let mut buf = [0_u8; 8];

        memory
            .read(&store, record_batch_ptr.try_into().unwrap(), &mut buf)
            .unwrap();

        let length = u64::from_be_bytes(buf);

        let mut buf = vec![0_u8; length as usize + 8];

        memory
            .read(&store, record_batch_ptr.try_into().unwrap(), &mut buf)
            .unwrap();

        let array = ArbitraryLengthBuffer::new(buf);

        let record_batch = read_arrow(array.data()).nth(0).unwrap().unwrap();

        record_batch
    };

    record_batch
}
