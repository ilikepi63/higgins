use arrow::array::RecordBatch;
use higgins_functions::{
    clone_record_batch, record_batch_to_wasm,
    utils::WasmAllocator,
    wasmtime::{Config, Engine, Linker, Module, OptLevel, Store},
};

/// Wrapper around the mapping functions.
pub fn run_map_function(batch: &RecordBatch, module: Vec<u8>) -> RecordBatch {
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

    tracing::info!("Copying batch {:#?} to Wasm", batch);

    let ptr = record_batch_to_wasm(batch.clone(), &mut allocator);

    let ptr = clone_record_batch(ptr, &mut allocator);

    let wasm_run_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "run")
        .unwrap();

    let result = wasm_run_fn.call(&mut store, ptr);

    // Get errors.

        let wasm_error_fn = instance
        .get_typed_func::<(), u32>(&mut store, "get_errors")
        .unwrap();

    let errors = wasm_error_fn.call(&mut store, ()).unwrap();

    let mut bytes = vec![0; 1000 * 10];

    memory
        .read(&mut store, errors.try_into().unwrap(), &mut bytes)
        .unwrap();

    for chunk in bytes.chunks(100) {
        let s = String::from_utf8_lossy(chunk);

        tracing::info!("{:#?}", s);
    }


    result.unwrap();

    RecordBatch::new_empty(batch.schema())
}
