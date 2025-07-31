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

    let _result = wasm_run_fn.call(&mut store, ptr).unwrap();

    RecordBatch::new_empty(batch.schema())
}
