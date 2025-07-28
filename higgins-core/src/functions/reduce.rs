use arrow::array::RecordBatch;
use higgins_functions::{
    clone_record_batch, record_batch_to_wasm,
    utils::WasmAllocator,
    wasmtime::{Config, Engine, Linker, Module, OptLevel, Store},
};


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
        let ptr = record_batch_to_wasm(curr.clone(), &mut allocator);
        let ptr = clone_record_batch(ptr, &mut allocator);
        ptr
    };

    let prev_ptr = {
        let ptr = record_batch_to_wasm(prev.clone(), &mut allocator);
        let ptr = clone_record_batch(ptr, &mut allocator);
        ptr
    };

    let wasm_run_fn = instance
        .get_typed_func::<(u32, u32), u32>(&mut store, "run")
        .unwrap();

    let _result = wasm_run_fn.call(&mut store, (prev_ptr, curr_ptr)).unwrap();

    // TODO: do we have a function for converting from pointer back to
    RecordBatch::new_empty(curr.schema())
}
