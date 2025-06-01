use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use higgins_functions::{
    clone_record_batch, record_batch_to_wasm, utils::WasmAllocator,
};
use wasmtime::{Config, Engine, Linker, Module, OptLevel, Store};

/*** */
#[test]
fn simple_record_batch() {

    let current_dir = std::env::current_dir();

    println!("{:#?}", current_dir);

    let wasm =
        std::fs::read("../target/wasm32-unknown-unknown/release/example_record_batch.wasm").unwrap();

    let engine = Engine::new(
        Config::new()
            .debug_info(true)
            .coredump_on_trap(true)
            .cranelift_opt_level(OptLevel::None),
    )
    .unwrap();

    let module = Module::new(&engine, wasm).unwrap();

    let linker = Linker::new(&engine);

    let mut store: Store<u32> = Store::new(&engine, 4);

    let instance = linker.instantiate(&mut store, &module).unwrap();

    let mut wasm_malloc_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "_malloc")
        .unwrap();

    let mut memory = instance.get_memory(&mut store, "memory").unwrap();

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();

    let mut allocator = WasmAllocator::from(&mut store, &mut wasm_malloc_fn, &mut memory);

    let ptr = record_batch_to_wasm(batch, &mut allocator);

    let ptr = clone_record_batch(ptr, &mut allocator);

    let wasm_run_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "run")
        .unwrap();

    let _result = wasm_run_fn.call(&mut store, ptr).unwrap();

    // TODO: this test basically just makes sure this does not panic. 
    // We need some more tests for this.

}

#[test]
fn can_sum_int32_array() {
    let array = Int32Array::from(vec![Some(1), None, Some(3)]);

    let data = array.into_data();

    let array = Int32Array::from(data);

    let result = array.iter().fold(0_i32, |mut acc, curr| match curr {
        Some(i) => {
            acc += i;
            acc
        }
        None => acc,
    });

    assert_eq!(result, 4);
}
