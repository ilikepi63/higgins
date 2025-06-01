use arrow::array::{Array, Int32Array};
use higgins_functions::{copy_array, copy_schema, utils::WasmAllocator};
use wasmtime::{Config, Engine, Linker, Module, OptLevel, Store};

/*** */
#[test]
fn multiple_simple_array() {
    let wasm =
        std::fs::read("tests/example_wasm/target/wasm32-unknown-unknown/release/example_wasm.wasm")
            .unwrap();

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

    let array = Int32Array::from(vec![Some(1), None, Some(3)]);

    let mut allocator = WasmAllocator::from(&mut store, &mut wasm_malloc_fn, &mut memory);

    let ptr = copy_array(&array.to_data(), &mut allocator);

    let ffi_schema_ptr = copy_schema(array.data_type(), &mut allocator).unwrap();

    let wasm_run_fn = instance
        .get_typed_func::<(u32, u32), u32>(&mut store, "run")
        .unwrap();

    let result = wasm_run_fn.call(&mut store, (ptr.inner(), ffi_schema_ptr.inner()));

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

        println!("{:#?}", s);
    }

    assert_eq!(result.unwrap(), 4);
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
