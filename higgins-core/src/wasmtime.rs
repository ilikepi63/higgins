use functions::test_ffi;
use wasmtime::{Engine, Instance, Module, Store};

mod functions;

fn main() -> Result<(), Box<dyn std::error::Error>> {

    // let result = test_ffi();

    let mut dir = std::env::current_dir().unwrap();

    dir.push("example_functions/example_reduce/target/wasm32-unknown-unknown/release/example_reduce.wasm");

    let wasm = std::fs::read(dir).unwrap();

    crate::functions::run(wasm);

    Ok(())
}
