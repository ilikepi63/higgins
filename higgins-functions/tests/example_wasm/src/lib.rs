#![allow(clippy::missing_safety_doc)]

use arrow::array::Int32Array;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};

const ERROR_MSG_SIZE: usize = 1000;

static mut ERRORS: [[u8; ERROR_MSG_SIZE]; 10] = [[0_u8; ERROR_MSG_SIZE]; 10];
static mut COUNTER: usize = 0;

fn log_error(s: &str) {
    unsafe {
        for (index, byte) in s.as_bytes().iter().enumerate() {
            ERRORS[COUNTER][index] = *byte;
        }
    };
    unsafe {
        COUNTER += 1;
    };
}

#[unsafe(no_mangle)]
pub unsafe fn get_errors() -> *const [u8; ERROR_MSG_SIZE] {
    unsafe {
        #[allow(static_mut_refs)]
        ERRORS.as_ptr()
    }
}

#[unsafe(no_mangle)]
pub unsafe fn _malloc(len: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(len.try_into().unwrap());
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

#[unsafe(no_mangle)]
pub unsafe fn run(array_ptr: *const FFI_ArrowArray, schema_ptr: *const FFI_ArrowSchema) -> u32 {
    // log_error!()

    let array = unsafe { FFI_ArrowArray::from_raw(array_ptr as *mut FFI_ArrowArray) };
    let schema = unsafe { FFI_ArrowSchema::from_raw(schema_ptr as *mut FFI_ArrowSchema) };

    log_error(&format!("{array:#?}"));

    let array = unsafe { from_ffi(array, &schema) }.unwrap();

    let array = Int32Array::from(array);

    let result = array.iter().fold(0_i32, |mut acc, curr| match curr {
        Some(i) => {
            acc += i;
            acc
        }
        None => acc,
    });

    std::mem::forget(array);
    std::mem::forget(schema);

    result.try_into().unwrap()
}
