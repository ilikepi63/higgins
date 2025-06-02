use arrow::{
    array::RecordBatch,
    ffi::{FFI_ArrowArray, to_ffi},
};
use higgins_functions::{
    FFIRecordBatch, record_batch_from_ffi, record_batch_to_ffi,
};

#[unsafe(no_mangle)]
pub unsafe fn _malloc(len: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(len.try_into().unwrap());
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

#[unsafe(no_mangle)]
pub unsafe fn run(rb_ptr: *const FFIRecordBatch) -> *const FFIRecordBatch {
    let record_batch = record_batch_from_ffi(*rb_ptr);

    let result = record_batch_to_ffi(record_batch);

    let heaped = Box::new(result);

    let ptr = Box::leak(heaped) as *const FFIRecordBatch;

    ptr
}
