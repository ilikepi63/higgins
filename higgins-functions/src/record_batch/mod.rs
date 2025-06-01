mod from_ffi;
mod to_ffi;
mod types;
#[cfg(feature = "full")]
mod wasm;

pub use types::{ArrowArray, ArrowSchema, FFIRecordBatch};

#[cfg(feature = "full")]
pub use wasm::{clone_record_batch, record_batch_to_wasm};

pub use from_ffi::record_batch_from_ffi;
pub use to_ffi::record_batch_to_ffi;
