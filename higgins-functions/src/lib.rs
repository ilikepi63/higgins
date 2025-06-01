#[cfg(feature = "full")]
mod array;
mod record_batch;
#[cfg(feature = "full")]
mod schema;
#[cfg(feature = "full")]
pub mod types;

#[cfg(feature = "full")]
pub mod utils;

#[cfg(feature = "full")]
pub use array::copy_array;

pub use record_batch::{
    ArrowArray, ArrowSchema, FFIRecordBatch, record_batch_from_ffi, record_batch_to_ffi,
};

#[cfg(feature = "full")]
pub use record_batch::{clone_record_batch, record_batch_to_wasm};

#[cfg(feature = "full")]
pub use schema::copy_schema;
