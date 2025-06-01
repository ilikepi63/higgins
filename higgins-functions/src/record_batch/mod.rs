#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]


mod from_ffi;
mod to_ffi;
#[cfg(feature = "full")]
mod wasm;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(feature = "full")]
pub use wasm::{clone_record_batch, record_batch_to_wasm};

pub use from_ffi::record_batch_from_ffi;
pub use to_ffi::record_batch_to_ffi;


