//! The C Definition equivalent for Webassembly.
//!
//! See https://arrow.apache.org/docs/format/CDataInterface.htmls
use std::os::raw::c_void;

use super::WasmPtr;

#[repr(C)]
#[derive(Debug)]
pub struct WasmArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: WasmPtr<WasmPtr<c_void>>,
    pub children: WasmPtr<WasmPtr<Self>>,
    pub dictionary: WasmPtr<Self>,
    pub release: Option<unsafe extern "C" fn(arg1: WasmPtr<Self>)>,
    pub private_data: WasmPtr<c_void>,
}
