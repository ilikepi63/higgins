//! The C Definition equivalent for Webassembly.
//! 
//! See https://arrow.apache.org/docs/format/CDataInterface.htmls 
use std::ffi::{c_char, c_void};

use super::WasmPtr;

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct WasmArrowSchema {
    pub format: WasmPtr<c_char>,
    pub name: WasmPtr<c_char>,
    pub metadata: WasmPtr<c_char>,
    /// Refer to [Arrow Flags](https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.flags)
    pub flags: i64,
    pub n_children: i64,
    pub children: WasmPtr<WasmPtr<Self>>,
    pub dictionary: WasmPtr<Self>,
    pub release: Option<unsafe extern "C" fn(arg1: *mut WasmPtr<Self>)>,
    pub private_data: WasmPtr<c_void>,
}