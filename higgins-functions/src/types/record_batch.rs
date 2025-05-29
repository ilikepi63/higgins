use super::{WasmArrowArray, WasmArrowSchema, WasmPtr};

#[repr(C)]
#[derive(Debug)]
pub struct WasmRecordBatch {
    pub n_columns: i64,
    pub schema: WasmPtr<WasmPtr<WasmArrowSchema>>,
    pub columns: WasmPtr<WasmPtr<WasmArrowArray>>,
}