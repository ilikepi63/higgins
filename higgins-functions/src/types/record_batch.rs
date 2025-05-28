use super::{WasmArrowArray, WasmArrowSchema, WasmPtr};

#[repr(C)]
#[derive(Debug)]
pub struct WasmRecordBatch {
    pub schema: WasmPtr<WasmArrowSchema>,
    pub n_columns: i64,
    pub columns: WasmPtr<WasmPtr<WasmArrowArray>>,
}
