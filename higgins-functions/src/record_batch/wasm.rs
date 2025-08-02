use crate::record_batch::FFIRecordBatch;
use arrow::array::RecordBatch;
use arrow::ffi::FFI_ArrowSchema;
use wasmtime::{Memory, Store};

use crate::schema;
use crate::{
    copy_array, copy_schema,
    errors::HigginsFunctionError,
    types::{WasmArrowArray, WasmArrowSchema, WasmPtr, WasmRecordBatch},
    utils::{WasmAllocator, u32_to_u8},
};

pub fn record_batch_to_wasm(rb: RecordBatch, allocator: &mut WasmAllocator) -> WasmRecordBatch {
    let len = rb.num_columns();

    let data = rb.columns().iter().map(|array| array.to_data());

    let schema = rb.schema();

    let schema_data = data.clone().zip(schema.fields());

    let arrays = data
        .clone() // hoping this clone is cheap somehow.
        .map(|data| {
            let array = copy_array(&data, allocator);

            array.inner()
        })
        .collect::<Box<[_]>>();

    let schema = schema_data
        .map(|(data, field)| {
            let schema = copy_schema(data.data_type(), field.clone(), allocator).unwrap();

            schema.inner()
        })
        .collect::<Box<[_]>>();

    let arrays_ptr = allocator.copy(u32_to_u8(&arrays));
    let schema_ptr = allocator.copy(u32_to_u8(&schema));

    let result = WasmRecordBatch {
        n_columns: len as i64,
        schema: WasmPtr::new(schema_ptr),
        columns: WasmPtr::new(arrays_ptr),
    };

    result
}

pub fn clone_record_batch(array: WasmRecordBatch, allocator: &mut WasmAllocator) -> u32 {
    let buffer: &[u8] = unsafe {
        &std::mem::transmute::<WasmRecordBatch, [u8; std::mem::size_of::<WasmRecordBatch>()]>(array)
    };

    allocator.copy(buffer)
}

pub fn record_batch_from_wasm(ptr: WasmPtr<WasmRecordBatch>) {}

pub fn wasm_array_from_wasm(ptr: WasmPtr<WasmArrowArray>, memory: Memory) {}

macro_rules! deref_wasm_ptr {
    ($name:ty, $memory:ident, $store:ident, $ptr:ident) => {{
        let mut buffer = [0_u8; std::mem::size_of::<$name>()];

        $memory.read($store, $ptr.inner().try_into()?, &mut buffer);

        let schema_ptr = buffer.as_ptr() as *const $name;

        schema_ptr
    }};
}

pub fn get_wasm_c_string(
    ptr: WasmPtr<i8>,
    memory: Memory,
    store: &mut Store<u32>,
) -> Result<String, HigginsFunctionError> {
    let mut str = String::new();

    'outer: loop {
        let mut buffer = [0_u8; 50];

        memory.read(&mut *store, ptr.inner().try_into()?, &mut buffer);

        for (i, c) in String::from_utf8_lossy(&buffer).chars().enumerate() {
            if c == char::MIN {
                let string = String::from_utf8(buffer[0..i].to_vec())?;

                str.push_str(&string);

                break 'outer;
            }
        }

        str.push_str(&String::from_utf8(buffer.to_vec())?);
    }

    Ok(str)
}

pub fn wasm_schema_from_wasm(
    ptr: WasmPtr<WasmArrowSchema>,
    memory: Memory,
    store: Store<u32>,
) -> Result<FFI_ArrowSchema, HigginsFunctionError> {
    let schema_ptr = deref_wasm_ptr!(WasmArrowSchema, memory, store, ptr);

    let format = (unsafe { *schema_ptr }).format;

    let format = get_wasm_c_string(format, memory, &mut store);

    // let format_ptr = allocator.copy(format.as_bytes());

    // // allocate and hold the children
    // let children = children_from_datatype(dtype, allocator)?;

    // let dictionary = if let DataType::Dictionary(_, value_data_type) = dtype {
    //     Some(copy_schema(value_data_type.as_ref(), field.clone(), allocator)?)
    // } else {
    //     None
    // };

    // let flags = match dtype {
    //     DataType::Map(_, true) => Flags::MAP_KEYS_SORTED,
    //     _ => Flags::empty(),
    // };

    // // Allocation happens here.
    // let children_box = children
    //     .into_iter()
    //     .map(|child_schema| clone_struct(child_schema, allocator))
    //     .collect::<Box<_>>();

    // let n_children = children_box.len() as i64;

    // // Malloc for the children pointers.
    // let children_ptr = allocator.copy(u32_to_u8(&children_box));

    // let dictionary_ptr = dictionary
    //     .map(|d| {
    //         let ptr = clone_struct(d, allocator);

    //         WasmPtr::new(ptr)
    //     })
    //     .unwrap_or(WasmPtr::null());

    // let dictionary = dictionary_ptr;

    // let name = allocator.copy(field.name().as_bytes());

    // let schema = WasmArrowSchema {
    //     format: WasmPtr::new(format_ptr),
    //     name: WasmPtr::new(name),
    //     metadata: WasmPtr::null(),
    //     flags: flags.bits(),
    //     n_children,
    //     children: WasmPtr::new(children_ptr),
    //     dictionary,
    //     release: None,
    //     private_data: WasmPtr::null(),
    // };

    // let buffer: &[u8] = unsafe {
    //     &std::mem::transmute::<WasmArrowSchema, [u8; std::mem::size_of::<WasmArrowSchema>()]>(
    //         schema,
    //     )
    // };

    // let ptr = allocator.copy(buffer);

    // Ok(WasmPtr::new(ptr))
}
