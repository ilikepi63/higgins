use crate::ArrowSchema;
use crate::{
    copy_array, copy_schema,
    errors::HigginsFunctionError,
    types::{WasmArrowArray, WasmArrowSchema, WasmPtr, WasmRecordBatch},
    utils::{WasmAllocator, u32_to_u8},
};
use arrow::array::RecordBatch;
use wasmtime::{Memory, Store};

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

        $memory
            .read(&mut *$store, $ptr.inner().try_into()?, &mut buffer)
            .unwrap();

        let schema_ptr = buffer.as_ptr() as *const $name;

        schema_ptr
    }};
}

pub fn get_wasm_c_string(
    ptr: WasmPtr<i8>,
    memory: Memory,
    store: &mut Store<u32>,
) -> Result<String, HigginsFunctionError> {
    if ptr.inner() == WasmPtr::<i8>::null().inner() {
        return Err(HigginsFunctionError::DereferenceNullPtr);
    }

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
    store: &mut Store<u32>,
) -> Result<ArrowSchema, HigginsFunctionError> {
    let schema_ptr = deref_wasm_ptr!(WasmArrowSchema, memory, store, ptr);

    let schema = unsafe { *schema_ptr };

    let format = get_wasm_c_string(schema.format, memory, store);

    let name = get_wasm_c_string(schema.name, memory, store);

    let children = schema.children;
    let n_children = schema.n_children;

    let mut dereferenced_children = vec![];

    for i in 0..n_children {
        let increment =
            TryInto::<i64>::try_into(std::mem::size_of::<WasmPtr<WasmPtr<WasmArrowSchema>>>())?;

        let ptr = WasmPtr::<WasmPtr<WasmArrowSchema>>::new(
            children.inner() + TryInto::<u32>::try_into(i * increment)?,
        );

        let ptr = deref_wasm_ptr!(WasmPtr<WasmArrowSchema>, memory, store, ptr);

        let ffi_schema: ArrowSchema = wasm_schema_from_wasm(unsafe { *ptr }, memory, store)?;

        let ptr = Box::into_raw(Box::new(ffi_schema));

        dereferenced_children.push(ptr);
    }

    let children_ptrs = dereferenced_children.iter().collect::<Box<_>>();

    let dictionary_ptr = schema.dictionary;

    let dictionary = wasm_schema_from_wasm(dictionary_ptr, memory, store)
        .map(Box::new)
        .map(Box::into_raw);

    Ok(ArrowSchema {
        format: format
            .map(|s| s.as_ptr() as *const i8)
            .unwrap_or(std::ptr::null()),
        name: name
            .map(|s| s.as_ptr() as *const i8)
            .unwrap_or(std::ptr::null()),
        metadata: std::ptr::null(),
        flags: schema.flags,
        n_children,
        children: Box::into_raw(children_ptrs) as *mut *mut ArrowSchema,
        dictionary: dictionary.unwrap_or(std::ptr::null_mut()),
        release: Some(noop),
        private_data: std::ptr::null_mut(),
    })
}

unsafe extern "C" fn noop(_: *mut ArrowSchema) {}
