use crate::{ArrowArray, ArrowSchema};
use crate::{
    copy_array, copy_schema,
    errors::HigginsFunctionError,
    types::{WasmArrowArray, WasmArrowSchema, WasmPtr, WasmRecordBatch},
    utils::{WasmAllocator, u32_to_u8},
};
use arrow::array::RecordBatch;
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, UnionMode};
use arrow::error::ArrowError;
use arrow::util::bit_util;
use ffi_convert::{AsRust, CArray};
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

pub fn record_batch_from_wasm(ptr: WasmPtr<WasmRecordBatch>) {}

pub fn wasm_array_from_wasm(
    ptr: WasmPtr<WasmArrowArray>,
    memory: Memory,
    store: &mut Store<u32>,
) -> Result<ArrowArray, HigginsFunctionError> {
    let array = unsafe { *deref_wasm_ptr!(WasmArrowArray, memory, store, ptr) };

    let buffer_ptrs = {
        let mut buf = vec![0_u8; array.n_buffers.try_into()? * WasmPtr::size()];

        memory.read(store, array.buffers.inner().try_into()?, &mut buf);

        
        buf.as_ptr() as *const u32
    };





    let buffers = array.buffers;

    for i in 0..array.n_buffers {
        let increment =
            TryInto::<i64>::try_into(std::mem::size_of::<WasmPtr<WasmPtr<i8>>>())?;

        let ptr = WasmPtr::<WasmPtr<i8>>::new(
            array.buffers.inner() + TryInto::<u32>::try_into(i * increment)?,
        );

        let ptr = deref_wasm_ptr!(WasmPtr<i8>, memory, store, ptr);

        let ffi_schema: ArrowSchema = wasm_schema_from_wasm(unsafe { *ptr }, memory, store)?;

        let ptr = Box::into_raw(Box::new(ffi_schema));

        dereferenced_children.push(ptr);
    }


    Ok(ArrowArray {
        length: array.length,
        null_count: array.null_count,
        offset: array.offset,
        n_buffers: array.n_buffers,
        n_children: array.n_children,
        buffers: ,
        children: todo!(),
        dictionary: todo!(),
        release: todo!(),
        private_data: todo!(),
    })
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


    /// returns all buffers, as organized by Rust (i.e. null buffer is skipped if it's present
    /// in the spec of the type)
    fn buffers(buffer_ptrs: *const u32, n_buffers: usize, can_contain_null_mask: bool, variadic: bool,
        memory: Memory,
    store: &mut Store<u32>, dt: DataType, array_offset: i64
) -> Result<Vec<Buffer>, HigginsFunctionError> {
        // + 1: skip null buffer
        let buffer_begin = can_contain_null_mask as usize;
        let buffer_end = n_buffers - usize::from(variadic);

        let variadic_buffer_lens = if variadic {
            // Each views array has 1 (optional) null buffer, 1 views buffer, 1 lengths buffer.
            // Rest are variadic.
            let num_variadic_buffers =
                n_buffers - (2 + usize::from(can_contain_null_mask));
            if num_variadic_buffers == 0 {
                &[]
            } else {

                // let lengths_ptr = 

                let lengths = unsafe { buffer_ptrs.add(n_buffers - 1) }; // Get the last buffer.
                // SAFETY: is lengths is non-null, then it must be valid for up to num_variadic_buffers.
                let lengths_ptr = {
                    let buf = vec![0_u8; 8 * num_variadic_buffers];

                    memory.read(store, (unsafe { *lengths }).try_into().unwrap(), &mut buf);

                    buf.as_ptr() as *const i64

                };

                // lengths_ptr
                 unsafe { std::slice::from_raw_parts(lengths_ptr, num_variadic_buffers) }
            }
        } else {
            &[]
        };

        (buffer_begin..buffer_end)
            .map(|index| {
                let len = buffer_len(index, variadic_buffer_lens, &dt, n_buffers.try_into()?, array_offset, buffer_ptrs as *const WasmPtr<i8>, store, memory)?;
                match unsafe { create_buffer(self.owner.clone(), self.array, index, len) } {
                    Some(buf) => Ok(buf),
                    None if len == 0 => {
                        // Null data buffer, which Rust doesn't allow. So create
                        // an empty buffer.
                        Ok(MutableBuffer::new(0).into())
                    }
                    None => Err(ArrowError::CDataInterface(format!(
                        "The external buffer at position {index} is null."
                    ))),
                }
            })
            .collect()
    }

    pub fn get_at_offset() {}


    fn buffer_len(
        i: usize,
        variadic_buffer_lengths: &[i64],
        dt: &DataType,
        array_len: i64,
        array_offset: i64,
        buffers: *const WasmPtr<i8>, 
        store: &mut Store<u32>, memory: Memory
    ) -> Result<usize, HigginsFunctionError> {
        // Special handling for dictionary type as we only care about the key type in the case.
        let data_type = match dt {
            DataType::Dictionary(key_data_type, _) => key_data_type.as_ref(),
            dt => dt,
        };

        // `ffi::ArrowArray` records array offset, we need to add it back to the
        // buffer length to get the actual buffer length.
        let length = TryInto::<usize>::try_into(array_len + array_offset)?;

        // Inner type is not important for buffer length.
        Ok(match (&data_type, i) {
            (DataType::Utf8, 1)
            | (DataType::LargeUtf8, 1)
            | (DataType::Binary, 1)
            | (DataType::LargeBinary, 1)
            | (DataType::List(_), 1)
            | (DataType::LargeList(_), 1)
            | (DataType::Map(_, _), 1) => {
                // the len of the offset buffer (buffer 1) equals length + 1
                let bits = bit_width(data_type, i)?;
                debug_assert_eq!(bits % 8, 0);
                (length + 1) * (bits / 8)
            }
            (DataType::Utf8, 2) | (DataType::Binary, 2) => {
                if array_len < 1{
                    return Ok(0);
                }

                // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
                let len = buffer_len(1, variadic_buffer_lengths, dt, array_len, array_offset, buffers, store, memory)?;

                let offset_buffer = unsafe { buffers.add(1) }; 

                let value = unsafe { *deref_wasm_ptr!(WasmPtr<i8>, memory, store, offset_buffer) }.cast::<i32>();

                let length_ptr = value.add((len / size_of::<i32>() - 1).try_into()?).inner(); 
 
                deref_wasm_ptr!(i32, memory, store, length_ptr)
            }
            (DataType::LargeUtf8, 2) | (DataType::LargeBinary, 2) => {
                if array_len < 1{
                    return Ok(0);
                }

                // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
                let len = buffer_len(1, variadic_buffer_lengths, dt, array_len, array_offset, buffers,store, memory)?;
                
                let offset_buffer = unsafe { buffers.add(1) }; 

                let value = unsafe { *deref_wasm_ptr!(WasmPtr<i8>, memory, store, offset_buffer) }.cast::<i64>();

                let length_ptr = value.add((len / size_of::<i64>() - 1).try_into()?).inner(); 
 
                deref_wasm_ptr!(i64, memory, store, length_ptr)
            }
            // View types: these have variadic buffers.
            // Buffer 1 is the views buffer, which stores 1 u128 per length of the array.
            // Buffers 2..N-1 are the buffers holding the byte data. Their lengths are variable.
            // Buffer N is of length (N - 2) and stores i64 containing the lengths of buffers 2..N-1
            (DataType::Utf8View, 1) | (DataType::BinaryView, 1) => {
                std::mem::size_of::<u128>() * length
            }
            (DataType::Utf8View, i) | (DataType::BinaryView, i) => {
                variadic_buffer_lengths[i - 2] as usize
            }
            // buffer len of primitive types
            _ => {
                let bits = bit_width(data_type, i)?;
                bit_util::ceil(length * bits, 8)
            }
        })
    }


    // returns the number of bits that buffer `i` (in the C data interface) is expected to have.
// This is set by the Arrow specification
fn bit_width(data_type: &DataType, i: usize) -> Result<usize, ArrowError> {
    if let Some(primitive) = data_type.primitive_width() {
        return match i {
            0 => Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" doesn't expect buffer at index 0. Please verify that the C data interface is correctly implemented."
            ))),
            1 => Ok(primitive * 8),
            i => Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            ))),
        };
    }

    Ok(match (data_type, i) {
        (DataType::Boolean, 1) => 1,
        (DataType::Boolean, _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        (DataType::FixedSizeBinary(num_bytes), 1) => *num_bytes as usize * u8::BITS as usize,
        (DataType::FixedSizeList(f, num_elems), 1) => {
            let child_bit_width = bit_width(f.data_type(), 1)?;
            child_bit_width * (*num_elems as usize)
        },
        (DataType::FixedSizeBinary(_), _) | (DataType::FixedSizeList(_, _), _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        },
        // Variable-size list and map have one i32 buffer.
        // Variable-sized binaries: have two buffers.
        // "small": first buffer is i32, second is in bytes
        (DataType::Utf8, 1) | (DataType::Binary, 1) | (DataType::List(_), 1) | (DataType::Map(_, _), 1) => i32::BITS as _,
        (DataType::Utf8, 2) | (DataType::Binary, 2) => u8::BITS as _,
        (DataType::List(_), _) | (DataType::Map(_, _), _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        (DataType::Utf8, _) | (DataType::Binary, _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 3 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        // Variable-sized binaries: have two buffers.
        // LargeUtf8: first buffer is i64, second is in bytes
        (DataType::LargeUtf8, 1) | (DataType::LargeBinary, 1) | (DataType::LargeList(_), 1) => i64::BITS as _,
        (DataType::LargeUtf8, 2) | (DataType::LargeBinary, 2) | (DataType::LargeList(_), 2)=> u8::BITS as _,
        (DataType::LargeUtf8, _) | (DataType::LargeBinary, _) | (DataType::LargeList(_), _)=> {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 3 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        // Variable-sized views: have 3 or more buffers.
        // Buffer 1 are the u128 views
        // Buffers 2...N-1 are u8 byte buffers
        (DataType::Utf8View, 1) | (DataType::BinaryView,1) => u128::BITS as _,
        (DataType::Utf8View, _) | (DataType::BinaryView, _) => {
            u8::BITS as _
        }
        // type ids. UnionArray doesn't have null bitmap so buffer index begins with 0.
        (DataType::Union(_, _), 0) => i8::BITS as _,
        // Only DenseUnion has 2nd buffer
        (DataType::Union(_, UnionMode::Dense), 1) => i32::BITS as _,
        (DataType::Union(_, UnionMode::Sparse), _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 1 buffer, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        (DataType::Union(_, UnionMode::Dense), _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffer, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        (_, 0) => {
            // We don't call this `bit_width` to compute buffer length for null buffer. If any types that don't have null buffer like
            // UnionArray, they should be handled above.
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" doesn't expect buffer at index 0. Please verify that the C data interface is correctly implemented."
            )))
        }
        _ => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" is still not supported in Rust implementation"
            )))
        }
    })
}
