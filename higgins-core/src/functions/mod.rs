use std::{ffi::c_void, sync::Arc};

use ::wasmtime::{Caller, Engine, Linker, Module, Store};
use arrow::{
    array::{Array, ArrayData, DataTypeLayout, Int32Array, layout},
    buffer::{Buffer, MutableBuffer, NullBuffer, ScalarBuffer},
    datatypes::DataType,
    ffi::{FFI_ArrowArray, from_ffi, to_ffi},
    util::bit_mask::set_bits,
};
use wasmtime::{Memory, TypedFunc};

pub fn test_ffi() -> Result<(), Box<dyn std::error::Error>> {
    let array = Int32Array::from(vec![Some(1), None, Some(3)]);
    let data = array.into_data();

    println!("{:#?}", data);

    // let out_array = new_ffi(&data);

    // println!("Array: {:#?}", out_array);

    let (out_array, out_schema) = to_ffi(&data)?;

    println!("Array: {:#?}", out_array);

    let buffer_ptr: *const Buffer = out_array.buffer(out_array.offset()) as *const Buffer;

    let x = unsafe { (*buffer_ptr).len() };

    println!("Size of buffer: {:#?}", x);

    // import it
    let data = unsafe { from_ffi(out_array, &out_schema) }?;
    let array = Int32Array::from(data);

    // verify
    assert_eq!(array, Int32Array::from(vec![Some(1), None, Some(3)]));

    Ok(())
}

pub fn run(wasm: impl AsRef<[u8]>) {
    let engine = Engine::default();

    let module = Module::new(&engine, wasm).unwrap();

    // Host functionality can be arbitrary Rust functions and is provided
    // to guests through a `Linker`.
    let mut linker = Linker::new(&engine);
    // linker.func_wrap(
    //     "host",
    //     "host_func",
    //     |caller: Caller<'_, u32>, param: i32| {
    //         println!("Got {} from WebAssembly", param);
    //         println!("my host state is: {}", caller.data());
    //     },
    // ).unwrap();

    let mut store: Store<u32> = Store::new(&engine, 4);

    let instance = linker.instantiate(&mut store, &module).unwrap();

    let wasm_malloc_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "_malloc")
        .unwrap();

    let mut memory = instance.get_memory(&mut store, "memory").unwrap();

    let array = Int32Array::from(vec![Some(1), None, Some(3)]);

    let result = copy_array(&array.to_data(), &wasm_malloc_fn, &mut store, &mut memory);

    let ptr = clone_array(result, &wasm_malloc_fn, &mut store, &mut memory);

    let wasm_run_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "run")
        .unwrap();

    let result = wasm_run_fn.call(&mut store, ptr).unwrap();

    println!("Result{result}");

}

pub fn copy_arrow_buffer_to_wasm_memory(
    buf: &Buffer,
    wasm_malloc_fn: &TypedFunc<u32, u32>,
    mut store: &mut Store<u32>,
    memory: &mut Memory,
) -> u32 {
    let ptr = wasm_malloc_fn
        .call(&mut store, buf.len().try_into().unwrap())
        .unwrap();

    memory
        .write(&mut store, ptr.try_into().unwrap(), buf.as_slice())
        .unwrap();

    ptr
}

pub fn clone_array(
    array: TestFfiArrowArray,
    wasm_malloc_fn: &TypedFunc<u32, u32>,
    mut store: &mut Store<u32>,
    memory: &mut Memory,
) -> u32 {
    let buffer: &[u8] = unsafe {
        std::slice::from_raw_parts(
            &array as *const _ as *const u8,
            std::mem::size_of::<TestFfiArrowArray>(),
        )
    };

    let ptr = wasm_malloc_fn
        .call(&mut store, buffer.len().try_into().unwrap())
        .unwrap();

    memory
        .write(&mut store, ptr.try_into().unwrap(), buffer)
        .unwrap();

    ptr
}

pub fn copy_array(
    data: &ArrayData,
    wasm_malloc_fn: &TypedFunc<u32, u32>,
    mut store: &mut Store<u32>,
    memory: &mut Memory,
) -> TestFfiArrowArray {
    let data_layout = layout(data.data_type());

    // Copy the buffers.
    let mut buffers = buffers_from_layout(&data_layout, data);

    // `n_buffers` is the number of buffers by the spec.
    let mut n_buffers = count_buffers(&data_layout);

    if data_layout.variadic {
        // Save the lengths of all variadic buffers into a new buffer.
        // The first buffer is `views`, and the rest are variadic.
        let mut data_buffers_lengths = Vec::new();
        for buffer in data.buffers().iter().skip(1) {
            data_buffers_lengths.push(buffer.len() as i64);
            n_buffers += 1;
        }

        buffers.push(Some(ScalarBuffer::from(data_buffers_lengths).into_inner()));
        n_buffers += 1;
    }

    let mut buffers_ptr = buffers
        .iter()
        .flat_map(|maybe_buffer| match maybe_buffer {
            Some(b) => {
                let ptr = copy_arrow_buffer_to_wasm_memory(b, wasm_malloc_fn, store, memory);

                Some(ptr as *const c_void)
            }
            // This is for null buffer. We only put a null pointer for
            // null buffer if by spec it can contain null mask.
            None if data_layout.can_contain_null_mask => Some(std::ptr::null()),
            None => None,
        })
        .collect::<Box<[_]>>();

    // Copy the children.
    let empty = vec![];
    let (child_data, dictionary) = match data.data_type() {
        DataType::Dictionary(_, _) => (
            empty.as_slice(),
            Box::into_raw(Box::new(copy_array(
                &data.child_data()[0],
                wasm_malloc_fn,
                store,
                memory,
            ))),
        ),
        _ => (data.child_data(), std::ptr::null_mut()),
    };

    let mut children = child_data
        .iter()
        .map(|child| Box::into_raw(Box::new(copy_array(child, wasm_malloc_fn, store, memory))))
        .collect::<Box<_>>();
    let n_children = children.len() as i64;

    // As in the IPC format, emit null_count = length for Null type
    let null_count = match data.data_type() {
        DataType::Null => data.len(),
        _ => data.null_count(),
    };

    TestFfiArrowArray {
        length: data.len() as i64,
        null_count: null_count as i64,
        offset: data.offset() as i64,
        n_buffers,
        n_children,
        buffers: buffers_ptr.as_mut_ptr(),
        children: children.as_mut_ptr(),
        dictionary,
        release: Some(release_array),
        private_data: Box::into_raw(Box::new(())) as *mut c_void, // Box::into_raw(private_data) as *mut c_void,
    }
}

fn buffers_from_layout(data_layout: &DataTypeLayout, data: &ArrayData) -> Vec<Option<Buffer>> {
    if data_layout.can_contain_null_mask {
        // * insert the null buffer at the start
        // * make all others `Option<Buffer>`.
        std::iter::once(align_nulls(data.offset(), data.nulls()))
            .chain(data.buffers().iter().map(|b| Some(b.clone())))
            .collect::<Vec<_>>()
    } else {
        data.buffers().iter().map(|b| Some(b.clone())).collect()
    }
}

fn count_buffers(data_layout: &DataTypeLayout) -> i64 {
    // `n_buffers` is the number of buffers by the spec.
    let n_buffers = {
        data_layout.buffers.len() + {
            // If the layout has a null buffer by Arrow spec.
            // Note that even the array doesn't have a null buffer because it has
            // no null value, we still need to count 1 here to follow the spec.
            usize::from(data_layout.can_contain_null_mask)
        }
    } as i64;

    n_buffers
}

/// Copied from FFIArrowArray::new
pub fn new_ffi(data: &ArrayData) -> TestFfiArrowArray {
    let data_layout = layout(data.data_type());

    let mut buffers = buffers_from_layout(&data_layout, data);

    // `n_buffers` is the number of buffers by the spec.
    let mut n_buffers = count_buffers(&data_layout);

    if data_layout.variadic {
        // Save the lengths of all variadic buffers into a new buffer.
        // The first buffer is `views`, and the rest are variadic.
        let mut data_buffers_lengths = Vec::new();
        for buffer in data.buffers().iter().skip(1) {
            data_buffers_lengths.push(buffer.len() as i64);
            n_buffers += 1;
        }

        buffers.push(Some(ScalarBuffer::from(data_buffers_lengths).into_inner()));
        n_buffers += 1;
    }

    let mut buffers_ptr = buffers
        .iter()
        .flat_map(|maybe_buffer| match maybe_buffer {
            Some(b) => Some(b.as_ptr() as *const c_void),
            // This is for null buffer. We only put a null pointer for
            // null buffer if by spec it can contain null mask.
            None if data_layout.can_contain_null_mask => Some(std::ptr::null()),
            None => None,
        })
        .collect::<Box<[_]>>();

    let empty = vec![];
    let (child_data, dictionary) = match data.data_type() {
        DataType::Dictionary(_, _) => (
            empty.as_slice(),
            Box::into_raw(Box::new(new_ffi(&data.child_data()[0]))),
        ),
        _ => (data.child_data(), std::ptr::null_mut()),
    };

    let mut children = child_data
        .iter()
        .map(|child| Box::into_raw(Box::new(new_ffi(child))))
        .collect::<Box<_>>();
    let n_children = children.len() as i64;

    // As in the IPC format, emit null_count = length for Null type
    let null_count = match data.data_type() {
        DataType::Null => data.len(),
        _ => data.null_count(),
    };

    // create the private data owning everything.
    // any other data must be added here, e.g. via a struct, to track lifetime.
    // let mut private_data = Box::new(ArrayPrivateData {
    //     buffers,
    //     buffers_ptr,
    //     children,
    //     dictionary,
    // });

    TestFfiArrowArray {
        length: data.len() as i64,
        null_count: null_count as i64,
        offset: data.offset() as i64,
        n_buffers,
        n_children,
        buffers: buffers_ptr.as_mut_ptr(),
        children: children.as_mut_ptr(),
        dictionary,
        release: Some(release_array),
        private_data: Box::into_raw(Box::new(())) as *mut c_void, // Box::into_raw(private_data) as *mut c_void,
    }
}

/// Aligns the provided `nulls` to the provided `data_offset`
///
/// This is a temporary measure until offset is removed from ArrayData (#1799)
fn align_nulls(data_offset: usize, nulls: Option<&NullBuffer>) -> Option<Buffer> {
    let nulls = nulls?;
    if data_offset == nulls.offset() {
        // Underlying buffer is already aligned
        return Some(nulls.buffer().clone());
    }
    if data_offset == 0 {
        return Some(nulls.inner().sliced());
    }
    let mut builder = MutableBuffer::new_null(data_offset + nulls.len());
    set_bits(
        builder.as_slice_mut(),
        nulls.validity(),
        data_offset,
        nulls.offset(),
        nulls.len(),
    );
    Some(builder.into())
}

struct ArrayPrivateData {
    #[allow(dead_code)]
    pub buffers: Vec<Option<Buffer>>,
    pub buffers_ptr: Box<[*const c_void]>,
    pub children: Box<[*mut FFI_ArrowArray]>,
    pub dictionary: *mut FFI_ArrowArray,
}

#[repr(C)]
#[derive(Debug)]
pub struct TestFfiArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut TestFfiArrowArray,
    pub dictionary: *mut TestFfiArrowArray,
    pub release: Option<unsafe extern "C" fn(arg1: *mut TestFfiArrowArray)>,
    // When exported, this MUST contain everything that is owned by this array.
    // for example, any buffer pointed to in `buffers` must be here, as well
    // as the `buffers` pointer itself.
    // In other words, everything in [FFI_ArrowArray] must be owned by
    // `private_data` and can assume that they do not outlive `private_data`.
    pub private_data: *mut c_void,
}

// callback used to drop [FFI_ArrowArray] when it is exported
unsafe extern "C" fn release_array(array: *mut TestFfiArrowArray) {
    if array.is_null() {
        return;
    }
    let array = &mut *array;

    // take ownership of `private_data`, therefore dropping it`
    let private = Box::from_raw(array.private_data as *mut ArrayPrivateData);
    for child in private.children.iter() {
        let _ = Box::from_raw(*child);
    }
    if !private.dictionary.is_null() {
        let _ = Box::from_raw(private.dictionary);
    }

    array.release = None;
}
