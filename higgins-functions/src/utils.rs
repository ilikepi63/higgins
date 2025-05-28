use wasmtime::{Memory, Store, TypedFunc};

/// A helper function to transmute a slice of u32's
/// to a slice of u8's.
///
///
/// ```
/// use higgins_functions::utils::u32_to_u8;
///
/// let u32_slice: [u32; 3] = [1, 2, 3];
///
/// let byte_slice = u32_to_u8(&u32_slice);
///
/// assert_eq!(byte_slice.len(), 3 * 4);
///
/// ```
pub fn u32_to_u8(arr: &[u32]) -> &[u8] {
    let len = 4 * arr.len();
    let ptr = arr.as_ptr() as *const u8;
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

/// A wrapper struct for being able to allocate data on the webassembly module's heap.
pub struct WasmAllocator<'a> {
    store: &'a mut Store<u32>,
    wasm_malloc_fn: &'a TypedFunc<u32, u32>,
    memory: &'a mut Memory,
}

impl<'a> WasmAllocator<'a> {
    pub fn from(
        store: &'a mut Store<u32>,
        malloc_fn: &'a mut TypedFunc<u32, u32>,
        memory: &'a mut Memory,
    ) -> Self {
        Self {
            store,
            wasm_malloc_fn: malloc_fn,
            memory,
        }
    }

    /// Copy directly from a slice.
    pub fn copy(&mut self, buffer: &[u8]) -> u32 {
        let ptr = self
            .wasm_malloc_fn
            .call(&mut self.store, buffer.len().try_into().unwrap())
            .unwrap();

        self.memory
            .write(&mut self.store, ptr.try_into().unwrap(), buffer)
            .unwrap();

        ptr
    }
}

/// Clone a generic struct.
pub fn clone_struct<T>(array: T, allocator: &mut WasmAllocator) -> u32 {
    let buffer: &[u8] = unsafe {
        std::slice::from_raw_parts(&array as *const _ as *const u8, std::mem::size_of::<T>())
    };

    let ptr = allocator.copy(buffer);

    ptr
}

#[cfg(test)]
pub mod test {
    use arrow::{
        array::{Array, Int32Array},
        ffi::{FFI_ArrowArray, to_ffi},
    };

    #[test]
    fn can_dereference_pt_clone_struct() {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);

        let (array, _) = to_ffi(&array.to_data()).unwrap();

        let test_array = array.buffer(0);

        let buffer: &[u8] = unsafe {
            std::slice::from_raw_parts(&array as *const _ as *const u8, std::mem::size_of::<FFI_ArrowArray>())
        };

        let p: *const FFI_ArrowArray = buffer.as_ptr() as *const FFI_ArrowArray;

        unsafe {
            assert_eq!(p.as_ref().unwrap().buffer(0), test_array);
        }
    }

    /// A test to ensure the transmutation of different structs to byte arrays.
    ///
    /// This is not implemented above, but in multiple different areas.
    #[test]
    fn can_dereference_ptr() {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);

        let (array, _) = to_ffi(&array.to_data()).unwrap();

        let test_array = array.buffer(0);

        let buffer: &[u8] = unsafe {
            &std::mem::transmute::<FFI_ArrowArray, [u8; std::mem::size_of::<FFI_ArrowArray>()]>(
                array,
            )
        };

        let p: *const FFI_ArrowArray = buffer.as_ptr() as *const FFI_ArrowArray;

        unsafe {
            assert_eq!(p.as_ref().unwrap().buffer(0), test_array);
        }
    }
}
