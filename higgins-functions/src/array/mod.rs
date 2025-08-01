use arrow::{
    array::{ArrayData, DataTypeLayout, layout},
    buffer::{Buffer, MutableBuffer, NullBuffer, ScalarBuffer},
    datatypes::DataType,
    util::bit_mask::set_bits,
};

use crate::{
    types::{WasmArrowArray, WasmPtr},
    utils::{WasmAllocator, u32_to_u8},
};

pub fn clone_array(array: WasmArrowArray, allocator: &mut WasmAllocator) -> u32 {
    let buffer: &[u8] = unsafe {
        &std::mem::transmute::<WasmArrowArray, [u8; std::mem::size_of::<WasmArrowArray>()]>(array)
    };

    allocator.copy(buffer)
}

pub fn copy_array(data: &ArrayData, allocator: &mut WasmAllocator) -> WasmPtr<WasmArrowArray> {

    println!("Copying Array Data: {:#?}", data);

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

    let buffers_ptr = buffers
        .iter()
        .flat_map(|maybe_buffer| match maybe_buffer {
            Some(b) => {
                let ptr = allocator.copy(b.as_slice());

                Some(ptr)
            }
            // This is for null buffer. We only put a null pointer for
            // null buffer if by spec it can contain null mask.
            None if data_layout.can_contain_null_mask => Some(0_u32),
            None => None,
        })
        .collect::<Box<[_]>>();

    // Malloc for the children pointers.
    let buf_ptr = allocator.copy(u32_to_u8(buffers_ptr.as_ref()));

    // Copy the children.
    let empty = vec![];
    let (child_data, dictionary) = match data.data_type() {
        DataType::Dictionary(_, _) => (empty.as_slice(), {
            let arr = copy_array(&data.child_data()[0], allocator);
            arr
        }),
        _ => (data.child_data(), WasmPtr::<WasmArrowArray>::null()),
    };

    let children_box = child_data
        .iter()
        .map(|child| {
            let arr = copy_array(child, allocator);
            arr.inner()
        })
        .collect::<Box<_>>();

    let children_ptr = allocator.copy(u32_to_u8(&children_box));

    let n_children = child_data.len() as i64;

    // As in the IPC format, emit null_count = length for Null type
    let null_count = match data.data_type() {
        DataType::Null => data.len(),
        _ => data.null_count(),
    };

    let array = WasmArrowArray {
        length: data.len() as i64,
        null_count: null_count as i64,
        offset: data.offset() as i64,
        n_buffers,
        n_children,
        buffers: WasmPtr::new(buf_ptr),
        children: WasmPtr::new(children_ptr),
        dictionary,
        release: None,
        private_data: WasmPtr::null(),
    };

    // TODO: There might be some actual gnarly lifetime elision happening here. If you copy
    // over the inner logic of this function into this, the webassembly module will fail.
    let ptr = clone_array(array, allocator);

    WasmPtr::new(ptr)
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

    ({
        data_layout.buffers.len() + {
            // If the layout has a null buffer by Arrow spec.
            // Note that even the array doesn't have a null buffer because it has
            // no null value, we still need to count 1 here to follow the spec.
            usize::from(data_layout.can_contain_null_mask)
        }
    }) as i64
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
