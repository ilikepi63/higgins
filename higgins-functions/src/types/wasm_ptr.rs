//! Module for the WasmPtr type.
use std::marker::PhantomData;

/// The WasmPtr type.
///
/// As Webassembly primarily targets a 32-bit architecture, we will need to communicate
/// all pointers for webassembly to the u32 memory address size.
#[repr(C)]
#[derive(Debug)]
pub struct WasmPtr<T>(u32, PhantomData<T>);

impl<T> WasmPtr<T> {
    /// Wraps the given u32 in a WasmPtr struct.
    pub fn new(ptr: u32) -> Self {
        WasmPtr(ptr, PhantomData)
    }

    /// Retrieves a nullptr.
    pub const fn null() -> Self {
        Self(0, PhantomData)
    }

    /// Unwraps this value into a u32, consuming self.
    pub fn inner(self) -> u32 {
        self.0
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn ptr_equals_size_of_u32() {
        assert_eq!(
            std::mem::size_of::<WasmPtr<String>>(),
            std::mem::size_of::<u32>()
        );

        let ptr: WasmPtr<String> = WasmPtr::new(1);

        let buffer: &[u8] = unsafe {
            std::slice::from_raw_parts(
                &ptr as *const _ as *const u8,
                std::mem::size_of::<WasmPtr<u32>>(),
            )
        };

        let buffer_two: &[u8] = unsafe {
            std::slice::from_raw_parts(
                &1 as *const _ as *const u8,
                std::mem::size_of::<WasmPtr<u32>>(),
            )
        };

        assert_eq!(buffer, buffer_two);
    }
}
