use memmap2::MmapMut;

use super::IndexError;

/// Represents a file that holds an index. These indexes can be retrieved directly through
/// the memory-mapped implementation of this file.
pub struct IndexFile {
    path: String,
    file_handle: std::fs::File,
    mmap: memmap2::MmapMut,
}

impl IndexFile {
    /// Create an instance from a path variable.
    pub fn new(path: &str) -> Result<Self, IndexError> {
        let file_handle = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        // SAFETY: This file needs to be protected from outside mutations/mutations from multiple concurrenct executions.
        let mmap = unsafe { MmapMut::map_mut(&file_handle)? };

        Ok(Self {
            path: path.to_owned(),
            file_handle,
            mmap,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        self.mmap.as_ref()
    }
}
