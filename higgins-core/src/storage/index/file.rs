use super::IndexType;
use super::{IndexError, IndexesView};
use std::io::Write as _;

/// Represents a file that holds an index. These indexes can be retrieved directly through
/// the memory-mapped implementation of this file.
pub struct IndexFile {
    file_handle: std::fs::File,
    mmap: memmap2::MmapMut,
    element_size: usize,
    index_type: crate::storage::index::IndexType,
}

impl IndexFile {
    /// Create an instance from a path variable.
    pub fn new<P: AsRef<std::path::Path>>(
        path: P,
        element_size: usize,
        index_type: IndexType,
    ) -> Result<Self, IndexError> {
        let file_handle = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        // SAFETY: This file needs to be protected from outside mutations/mutations from multiple concurrent executions.
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file_handle)? };

        Ok(Self {
            file_handle,
            mmap,
            element_size,
            index_type,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        self.mmap.as_ref()
    }

    pub fn append(&mut self, b: &[u8]) -> Result<(), IndexError> {
        self.file_handle.write_all(b)?;
        self.mmap = unsafe { memmap2::MmapMut::map_mut(&self.file_handle)? };
        Ok(())
    }

    // Put the index at a specific offset.
    pub fn put_at(&mut self, offset: u64, bytes: &mut [u8]) -> Result<(), IndexError> {
        // length check to avoid panic.
        if bytes.len() != self.element_size {
            return Err(IndexError::IndexSwapSizeError);
        }

        // Get the byte offset.
        let offset = offset as usize * self.element_size;

        self.mmap[offset..offset + self.element_size].swap_with_slice(bytes);
        Ok(())
    }

    pub fn as_view(&self) -> IndexesView<'_> {
        IndexesView {
            buffer: self.as_slice(),
            element_size: self.element_size,
            index_type: self.index_type.clone(),
        }
    }
}
#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::storage::index::IndexType;
    use crate::storage::index::default::DefaultIndex;
    use std::fs;
    use std::path::PathBuf;

    fn new_file() -> PathBuf {
        let filename = format!("{}.idx", Uuid::new_v4());

        let mut path = std::env::temp_dir();
        path.push(filename);
        path
    }

    #[test]
    fn new_creates_empty_file_when_not_exists() {
        let path = new_file();

        let file = IndexFile::new(&path, DefaultIndex::size_of(), IndexType::Default).unwrap();

        assert_eq!(file.element_size, DefaultIndex::size_of());
        assert_eq!(file.index_type, IndexType::Default);
        assert_eq!(file.as_slice().len(), 0);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn append_grows_file_and_remaps() {
        let path = new_file();

        let mut file = IndexFile::new(&path, DefaultIndex::size_of(), IndexType::Default).unwrap();

        let mut val = vec![0; DefaultIndex::size_of()];

        // TODO: change this to reflect the new reference API
        DefaultIndex::put(
            0,
            crate::storage::dereference::Reference::Null,
            1,
            1,
            100,
            &mut val,
        )
        .unwrap();

        file.append(&val).unwrap();

        assert_eq!(file.as_view().count(), 1);

        file.append(&val).unwrap();

        assert_eq!(file.as_view().count(), 2);

        file.append(&val).unwrap();

        assert_eq!(file.as_view().count(), 3);

        fs::remove_file(path).unwrap();
    }
}
