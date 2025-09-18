use super::IndexError;
use memmap2::MmapMut;
use std::{io::Write as _, marker::PhantomData};

/// Represents a file that holds an index. These indexes can be retrieved directly through
/// the memory-mapped implementation of this file.
pub struct IndexFile<T> {
    // path: String,
    // file_handle: std::fs::File,
    mmap: memmap2::MmapMut,
    _t: PhantomData<T>,
}

impl<T> IndexFile<T> {
    /// Create an instance from a path variable.
    pub fn new(path: &str) -> Result<Self, IndexError> {
        let file_handle = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&path)?;

        // SAFETY: This file needs to be protected from outside mutations/mutations from multiple concurrenct executions.
        let mmap = unsafe { MmapMut::map_mut(&file_handle)? };

        Ok(Self {
            // path: path.to_owned(),
            // file_handle,
            mmap,
            _t: PhantomData,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        self.mmap.as_ref()
    }

    pub fn append(&mut self, b: &[u8]) -> Result<(), IndexError> {
        (&mut self.mmap[..]).write_all(b)?;
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::IndexesMut;
    use super::super::default::DefaultIndex;
    use super::*;
    use bytes::BytesMut;
    use std::fs::{File as StdFile, OpenOptions};
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;
    use crate::storage::index::default::ArchivedDefaultIndex;

    fn create_temp_file() -> (String, Arc<StdFile>) {
        let temp_dir = std::env::temp_dir();
        let file_name = format!("test_index_{}.bin", rand::random::<u64>());
        let file_path = temp_dir.join(file_name).to_str().unwrap().to_string();

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(&file_path)
            .unwrap();
        (file_path, Arc::new(file))
    }

    // Helper function to create a temporary file with serialized DefaultIndex data
    fn create_temp_file_with_indexes(indexes: Vec<DefaultIndex>) -> (String, Arc<StdFile>) {
        let temp_dir = std::env::temp_dir();
        let file_name = format!("test_index_{}.bin", rand::random::<u64>());
        let file_path = temp_dir.join(file_name).to_str().unwrap().to_string();

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(&file_path)
            .unwrap();
        let mut buffer = BytesMut::new();
        for index in indexes {
            buffer.extend_from_slice(&index.to_bytes());
        }

        file.write_all(&buffer).unwrap();
        file.flush().unwrap();
        let file = Arc::new(file);
        (file_path, file)
    }

    // Helper function to clean up a temporary file
    fn cleanup_temp_file(file_path: &str) {
        if Path::new(file_path).exists() {
            std::fs::remove_file(file_path).unwrap();
        }
    }

    // Helper function to create a DefaultIndex
    fn create_default_index(offset: u32, timestamp: u64, size: u64) -> DefaultIndex {
        DefaultIndex {
            offset,
            object_key: [0u8; 16],
            position: 0,
            timestamp,
            size,
        }
    }

    #[tokio::test]
    async fn test_load_all_indexes_success() {
        // Arrange: Create a temp file with two valid indexes
        let indexes = vec![
            DefaultIndex {
                offset: 0,
                object_key: [0u8; 16],
                position: 0,
                timestamp: 1000,
                size: 1024,
            },
            DefaultIndex {
                offset: 512,
                object_key: [0u8; 16],
                position: 0,
                timestamp: 2000,
                size: 2048,
            },
        ];
        // let (file_path, file) = create_temp_file_with_indexes(indexes);
        let (file_path, _) = create_temp_file();
        let mut index_file: IndexFile<DefaultIndex> = IndexFile::new(&file_path).unwrap();

        for index in indexes {
            index_file.append(&index.to_bytes()).unwrap();
        }

        // Act
        let indexes_mut: IndexesMut<ArchivedDefaultIndex> = IndexesMut {
            buffer: index_file.as_slice(),
            _t: PhantomData,
        };

        // Assert
        assert_eq!(indexes_mut.count(), 2, "Expected 2 indexes");

        // Verify the first index
        let index_view = indexes_mut.get(0).unwrap();

        assert_eq!(
            index_view.timestamp,
            1000,
            "Incorrect timestamp for first index"
        );

        // Verify the second index
        let index_view = indexes_mut.get(1).unwrap();
        assert_eq!(
            index_view.timestamp,
            2000,
            "Incorrect timestamp for second index"
        );

        // Cleanup
        cleanup_temp_file(&file_path);
    }

    #[tokio::test]
    async fn test_load_all_indexes_empty_file() {
        // Arrange: Create an empty temp file
        let temp_dir = std::env::temp_dir();
        let file_name = format!("test_index_empty_{}.bin", rand::random::<u64>());
        let file_path = temp_dir.join(file_name).to_str().unwrap().to_string();
        // Act
        let index_file: IndexFile<DefaultIndex> = IndexFile::new(&file_path).unwrap();
        let indexes_mut: IndexesMut<ArchivedDefaultIndex> = IndexesMut {
            buffer: index_file.as_slice(),
            _t: PhantomData,
        };

        // Assert
        assert!(indexes_mut.is_empty(), "Expected empty IndexesMut");
        assert_eq!(indexes_mut.count(), 0, "Expected count to be 0");

        // Cleanup
        cleanup_temp_file(&file_path);
    }

    #[tokio::test]
    async fn test_load_all_indexes_unexpected_eof() {
        // Arrange: Create a temp file with incomplete index data
        let temp_dir = std::env::temp_dir();
        let file_name = format!("test_index_eof_{}.bin", rand::random::<u64>());
        let file_path = temp_dir.join(file_name).to_str().unwrap().to_string();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(&file_path)
            .unwrap();
        let index = create_default_index(0, 1000, 1024);
        let mut buffer = index.to_bytes();
        // Truncate the buffer to simulate incomplete data
        buffer.truncate(DefaultIndex::size() / 2);
        file.write_all(&buffer).unwrap();
        file.flush().unwrap();

        // Act
        let index_file: IndexFile<DefaultIndex> = IndexFile::new(&file_path).unwrap();
        let indexes_mut: IndexesMut<DefaultIndex> = IndexesMut {
            buffer: index_file.as_slice(),
            _t: PhantomData,
        };

        // Assert
        // assert!(result.is_ok(), "Expected successful load, got {:?}", result);
        // let indexes_mut = result.unwrap();
        assert!(indexes_mut.is_empty(), "Expected empty IndexesMut on EOF");

        // Cleanup
        cleanup_temp_file(&file_path);
    }

    #[tokio::test]
    async fn test_load_all_indexes_corrupted_file() {
        // Arrange: Create a temp file with valid and invalid index data
        let indexes = vec![create_default_index(0, 1000, 1024)];
        let (file_path, _) = create_temp_file_with_indexes(indexes);
        // Append invalid data to simulate corruption
        let mut file_to_corrupt = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&file_path)
            .unwrap();
        file_to_corrupt.write_all(&[0u8; 10]).unwrap();
        file_to_corrupt.flush().unwrap();

        // Act
        let index_file: IndexFile<DefaultIndex> = IndexFile::new(&file_path).unwrap();
        let indexes_mut: IndexesMut<ArchivedDefaultIndex> = IndexesMut {
            buffer: index_file.as_slice(),
            _t: PhantomData,
        };

        assert_eq!(
            indexes_mut.count(),
            1,
            "Expected only valid indexes to be loaded"
        );

        // Cleanup
        cleanup_temp_file(&file_path);
    }

    #[tokio::test]
    async fn test_load_all_indexes_large_file() {
        // Arrange: Create a temp file with many indexes
        let indexes = (0..100)
            .map(|i| create_default_index(i * 512, i as u64 * 1000, 1024))
            .collect::<Vec<_>>();

        let (file_path, _) = create_temp_file();
        let mut index_file: IndexFile<DefaultIndex> = IndexFile::new(&file_path).unwrap();

        for index in indexes {
            index_file.append(&index.to_bytes()).unwrap();
        }

        // Act
        let indexes_mut: IndexesMut<ArchivedDefaultIndex> = IndexesMut {
            buffer: index_file.as_slice(),
            _t: PhantomData,
        };

        // Assert
        assert_eq!(indexes_mut.count(), 100, "Expected 100 indexes");

        // Verify a few indexes
        for i in 0..100 {
            let index_view = indexes_mut.get(i).unwrap();
            assert_eq!(
                index_view.timestamp,
                i as u64 * 1000,
                "Incorrect timestamp for index {}",
                i
            );
        }

        // Cleanup
        cleanup_temp_file(&file_path);
    }
}
