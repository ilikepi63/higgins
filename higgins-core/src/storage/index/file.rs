use super::{IndexError, IndexesView};
use std::{io::Write as _, marker::PhantomData};

/// Represents a file that holds an index. These indexes can be retrieved directly through
/// the memory-mapped implementation of this file.
pub struct IndexFile<T> {
    file_handle: std::fs::File,
    mmap: memmap2::Mmap,
    _t: PhantomData<T>,
}

impl<T> IndexFile<T> {
    /// Create an instance from a path variable.
    pub fn new(path: &str) -> Result<Self, IndexError> {
        let file_handle = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)?;

        // SAFETY: This file needs to be protected from outside mutations/mutations from multiple concurrent executions.
        let mmap = unsafe { memmap2::Mmap::map(&file_handle)? };

        Ok(Self {
            file_handle,
            mmap,
            _t: PhantomData,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        self.mmap.as_ref()
    }

    pub fn append(&mut self, b: &[u8]) -> Result<(), IndexError> {
        self.file_handle.write_all(b)?;
        self.mmap = unsafe { memmap2::Mmap::map(&self.file_handle)? };
        Ok(())
    }

    pub fn as_view(&self) -> IndexesView<'_, T> {
        IndexesView {
            buffer: self.as_slice(),
            _t: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::IndexesView;
    use super::super::default::DefaultIndex;
    use super::*;
    use crate::storage::index::default::ArchivedDefaultIndex;
    use bytes::BytesMut;
    use std::fs::{File as StdFile, OpenOptions};
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;

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
        let indexes_mut: IndexesView<ArchivedDefaultIndex> = IndexesView {
            buffer: index_file.as_slice(),
            _t: PhantomData,
        };

        // Assert
        assert_eq!(indexes_mut.count(), 2, "Expected 2 indexes");

        // Verify the first index
        let index_view = indexes_mut.get(0).unwrap();

        assert_eq!(
            index_view.timestamp, 1000,
            "Incorrect timestamp for first index"
        );

        // Verify the second index
        let index_view = indexes_mut.get(1).unwrap();
        assert_eq!(
            index_view.timestamp, 2000,
            "Incorrect timestamp for second index"
        );

        // Cleanup
        cleanup_temp_file(&file_path);
    }

    #[tokio::test]
    async fn test_iterative_append_record() {
        // Arrange: Create a temp file with two valid indexes
        let first_index = DefaultIndex {
            offset: 0,
            object_key: [0u8; 16],
            position: 0,
            timestamp: 1000,
            size: 1024,
        };
        let second_index = DefaultIndex {
            offset: 512,
            object_key: [0u8; 16],
            position: 0,
            timestamp: 2000,
            size: 2048,
        };
        // let (file_path, file) = create_temp_file_with_indexes(indexes);
        let (file_path, _) = create_temp_file();
        let mut index_file: IndexFile<DefaultIndex> = IndexFile::new(&file_path).unwrap();

        index_file.append(&first_index.to_bytes()).unwrap();

        // Act
        let indexes_mut: IndexesView<ArchivedDefaultIndex> = IndexesView {
            buffer: index_file.as_slice(),
            _t: PhantomData,
        };

        // Assert
        assert_eq!(indexes_mut.count(), 1);

        let first_index_archived = indexes_mut.get(0).unwrap();

        assert_eq!(
            (
                first_index_archived.object_key,
                first_index_archived.offset.to_native(),
                first_index_archived.position.to_native(),
                first_index_archived.size.to_native(),
                first_index_archived.timestamp.to_native()
            ),
            (
                first_index.object_key,
                first_index.offset,
                first_index.position,
                first_index.size,
                first_index.timestamp
            )
        );

        drop(indexes_mut);

        // Act
        index_file.append(&second_index.to_bytes()).unwrap();

        let indexes_mut: IndexesView<ArchivedDefaultIndex> = IndexesView {
            buffer: index_file.as_slice(),
            _t: PhantomData,
        };

        // Assert
        assert_eq!(indexes_mut.count(), 2);

        let second_index_archived = indexes_mut.get(1).unwrap();

        assert_eq!(
            (
                second_index_archived.object_key,
                second_index_archived.offset.to_native(),
                second_index_archived.position.to_native(),
                second_index_archived.size.to_native(),
                second_index_archived.timestamp.to_native()
            ),
            (
                second_index.object_key,
                second_index.offset,
                second_index.position,
                second_index.size,
                second_index.timestamp
            )
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
        {
            std::fs::File::create(&file_path).unwrap();
        }
        let index_file: IndexFile<DefaultIndex> = IndexFile::new(&file_path).unwrap();
        let indexes_mut: IndexesView<ArchivedDefaultIndex> = IndexesView {
            buffer: index_file.as_slice(),
            _t: PhantomData,
        };

        // Assert
        assert!(indexes_mut.is_empty(), "Expected empty IndexesView");
        assert_eq!(indexes_mut.count(), 0, "Expected count to be 0");

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
        let indexes_mut: IndexesView<ArchivedDefaultIndex> = IndexesView {
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
        let indexes_mut: IndexesView<ArchivedDefaultIndex> = IndexesView {
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
