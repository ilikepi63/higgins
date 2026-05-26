use super::IndexType;
use super::JoinedIndex;
use super::{IndexError, IndexesView};
use std::io::{Read, Seek, SeekFrom, Write as _};
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
pub mod windowed_index_file;

/// Represents a file that holds an index. These indexes can be retrieved directly through
/// the memory-mapped implementation of this file.
pub struct IndexFile {
    path: PathBuf,
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
        let path = PathBuf::from(path.as_ref());

        let file_handle = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;

        // SAFETY: This file needs to be protected from outside mutations/mutations from multiple concurrent executions.
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file_handle)? };

        Ok(Self {
            path,
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

    // Put the index at a specific offset.
    pub fn range_put_at(
        &mut self,
        offset: std::ops::Range<usize>,
        bytes: &mut [u8],
    ) -> Result<(), IndexError> {
        // Normalize the buffer, so that you can write the entirety of it.
        let buffer_to_put =
            &bytes[(offset.start - offset.start)..(offset.end - offset.start) * self.element_size];

        if buffer_to_put.len() != (offset.end - offset.start) * self.element_size {
            return Err(IndexError::PutIndexOutOfRange);
        }

        let mut file_handle = std::fs::OpenOptions::new().write(true).open(&self.path)?;

        let offset = offset.start * self.element_size;

        file_handle.seek(SeekFrom::Start(offset as u64))?;

        file_handle.write_all(buffer_to_put)?;

        file_handle.flush()?;

        Ok(())
    }

    pub fn as_view(&self) -> IndexesView<'_> {
        IndexesView {
            buffer: self.as_slice(),
            element_size: self.element_size,
            index_type: self.index_type.clone(),
        }
    }

    /// Retrieves the length of this index file in indexes.
    pub fn len(&self) -> Result<usize, IndexError> {
        Ok(self.file_handle.metadata()?.size() as usize / self.element_size)
    }

    /// Reads indexes at the given offset until the buffer has been filled.
    ///
    /// Note: offsets are the offsets of the indexes themselves, not the byte offset.
    pub fn read_at(&mut self, offset: usize, buffer: &mut [u8]) -> Result<(), IndexError> {
        // Adjust the cursor to read from the specific offset.
        self.file_handle
            .seek(SeekFrom::Start((offset * self.element_size) as u64))?;

        self.file_handle.read_exact(buffer)?;

        // Reset cursor.
        self.file_handle.seek(SeekFrom::Start(0))?;

        Ok(())
    }

    /// Reads indexes at the given offset, returning how many indexes were read.
    ///
    /// Note: offsets are the offsets of the indexes themselves, not the byte offset.
    pub fn read_at_until(&mut self, offset: u64, buffer: &mut [u8]) -> Result<u64, IndexError> {
        tracing::debug!("Buffer length: {}", buffer.len());

        tracing::debug!("Index File: {}", self.path.to_string_lossy());

        tracing::debug!("File length: {}", self.file_handle.metadata()?.len());

        let n = self.file_handle.read_at(buffer, offset as u64)?;

        tracing::debug!("Read {n} bytes from index");

        Ok((n / self.element_size) as u64)
    }

    /// Binary searches through this index file for the boundary where the index
    /// is completed and non-completed.
    ///
    /// Note: this only works on indexes that are completed ie JoinIndexes.
    /// Unsafe: Ideally this should only be available to JoinIndex/completed value
    /// type indexes. Perhaps a refactor will do to make this a little better.
    pub fn binary_search_completed(&mut self) -> CompletedBinarySearchResult {
        let file_size = self.len().unwrap();

        // Logic to handle 0..1 indexes.
        match file_size {
            0 => return CompletedBinarySearchResult::All,
            1 => {
                let mut buffer = vec![0_u8; self.element_size];
                self.read_at(0, &mut buffer).unwrap();
                let index = JoinedIndex::of(&buffer);
                if index.completed() {
                    return CompletedBinarySearchResult::All;
                } else {
                    return CompletedBinarySearchResult::None;
                }
            }
            _ => {}
        }

        let mut buffer = vec![0_u8; self.element_size * 2];

        let mut low = 0;
        let mut high = file_size - 1;

        while low <= high {
            let mid = low + (high - low) / 2;

            match self.read_at(mid, &mut buffer) {
                Ok(_) => {
                    let (first, second) = {
                        let mut chunks = buffer.chunks(self.element_size);
                        (
                            chunks.next().map(JoinedIndex::of),
                            chunks.next().map(JoinedIndex::of),
                        )
                    };

                    match (first, second) {
                        (Some(first), Some(second)) => {
                            match (first.completed(), second.completed()) {
                                (true, false) => return CompletedBinarySearchResult::Found(mid),
                                (false, false) => {
                                    if mid == low {
                                        return CompletedBinarySearchResult::None;
                                    }
                                    high = mid - 1
                                }
                                (true, true) => {
                                    if mid == high - 1 {
                                        return CompletedBinarySearchResult::All;
                                    }
                                    low = mid + 1
                                }
                                _ => panic!(), // illegal state
                            }
                        }
                        _ => return CompletedBinarySearchResult::None,
                    }
                }
                Err(_) => {
                    tracing::error!("Error occurred with reading buffer. File size: {file_size}");
                }
            };
        }

        CompletedBinarySearchResult::None
    }

    // Shards the file into indexes that are of range size.
    //
    // For instance, given 0..5, this will return a shard that will actively iterate over that
    // range.
    pub fn shard(&mut self, range: std::ops::Range<usize>) -> IndexFileShard<'_> {
        IndexFileShard(range, self)
    }

    /// Test function for retrieving this index files complete contents.
    #[cfg(test)]
    pub fn read_contents(&mut self) -> Vec<u8> {
        let mut result = vec![];
        self.file_handle.read_to_end(&mut result).unwrap();
        result
    }
}

/// An enumeration that encapsulates the semantics of how a
/// Binary Search for completion status resulted.
pub enum CompletedBinarySearchResult {
    /// Found, with the index.
    Found(usize),
    /// No indexes in this file are completed.
    None,
    /// All indexes in this file are completed.
    All,
}

/// A "Shard" of a range of a file. This is so that we can have a view into
/// the file at a certain range, load the contents of the file at that range into a sized
/// buffer one at a time.
///
/// TODO: Perhaps we need to generalize this to be a general file shard?
pub struct IndexFileShard<'a>(std::ops::Range<usize>, &'a mut IndexFile);

impl<'a> IndexFileShard<'a> {
    /// Take the next set of indexes from this shard.
    ///
    /// Adds the set range to the buffer, filling it from the front. Once filled, adjusts
    /// the start to be done again.
    pub fn next(&mut self, buffer: &mut [u8]) -> Option<std::ops::Range<usize>> {
        let buffer_len_in_offsets = buffer.len() / self.1.element_size;

        if self.0.start == self.0.end {
            return None;
        }

        let start = self.0.start;
        let end = std::cmp::min(self.0.end, self.0.start + buffer_len_in_offsets);

        self.1
            .read_at(start, &mut buffer[0..(end - start) * self.1.element_size])
            .inspect_err(|err| {
                tracing::error!("Error reading: {:#?}", err);
            })
            .ok()?;

        self.0.start = end;

        Some(std::ops::Range { start, end })
    }

    /// Get a reference back to the file for this shard.
    ///
    /// This is required to be able to mutate this file whilst holding the reference to this section.
    pub fn file_mut(&mut self) -> &mut IndexFile {
        self.1
    }

    pub fn reverse(self) -> ReverseIndexFileShard<'a> {
        ReverseIndexFileShard(self)
    }
}

/// implements
pub struct ReverseIndexFileShard<'a>(IndexFileShard<'a>);

impl<'a> ReverseIndexFileShard<'a> {
    /// Take the next set of indexes from this shard.
    ///
    /// Adds the set range to the buffer, filling it from the front. Once filled, adjusts
    /// the start to be done again.
    pub fn next(&mut self, buffer: &mut [u8]) -> Option<std::ops::Range<usize>> {
        let Self(shard) = self;

        let buffer_len_in_offsets = buffer.len() / shard.1.element_size;

        if shard.0.start == shard.0.end {
            return None;
        }

        let start = shard.0.end.saturating_sub(buffer_len_in_offsets);
        let end = shard.0.end;

        shard
            .1
            .read_at(start, &mut buffer[0..(end - start) * shard.1.element_size])
            .inspect_err(|err| {
                tracing::error!("Error reading: {:#?}", err);
            })
            .ok()?;

        // Update the shard, so that you can pull next.
        shard.0.end = start;

        Some(std::ops::Range { start, end })
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::derive::utils::iter_buffer;
    use crate::storage::index::IndexType;
    use crate::storage::index::default::DefaultIndex;
    use crate::storage::index::joined_index::JoinedIndex;
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

        assert_eq!(file.as_view().get(0).unwrap(), val);

        file.append(&val).unwrap();

        assert_eq!(file.as_view().count(), 2);

        assert_eq!(file.as_view().get(1).unwrap(), val);

        file.append(&val).unwrap();

        assert_eq!(file.as_view().count(), 3);

        assert_eq!(file.as_view().get(2).unwrap(), val);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn put_at_works_correctly() {
        let path = new_file();

        let mut file = IndexFile::new(&path, DefaultIndex::size_of(), IndexType::Default).unwrap();

        let mut val = vec![0; DefaultIndex::size_of()];

        for i in 0..10 {
            DefaultIndex::put(
                i,
                crate::storage::dereference::Reference::Null,
                1,
                1,
                100,
                &mut val,
            )
            .unwrap();

            file.append(&val).unwrap();
        }

        let mut buffer = vec![0_u8; DefaultIndex::size_of() * 10];

        file.read_at(0, &mut buffer).unwrap();

        let mut val = vec![0; DefaultIndex::size_of() * 3];

        for i in 0..3 {
            let start = i * DefaultIndex::size_of();
            let end = start + DefaultIndex::size_of();

            DefaultIndex::put(
                6,
                crate::storage::dereference::Reference::Null,
                12,
                12,
                142,
                &mut val[start..end],
            )
            .unwrap();
        }

        let length = file.len().unwrap();

        let _ = file
            .range_put_at(std::ops::Range { start: 1, end: 4 }, &mut val)
            .unwrap();

        assert_eq!(file.len().unwrap(), length);

        let mut buffer = vec![0_u8; DefaultIndex::size_of() * 10];

        file.read_at(0, &mut buffer).unwrap();

        let start = 1 * DefaultIndex::size_of();
        let end = 4 * DefaultIndex::size_of();

        for chunk in buffer[start..end].chunks(DefaultIndex::size_of()) {
            let index = DefaultIndex::of(chunk);
            assert_eq!(index.offset(), 6);
            assert_eq!(index.position(), 12);
            assert_eq!(index.timestamp(), 12);
            assert_eq!(index.size(), 142);
        }
    }

    #[test]
    fn read_at_works_correctly() {
        let path = new_file();

        let mut file = IndexFile::new(&path, DefaultIndex::size_of(), IndexType::Default).unwrap();

        let mut val = vec![0; DefaultIndex::size_of()];

        for i in 0..100 {
            DefaultIndex::put(
                i,
                crate::storage::dereference::Reference::Null,
                1,
                1,
                100,
                &mut val,
            )
            .unwrap();

            file.append(&val).unwrap();
        }

        let mut buffer = vec![0_u8; DefaultIndex::size_of() * 10];

        file.read_at(40, &mut buffer).unwrap();

        const DEFAULT_INDEX_SIZE: usize = DefaultIndex::size_of();

        for (i, buf) in buffer
            .as_chunks::<DEFAULT_INDEX_SIZE>()
            .0
            .iter()
            .enumerate()
        {
            let index = DefaultIndex::of(buf);

            assert_eq!((i + 40) as u64, index.offset());
        }
    }

    #[test]
    fn can_find_intersection_of_completed_and_not() {
        let path = new_file();

        let index_size = JoinedIndex::size_of(2);

        let mut file = IndexFile::new(&path, index_size, IndexType::Join).unwrap();

        let mut val = vec![0; index_size];

        for i in 0..100 {
            JoinedIndex::put(
                i,
                crate::storage::dereference::Reference::Null,
                1,
                &[Some(1), Some(1)],
                &mut val,
            )
            .unwrap();

            if i < 50 {
                JoinedIndex::set_completed(&mut val);
            }

            file.append(&val).unwrap();
        }

        let index = file.binary_search_completed();

        assert!(matches!(index, CompletedBinarySearchResult::Found(49)));
    }

    #[test]
    fn can_find_intersection_of_completed_with_none_completed() {
        let path = new_file();

        let index_size = JoinedIndex::size_of(2);

        let mut file = IndexFile::new(&path, index_size, IndexType::Join).unwrap();

        let mut val = vec![0; index_size];

        for i in 0..100 {
            JoinedIndex::put(
                i,
                crate::storage::dereference::Reference::Null,
                1,
                &[Some(1), Some(1)],
                &mut val,
            )
            .unwrap();

            file.append(&val).unwrap();
        }

        let index = file.binary_search_completed();

        assert!(matches!(index, CompletedBinarySearchResult::None));
    }

    #[test]
    fn can_find_intersection_of_completed_with_all_completed() {
        let path = new_file();

        let index_size = JoinedIndex::size_of(2);

        let mut file = IndexFile::new(&path, index_size, IndexType::Join).unwrap();

        let mut val = vec![0; index_size];

        for i in 0..100 {
            JoinedIndex::put(
                i,
                crate::storage::dereference::Reference::Null,
                1,
                &[Some(1), Some(1)],
                &mut val,
            )
            .unwrap();

            JoinedIndex::set_completed(&mut val);

            file.append(&val).unwrap();
        }

        let index = file.binary_search_completed();

        assert!(matches!(index, CompletedBinarySearchResult::All));
    }

    #[test]
    fn can_fold_from_specified_index() {
        let path = new_file();

        let mut file = IndexFile::new(&path, DefaultIndex::size_of(), IndexType::Default).unwrap();

        let mut val = vec![0; DefaultIndex::size_of()];

        for i in 0..100 {
            DefaultIndex::put(
                i,
                crate::storage::dereference::Reference::Null,
                1,
                1,
                100,
                &mut val,
            )
            .unwrap();

            file.append(&val).unwrap();
        }

        let mut buffer = vec![0_u8; DefaultIndex::size_of() * 10];

        let mut shard = file.shard(0..50);

        while let Some(range) = shard.next(&mut buffer) {
            let mut i = 0;
            for val in range {
                let index = i * DefaultIndex::size_of();

                let end = index + DefaultIndex::size_of();

                let index = DefaultIndex::of(&buffer[index..end]);

                i += 1;

                assert_eq!(val as u64, index.offset());
            }
        }
    }

    #[test]
    fn can_find_intersection_of_completed_with_one_index() {
        let path = new_file();

        let index_size = JoinedIndex::size_of(2);

        let mut file = IndexFile::new(&path, index_size, IndexType::Join).unwrap();

        let index = file.binary_search_completed();

        assert!(matches!(index, CompletedBinarySearchResult::All));

        let mut val = vec![0; index_size];

        JoinedIndex::put(
            0,
            crate::storage::dereference::Reference::Null,
            1,
            &[Some(1), Some(1)],
            &mut val,
        )
        .unwrap();

        file.append(&val).unwrap();

        let index = file.binary_search_completed();

        assert!(matches!(index, CompletedBinarySearchResult::None));

        JoinedIndex::set_completed(&mut val);

        file.put_at(0, &mut val).unwrap();

        let index = file.binary_search_completed();

        assert!(matches!(index, CompletedBinarySearchResult::All));
    }

    #[test]
    fn test_file_sharding_works() {
        let path = new_file();

        let mut file = IndexFile::new(&path, DefaultIndex::size_of(), IndexType::Default).unwrap();

        let mut val = vec![0; DefaultIndex::size_of()];

        for i in 0..10 {
            DefaultIndex::put(
                i,
                crate::storage::dereference::Reference::Null,
                1,
                1,
                i * 2,
                &mut val,
            )
            .unwrap();

            file.append(&val).unwrap();
        }

        let mut val = vec![0; DefaultIndex::size_of() * 3];

        let mut shard = file.shard(0..10);

        let mut i = 0;

        while let Some(value) = shard.next(&mut val) {
            for value in iter_buffer(value, DefaultIndex::size_of(), &val).map(DefaultIndex::of) {
                assert_eq!(i, value.offset());
                i += 1;
            }
        }

        let mut i = 9;
        let shard = file.shard(0..10);
        let mut reverse_shard = shard.reverse();

        while let Some(value) = reverse_shard.next(&mut val) {
            for value in iter_buffer(value, DefaultIndex::size_of(), &val)
                .rev()
                .map(DefaultIndex::of)
            {
                assert_eq!(i, value.offset());
                i = i.saturating_sub(1);
            }
        }
    }
}
