//! Serves the purpose of giving more opinionated algorithms on
//! certain index types and how to extract/work with them.

use crate::{
    derive::utils::iter_buffer,
    storage::index::{IndexFile, windowed_index::WindowedIndex},
    utils::epoch,
};
use std::ops::{Range, Sub};

pub struct WindowedIndexFileOffset(u64);

pub struct WindowedIndexFile<'a>(&'a mut IndexFile);

impl<'a> WindowedIndexFile<'a> {
    /// Constructor for creating this from an index file.
    pub fn of(index_file: &'a mut IndexFile) -> Self {
        Self(index_file)
    }

    // /// Returns a mutable value that represents this offset in the index file.
    // /// Each range will have an index, or currently what is the required index for the
    // /// given range.
    // pub fn shard(&mut self, ranges: &[(Range<u64>, Range<u64>)]) {
    //     self.0.shard(0..1);
    // }

    /// Given a list of ranges, we will need to add this
    /// offset/index to the given ranges and ensure that they are not
    /// "completed".
    ///
    /// A range is completed when it can no longer be appended to (ie, the timestamp is closed or the value has been added)
    ///
    /// `index` refers to the underlying offset from the derivative stream, whilst the ranges are the identifiers for which ranges need to be
    /// added.
    pub fn put_ranges(&mut self, range: Range<u64>, ranges: &[Range<u64>]) {
        let mut buf =
            vec![0_u8; usize::try_from(size_of_range(&range)).unwrap() * WindowedIndex::size_of()]; //create buffer to pull.

        let put_range = Range {
            start: range.start.clone() as usize,
            end: range.end.clone() as usize,
        };

        for (index, range) in range.zip(ranges) {
            let normalized_index = index as usize * WindowedIndex::size_of();
            let end = normalized_index as usize + WindowedIndex::size_of();

            let mut buf = &mut buf[normalized_index..end];

            WindowedIndex::put(
                crate::storage::dereference::Reference::Null,
                epoch(),
                range.clone(),
                &mut buf,
            )
            .unwrap();
        }

        self.0.range_put_at(put_range, &mut buf).unwrap();
    }

    /// Finds the index at which the range start begins.
    ///
    /// This will begin from the back so effectively O(n) but general time would be O(1) as
    /// these indexes are generally appended to the back.
    fn find_by_range_start(&mut self) -> Option<WindowedIndexFileOffset> {
        let start = 0; // we are always beginning from the start
        let end = self.0.len().ok()?.checked_sub(1)?; // Get the last index

        let mut shard = self.0.shard(start..end);

        let mut buffer = vec![0_u8; WindowedIndex::size_of() * 10]; // 10 just being the amount of indexes we want to pull at this point.

        while let Some(values) = shard.next(&mut buffer) {
            for index in iter_buffer(values, WindowedIndex::size_of(), &buffer)
                .map(WindowedIndex::of)
                .rev()
            {
                // we can use the index here. But how do we reverse this logic?
            }
        }

        None
    }

    /// Gets the range give a specific start and end position range.
    pub fn get_ranges(&'a mut self, ranges: &[Range<u64>]) -> Range<u64> {
        // We want to use the size of this range. perhaps it
        let mut buf = vec![0_u8; WindowedIndex::size_of() * 10];

        let end = self.0.len().unwrap_or(0).saturating_sub(1);

        let mut shard = self.0.shard(0..end).reverse();

        let mut start = end;

        while let Some(value) = shard.next(&mut buf) {
            start = value.end;

            for value in iter_buffer(value, WindowedIndex::size_of(), &mut buf)
                .rev()
                .map(WindowedIndex::of)
            {
                // If we value is contained in the given list of ranges, we
                // need to reduce the starting index.
                if ranges.contains(&value.range()) {
                    start -= 1;
                } else {
                    // if not, we break, as this is the last one here.
                    break;
                };
            }
        }

        Range {
            start: start as u64,
            end: (start + ranges.len()) as u64,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::storage::index::file::windowed_index_file::size_of_range;
    use crate::storage::index::{
        IndexFile, IndexType, file::windowed_index_file::WindowedIndexFile,
        windowed_index::WindowedIndex,
    };
    use crate::storage::windowing::assign_sliding_windows_range;

    fn inspect_index_file(file: WindowedIndexFile) {
        let content = file.0.as_slice();
    }

    #[test]
    fn windowed_index_file_base_test() {
        let mut index_file = IndexFile::new(
            uuid::Uuid::new_v4().to_string(),
            WindowedIndex::size_of(),
            IndexType::Window,
        )
        .unwrap();

        let mut windowed_index_file = WindowedIndexFile::of(&mut index_file);

        // We get the ranges. It's important to note that these ranges must always be contiguous
        let ranges = assign_sliding_windows_range(1..5, 5, 1, 0);

        let index_file_range = &ranges.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>();
        let underlying_file_range = &ranges.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>();

        let put_at_range = windowed_index_file.get_ranges(&index_file_range);

        let cloned = put_at_range.clone();

        // read the values in from the index file, keeping track of which are actual WindowedIndexes and which are not.
        windowed_index_file.put_ranges(cloned, &underlying_file_range);
    }
}

fn print_windowed_index_file() {}

pub fn size_of_range<T: Sub<T> + Copy>(range: &Range<T>) -> T::Output {
    range.end - range.start
}
