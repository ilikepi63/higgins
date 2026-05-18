//! Serves the purpose of giving more opinionated algorithms on
//! certain index types and how to extract/work with them.

use crate::{
    derive::utils::iter_buffer,
    storage::index::{
        IndexError, IndexFile, Reference,
        windowed_index::{self, WindowedIndex},
    },
    utils::epoch,
};
use std::{
    io::Read,
    ops::{Range, Sub},
};

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
    pub fn put_ranges(
        &mut self,
        ranges: &mut [(Range<u64>, Range<u64>)],
    ) -> Result<(), IndexError> {
        // sort in ascending order.
        ranges.sort_by(|a, b| a.0.clone().cmp(b.0.clone())); // TODO: There is probably a more sensible way than a clone here.

        // Find the current first range. Ranges should always be sorted.
        let first_range = ranges.first().unwrap().0.clone(); // TODO: unwrap not necessary here, should always exist

        let offset = self.index_of_range_rev(first_range).unwrap_or(0);

        dbg!(&offset);

        // Pull the first offset from the file, this will then be aggregated into the first range of
        // the put range.
        if self.0.len().unwrap() > 0 {
            let mut first_range_buffer = vec![0_u8; WindowedIndex::size_of()];

            self.0
                .read_at(offset as usize, &mut first_range_buffer)
                .unwrap();
            let index = WindowedIndex::of(&first_range_buffer);
            let first_current_range = ranges.first_mut().unwrap();

            if index.inclusive_range() == first_current_range.0 {
                first_current_range.1.start = index.derivative_range().start;
            }
        }

        let mut v = vec![0_u8; ranges.len() * WindowedIndex::size_of()];

        for ((inclusive_range, derivative_range), data) in ranges
            .iter_mut()
            .zip(v.chunks_mut(WindowedIndex::size_of()))
        {
            WindowedIndex::put(
                Reference::Null,
                epoch(),
                inclusive_range.clone(),
                derivative_range.clone(),
                data,
            )?;
        }

        self.0
            .range_put_at(offset as usize..(offset as usize + ranges.len()), &mut v)?;

        Ok(())
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

    /// Considering this index file holds indexes tha map ranges to their holding values:
    ///
    /// 0..5 -> 0..3 reads as range 0..5, currently has the values 0..3 available.
    ///
    /// We can also retrieve the current index of a range then (from the back of this file).
    pub fn index_of_range_rev(&mut self, range: Range<u64>) -> Option<u64> {
        let mut buf = vec![0_u8; WindowedIndex::size_of() * 10];

        let end = self.0.len().unwrap_or(0);

        let mut shard = self.0.shard(0..end).reverse();

        let mut index = (end as u64).saturating_sub(1);

        while let Some(value) = shard.next(&mut buf) {
            for value in iter_buffer(value, WindowedIndex::size_of(), &mut buf)
                .rev()
                .map(WindowedIndex::of)
            {
                // If we value is contained in the given list of ranges, we
                // need to reduce the starting index.
                let inclusive_range = value.inclusive_range();
                if range == inclusive_range || range.end < inclusive_range.start {
                    return Some(index);
                }

                index -= 1;
            }
        }

        return None;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::index::file::windowed_index_file::{
        print_windowed_index_file, size_of_range,
    };
    use crate::storage::index::{
        IndexFile, IndexType, file::windowed_index_file::WindowedIndexFile,
        windowed_index::WindowedIndex,
    };
    use crate::storage::windowing::assign_sliding_windows_range;

    fn inspect_index_file(file: WindowedIndexFile) {
        let content = file.0.as_slice();
    }

    #[test]
    fn put_ranges_basic() {
        let path = uuid::Uuid::new_v4().to_string();

        let remove_path = path.clone();

        let result = std::panic::catch_unwind(|| {
            let mut index_file =
                IndexFile::new(&path, WindowedIndex::size_of(), IndexType::Window).unwrap();
            let mut file = WindowedIndexFile::of(&mut index_file);

            let mut ranges = vec![(0..5, 3..5), (1..6, 3..5), (2..7, 3..5)];

            file.put_ranges(&mut ranges).unwrap();

            assert_file_holds_ranges(path, &ranges);
        });

        std::fs::remove_file(remove_path).unwrap();

        result.unwrap();
    }

    #[test]
    fn put_ranges_overwrites() {
        let path = uuid::Uuid::new_v4().to_string();

        let remove_path = path.clone();

        let result = std::panic::catch_unwind(|| {
            let mut index_file =
                IndexFile::new(&path, WindowedIndex::size_of(), IndexType::Window).unwrap();
            let mut file = WindowedIndexFile::of(&mut index_file);

            let mut ranges = vec![(0..5, 0..5), (5..10, 5..10), (10..15, 10..12)];

            file.put_ranges(&mut ranges).unwrap();

            assert_file_holds_ranges(&path, &ranges);

            println!("#### PUT FIRST ####");

            let mut ranges = vec![(10..15, 12..13)];

            file.put_ranges(&mut ranges).unwrap();

            assert_file_holds_ranges(&path, &[(0..5, 0..5), (5..10, 5..10), (10..15, 10..13)]);

            println!("#### PUT SECOND ####");

            let mut ranges = vec![(10..15, 13..15), (15..20, 15..20), (20..25, 20..22)];

            file.put_ranges(&mut ranges).unwrap();

            assert_file_holds_ranges(
                &path,
                &[
                    (0..5, 0..5),
                    (5..10, 5..10),
                    (10..15, 10..15),
                    (15..20, 15..20),
                    (20..25, 20..22),
                ],
            );

            println!("#### PUT THIRD ####");
        });

        std::fs::remove_file(remove_path);

        result.unwrap();
    }

    // #[test]
    // fn windowed_index_file_base_test() {
    //     let path = uuid::Uuid::new_v4().to_string();

    //     let remove_path = path.clone();

    //     let result = std::panic::catch_unwind(|| {
    //         let mut index_file =
    //             IndexFile::new(path.clone(), WindowedIndex::size_of(), IndexType::Window).unwrap();

    //         let mut windowed_index_file = WindowedIndexFile::of(&mut index_file);

    //         // We get the ranges. It's important to note that these ranges must always be contiguous
    //         let ranges = assign_sliding_windows_range(1..5, 5, 1, 0);

    //         dbg!(&ranges);

    //         let index_file_range = &ranges.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>();

    //         let underlying_file_range = &ranges.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>();

    //         let put_at_range = windowed_index_file.get_ranges(&index_file_range);

    //         let cloned = put_at_range.clone();

    //         // read the values in from the index file, keeping track of which are actual WindowedIndexes and which are not.
    //         windowed_index_file.put_ranges(cloned, &underlying_file_range);

    //         assert_file_holds_ranges(&path, &[1..5, 1..5, 2..5, 3..5, 4..5, 5..5]);

    //         /// Retrieves all the windows that each index will be part of.
    //         let ranges = assign_sliding_windows_range(6..10, 5, 1, 0);

    //         dbg!(&ranges);

    //         /// these two basically just get unzipped.
    //         //let index_file_range = &ranges.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>();
    //         //let underlying_file_range = &ranges.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>();

    //         /// Retrieves the index that this range should be put at.
    //         //let put_at_range = windowed_index_file.get_ranges(&index_file_range);
    //         //let cloned = put_at_range.clone();

    //         // read the values in from the index file, keeping track of which are actual WindowedIndexes and which are not.
    //         //windowed_index_file.put_ranges(cloned, &underlying_file_range);
    //         assert_file_holds_ranges(
    //             path,
    //             &[
    //                 /*0 */ 1..5,
    //                 /*1 */ 1..6,
    //                 /*2 */ 2..7,
    //                 /*3 */ 3..8,
    //                 /*4 */ 4..9,
    //                 /*5 */ 5..10,
    //                 /*6 */ 6..10,
    //                 /*7 */ 7..10,
    //                 /*8 */ 8..10,
    //                 /*9 */ 9..10,
    //                 /*10 */ 0..10,
    //             ],
    //         );

    //         panic!();
    //     });

    //     std::fs::remove_file(remove_path).unwrap();

    //     result.unwrap();
    // }
}

fn print_windowed_index_file<F: AsRef<std::path::Path>>(file: F) {
    let mut fd = std::fs::File::open(file).unwrap();

    let mut buf = Vec::new();

    let _ = fd.read_to_end(&mut buf).unwrap();

    for index in buf.chunks(WindowedIndex::size_of()).map(WindowedIndex::of) {
        dbg!(index);
    }
}

fn assert_file_holds_ranges<F: AsRef<std::path::Path>>(
    file: F,
    ranges: &[(Range<u64>, Range<u64>)],
) {
    let mut fd = std::fs::File::open(file).unwrap();

    let mut buf = Vec::new();

    let _ = fd.read_to_end(&mut buf).unwrap();

    let buf_iter = buf.chunks(WindowedIndex::size_of()).map(WindowedIndex::of);

    if buf_iter.len() != ranges.len() {
        dbg!(
            buf.chunks(WindowedIndex::size_of())
                .map(WindowedIndex::of)
                .collect::<Vec<_>>()
        );
        panic!("Expected {} ranges, got {}", ranges.len(), buf_iter.len());
    }

    for (index, (inclusive_range, derivative_range)) in buf
        .chunks(WindowedIndex::size_of())
        .map(WindowedIndex::of)
        .zip(ranges)
    {
        dbg!(&index.inclusive_range());
        dbg!(&index.derivative_range());

        assert_eq!(index.inclusive_range(), *inclusive_range);
        assert_eq!(index.derivative_range(), *derivative_range);
    }
}

pub fn size_of_range<T: Sub<T> + Copy>(range: &Range<T>) -> T::Output {
    range.end - range.start
}
