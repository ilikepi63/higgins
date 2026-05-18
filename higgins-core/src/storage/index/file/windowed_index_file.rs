//! Serves the purpose of giving more opinionated algorithms on
//! certain index types and how to extract/work with them.

use crate::{
    derive::utils::iter_buffer,
    storage::index::{IndexError, IndexFile, Reference, windowed_index::WindowedIndex},
    utils::epoch,
};
use std::ops::Range;

pub struct WindowedIndexFile<'a>(&'a mut IndexFile);

impl<'a> WindowedIndexFile<'a> {
    /// Constructor for creating this from an index file.
    pub fn of(index_file: &'a mut IndexFile) -> Self {
        Self(index_file)
    }

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
    use crate::storage::index::{
        IndexFile, IndexType, file::windowed_index_file::WindowedIndexFile,
        windowed_index::WindowedIndex,
    };
    use std::io::Read as _;

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

            let mut ranges = vec![(10..15, 12..13)];

            file.put_ranges(&mut ranges).unwrap();

            assert_file_holds_ranges(&path, &[(0..5, 0..5), (5..10, 5..10), (10..15, 10..13)]);

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

        std::fs::remove_file(remove_path).unwrap();

        result.unwrap();
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
}
