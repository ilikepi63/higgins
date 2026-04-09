//! Serves the purpose of giving more opinionated algorithms on
//! certain index types and how to extract/work with them.

use crate::storage::index::{IndexFile, windowed_index::WindowedIndex};
use std::ops::Range;

pub struct WindowedIndexFile(IndexFile);

impl WindowedIndexFile {
    /// Given a list of ranges, we will need to add this
    /// offset/index to the given ranges and ensure that they are not
    /// "completed".
    ///
    /// A range is completed when it can no longer be appended to (ie, the timestamp is closed or the value has been added)
    ///
    /// `index` refers to the underlying offset from the derivative stream, whilst the ranges are the identifiers for which ranges need to be
    /// added.
    pub fn add_ranges(&mut self, ranges: &[Range<u64>], index: u64) {
        // Ranges will likely always be sequential, is it not better to design this API around that then?
        // ie get range by start and end, iterate through each and boom we have it.

        // For each of the given ranges, the index file should have a corresponding offset value for it
        // and have the above `index` present in those offset values.
    }

    /// Gets the range give a specific start and end position range.
    ///
    /// uses binary sort as ranges are expected to always be in incrementing order.
    pub fn get_ranges_binary<'a>(&'a self, ranges: &[Range<u64>]) -> &'a [u8] {
        // We are expecting that if we are putting onto ranges, it will start from the end.
        let start = ranges.first().unwrap().start;
        let end = ranges.first().unwrap().end;

        let mut buf = vec![0_u8; WindowedIndex::size_of() * ranges.len()];

        // load the buffer into memory

        buf;

        todo!();
    }
}
