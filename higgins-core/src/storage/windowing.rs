use std::ops::Range;

fn get_window_start_with_offset(timestamp: u64, offset: u64, slide: u64) -> u64 {
    // If the slide is zero, then the window start is always the timestamp.
    // Not sure why this doesn't have logic to snap it to the
    if slide == 0 {
        return timestamp;
    }

    let remainder = if offset > timestamp {
        slide - ((offset - timestamp) % slide)
    } else {
        (timestamp - offset) % slide
    };

    timestamp - remainder
}

//

pub fn assign_sliding_windows(
    value: u64, // can be reused as an index right?
    size: u64,
    slide: u64,
    offset: u64,
) -> Vec<Range<u64>> {
    let mut windows = Vec::new();

    let mut start = get_window_start_with_offset(value as u64, offset as u64, slide as u64);

    let bound = value.checked_sub(size).unwrap_or(0);

    while start >= bound {
        if u64::MAX - size < start {
            break;
        }

        windows.push(Range {
            start,
            end: start + size,
        });

        if 0 + slide > start {
            break;
        }

        start = start - slide;
    }

    windows
}

#[derive(PartialEq, Eq)]
pub struct InclusiveRanges((Range<u64>, Range<u64>));

impl InclusiveRanges {
    pub fn as_tuple(&self) -> &(Range<u64>, Range<u64>) {
        &self.0
    }

    pub fn as_mut_tuple(&mut self) -> &mut (Range<u64>, Range<u64>) {
        &mut self.0
    }
}

impl Ord for InclusiveRanges {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.0.start.cmp(&other.0.0.start)
    }
}

impl PartialOrd for InclusiveRanges {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Completely naive implementation of this algorithm.
///
/// Maps the ranges directly to the values that have been queried for. For instance, given a range of:
///
/// 1..5, with offset 0, size 5 and slide 1, you will get something like:
///
/// 0..5 : 1..5,
/// 1..6 : 1..5
/// 2..7 : 2..5
/// 3..8 : 3..5
///
/// and so forth. This is to give us the values that we need to recreate for the underlying index of the windowed stream.
///
/// TODO: there is probably much that can be
/// done to reuse the buffers and all, but this can be done later.
pub fn assign_sliding_windows_range(
    values: Range<u64>, // can be reused as an index right?
    size: u64,
    slide: u64,
    offset: u64,
) -> Vec<(Range<u64>, Range<u64>)> {
    let mut result: Vec<InclusiveRanges> = Vec::with_capacity((values.end - values.start) as usize);

    for value in values.start..=values.end {
        dbg!(value);

        let range_for_value = assign_sliding_windows(value, size, slide, offset);

        dbg!(&range_for_value);

        for range in range_for_value {
            // find the range that matches this, then append this
            if let Some((_, range)) = result
                .iter_mut()
                .find(|ranges| ranges.0.0 == range)
                .map(|v| v.as_mut_tuple())
            {
                range.end = if value > range.end { value } else { range.end };
            } else {
                result.push(InclusiveRanges((range, value..value)));
            };
        }
    }

    result.sort();

    result
        .iter()
        .map(|range| range.0.clone())
        .collect::<Vec<_>>()
}

pub fn merge_ranges() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn basic_sliding_windows() {
        assert_eq!(assign_sliding_windows(0, 5, 1, 0), &[0..5]);

        assert_eq!(assign_sliding_windows(1, 5, 1, 0), &[1..6, 0..5]);

        assert_eq!(assign_sliding_windows(2, 5, 1, 0), &[2..7, 1..6, 0..5]);

        assert_eq!(
            assign_sliding_windows(6, 5, 1, 0),
            &[6..11, 5..10, 4..9, 3..8, 2..7, 1..6,]
        );
    }

    #[test]
    pub fn test_window_start_with_offset() {
        assert_eq!(get_window_start_with_offset(5, 0, 0), 5);

        assert_eq!(get_window_start_with_offset(5, 10, 0), 5);

        assert_eq!(get_window_start_with_offset(5, 10, 2), 4);

        assert_eq!(get_window_start_with_offset(5, 10, 3), 4);
    }

    #[test]
    pub fn test_assign_sliding_windows_range() {
        assert_eq!(
            assign_sliding_windows_range(0..5, 5, 1, 0),
            vec![
                (0..5, 0..5),
                (1..6, 1..5),
                (2..7, 2..5),
                (3..8, 3..5),
                (4..9, 4..5),
                (5..10, 5..5)
            ]
        );
    }
}
