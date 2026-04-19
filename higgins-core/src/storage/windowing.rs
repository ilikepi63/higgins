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

pub fn assign_sliding_windows(
    timestamp: u64, // can be reused as an index right?
    size: u64,
    slide: u64,
    offset: u64,
) -> Vec<Range<u64>> {
    let mut windows = Vec::new();

    let mut start = get_window_start_with_offset(timestamp as u64, offset as u64, slide as u64);

    let bound = timestamp.checked_sub(size).unwrap_or(0);

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
}
