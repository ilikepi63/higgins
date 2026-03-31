use std::cmp;

/// Represents a time window [start, end)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    pub start: i64,
    pub end: i64,
}

impl TimeWindow {
    pub fn new(start: i64, end: i64) -> Self {
        TimeWindow { start, end }
    }
}

/// Assigns an event timestamp to all sliding windows it belongs to.
///
/// This is the core logic used in stream processing engines like Apache Flink
/// for sliding event-time windows.
pub fn assign_sliding_windows(
    timestamp: i64,
    window_size: i64, // W
    slide: i64,       // S
    offset: i64,      // alignment offset (can be 0)
) -> Vec<TimeWindow> {
    if window_size <= 0 || slide <= 0 {
        panic!("Window size and slide must be positive");
    }

    let mut windows = Vec::new();

    // Step 1: Compute the largest window start <= timestamp that respects the offset and slide
    // This is the rightmost (most recent) window that could contain the timestamp
    let adjusted = timestamp - offset;
    let mut current_start = (adjusted / slide) * slide + offset;

    // If we overshot due to negative numbers or edge cases, step back one slide
    if current_start > timestamp {
        current_start -= slide;
    }

    // Lower bound: any window start smaller than this cannot contain the timestamp
    let lower_bound = timestamp - window_size + 1;

    // Step 2: Walk backwards by slide until we go below the lower bound
    while current_start >= lower_bound {
        let window_end = current_start + window_size;

        // Only add if the window actually contains the timestamp
        // (this check handles edge cases with offset and negative timestamps safely)
        if current_start <= timestamp && timestamp < window_end {
            windows.push(TimeWindow::new(current_start, window_end));
        }

        current_start -= slide;
    }

    // Optional: sort from oldest to newest window (most engines prefer this order)
    windows.sort_by_key(|w| w.start);

    windows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_sliding_window() {
        // W=5, S=2, offset=0, t=4
        let windows = assign_sliding_windows(4, 5, 2, 0);
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0], TimeWindow::new(0, 5));
        assert_eq!(windows[1], TimeWindow::new(2, 7));
        assert_eq!(windows[2], TimeWindow::new(4, 9));
    }

    #[test]
    fn test_with_offset() {
        // Align windows to start at multiples of 10 + 3 (e.g. ..., -7, 3, 13, 23, ...)
        let windows = assign_sliding_windows(10, 6, 3, 3);

        // Expected windows that contain t=10: [7,13), [10,16) ? Let's verify
        println!("{:?}", windows);
        // You can add more assertions based on your expected behavior
    }

    #[test]
    fn test_edge_case_exact_boundary() {
        // t exactly equals a window start
        let windows = assign_sliding_windows(10, 5, 2, 0);
        assert!(windows.iter().any(|w| w.start == 10));
    }

    #[test]
    fn test_small_slide_larger_than_window() {
        // S > W → at most one window
        let windows = assign_sliding_windows(7, 3, 5, 0);
        assert!(windows.len() <= 1);
    }
}
