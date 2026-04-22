use crate::storage::dereference::Reference;
use std::{fmt::Debug, ops::Range};

/// WindowedIndex represents an index that holds the derivative values
/// of an underlying windowed stream. Each Index would keep a list of ranges of the underlying stream
/// that are part of each value.
pub struct WindowedIndex<'a>(&'a [u8]);

const TIMESTAMP_INDEX: usize = 0;
const REFERENCE_INDEX: usize = TIMESTAMP_INDEX + size_of::<u64>();
const RANGE: usize = REFERENCE_INDEX + Reference::size_of();

impl<'a> WindowedIndex<'a> {
    /// Creates a instance of this, wrapping the given bytes.
    pub fn of(val: &'a [u8]) -> Self {
        Self(val)
    }

    pub const fn size_of() -> usize {
        RANGE + (size_of::<u64>() * 2)
    }

    /// Puts the data into the mutable slice, returning this struct as a reference over it.
    pub fn put(
        reference: Reference,
        timestamp: u64,
        range: Range<u64>,
        data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        data[TIMESTAMP_INDEX..TIMESTAMP_INDEX + size_of::<u64>()]
            .copy_from_slice(timestamp.to_be_bytes().as_slice());

        reference.to_bytes(&mut data[REFERENCE_INDEX..REFERENCE_INDEX + Reference::size_of()])?;

        data[RANGE..RANGE + size_of::<u64>()].copy_from_slice(&range.start.to_be_bytes());
        data[RANGE + size_of::<u64>()..RANGE + size_of::<u64>() * 2]
            .copy_from_slice(&range.end.to_be_bytes());

        Ok(())
    }

    // Destructors
    pub fn inner(self) -> &'a [u8] {
        self.0
    }

    pub fn timestamp(&self) -> u64 {
        u64::from_be_bytes(
            self.0[TIMESTAMP_INDEX..TIMESTAMP_INDEX + size_of::<u64>()]
                .try_into()
                .unwrap(),
        )
    }

    /// Retrieve the reference of this Index.
    pub fn reference(&self) -> Reference {
        Reference::from_bytes(&self.0[REFERENCE_INDEX..REFERENCE_INDEX + Reference::size_of()])
    }

    /// Update the reference for this.
    pub fn put_reference(&mut self, reference: Reference) -> Vec<u8> {
        let mut cloned = self.0.to_vec();
        reference
            .to_bytes(&mut cloned[REFERENCE_INDEX..REFERENCE_INDEX + Reference::size_of()])
            .unwrap();

        cloned
    }

    pub fn range(&self) -> Range<u64> {
        let start = u64::from_be_bytes(self.0[RANGE..RANGE + size_of::<u64>()].try_into().unwrap());
        let end = u64::from_be_bytes(
            self.0[RANGE + size_of::<u64>()..RANGE + size_of::<u64>() * 2]
                .try_into()
                .unwrap(),
        );
        start..end
    }

    pub fn put_range(range: Range<u64>, val: &mut [u8]) {
        val[RANGE..RANGE + size_of::<u64>()].copy_from_slice(&range.start.to_be_bytes());
        val[RANGE + size_of::<u64>()..RANGE + size_of::<u64>() * 2]
            .copy_from_slice(&range.start.to_be_bytes());
    }
}

impl<'a> Debug for WindowedIndex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowedIndex")
            .field("range", &self.range())
            .field("reference", &self.reference())
            .field("timestamp", &self.timestamp())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::dereference::S3Reference;

    use super::*;
    use std::ops::Range;

    #[test]
    fn test_constants_layout() {
        let expected_size = size_of::<u64>() * 3 + Reference::size_of();
        assert_eq!(
            RANGE + size_of::<u64>() * 2,
            expected_size,
            "Layout constants should cover timestamp + reference + range"
        );
    }

    #[test]
    fn test_put_and_read_all_fields() {
        let mut buffer = vec![0u8; 200];
        let timestamp = 1_694_000_000_123u64;
        let reference = Reference::Null;
        let range = 500..1500u64;

        WindowedIndex::put(reference, timestamp, range.clone(), &mut buffer)
            .expect("put should succeed");

        let index = WindowedIndex::of(&buffer);

        assert_eq!(index.timestamp(), timestamp);
        assert!(matches!(index.reference(), Reference::Null));
        assert_eq!(index.range(), range);
    }

    #[test]
    fn test_inner_returns_original_slice() {
        let data = vec![0xAAu8; 64];
        let index = WindowedIndex::of(&data);
        let inner = index.inner();

        assert_eq!(
            inner.as_ptr(),
            data.as_ptr(),
            "Should return the exact same slice"
        );
        assert_eq!(inner.len(), data.len());
    }

    #[test]
    fn test_put_reference_updates_only_reference() {
        let mut buffer = vec![0u8; 200];
        let original_ts = 123456789u64;
        let original_ref = Reference::Null;
        let original_range = 1000..2000u64;

        WindowedIndex::put(
            original_ref,
            original_ts,
            original_range.clone(),
            &mut buffer,
        )
        .unwrap();

        let mut index = WindowedIndex::of(&buffer);

        let new_ref = Reference::S3(S3Reference {
            object_key: [0; 16],
            position: 1,
            size: 1,
        });
        let updated_bytes = index.put_reference(new_ref.clone());

        let updated_index = WindowedIndex::of(&updated_bytes);

        assert_eq!(
            updated_index.timestamp(),
            original_ts,
            "Timestamp should be preserved"
        );
        assert_eq!(
            updated_index.reference(),
            new_ref,
            "Reference should be updated"
        );
        assert_eq!(
            updated_index.range(),
            original_range,
            "Range should be preserved"
        );
    }

    #[test]
    fn test_various_timestamps() {
        let timestamps = vec![
            0u64,
            1u64,
            42u64,
            u64::MAX,
            1_000_000_000_000u64,
            0x0123456789ABCDEFu64,
        ];

        for ts in timestamps {
            let mut buffer = vec![0u8; 200];
            let range = 10..20;

            WindowedIndex::put(Reference::Null, ts, range.clone(), &mut buffer).unwrap();

            let index = WindowedIndex::of(&buffer);
            assert_eq!(index.timestamp(), ts, "Failed for timestamp {}", ts);
            assert_eq!(index.range(), range);
        }
    }

    #[test]
    fn test_various_ranges() {
        let ranges: Vec<Range<u64>> = vec![
            0..0,
            0..1,
            100..200,
            999_999..1_000_000,
            u64::MAX - 100..u64::MAX,
            0..u64::MAX,
            1_234_567_890_123..1_234_567_890_456,
        ];

        for range in ranges {
            let mut buffer = vec![0u8; 200];
            let ts = 987654321u64;
            let reference = Reference::Null;

            WindowedIndex::put(reference.clone(), ts, range.clone(), &mut buffer).unwrap();

            let index = WindowedIndex::of(&buffer);
            assert_eq!(
                index.range(),
                range,
                "Range roundtrip failed for {:?}",
                range
            );
            assert_eq!(index.timestamp(), ts);
        }
    }

    #[test]
    fn test_reference_roundtrip() {
        let mut buffer = vec![0u8; 200];
        let original_ref = Reference::Null;

        WindowedIndex::put(original_ref.clone(), 100, 50..150, &mut buffer).unwrap();

        let index = WindowedIndex::of(&buffer);
        assert_eq!(index.reference(), original_ref);
    }

    #[test]
    fn test_put_with_minimum_buffer_size() {
        let min_size = RANGE + size_of::<u64>() * 2;
        let mut buffer = vec![0u8; min_size];

        let result = WindowedIndex::put(Reference::Null, 42u64, 10..20, &mut buffer);

        assert!(
            result.is_ok(),
            "Should succeed with exact minimum buffer size"
        );
    }

    #[test]
    #[should_panic(expected = "range end index")]
    fn test_put_with_too_small_buffer_panics() {
        let mut buffer = vec![0u8; RANGE + size_of::<u64>() - 1]; // intentionally too small
        let _ = WindowedIndex::put(Reference::Null, 0, 0..10, &mut buffer);
    }

    #[test]
    fn test_put_returns_ok_on_success() {
        let mut buffer = vec![0u8; 200];
        let result = WindowedIndex::put(Reference::Null, 123u64, 0..100, &mut buffer);
        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_puts_on_same_buffer() {
        let mut buffer = vec![0u8; 200];

        // First put
        WindowedIndex::put(Reference::Null, 100, 10..20, &mut buffer).unwrap();
        let idx1 = WindowedIndex::of(&buffer);
        assert_eq!(idx1.timestamp(), 100);
        assert_eq!(idx1.range(), 10..20);

        // Overwrite with different values
        let new_ref = Reference::S3(S3Reference {
            object_key: [0; 16],
            position: 1,
            size: 1,
        });
        WindowedIndex::put(new_ref.clone(), 999, 500..600, &mut buffer).unwrap();

        let idx2 = WindowedIndex::of(&buffer);
        assert_eq!(idx2.timestamp(), 999);
        assert_eq!(idx2.reference(), new_ref);
        assert_eq!(idx2.range(), 500..600);
    }

    #[test]
    fn test_put_reference_on_fresh_index() {
        let mut buffer = vec![0u8; 200];
        let ts = 777u64;
        let original_range = 123..456;

        WindowedIndex::put(Reference::Null, ts, original_range.clone(), &mut buffer).unwrap();

        let mut index = WindowedIndex::of(&buffer);
        let new_ref = Reference::S3(S3Reference {
            object_key: [0; 16],
            position: 1,
            size: 1,
        });
        let updated = index.put_reference(new_ref.clone());

        let updated_index = WindowedIndex::of(&updated);
        assert_eq!(updated_index.timestamp(), ts);
        assert_eq!(updated_index.range(), original_range);
        assert_eq!(updated_index.reference(), new_ref);
    }

    // Property-like tests (manual)
    #[test]
    fn test_put_reference_does_not_change_length() {
        let mut buffer = vec![0u8; 200];
        WindowedIndex::put(Reference::Null, 42, 0..100, &mut buffer).unwrap();

        let original_len = buffer.len();
        let mut index = WindowedIndex::of(&buffer);
        let updated = index.put_reference(Reference::S3(S3Reference {
            object_key: [0; 16],
            position: 1,
            size: 1,
        }));

        assert_eq!(updated.len(), original_len);
    }
}
