use crate::storage::dereference::Reference;
use std::{fmt::Debug, ops::Range};

/// WindowedIndex represents an index that holds the derivative values
/// of an underlying windowed stream. Each Index would keep a list of ranges of the underlying stream
/// that are part of each value.
pub struct WindowedIndex<'a>(&'a [u8]);

const TIMESTAMP_INDEX: usize = 0;
const REFERENCE_INDEX: usize = TIMESTAMP_INDEX + size_of::<u64>();
/// This range holds the from/to of the values that should go into here.
/// This is required as there is no real way for the stream to know what each index
/// might include.
const INCLUSIVE_RANGE_OFFSET: usize = REFERENCE_INDEX + Reference::size_of();
/// The derivative range is the values from the derivative stream that form part of this index. This
/// holds the actual references to the derivative stream.
const DERIVATIVE_RANGE_OFFSET: usize = INCLUSIVE_RANGE_OFFSET + Reference::size_of();

impl<'a> WindowedIndex<'a> {
    /// Creates a instance of this, wrapping the given bytes.
    pub fn of(val: &'a [u8]) -> Self {
        Self(val)
    }

    pub const fn size_of() -> usize {
        DERIVATIVE_RANGE_OFFSET + (size_of::<u64>() * 2)
    }

    /// Puts the data into the mutable slice, returning this struct as a reference over it.
    pub fn put(
        reference: Reference,
        timestamp: u64,
        inclusive_range: Range<u64>,
        derivative_range: Range<u64>,
        data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        data[TIMESTAMP_INDEX..TIMESTAMP_INDEX + size_of::<u64>()]
            .copy_from_slice(timestamp.to_be_bytes().as_slice());

        reference.to_bytes(&mut data[REFERENCE_INDEX..REFERENCE_INDEX + Reference::size_of()])?;

        data[INCLUSIVE_RANGE_OFFSET..INCLUSIVE_RANGE_OFFSET + size_of::<u64>()]
            .copy_from_slice(&inclusive_range.start.to_be_bytes());
        data[INCLUSIVE_RANGE_OFFSET + size_of::<u64>()
            ..INCLUSIVE_RANGE_OFFSET + size_of::<u64>() * 2]
            .copy_from_slice(&inclusive_range.end.to_be_bytes());

        data[DERIVATIVE_RANGE_OFFSET..DERIVATIVE_RANGE_OFFSET + size_of::<u64>()]
            .copy_from_slice(&derivative_range.start.to_be_bytes());
        data[DERIVATIVE_RANGE_OFFSET + size_of::<u64>()
            ..DERIVATIVE_RANGE_OFFSET + size_of::<u64>() * 2]
            .copy_from_slice(&derivative_range.end.to_be_bytes());

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
                ?,
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
            ?;

        cloned
    }

    pub fn inclusive_range(&self) -> Range<u64> {
        let start = u64::from_be_bytes(
            self.0[INCLUSIVE_RANGE_OFFSET..INCLUSIVE_RANGE_OFFSET + size_of::<u64>()]
                .try_into()
                ?,
        );
        let end = u64::from_be_bytes(
            self.0[INCLUSIVE_RANGE_OFFSET + size_of::<u64>()
                ..INCLUSIVE_RANGE_OFFSET + size_of::<u64>() * 2]
                .try_into()
                ?,
        );
        start..end
    }

    pub fn put_inclusive_range(range: Range<u64>, val: &mut [u8]) {
        val[INCLUSIVE_RANGE_OFFSET..INCLUSIVE_RANGE_OFFSET + size_of::<u64>()]
            .copy_from_slice(&range.start.to_be_bytes());
        val[INCLUSIVE_RANGE_OFFSET + size_of::<u64>()
            ..INCLUSIVE_RANGE_OFFSET + size_of::<u64>() * 2]
            .copy_from_slice(&range.start.to_be_bytes());
    }

    pub fn derivative_range(&self) -> Range<u64> {
        let start = u64::from_be_bytes(
            self.0[DERIVATIVE_RANGE_OFFSET..DERIVATIVE_RANGE_OFFSET + size_of::<u64>()]
                .try_into()
                ?,
        );
        let end = u64::from_be_bytes(
            self.0[DERIVATIVE_RANGE_OFFSET + size_of::<u64>()
                ..DERIVATIVE_RANGE_OFFSET + size_of::<u64>() * 2]
                .try_into()
                ?,
        );
        start..end
    }

    pub fn put_derivative_range(range: Range<u64>, val: &mut [u8]) {
        val[DERIVATIVE_RANGE_OFFSET..DERIVATIVE_RANGE_OFFSET + size_of::<u64>()]
            .copy_from_slice(&range.start.to_be_bytes());
        val[DERIVATIVE_RANGE_OFFSET + size_of::<u64>()
            ..DERIVATIVE_RANGE_OFFSET + size_of::<u64>() * 2]
            .copy_from_slice(&range.start.to_be_bytes());
    }
}

impl<'a> Debug for WindowedIndex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowedIndex")
            .field("inclusive range", &self.inclusive_range())
            .field("derivative range", &self.derivative_range())
            .field("reference", &self.reference())
            .field("timestamp", &self.timestamp())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::dereference::S3Reference;

    use super::*;

    #[test]
    fn test_put_and_read_all_fields() {
        let mut buffer = vec![0u8; 200];
        let timestamp = 1_694_000_000_123u64;
        let reference = Reference::Null;
        let inclusive_range = 100..1100u64;
        let derivative_range = 500..1500u64;

        WindowedIndex::put(
            reference,
            timestamp,
            inclusive_range.clone(),
            derivative_range.clone(),
            &mut buffer,
        )
        .expect("put should succeed");

        let index = WindowedIndex::of(&buffer);

        assert_eq!(index.timestamp(), timestamp);
        assert!(matches!(index.reference(), Reference::Null));
        assert_eq!(index.inclusive_range(), inclusive_range);
        assert_eq!(index.derivative_range(), derivative_range);
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
        let original_inclusive_range = 1000..2000u64;
        let original_derivative_range = 1000..2000u64;

        WindowedIndex::put(
            original_ref,
            original_ts,
            original_inclusive_range.clone(),
            original_derivative_range.clone(),
            &mut buffer,
        )
        ?;

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
            updated_index.inclusive_range(),
            original_inclusive_range,
            "Inclusive Range should be preserved"
        );

        assert_eq!(
            updated_index.derivative_range(),
            original_derivative_range,
            "Derivative Range should be preserved"
        );
    }

    #[test]
    fn test_reference_roundtrip() {
        let mut buffer = vec![0u8; 200];
        let original_ref = Reference::Null;

        WindowedIndex::put(original_ref.clone(), 100, 50..150, 50..150, &mut buffer)?;

        let index = WindowedIndex::of(&buffer);
        assert_eq!(index.reference(), original_ref);
    }
}
