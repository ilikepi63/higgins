use crate::storage::{dereference::Reference, index::IndexError};
use std::fmt::Debug;

/// JoinedIndex represents the index metadata that one will use to
/// keep track of both offsets of each stream this is derived from.
pub struct JoinedIndex<'a>(&'a [u8]);
// /// The offset of the resultant index.
// pub offset: u64,
// /// The timestamp for this index.
// pub timestamp: u64,
// /// Whether or not this join has been completed by alternative join data. This
// /// Generally means that the join has been appended with the other joined data.
// pub completed: bool,
// /// The object key holding the resultant data from the joining.
// pub object_key: Option<[u8; 16]>,
// /// The offsets of the derivative streams.
// pub offsets: T,

const OFFSET_INDEX: usize = 0;
const TIMESTAMP_INDEX: usize = OFFSET_INDEX + size_of::<u64>();
const COMPLETED_INDEX: usize = TIMESTAMP_INDEX + size_of::<u64>();
const OBJECT_KEY_INDEX: usize = COMPLETED_INDEX + size_of::<bool>();
const INDEXES_INDEX: usize = OBJECT_KEY_INDEX + Reference::size_of();

impl<'a> JoinedIndex<'a> {
    // Properties.
    /// Offset
    pub fn offset(&self) -> u64 {
        u64::from_be_bytes(
            self.0[OFFSET_INDEX..OFFSET_INDEX + size_of::<u64>()]
                .try_into()
                .unwrap(),
        )
    }

    /// Retrieve whether or not this join is completed.
    pub fn completed(&self) -> bool {
        u8::from_be_bytes(
            self.0[COMPLETED_INDEX..COMPLETED_INDEX + size_of::<u8>()]
                .try_into()
                .unwrap(),
        ) == 1
    }

    // Constructors

    /// Creates a instance of this, wrapping the given bytes.
    pub fn of(val: &'a [u8]) -> Self {
        Self(val)
    }

    fn put_offsets(offsets: &[Option<u64>], data: &mut [u8]) {
        for (index, offset) in offsets.iter().enumerate() {
            let current_offset = size_of::<u8>() + size_of::<u64>();

            let (discriminator, bytes) = match offset {
                Some(offset) => (1_u8.to_be_bytes(), offset.to_be_bytes()),
                None => (0_u8.to_be_bytes(), [0; 8]),
            };

            let start = index * current_offset;

            data[start..start + 1].copy_from_slice(discriminator.as_slice());
            data[start + 1..start + 9].copy_from_slice(bytes.as_slice());
        }
    }

    /// Puts the data into the mutable slice, returning this struct as a reference over it.
    pub fn put(
        offset: u64,
        reference: Reference,
        timestamp: u64,
        offsets: &[Option<u64>],
        data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        data[OFFSET_INDEX..OFFSET_INDEX + size_of::<u64>()]
            .copy_from_slice(offset.to_be_bytes().as_slice());
        data[TIMESTAMP_INDEX..TIMESTAMP_INDEX + size_of::<u64>()]
            .copy_from_slice(timestamp.to_be_bytes().as_slice());
        data[COMPLETED_INDEX..COMPLETED_INDEX + size_of::<bool>()]
            .copy_from_slice(0_u8.to_be_bytes().as_slice());
        data[COMPLETED_INDEX..COMPLETED_INDEX + size_of::<bool>()]
            .copy_from_slice(0_u8.to_be_bytes().as_slice());

        reference.to_bytes(&mut data[OBJECT_KEY_INDEX..OBJECT_KEY_INDEX + Reference::size_of()])?;

        Self::put_offsets(offsets, &mut data[INDEXES_INDEX..]);

        Ok(())
    }

    // Destructors
    pub fn inner(self) -> &'a [u8] {
        self.0
    }

    /// Gets the offset at the specified index.
    pub fn get_offset(&self, index: usize) -> Option<u64> {
        match Self::within_bounds(self.0, index) {
            true => {
                let indexes = &self.0[INDEXES_INDEX..];

                tracing::trace!("Indexes: {:#?}", indexes);

                let relative_index = (index * (size_of::<u8>() + size_of::<u64>())) + INDEXES_INDEX;

                let offset =
                    &self.0[relative_index..relative_index + (size_of::<u8>() + size_of::<u64>())];

                let (optional, offset) = offset.split_at(1);

                let result_value = u8::from_be_bytes(optional.try_into().unwrap());

                match result_value {
                    1 => Some(u64::from_be_bytes(offset.try_into().unwrap())),
                    0 => None,
                    _ => {
                        tracing::error!(
                            "Unexpected value in optional for index presence: {}",
                            result_value
                        );
                        unimplemented!()
                    }
                }
            }
            false => {
                tracing::error!("Attempt to query index that is out of bounds: {}", index);
                None
            }
        }
    }

    /// Puts the offset at the specified index.
    pub fn put_offset(buffer: &mut [u8], index: usize, put_offset: u64) -> Result<(), IndexError> {
        match Self::within_bounds(buffer, index) {
            true => {
                let relative_index = (index * (size_of::<u8>() + size_of::<u64>())) + INDEXES_INDEX;

                let offset = &mut buffer
                    [relative_index..relative_index + (size_of::<u8>() + size_of::<u64>())][..];

                let (optional, offset) = offset.split_at_mut(1);

                let optional: &mut [u8; 1] = optional.try_into().unwrap();
                let offset: &mut [u8; 8] = offset.try_into().unwrap();

                *optional = u8::to_be_bytes(1);
                *offset = put_offset.to_be_bytes();

                Ok(())
            }
            false => Err(IndexError::IndexGivenOutOfBoundsForJoinedIndex),
        }
    }

    /// Get the amount of offsets that are in this index.
    pub fn offset_len(&self) -> usize {
        (self.0.len() - INDEXES_INDEX + 1) / size_of::<u64>()
    }

    // Helpers
    pub fn size_of(n_offsets: usize) -> usize {
        // last index (add one to make length), plus the amount of indexes times the size of the optional and the size of the offset.
        INDEXES_INDEX + (n_offsets * (size_of::<u8>() + size_of::<u64>()))
    }

    /// Checks whether an index given is within the specific bounds of this JoinedIndex.
    fn within_bounds(buffer: &[u8], index: usize) -> bool {
        let resultant_length = buffer.len() - INDEXES_INDEX;

        println!("Resultant Length: {}", resultant_length);

        let length = resultant_length / (size_of::<u8>() + size_of::<u64>());

        index < length
    }

    /// Iterates through the other joined index's offsets, copying them over to the current
    /// index's offsets if the current ones are not available.
    pub fn copy_filled_from(current: &mut [u8], other: &[u8]) {
        const OFFSET_SIZE: usize = size_of::<u8>() + size_of::<u64>();

        tracing::trace!("Current Len: {}", current.len());

        let length = (current.len() - INDEXES_INDEX - 1) / OFFSET_SIZE;

        for i in 0..length {
            let current_index = (i * (OFFSET_SIZE)) + INDEXES_INDEX;

            let current_joined_offset =
                JoinedIndexOffset::of(&current[current_index..current_index + OFFSET_SIZE]);

            let other_joined_offset =
                JoinedIndexOffset::of(&other[current_index..current_index + OFFSET_SIZE]);

            if !current_joined_offset.present() && other_joined_offset.present() {
                current[current_index..current_index + OFFSET_SIZE]
                    .iter_mut()
                    .zip(other[current_index..current_index + OFFSET_SIZE].iter())
                    .for_each(|(current, other)| *current = *other);
            }
        }
    }

    /// Retrieve whether or not this join is completed.
    pub fn set_completed(buf: &mut [u8]) {
        buf[COMPLETED_INDEX..(COMPLETED_INDEX + size_of::<u8>())]
            .iter_mut()
            .for_each(|val| {
                *val = 1_u8;
            });
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
        Reference::from_bytes(&self.0[OBJECT_KEY_INDEX..OBJECT_KEY_INDEX + Reference::size_of()])
    }

    /// Update the reference for this.
    pub fn put_reference(&mut self, reference: Reference) -> Vec<u8> {
        let mut cloned = self.0.to_vec();
        reference
            .to_bytes(&mut cloned[OBJECT_KEY_INDEX..OBJECT_KEY_INDEX + Reference::size_of()])
            .unwrap();

        cloned
    }
}

impl<'a> Debug for JoinedIndex<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let offsets = (0..self.offset_len())
            .map(|offset_index| self.get_offset(offset_index))
            .collect::<Vec<_>>();
        f.debug_struct("JoinedStruct")
            .field("offset", &self.offset())
            .field("timestamp", &self.timestamp())
            .field("reference", &self.reference())
            .field("offsets", &offsets)
            .finish()
    }
}

impl<'a> From<&'a [u8]> for JoinedIndex<'a> {
    fn from(src: &'a [u8]) -> Self {
        Self(src)
    }
}

/// A byte sequence representing an Optional offset,
/// which is a big-endian u64 value prepended by a single
/// big-endian byte that is either 1 or 2.
pub struct JoinedIndexOffset<'a>(&'a [u8]);

impl<'a> JoinedIndexOffset<'a> {
    /// Create this from a byte array.
    pub fn of(val: &'a [u8]) -> JoinedIndexOffset<'a> {
        Self(val)
    }

    pub fn present(&self) -> bool {
        self.0[0] == 1
    }

    /// Check if this value is Some or None.
    pub fn get_unchecked(&self) -> u64 {
        u64::from_be_bytes(self.0[1..9].try_into().unwrap())
    }

    /// Check if this value is Some or None.
    pub fn get(&self) -> Option<u64> {
        match self.present() {
            true => Some(self.get_unchecked()),
            false => None,
        }
    }
}

#[cfg(test)]
mod test {
    use colored::Color;

    use super::*;
    #[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
    struct ByteInterval(pub usize, pub usize);

    #[derive(PartialEq, Eq, Debug)]
    struct Interval(ByteInterval, Color, String);

    impl Ord for Interval {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.0.cmp(&other.0)
        }
    }

    impl PartialOrd for Interval {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.0.cmp(&other.0))
        }
    }

    // const OFFSET_INDEX: usize = 0;
    // const TIMESTAMP_INDEX: usize = OFFSET_INDEX + size_of::<u64>();
    // const COMPLETED_INDEX: usize = TIMESTAMP_INDEX + size_of::<u64>();
    // const OBJECT_KEY_INDEX: usize = COMPLETED_INDEX + size_of::<bool>();
    // const INDEXES_INDEX: usize = OBJECT_KEY_INDEX + Reference::size_of();

    fn print_bytes_coloured(bytes: &[u8], colours: &mut [Interval]) {
        use colored::Colorize;

        colours.sort();

        // ensure that the intervals don't overlap.
        for window in colours.windows(3) {
            let first = window.first().unwrap();
            let second = window.get(1).unwrap();
            let third = window.get(2).unwrap();

            assert!(first.0.1 <= second.0.0);
            assert!(second.0.1 <= third.0.0);
        }

        let mut i = 0;

        println!("[");

        for colour in colours {
            // For each colour, we want to iterate over the bytes basically
            while i < colour.0.0 {
                println!(" {}", bytes.get(i).unwrap());
                i += 1;
            }

            while i < colour.0.1 {
                if i == colour.0.0 {
                    println!(
                        " {} {}",
                        format!("{}", bytes.get(i).unwrap())
                            .to_string()
                            .color(colour.1),
                        colour.2
                    );
                } else {
                    println!(
                        " {}",
                        format!("{}", bytes.get(i).unwrap())
                            .to_string()
                            .color(colour.1)
                    );
                }

                i += 1;
            }
        }

        while i < bytes.len() {
            println!(" {}", bytes.get(i).unwrap());
            i += 1;
        }

        print!("]");
    }

    #[test]
    fn print_bytes_coloured_test() {
        let bytes = [1_u8; 10];
        let intervals = &mut [
            Interval(ByteInterval(1, 3), Color::Blue, "First".to_string()),
            Interval(ByteInterval(3, 6), Color::Green, "Second".to_string()),
        ];

        print_bytes_coloured(&bytes, intervals);
    }

    fn debug_join_index_bytes(join_index_bytes: &[u8]) {
        let intervals = &mut [
            Interval(
                ByteInterval(OFFSET_INDEX, TIMESTAMP_INDEX),
                Color::Blue,
                "Offset".to_string(),
            ),
            Interval(
                ByteInterval(TIMESTAMP_INDEX, COMPLETED_INDEX),
                Color::Green,
                "Timestamp".to_string(),
            ),
            Interval(
                ByteInterval(COMPLETED_INDEX, OBJECT_KEY_INDEX),
                Color::Red,
                "Completed".to_string(),
            ),
            Interval(
                ByteInterval(OBJECT_KEY_INDEX, INDEXES_INDEX),
                Color::Yellow,
                "Reference".to_string(),
            ),
            // TODO: Probably make this dynamic?
            Interval(
                ByteInterval(INDEXES_INDEX, INDEXES_INDEX + 9),
                Color::Blue,
                "First Index".to_string(),
            ),
            Interval(
                ByteInterval(INDEXES_INDEX + 9, INDEXES_INDEX + 18),
                Color::Red,
                "Second Index".to_string(),
            ),
            // Interval(
            //     ByteInterval(INDEXES_INDEX + 18, INDEXES_INDEX + 27),
            //     Color::Yellow,
            //     "Last Index".to_string(),
            // ),
        ];

        print_bytes_coloured(join_index_bytes, intervals);
    }

    #[test]
    pub fn can_put_joined_index() {
        let mut joined_index_bytes = vec![0_u8; JoinedIndex::size_of(3)];

        JoinedIndex::put(
            0,
            Reference::Null,
            2,
            &[Some(1), None, Some(2)],
            &mut joined_index_bytes,
        )
        .inspect_err(|err| {
            tracing::error!(
                "Failed to put Joined Index bytes into buffer with error: {:#?}",
                err,
            );
        })
        .unwrap();

        dbg!(&joined_index_bytes);

        debug_join_index_bytes(&joined_index_bytes);

        let joined_index = JoinedIndex::of(&joined_index_bytes);

        assert_eq!(joined_index.offset(), 0);
        assert_eq!(joined_index.timestamp(), 2);
        assert!(matches!(joined_index.reference(), Reference::Null));

        dbg!(&joined_index);

        assert!(joined_index.get_offset(0).is_some_and(|val| val == 1));
        assert!(joined_index.get_offset(1).is_none());
        assert!(joined_index.get_offset(2).is_some_and(|val| val == 2));

        dbg!(&joined_index);
    }

    #[test]
    fn test_size_of() {
        assert_eq!(JoinedIndex::size_of(0), INDEXES_INDEX);
        assert_eq!(
            JoinedIndex::size_of(1),
            INDEXES_INDEX + size_of::<u8>() + size_of::<u64>()
        );
        assert_eq!(
            JoinedIndex::size_of(2),
            INDEXES_INDEX + 2 * (size_of::<u8>() + size_of::<u64>())
        );
    }

    #[test]
    fn test_offset() {
        let offset = 123456789u64;
        let mut data = vec![0u8; INDEXES_INDEX];
        data[0..8].copy_from_slice(&offset.to_be_bytes());
        let ji = JoinedIndex::of(&data);
        assert_eq!(ji.offset(), offset);
    }

    #[test]
    fn test_timestamp() {
        let timestamp = 987654321u64;
        let mut data = vec![0u8; INDEXES_INDEX];
        data[8..16].copy_from_slice(&timestamp.to_be_bytes());
        let ji = JoinedIndex::of(&data);
        assert_eq!(ji.timestamp(), timestamp);
    }

    #[test]
    fn test_completed() {
        let mut data = vec![0u8; INDEXES_INDEX];
        // Test false (0)
        let ji_false = JoinedIndex::of(&data);
        assert!(!ji_false.completed());

        // Test true (1)
        data[16] = 1u8;
        let ji_true = JoinedIndex::of(&data);
        assert!(ji_true.completed());
    }

    #[test]
    fn test_offset_len() {
        // For 0 offsets
        let data = vec![0u8; INDEXES_INDEX];
        let ji = JoinedIndex::of(&data);
        assert_eq!(ji.offset_len(), 0);

        // For 1 offset
        let data = vec![0u8; INDEXES_INDEX + size_of::<u8>() + size_of::<u64>()];
        let ji = JoinedIndex::of(&data);
        assert_eq!(ji.offset_len(), 1);

        // For 2 offsets
        let data = vec![0u8; INDEXES_INDEX + 2 * (size_of::<u8>() + size_of::<u64>())];
        let ji = JoinedIndex::of(&data);
        assert_eq!(ji.offset_len(), 2);

        // Note: This tests the current implementation, which may have precision issues for larger n.
    }

    #[test]
    fn test_get_offset() {
        let n_offsets = 2;
        let total_size = INDEXES_INDEX + n_offsets * (size_of::<u8>() + size_of::<u64>());
        let mut data = vec![0u8; total_size];

        // Set offset 0 to Some(100)
        let offset0 = 100u64;
        let start0 = INDEXES_INDEX;
        data[start0] = 1u8;
        data[start0 + 1..start0 + 9].copy_from_slice(&offset0.to_be_bytes());

        // Set offset 1 to None
        let start1 = start0 + size_of::<u8>() + size_of::<u64>();
        data[start1] = 0u8;

        let ji = JoinedIndex::of(&data);

        assert_eq!(ji.get_offset(0), Some(offset0));
        assert_eq!(ji.get_offset(1), None);
        // Out of bounds
        assert_eq!(ji.get_offset(2), None);
    }

    #[test]
    fn test_put_offset() {
        let n_offsets = 2;
        let total_size = INDEXES_INDEX + n_offsets * (size_of::<u8>() + size_of::<u64>());
        let mut data = vec![0u8; total_size];

        // Put offset 0 to 200
        let offset0 = 200u64;
        assert!(JoinedIndex::put_offset(&mut data, 0, offset0).is_ok());

        // Verify
        let start0 = INDEXES_INDEX;
        assert_eq!(data[start0], 1u8);
        assert_eq!(
            u64::from_be_bytes(data[start0 + 1..start0 + 9].try_into().unwrap()),
            offset0
        );

        // Put offset 1 to 300
        let offset1 = 300u64;
        assert!(JoinedIndex::put_offset(&mut data, 1, offset1).is_ok());

        let start1 = start0 + 9;
        assert_eq!(data[start1], 1u8);
        assert_eq!(
            u64::from_be_bytes(data[start1 + 1..start1 + 9].try_into().unwrap()),
            offset1
        );

        // Out of bounds should error
        match JoinedIndex::put_offset(&mut data, 2, 400u64) {
            Err(IndexError::IndexGivenOutOfBoundsForJoinedIndex) => {}
            _ => panic!("Expected IndexError"),
        }
    }

    #[test]
    fn test_put() {
        let offset = 111u64;
        let timestamp = 222u64;
        let offsets = [Some(333_u64), None];
        let n_offsets = offsets.len();
        let total_size = INDEXES_INDEX + n_offsets * (size_of::<u8>() + size_of::<u64>());
        let mut data = vec![0u8; total_size];

        data[0..8].copy_from_slice(&offset.to_be_bytes());
        data[8..16].copy_from_slice(&timestamp.to_be_bytes());
        data[16] = 0u8; // completed false
        data[17..17 + Reference::size_of()].copy_from_slice(&[0u8; 34]); // mock reference bytes

        // Simulate put_offsets
        for (index, off) in offsets.iter().enumerate() {
            let start = INDEXES_INDEX + index * 9;
            match off {
                Some(o) => {
                    data[start] = 1u8;
                    data[start + 1..start + 9].copy_from_slice(&o.to_be_bytes());
                }
                None => {
                    data[start] = 0u8;
                    data[start + 1..start + 9].fill(0u8);
                }
            }
        }

        debug_join_index_bytes(&data);

        let ji = JoinedIndex::of(&data);
        assert_eq!(ji.offset(), offset);
        assert_eq!(ji.timestamp(), timestamp);
        assert!(!ji.completed());
        // assert_eq!(ji.reference(), reference); // Would require actual impl
        assert_eq!(ji.get_offset(0), Some(333u64));
        assert_eq!(ji.get_offset(1), None);
    }

    #[test]
    fn test_set_completed() {
        let total_size = INDEXES_INDEX;
        let mut data = vec![0u8; total_size];
        data[16] = 0u8;

        JoinedIndex::set_completed(&mut data);

        assert_eq!(data[16], 1u8);
    }

    #[test]
    fn test_copy_filled_from() {
        let n_offsets = 2;
        let total_size = INDEXES_INDEX + n_offsets * (size_of::<u8>() + size_of::<u64>());
        let mut current = vec![0u8; total_size];
        let mut other = vec![0u8; total_size];

        // Set other offset 0 to present (100), offset 1 None
        let start0 = INDEXES_INDEX;
        other[start0] = 1u8;
        other[start0 + 1..start0 + 9].copy_from_slice(&100u64.to_be_bytes());
        other[start0 + 9] = 0u8;

        // Current: offset 0 None, offset 1 present (but we'll overwrite if condition)
        current[start0] = 0u8;
        let start1 = start0 + 9;
        current[start1] = 1u8;
        current[start1 + 1..start1 + 9].copy_from_slice(&200u64.to_be_bytes());

        JoinedIndex::copy_filled_from(&mut current, &other);

        // offset 0 should now be filled from other (100)
        assert_eq!(current[start0], 1u8);
        assert_eq!(
            u64::from_be_bytes(current[start0 + 1..start0 + 9].try_into().unwrap()),
            100u64
        );
        // offset 1 remains 200 since current was present
        assert_eq!(
            u64::from_be_bytes(current[start1 + 1..start1 + 9].try_into().unwrap()),
            200u64
        );
    }

    #[test]
    fn test_put_reference() {
        let total_size = INDEXES_INDEX;
        let data = vec![0u8; total_size];
        let mut ji = JoinedIndex::of(&data);

        let new_ref = Reference::Null;
        // Simulate update
        let updated = ji.put_reference(new_ref);
        assert_eq!(updated.len(), total_size);
        // Check that object key bytes are updated (mock sets to 0s)
        assert!(
            updated[17..17 + Reference::size_of()]
                .iter()
                .all(|&b| b == 0u8)
        );
    }

    #[test]
    fn test_joined_index_offset() {
        // Present
        let mut bytes_present = vec![0u8; 9];
        bytes_present[0] = 1u8;
        bytes_present[1..9].copy_from_slice(&42u64.to_be_bytes());
        let off_present = JoinedIndexOffset::of(&bytes_present);
        assert!(off_present.present());
        assert_eq!(off_present.get(), Some(42u64));
        assert_eq!(off_present.get_unchecked(), 42u64);

        // Absent
        let mut bytes_absent = vec![0u8; 9];
        bytes_absent[0] = 0u8;
        let off_absent = JoinedIndexOffset::of(&bytes_absent);
        assert!(!off_absent.present());
        assert_eq!(off_absent.get(), None);
    }

    #[test]
    fn test_debug() {
        let n_offsets = 1;
        let total_size = INDEXES_INDEX + n_offsets * (size_of::<u8>() + size_of::<u64>());
        let mut data = vec![0u8; total_size];
        data[0..8].copy_from_slice(&1u64.to_be_bytes()); // offset
        data[8..16].copy_from_slice(&2u64.to_be_bytes()); // timestamp
        data[16] = 0u8; // completed
        data[17..17 + Reference::size_of()].copy_from_slice(&[0u8; 34]); // mock ref
        let start = INDEXES_INDEX;
        data[start] = 1u8;
        data[start + 1..start + 9].copy_from_slice(&4u64.to_be_bytes()); // offset 0

        let ji = JoinedIndex::of(&data);
        let debug_str = format!("{:?}", ji);
        assert!(debug_str.contains("offset: 1"));
        assert!(debug_str.contains("timestamp: 2"));
        assert!(debug_str.contains("offsets: [Some(4)]"));
        // Reference debug would depend on impl, but assumes it prints something.
    }

    #[test]
    fn test_from_bytes() {
        let data = vec![0u8; INDEXES_INDEX];
        let ji1 = JoinedIndex::of(&data);
        let ji2: JoinedIndex = (&data[..]).into();
        assert_eq!(ji1.offset(), ji2.offset());
    }

    #[test]
    fn test_within_bounds_helper() {
        let total_size = INDEXES_INDEX + 2 * 9;
        let data = vec![0u8; total_size];

        // index 0 and 1 in bounds
        assert!(JoinedIndex::within_bounds(&data, 0));
        assert!(JoinedIndex::within_bounds(&data, 1));
        // index 2 out
        assert!(!JoinedIndex::within_bounds(&data, 2));
    }
}
