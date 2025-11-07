use crate::storage::{dereference::Reference, index::IndexError};
use std::{fmt::Debug, io::Write};

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

    /// Puts the data into the mutable slice, returning this struct as a reference over it.
    pub fn put(
        offset: u64,
        object_key: Option<[u8; 16]>,
        timestamp: u64,
        offsets: &[Option<u64>],
        mut data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        data.write_all(offset.to_be_bytes().as_slice())?;
        data.write_all(timestamp.to_be_bytes().as_slice())?;
        // Completed is false by default.
        data.write_all(0_u8.to_be_bytes().as_slice())?;

        match object_key {
            Some(object_key) => {
                data.write_all(&u8::to_be_bytes(1))?;
                data.write_all(object_key.as_slice())?;
            }
            None => {
                data.write_all(&u8::to_be_bytes(0))?;
                data.write_all([0_u8; 16].as_slice())?;
            }
        }

        for offset in offsets {
            match offset {
                Some(offset) => {
                    data.write_all(&u8::to_be_bytes(1))?;
                    data.write_all(offset.to_be_bytes().as_slice())?;
                }
                None => {
                    data.write_all(&u8::to_be_bytes(0))?;
                    // Write an empty byte array.
                    data.write_all([0_u8; 8].as_slice())?;
                }
            }
        }

        Ok(())
    }

    // Destructors
    pub fn inner(self) -> &'a [u8] {
        self.0
    }

    /// Gets the offset at the specified index.
    pub fn get_offset(&self, index: usize) -> Result<u64, IndexError> {
        match Self::within_bounds(self.0, index) {
            true => {
                let relative_index = (index * (size_of::<u8>() + size_of::<u64>())) + INDEXES_INDEX;

                let offset =
                    &self.0[relative_index..relative_index + (size_of::<u8>() + size_of::<u64>())];

                let (optional, offset) = offset.split_at(1);

                if u8::from_be_bytes(optional.try_into().unwrap()) == 1 {
                    Ok(u64::from_be_bytes(offset.try_into().unwrap()))
                } else {
                    Err(IndexError::IndexInJoinedIndexNotFound)
                }
            }
            false => Err(IndexError::IndexGivenOutOfBoundsForJoinedIndex),
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
        INDEXES_INDEX + 1 + (n_offsets * (size_of::<u8>() + size_of::<u64>()))
    }

    /// Checks whether an index given is within the specific bounds of this JoinedIndex.
    fn within_bounds(buffer: &[u8], index: usize) -> bool {
        index + INDEXES_INDEX < (buffer.len() - 1)
            && (index + (size_of::<u8>() + size_of::<u64>())) < (buffer.len() - 1)
    }

    /// Iterates through the other joined index's offsets, copying them over to the current
    /// index's offsets if the current ones are not available.
    pub fn copy_filled_from(current: &mut [u8], other: &[u8]) {
        const OFFSET_SIZE: usize = size_of::<u8>() + size_of::<u64>();

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
        u64::from_be_bytes(self.0[TIMESTAMP_INDEX..INDEXES_INDEX].try_into().unwrap())
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
        u64::from_be_bytes(self.0[1..=9].try_into().unwrap())
    }

    /// Check if this value is Some or None.
    pub fn get(&self) -> Option<u64> {
        match self.present() {
            true => Some(self.get_unchecked()),
            false => None,
        }
    }
}
