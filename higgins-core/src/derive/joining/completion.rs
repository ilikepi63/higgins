use crate::storage::index::IndexError;
use crate::storage::index::joined_index::JoinedIndex;

/// Completes a given index from another completed given index.
pub fn complete_from(
    index: &mut [u8],
    completed_index: &[u8],
    n_offsets: usize,
) -> Result<(), IndexError> {
    if !JoinedIndex::of(completed_index).completed() {
        return Err(IndexError::Unknown);
    }

    for i in 0..n_offsets {
        let offset = JoinedIndex::get_offset_buf(index, i);
        let completed_offset = JoinedIndex::get_offset_buf(completed_index, i);

        match (offset, completed_offset) {
            (None, Some(value)) => {
                // Update the offset with the value from the above vec.
                JoinedIndex::put_offset(index, i, value).unwrap();
            }
            _ => {
                // no op -> leave the current joined index as is.
            }
        }
    }

    JoinedIndex::set_completed(index);

    Ok(())
}
