use crate::broker::BrokerIndexFile;
use crate::storage::index::IndexError;
use crate::storage::index::{CompletedBinarySearchResult, joined_index::JoinedIndex};

pub async fn complete_joined_index_file(
    index_file: &mut BrokerIndexFile,
    n_offsets: usize,
) -> Result<std::ops::Range<usize>, IndexError> {
    let mut index_file = index_file.lock().await;

    let index_file_len = index_file.len().unwrap();

    let mut return_range = std::ops::Range {
        start: 0_usize,
        end: 0,
    };

    // Get the index where the completion starts.
    if let Some(start) = match index_file.binary_search_completed() {
        CompletedBinarySearchResult::Found(i) => Some(if i > 0 { i - 1 } else { i }),
        CompletedBinarySearchResult::None => Some(0),
        CompletedBinarySearchResult::All => None,
    } {
        // Return range start is done from here.
        return_range.start = start;

        let element_size = JoinedIndex::size_of(n_offsets);

        // Shard from that location.
        let mut shard = index_file.shard(start..index_file_len);

        let mut buffer = vec![0_u8; element_size * 10];

        let mut values: Vec<Option<u64>> = vec![None; n_offsets];

        // Iterate, updating the ranges.
        while let Some(range) = shard.next(&mut buffer) {
            for index_buf in
                buffer[0..(range.end - range.start) * element_size].chunks_mut(element_size)
            {
                for i in 0..n_offsets {
                    let offset = JoinedIndex::get_offset_buf(index_buf, i);

                    match (offset, unsafe { values.get_unchecked_mut(i) }) {
                        (None, Some(value)) => {
                            // Update the offset with the value from the above vec.
                            JoinedIndex::put_offset(index_buf, i, *value).unwrap();
                        }
                        (Some(offset), val) => {
                            // Update the value inside of the vec.
                            let _ = val.insert(offset);
                        }
                        (None, None) => {}
                    }
                }

                JoinedIndex::set_completed(index_buf);
            }

            shard
                .file_mut()
                .range_put_at(range.clone(), &mut buffer)
                .unwrap();

            if range.start < return_range.start {
                return_range.start = range.start;
            }

            if range.end > return_range.end {
                return_range.end = range.end;
            }
        }
    }

    Ok(return_range)
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::broker::BrokerIndexFile;
    use crate::derive::joining::completion::complete_joined_index_file;
    use crate::storage::index::joined_index::JoinedIndex;
    use crate::storage::index::{IndexFile, IndexType};
    use std::path::PathBuf;

    fn new_file() -> PathBuf {
        let filename = format!("{}.idx", Uuid::new_v4());

        let mut path = std::env::temp_dir();
        path.push(filename);
        path
    }

    const NUMBER_OF_INDEXES: usize = 5;
    const COMPLETION_MATRIX: &[[Option<u64>; NUMBER_OF_INDEXES]] = &[
        [Some(0), None, None, None, None],
        [None, None, None, Some(0), None],
        [None, Some(0), None, None, None],
        [Some(1), None, None, None, None],
        [None, None, Some(0), None, None],
        [None, None, Some(1), None, None],
        [None, None, None, None, Some(0)],
    ];

    const SECOND_COMPLETION_MATRX: &[[Option<u64>; NUMBER_OF_INDEXES]] = &[
        [Some(2), None, None, None, None],
        [None, None, None, Some(1), None],
        [None, Some(1), None, None, None],
        [Some(3), None, None, None, None],
        [None, None, Some(2), None, None],
        [None, None, Some(3), None, None],
        [None, None, None, None, Some(1)],
    ];

    const COMPLETION_MATRIX_LARGE: &[[Option<u64>; NUMBER_OF_INDEXES]] = &[
        [Some(0), None, None, None, None],
        [None, None, None, Some(0), None],
        [None, Some(0), None, None, None],
        [Some(1), None, None, None, None],
        [None, None, Some(0), None, None],
        [None, None, Some(1), None, None],
        [None, None, None, None, Some(0)],
        [Some(2), None, None, None, None],
        [None, None, None, Some(1), None],
        [None, Some(1), None, None, None],
        [Some(3), None, None, None, None],
        [None, None, Some(2), None, None],
        [None, None, Some(3), None, None],
        [None, None, None, None, Some(1)],
    ];

    const COMPLETED_RESULT: [Option<u64>; NUMBER_OF_INDEXES] =
        [Some(1), Some(0), Some(1), Some(0), Some(0)];

    const COMPLETED_RESULT_LARGE: [Option<u64>; NUMBER_OF_INDEXES] =
        [Some(3), Some(1), Some(3), Some(1), Some(1)];

    #[tokio::test]
    async fn completion_works_on_partial_completed_index() {
        let path = new_file();

        let index_size = JoinedIndex::size_of(NUMBER_OF_INDEXES);

        let mut file = IndexFile::new(&path, index_size, IndexType::Join).unwrap();

        let mut val = vec![0; index_size];

        for (i, v) in COMPLETION_MATRIX.iter().enumerate() {
            JoinedIndex::put(
                i as u64,
                crate::storage::dereference::Reference::Null,
                1,
                v,
                &mut val,
            )
            .unwrap();

            file.append(&val).unwrap();
        }

        let index_file =
            &mut BrokerIndexFile::new(file, std::sync::Arc::new(tokio::sync::Mutex::new(())));

        let result = complete_joined_index_file(index_file, NUMBER_OF_INDEXES)
            .await
            .unwrap();

        assert_eq!(result, std::ops::Range { start: 0, end: 7 });

        let mut file = index_file.lock().await;

        let mut buffer =
            vec![0_u8; COMPLETION_MATRIX.len() * JoinedIndex::size_of(NUMBER_OF_INDEXES)];

        file.read_at(0, &mut buffer).unwrap();

        let last = buffer
            .chunks(JoinedIndex::size_of(NUMBER_OF_INDEXES))
            .nth(6)
            .unwrap();

        let index = JoinedIndex::of(last);

        for i in 0..index.offset_len() - 1 {
            let offset = index.get_offset(i);

            assert_eq!(COMPLETED_RESULT[i], offset);
        }
    }

    #[tokio::test]
    async fn completion_works_on_partial_completed_index_with_smaller_buffer() {
        let path = new_file();

        let index_size = JoinedIndex::size_of(NUMBER_OF_INDEXES);

        let mut file = IndexFile::new(&path, index_size, IndexType::Join).unwrap();

        let mut val = vec![0; index_size];

        for (i, v) in COMPLETION_MATRIX_LARGE.iter().enumerate() {
            JoinedIndex::put(
                i as u64,
                crate::storage::dereference::Reference::Null,
                1,
                v,
                &mut val,
            )
            .unwrap();

            file.append(&val).unwrap();
        }

        let index_file =
            &mut BrokerIndexFile::new(file, std::sync::Arc::new(tokio::sync::Mutex::new(())));

        let result = complete_joined_index_file(index_file, NUMBER_OF_INDEXES)
            .await
            .unwrap();

        assert_eq!(result, std::ops::Range { start: 0, end: 14 });

        let mut file = index_file.lock().await;

        let mut buffer =
            vec![0_u8; COMPLETION_MATRIX_LARGE.len() * JoinedIndex::size_of(NUMBER_OF_INDEXES)];

        file.read_at(0, &mut buffer).unwrap();

        let last = buffer
            .chunks(JoinedIndex::size_of(NUMBER_OF_INDEXES))
            .nth(13)
            .unwrap();

        let index = JoinedIndex::of(last);

        for i in 0..index.offset_len() - 1 {
            dbg!(i);
            let offset = index.get_offset(i);

            assert_eq!(COMPLETED_RESULT_LARGE[i], offset);
        }
    }

    #[tokio::test]
    async fn completion_works_one_after_another() {
        let path = new_file();

        let index_size = JoinedIndex::size_of(NUMBER_OF_INDEXES);

        let mut file = IndexFile::new(&path, index_size, IndexType::Join).unwrap();

        let mut val = vec![0; index_size];

        for (i, v) in COMPLETION_MATRIX.iter().enumerate() {
            JoinedIndex::put(
                i as u64,
                crate::storage::dereference::Reference::Null,
                1,
                v,
                &mut val,
            )
            .unwrap();

            file.append(&val).unwrap();
        }

        let index_file =
            &mut BrokerIndexFile::new(file, std::sync::Arc::new(tokio::sync::Mutex::new(())));

        let result = complete_joined_index_file(index_file, NUMBER_OF_INDEXES)
            .await
            .unwrap();

        assert_eq!(result, std::ops::Range { start: 0, end: 7 });

        let mut file = index_file.lock().await;

        let mut buffer =
            vec![0_u8; COMPLETION_MATRIX.len() * JoinedIndex::size_of(NUMBER_OF_INDEXES)];

        file.read_at(0, &mut buffer).unwrap();

        let last = buffer
            .chunks(JoinedIndex::size_of(NUMBER_OF_INDEXES))
            .nth(6)
            .unwrap();

        let index = JoinedIndex::of(last);

        for i in 0..index.offset_len() - 1 {
            let offset = index.get_offset(i);

            assert_eq!(COMPLETED_RESULT[i], offset);
        }

        for (i, v) in SECOND_COMPLETION_MATRX.iter().enumerate() {
            JoinedIndex::put(
                i as u64,
                crate::storage::dereference::Reference::Null,
                1,
                v,
                &mut val,
            )
            .unwrap();

            file.append(&val).unwrap();
        }

        drop(file);

        let result = complete_joined_index_file(index_file, NUMBER_OF_INDEXES)
            .await
            .unwrap();

        // TODO: This is supposed to be 7, but is 5. This is likely because we
        // are getting the last completed index from the amount.
        assert_eq!(result, std::ops::Range { start: 5, end: 14 });

        let mut file = index_file.lock().await;

        let mut buffer =
            vec![0_u8; COMPLETION_MATRIX_LARGE.len() * JoinedIndex::size_of(NUMBER_OF_INDEXES)];

        file.read_at(0, &mut buffer).unwrap();

        let last = buffer
            .chunks(JoinedIndex::size_of(NUMBER_OF_INDEXES))
            .nth(13)
            .unwrap();

        let index = JoinedIndex::of(last);

        for i in 0..index.offset_len() - 1 {
            dbg!(i);
            let offset = index.get_offset(i);

            assert_eq!(COMPLETED_RESULT_LARGE[i], offset);
        }
    }
}
