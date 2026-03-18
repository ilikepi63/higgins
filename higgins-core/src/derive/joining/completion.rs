use crate::broker::BrokerIndexFile;
use crate::storage::index::{CompletedBinarySearchResult, joined_index::JoinedIndex};

pub async fn complete_joined_index_file(index_file: &mut BrokerIndexFile, n_offsets: usize) {
    let mut index_file = index_file.lock().await;

    let index_file_len = index_file.len().unwrap();

    dbg!(&index_file_len);

    // Get the index where the completion starts.
    if let Some(start) = match index_file.binary_search_completed() {
        CompletedBinarySearchResult::Found(i) => Some(i - 1),
        CompletedBinarySearchResult::None => Some(0),
        CompletedBinarySearchResult::All => None,
    } {
        let element_size = JoinedIndex::size_of(n_offsets);

        // Shard from that location.
        let mut shard = index_file.shard(start..index_file_len);

        let mut buffer = vec![0_u8; element_size * 10];

        let mut values: Vec<Option<u64>> = vec![None; n_offsets];

        // Iterate, updating the ranges.
        while let Some(range) = shard.next(&mut buffer) {
            for index_buf in buffer[range.start * element_size..range.end * element_size]
                .chunks_mut(element_size)
            {
                for i in 0..n_offsets {
                    let offset = JoinedIndex::get_offset_buf(index_buf, i);

                    match (offset, unsafe { values.get_unchecked_mut(i) }) {
                        (None, Some(value)) => {
                            // Update the offset with the value from the above vec.
                            JoinedIndex::put_offset(index_buf, i, value.clone()).unwrap();
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

            shard.file_mut().range_put_at(range, &mut buffer).unwrap();
        }
    }
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

    const COMPLETED_RESULT: [Option<u64>; NUMBER_OF_INDEXES] =
        [Some(1), Some(0), Some(1), Some(0), Some(0)];

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

        complete_joined_index_file(index_file, NUMBER_OF_INDEXES).await;

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
}
