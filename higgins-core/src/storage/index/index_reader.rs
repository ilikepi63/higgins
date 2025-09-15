use super::IndexError;

use bytes::BytesMut;
use std::{fs::File as StdFile, io::ErrorKind, os::unix::fs::FileExt, sync::Arc};
use tokio::task::spawn_blocking;
use tracing::error;

use super::{INDEX_SIZE, IndexesMut};

fn file_size(path: &str) -> Result<u32, IndexError> {
    Ok(std::fs::metadata(path)
        .map(|metadata| metadata.len())?
        .try_into()?)
}

async fn read_at(file: Arc<StdFile>, offset: u32, len: u32) -> Result<BytesMut, std::io::Error> {
    let file = file.clone();
    spawn_blocking(move || {
        let mut buf = BytesMut::with_capacity(len as usize);
        unsafe { buf.set_len(len as usize) };
        file.read_exact_at(&mut buf, offset as u64)?;
        Ok(buf)
    })
    .await?
}

pub async fn load_all_indexes_from_disk(
    file_path: String,
    file: Arc<StdFile>,
) -> Result<IndexesMut, Box<dyn std::error::Error>> {
    let file_size = file_size(&file_path)?;
    if file_size == 0 {
        return Ok(IndexesMut::empty());
    }

    let buf = match read_at(file, 0, file_size).await {
        Ok(buf) => buf,
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
            return Ok(IndexesMut::empty());
        }
        Err(error) => {
            error!(
                "Error reading batch header at offset 0 in file {}: {error}",
                file_path
            );
            return Err(Box::new(error));
        }
    };
    let index_count = file_size / INDEX_SIZE as u32;
    let indexes = IndexesMut::from_bytes(buf);
    if indexes.count() != index_count {
        error!(
            "Loaded {} indexes from disk, expected {}, file {} is probably corrupted!",
            indexes.count(),
            index_count,
            file_path
        );
    }

    Ok(indexes)
}
