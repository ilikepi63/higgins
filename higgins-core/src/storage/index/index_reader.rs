/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/***
 * THIS IS NOT MY WORK -> THIS IS BORROWED FROM THE APACHE IGGY PROJECT.
 */

use super::IndexError;

use bytes::BytesMut;
use std::{
    fs::File as StdFile,
    io::ErrorKind,
    os::unix::fs::FileExt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::fs::OpenOptions;
use tokio::task::spawn_blocking;
use tracing::{error, trace};

use super::{INDEX_SIZE, Index, IndexView, IndexesMut};

/// A dedicated struct for reading from the index file.
#[derive(Debug)]
pub struct IndexReader {
    file_path: String,
    file: Arc<StdFile>,
    index_size_bytes: Arc<AtomicU64>,
}

impl IndexReader {
    /// Opens the index file in read-only mode.
    pub async fn new(
        file_path: &str,
        index_size_bytes: Arc<AtomicU64>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file = OpenOptions::new().read(true).open(file_path).await?;

        trace!(
            "Opened index file for reading: {file_path}, size: {}",
            index_size_bytes.load(Ordering::Acquire)
        );
        Ok(Self {
            file_path: file_path.to_string(),
            file: Arc::new(file.into_std().await),
            index_size_bytes,
        })
    }

    /// Loads all indexes from the index file into the optimized binary format.
    /// Note that this function does not use the pool, as the messages are not cached.
    /// This is expected - this method is called at startup and we want to preserve
    /// memory pool usage.

    /// Returns the size of the index file in bytes.
    fn file_size(&self) -> u32 {
        self.index_size_bytes.load(Ordering::Acquire) as u32
    }
}

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
    let indexes = IndexesMut::from_bytes(buf, 0);
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
