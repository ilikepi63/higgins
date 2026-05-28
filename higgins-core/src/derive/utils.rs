use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::Field,
    util::display::array_value_to_string,
};
use higgins_shared::PartitionName;
use tokio::sync::{RwLock, RwLockWriteGuard};

/// Helper function to retrieve the field and array given a column name.
#[allow(unused)]
pub fn col_name_to_field_and_col(batch: &RecordBatch, col_name: &str) -> (ArrayRef, Field) {
    tracing::info!("Attempting to retrieve data from RecordBatch: {:#?}", batch);

    let schema = batch.schema();

    let schema_index = schema
        .index_of(col_name)
        .inspect(|err| {
            tracing::error!(
                "Unexpected error not being able to retrieve partition key by name: {:#?}",
                err
            );
        })
        .unwrap();

    let col = batch.column(schema_index);
    let field = schema.field(schema_index);

    (col.clone(), field.clone())
}

/// Represents a column name of an apache arrow record batch.
pub struct ColumnName(String);

impl ColumnName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&StreamDefinition> for ColumnName {
    fn from(value: &StreamDefinition) -> Self {
        Self(String::from_utf8_lossy(value.partition_key.as_bytes()).into_owned()) // TODO: Remove this when we enforce stream keys to be strings.
    }
}

pub fn get_partition_key_from_record_batch(batch: &RecordBatch, col_name: &ColumnName) -> Vec<u8> {
    let schema_index = batch
        .schema()
        .index_of(col_name.as_str())
        .inspect_err(|err| {
            tracing::error!(
                "Unexpected error not being able to retrieve partition key by name: {:#?}",
                err
            );
        })
        .unwrap();

    let col = batch.column(schema_index);

    let value = array_value_to_string(col, 0);

    value.unwrap().as_bytes().to_vec()
}

use std::ops::{DerefMut, Range};

use crate::{
    broker::Broker, error::HigginsError, storage::dereference::Reference,
    topography::StreamDefinition,
};

pub fn iter_buffer(
    range: Range<usize>,
    element_size_in_bytes: usize,
    buffer: &[u8],
) -> std::slice::Chunks<'_, u8> {
    buffer[0..(range.end - range.start) * element_size_in_bytes].chunks(element_size_in_bytes)
}

use crate::storage::index::default::DefaultIndex;

/// Helper for this specific operaion
pub async fn put_default_index_at_range(
    stream: String,
    partition: &PartitionName,
    offset: u64,
    broker: &mut RwLockWriteGuard<'_, Broker>,
    reference: Reference,
) -> Result<(), HigginsError> {
    let mut index_file = broker.get_index_file(stream.clone(), partition).unwrap();

    let mut index_file_guard = index_file.lock().await;

    tracing::info!(
        "Retrieved indexfile for stream {stream} and partition {:#?}",
        partition
    );

    let mut buf = [0_u8; DefaultIndex::size_of()];

    DefaultIndex::put(offset, reference, 0, crate::utils::epoch(), 0, &mut buf)?;

    let offset_usize = offset as usize;

    index_file_guard
        .try_range_put_at(offset_usize..offset_usize.saturating_add(1), &mut buf)
        .inspect_err(|err| {
            tracing::error!("{:#?}", err);
        })?;

    tracing::debug!("{:#?}", index_file_guard.len());

    Ok(())
}
