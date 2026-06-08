use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::Field,
    util::display::array_value_to_string,
};
use higgins_shared::{PartitionName, StreamName};
use std::ops::Range;
use tokio::sync::RwLockWriteGuard;

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
        Self(value.partition_key.to_string().unwrap()) // TODO: Remove this when we enforce stream keys to be strings.
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

/// Helper for putting a set of DefaultIndexes at a range.
pub async fn put_default_index_at_range(
    stream: StreamName,
    partition: &PartitionName,
    offset: Range<u64>,
    broker: &mut RwLockWriteGuard<'_, Broker>,
    references: &[Reference],
) -> Result<(), HigginsError> {
    // References and offsets need to be the same length.
    let offsets_len = (offset.end - offset.start + 1) as usize;
    if offsets_len != references.len() {
        return Err(HigginsError::Unknown);
    }

    let mut index_file = broker.get_index_file(stream.clone(), partition).unwrap();

    let mut index_file_guard = index_file.lock().await;

    tracing::info!(
        "Retrieved indexfile for stream {stream} and partition {:#?}",
        partition
    );

    let mut buf = vec![0_u8; DefaultIndex::size_of() * offsets_len];

    buf.chunks_mut(DefaultIndex::size_of())
        .zip(offset.start..=offset.end)
        .zip(references)
        .map(|((mut chunk, offset), reference)| {
            DefaultIndex::put(
                offset,
                reference.clone(),
                0,
                crate::utils::epoch(),
                0,
                &mut chunk,
            )
        })
        .collect::<Result<Vec<()>, std::io::Error>>()?;

    let offset_start_usize = offset.start as usize;
    let offset_end_usize = offset.end as usize;

    index_file_guard
        .try_range_put_at(
            offset_start_usize..offset_end_usize.saturating_add(1),
            &mut buf,
        )
        .inspect_err(|err| {
            tracing::error!("{:#?}", err);
        })?;

    tracing::debug!("{:#?}", index_file_guard.len());

    Ok(())
}
