//! Implementation of the Arrow IPC format for writing
//! Arrow RecordBatches to an array of bytes.

use arrow::{
    array::RecordBatch,
    ipc::{reader::StreamReader, writer::StreamWriter},
};

use crate::HigginsError;

pub fn write_arrow(batch: &RecordBatch) -> Result<Vec<u8>, HigginsError> {
    let mut buf = Vec::new();

    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;

    writer.write(batch)?;

    writer.finish()?;

    Ok(buf)
}

pub fn read_arrow(bytes: &[u8]) -> Result<StreamReader<&[u8]>, HigginsError> {
    let projection = None; // read all columns

    Ok(StreamReader::try_new(bytes, projection)?)
}
