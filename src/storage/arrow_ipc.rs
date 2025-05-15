//! Implementation of the Arrow IPC format for writing
//! Arrow RecordBatches to an array of bytes.

use arrow::{
    array::RecordBatch,
    ipc::{
        MetadataVersion,
        reader::{FileReader, StreamReader},
        writer::{FileWriter, IpcWriteOptions, StreamWriter},
    },
};
use bytes::BytesMut;

pub fn write_arrow(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();

    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();

    writer.write(&batch).unwrap();

    writer.finish().unwrap();

    buf
}

pub fn read_arrow(bytes: &[u8]) -> StreamReader<&[u8]> {
    let projection = None; // read all columns
    let reader = StreamReader::try_new(bytes, projection).unwrap();
    reader
}
