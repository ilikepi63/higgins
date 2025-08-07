use arrow::array::Array;
use arrow::array::{ArrayRef, AsArray, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use higgins_functions::{FFIRecordBatch, record_batch_from_ffi, record_batch_to_ffi, ArbitraryLengthBuffer};
use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi};

const ERROR_MSG_SIZE: usize = 1000;

static mut ERRORS: [[u8; ERROR_MSG_SIZE]; 10] = [[0_u8; ERROR_MSG_SIZE]; 10];
static mut COUNTER: usize = 0;

use log::{Level, Metadata, Record};

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            log_error(&format!("{} - {}", record.level(), record.args()));
        }
    }

    fn flush(&self) {}
}

use log::{LevelFilter, SetLoggerError};

static LOGGER: SimpleLogger = SimpleLogger;

fn log_error(s: &str) {
    unsafe {
        for (index, byte) in s.as_bytes().iter().enumerate() {
            ERRORS[COUNTER][index] = *byte;
        }
    };
    unsafe {
        COUNTER += 1;
    };
}

#[unsafe(no_mangle)]
pub unsafe fn get_errors() -> *const [u8; ERROR_MSG_SIZE] {
    unsafe {
        #[allow(static_mut_refs)]
        ERRORS.as_ptr()
    }
}

#[unsafe(no_mangle)]
pub unsafe fn _malloc(len: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(len.try_into().unwrap());
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

#[unsafe(no_mangle)]
pub unsafe fn run(rb_ptr: *const u8) -> *const u8 {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Trace));

    // Retrieve record batch from FFI ptr.
    let buffer: Vec<u8> = ArbitraryLengthBuffer::from(rb_ptr).into_inner();

    let record_batch = read_arrow(&buffer).nth(0).unwrap().unwrap(); 

    // log::info!("Resultant Record Batch: {:#?}", record_batch);

    // Retrieve the data col name.
    let col = col_name_to_field_and_col(&record_batch, "data");

    // Cast to primitive type.
    let col = col.0.as_primitive::<Int32Type>();

    let arr = {
        let mut result = vec![];

        for val in col.iter() {
            result.push(val.map(|val| val * 2));
        }

        Int32Array::from(result)
    };

    let batch = RecordBatch::try_new(
        record_batch.schema(),
        vec![
            Arc::new(arr),
            col_name_to_field_and_col(&record_batch, "id").0,
        ],
    )
    .inspect_err(|e| log::error!("Error: {:#?}", e))
    .unwrap();

    // let result = record_batch_to_ffi(batch);
    let result = write_arrow(&batch);

    let buffer: Vec<u8> = ArbitraryLengthBuffer::from(result.as_ref()).into_inner();

    let ptr = buffer.as_ptr();

    buffer.leak();

    ptr as *const u8
}

pub fn col_name_to_field_and_col(batch: &RecordBatch, col_name: &str) -> (ArrayRef, Field) {
    let schema = batch.schema();

    let schema_index = schema.index_of(col_name);

    log::info!("Schema Index: {:#?}", schema_index);

    let schema_index = schema_index.unwrap();

    let col = batch.column(schema_index);
    let field = schema.field(schema_index);

    (col.clone(), field.clone())
}

use arrow::{
    ipc::{reader::StreamReader, writer::StreamWriter},
};

pub fn write_arrow(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();

    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();

    writer.write(batch).unwrap();

    writer.finish().unwrap();

    buf
}

pub fn read_arrow(bytes: &[u8]) -> StreamReader<&[u8]> {
    let projection = None; // read all columns

    StreamReader::try_new(bytes, projection).unwrap()
}
