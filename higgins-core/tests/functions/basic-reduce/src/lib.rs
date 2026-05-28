use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::datatypes::{Field, Int32Type};
use arrow::record_batch::RecordBatch;
use higgins_functions::ArbitraryLengthBuffer;
use std::sync::Arc;

use log::{Level, Metadata, Record};

const ERROR_MSG_SIZE: usize = 1000;

static mut ERRORS: [[u8; ERROR_MSG_SIZE]; 10] = [[0_u8; ERROR_MSG_SIZE]; 10];
static mut COUNTER: usize = 0;

struct SimpleLogger;

#[unsafe(no_mangle)]
pub unsafe fn get_errors() -> *const [u8; ERROR_MSG_SIZE] {
    unsafe {
        #[allow(static_mut_refs)]
        ERRORS.as_ptr()
    }
}

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            log_error(&format!(
                "Line {}:{} - {}",
                record.line().unwrap_or(0),
                record.level(),
                record.args()
            ));
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

fn to_batch(rb_ptr: *const u8) -> RecordBatch {
    let buffer: Vec<u8> = ArbitraryLengthBuffer::from(rb_ptr).into_inner();

    let record_batch = read_arrow(&buffer).nth(0).unwrap().unwrap();

    record_batch
}

fn reduce(record_batch: &RecordBatch, prev_record_batch: &RecordBatch) -> RecordBatch {
    let col = col_name_to_field_and_col(&record_batch, "data");
    let prev_col = col_name_to_field_and_col(&prev_record_batch, "data");

    // Cast to primitive type.
    let curr_col = col.0.as_primitive::<Int32Type>();
    let prev_col = prev_col.0.as_primitive::<Int32Type>();

    let arr = {
        let mut result = vec![];

        for index in 0..curr_col.len() {
            let curr_val = curr_col.value(index);
            let prev_val = prev_col.value(index);

            result.push(curr_val + prev_val);
        }

        Int32Array::from(result)
    };

    let batch = RecordBatch::try_new(
        record_batch.schema(),
        vec![
            col_name_to_field_and_col(&record_batch, "id").0,
            Arc::new(arr),
        ],
    )
    .unwrap();

    batch
}

#[unsafe(no_mangle)]
pub unsafe fn _malloc(len: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(len.try_into().unwrap());
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

#[unsafe(no_mangle)]
pub unsafe fn run(prev_rb_ptr: *const u8, rb_ptr: *const u8) -> *const u8 {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Trace));

    log::info!("Calling");

    // Retrieve record batch from FFI ptr.
    let record_batch = to_batch(rb_ptr);
    let prev_record_batch = to_batch(prev_rb_ptr);
    log::info!("Current Record Batch: {:#?}", record_batch);
    log::info!("Previous Record Batch: {:#?}", prev_record_batch);

    let batch = reduce(&record_batch, &prev_record_batch);

    log::info!("Resultant Record Batch: {:#?}", batch);

    let result = write_arrow(&batch);

    let buffer: Vec<u8> = ArbitraryLengthBuffer::from(result.as_ref()).into_inner();

    let ptr = buffer.as_ptr();

    buffer.leak();

    ptr as *const u8
}

pub fn col_name_to_field_and_col(batch: &RecordBatch, col_name: &str) -> (ArrayRef, Field) {
    let schema = batch.schema();

    let schema_index = schema.index_of(col_name).unwrap();

    let col = batch.column(schema_index);
    let field = schema.field(schema_index);

    (col.clone(), field.clone())
}

use arrow::ipc::{reader::StreamReader, writer::StreamWriter};

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

#[cfg(test)]
mod test {

    use super::*;

    static CURR_RECOR: &[u8] = &[
        255, 255, 255, 255, 184, 0, 0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 12, 0, 10, 0, 9, 0, 4, 0, 10,
        0, 0, 0, 16, 0, 0, 0, 0, 1, 4, 0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 2, 0, 0,
        0, 80, 0, 0, 0, 4, 0, 0, 0, 200, 255, 255, 255, 24, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 2, 28,
        0, 0, 0, 8, 0, 12, 0, 4, 0, 11, 0, 8, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0,
        0, 0, 100, 97, 116, 97, 0, 0, 0, 0, 16, 0, 20, 0, 16, 0, 0, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16,
        0, 0, 0, 24, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 5, 16, 0, 0, 0, 0, 0, 0, 0, 4, 0, 4, 0, 4, 0,
        0, 0, 2, 0, 0, 0, 105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255,
        255, 255, 248, 0, 0, 0, 16, 0, 0, 0, 12, 0, 26, 0, 24, 0, 23, 0, 4, 0, 8, 0, 12, 0, 0, 0,
        32, 0, 0, 0, 64, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 0, 10, 0, 24, 0, 12, 0, 8,
        0, 4, 0, 10, 0, 0, 0, 60, 0, 0, 0, 16, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0,
        0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 64, 0,
        0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        192, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 49, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0,
    ];

    static PREV_RECORD: &[u8] = &[
        255, 255, 255, 255, 184, 0, 0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 12, 0, 10, 0, 9, 0, 4, 0, 10,
        0, 0, 0, 16, 0, 0, 0, 0, 1, 4, 0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 2, 0, 0,
        0, 80, 0, 0, 0, 4, 0, 0, 0, 200, 255, 255, 255, 24, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 2, 28,
        0, 0, 0, 8, 0, 12, 0, 4, 0, 11, 0, 8, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0,
        0, 0, 100, 97, 116, 97, 0, 0, 0, 0, 16, 0, 20, 0, 16, 0, 0, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16,
        0, 0, 0, 24, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 5, 16, 0, 0, 0, 0, 0, 0, 0, 4, 0, 4, 0, 4, 0,
        0, 0, 2, 0, 0, 0, 105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255,
        255, 255, 248, 0, 0, 0, 16, 0, 0, 0, 12, 0, 26, 0, 24, 0, 23, 0, 4, 0, 8, 0, 12, 0, 0, 0,
        32, 0, 0, 0, 64, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 0, 10, 0, 24, 0, 12, 0, 8,
        0, 4, 0, 10, 0, 0, 0, 60, 0, 0, 0, 16, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0,
        0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 64, 0,
        0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        192, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 49, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0,
    ];

    #[test]
    pub fn reduce_function_test() {
        let record_batch = read_arrow(&CURR_RECOR).nth(0).unwrap().unwrap();

        let prev_record_batch = read_arrow(&PREV_RECORD).nth(0).unwrap().unwrap();

        let batch = reduce(&record_batch, &prev_record_batch);

        assert!(
            batch
                .column_by_name("data")
                .unwrap()
                .as_primitive::<Int32Type>()
                .iter()
                .next()
                .unwrap()
                .unwrap()
                == 2
        );
    }
}
