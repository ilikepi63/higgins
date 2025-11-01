use crate::common::query::query_latest;
use arrow::array::RecordBatch;
use higgins::storage::arrow_ipc::read_arrow;

pub fn query_latest_arrow(
    stream: &[u8],
    partition: &[u8],
    socket: &mut std::net::TcpStream,
) -> Option<RecordBatch> {
    let result = query_latest(stream, partition, socket).ok()?;

    let result = result.iter().nth(0)?;

    let arrow_reader = read_arrow(&result.data).nth(0)?.ok()?;

    Some(arrow_reader)
}
