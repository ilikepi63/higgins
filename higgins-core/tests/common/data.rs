use crate::common::query::query_latest;
use arrow::array::RecordBatch;
use higgins::storage::arrow_ipc::read_arrow;

#[allow(unused)]
pub fn query_latest_arrow(
    stream: &[u8],
    partition: PartitionName,
    socket: &mut std::net::TcpStream,
) -> Option<RecordBatch> {
    let result = query_latest(stream, partition, socket).ok()?;

    let result = result.first()?;

    let arrow_reader = read_arrow(&result.data).next()?.ok()?;

    Some(arrow_reader)
}
