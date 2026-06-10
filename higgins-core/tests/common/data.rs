use crate::common::query::query_latest;
use arrow::array::RecordBatch;
use higgins_shared::{PartitionName, read_arrow};

pub fn query_latest_arrow(
    stream: &[u8],
    partition: &PartitionName,
    socket: &mut std::net::TcpStream,
) -> Option<RecordBatch> {
    let result = query_latest(stream, partition, socket).ok()?;

    let result = result.first()?;

    let arrow_reader = read_arrow(&result.data).next()?.ok()?;

    Some(arrow_reader)
}

pub fn assert_customer_data(arrow: RecordBatch) {
    assert_eq!(
        arrow
            .column_by_name("age")?
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()?
            .iter()
            .next()??,
        21
    );

    assert_eq!(
        arrow
            .column_by_name("first_name")?
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()?
            .iter()
            .next()??,
        "John"
    );

    assert_eq!(
        arrow
            .column_by_name("id")?
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()?
            .iter()
            .next()??,
        "1"
    );

    assert_eq!(
        arrow
            .column_by_name("last_name")?
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()?
            .iter()
            .next()??,
        "Doe"
    );
}
