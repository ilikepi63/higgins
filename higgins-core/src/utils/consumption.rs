use higgins_codec::GetIndexResponse;
use riskless::messages::ConsumeResponse;
use tokio::sync::mpsc::Receiver;
use higgins_codec::Record;

use crate::storage::arrow_ipc::read_arrow;
pub async fn collect_consume_responses(
    mut consumption: Receiver<ConsumeResponse>,
) -> Vec<GetIndexResponse> {
    let mut return_vec = vec![];

    while let Some(val) = consumption.recv().await {
        let resp = GetIndexResponse {
            records: val
                .batches
                .iter()
                .map(|batch| {
                    let stream_reader = read_arrow(&batch.data);

                    let batches = stream_reader.filter_map(|val| val.ok()).collect::<Vec<_>>();

                    let batch_refs = batches.iter().collect::<Vec<_>>();

                    // Infer the batches
                    let buf = Vec::new();
                    let mut writer = arrow_json::LineDelimitedWriter::new(buf);
                    writer.write_batches(&batch_refs).unwrap();
                    writer.finish().unwrap();

                    // Get the underlying buffer back,
                    let buf = writer.into_inner();

                    Record {
                        data: buf,
                        stream: batch.topic.as_bytes().to_vec(),
                        offset: batch.offset,
                        partition: batch.partition.clone(),
                    }
                })
                .collect::<Vec<_>>(),
        };

        return_vec.push(resp);
    }

    return_vec
}
