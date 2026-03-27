use higgins_codec::GetIndexResponse;
use higgins_codec::Record;
use higgins_shared::read_arrow;
use riskless::messages::ConsumeResponse;
use tokio::sync::mpsc::Receiver;

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

                    let data = higgins_shared::write_arrow(batches.iter().next().unwrap());

                    Record {
                        data,
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
