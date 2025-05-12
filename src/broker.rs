use std::{collections::BTreeMap, marker::PhantomData};

use arrow::array::RecordBatch;

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

/// This is a pretty naive implementation of what the broker might look like.
pub struct Broker {
    streams: BTreeMap<String, (Sender, Receiver)>, //tokio::sync::broadcast::Receiver<RecordBatch>>,
    listeners: BTreeMap<String, Vec<Sender>>,
}

impl Broker {
    pub async fn produce(&self, stream_name: &str, record: RecordBatch) {
        let (_stream_id, (tx, _rx)) = self
            .streams
            .iter()
            .find(|(id, _)| *id == stream_name)
            .unwrap();

        tx.send(record).unwrap();
    }

    pub fn get_receiver(&self, stream_name: &str) -> Option<Receiver> {
        self.streams
            .iter()
            .find(|(id, _)| *id == stream_name)
            .map(|(_, (tx, _rx))| tx.subscribe())
    }
}

trait ReductionFnTypeSig: Fn(&Option<RecordBatch>, &RecordBatch) -> RecordBatch {}

pub struct ReduceFunction<F: ReductionFnTypeSig>(PhantomData<F>);

impl<F: ReductionFnTypeSig + Send + 'static> ReduceFunction<F> {
    /// Create a new instance of a reduction function.
    pub fn new(func: F, mut rx: Receiver, tx: Sender) {
        tokio::spawn(async move {
            let last_value: Option<RecordBatch> = None;

            while let Ok(value) = rx.recv().await {
                let result = func(&last_value, &value);

                if let Err(e) = tx.send(result) {
                    tracing::error!("Error sending result: {:#?}", e);
                };
            }
        });
    }
}
