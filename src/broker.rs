use std::{collections::BTreeMap, marker::PhantomData};

use arrow::array::RecordBatch;

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

/// This is a pretty naive implementation of what the broker might look like.
pub struct Broker {
    streams: BTreeMap<String, (Sender, Receiver)>,
}

impl Broker {
    /// Produce a data set onto the named stream.
    pub async fn produce(&self, stream_name: &str, record: RecordBatch) {
        let (_stream_id, (tx, _rx)) = self
            .streams
            .iter()
            .find(|(id, _)| *id == stream_name)
            .unwrap();

        tx.send(record).unwrap();
    }

    /// Retrieve the receiver for a named stream.
    pub fn get_receiver(&self, stream_name: &str) -> Option<Receiver> {
        self.streams
            .iter()
            .find(|(id, _)| *id == stream_name)
            .map(|(_, (tx, _rx))| tx.subscribe())
    }

    /// Create a Stream.
    pub fn create_stream(&mut self, stream_name: &str) {
        self.streams.insert(
            stream_name.to_string(),
            tokio::sync::broadcast::channel(100),
        );
    }

    /// Apply a reduction function to the stream.
    pub fn reduce<F: ReductionFnTypeSig>(
        &mut self,
        stream_name: &str,
        reduced_stream_name: &str,
        func: F,
    ) {
        let current_rx = self.get_receiver(stream_name).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(100);

        ReduceFunction::new(func, current_rx, tx.clone());

        self.streams
            .insert(reduced_stream_name.to_string(), (tx, rx));
    }
}

trait ReductionFnTypeSig:
    Fn(&Option<RecordBatch>, &RecordBatch) -> RecordBatch + Send + 'static
{
}

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
