use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema};

use crate::config::{Configuration, schema_to_arrow_schema};

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

/// This is a pretty naive implementation of what the broker might look like.
#[derive(Debug)]
pub struct Broker {
    streams: BTreeMap<String, (Arc<Schema>, Sender, Receiver)>,
}

impl Broker {
    /// Creates a new instance of a Broker.
    pub fn new() -> Self {
        Self {
            streams: BTreeMap::new(),
        }
    }

    /// Create a new instance from a given configuration.
    pub fn from_config(config: Configuration) -> Self {
        let mut broker = Broker::new();

        let schema = config
            .schema
            .iter()
            .map(|(name, schema)| (name.clone(), Arc::new(schema_to_arrow_schema(schema))))
            .collect::<BTreeMap<String, Arc<arrow::datatypes::Schema>>>();

        // Create the non-derived streams first.
        for (stream_name, topic_defintion) in config
            .topics
            .iter()
            .filter(|(_, def)| def.derived.is_none())
        {
            match &topic_defintion.derived {
                Some(_derived_from) => unreachable!(),
                None => {
                    /// Create just normal schema.
                    let schema = schema.get(&topic_defintion.schema).expect(&format!(
                        "No Schema defined for key {}",
                        topic_defintion.schema
                    ));

                    broker.create_stream(&stream_name, schema.clone());
                }
            }
        }

        for (stream_name, topic_defintion) in config
            .topics
            .iter()
            .filter(|(_, def)| def.derived.is_some())
        {
            match &topic_defintion.derived {
                Some(derived_from) => {
                    /// Create just normal schema.
                    let schema = schema.get(&topic_defintion.schema).expect(&format!(
                        "No Schema defined for key {}",
                        topic_defintion.schema
                    ));

                    let topic_type = topic_defintion.fn_type.as_ref().unwrap();

                    match topic_type.as_ref() {
                        "reduce" => {
                            broker.reduce(
                                &derived_from,
                                topic_defintion.schema.as_str(),
                                schema.clone(),
                                |a, b| RecordBatch::new_empty(b.schema()),
                            );
                        }
                        _ => unimplemented!(),
                    }

                    broker.create_stream(&stream_name, schema.clone());
                }
                None => unreachable!(),
            }
        }

        broker
    }

    /// Produce a data set onto the named stream.
    pub async fn produce(&self, stream_name: &str, record: RecordBatch) {
        let (_stream_id, (_, tx, _rx)) = self
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
            .map(|(_, (_, tx, _rx))| tx.subscribe())
    }

    /// Create a Stream.
    pub fn create_stream(&mut self, stream_name: &str, schema: Arc<Schema>) {
        let (tx, rx) = tokio::sync::broadcast::channel(100);

        self.streams
            .insert(stream_name.to_string(), (schema, tx, rx));
    }

    /// Apply a reduction function to the stream.
    pub fn reduce(
        &mut self,
        stream_name: &str,
        reduced_stream_name: &str,
        reduced_stream_schema: Arc<Schema>,
        func: ReductionFn,
    ) {
        let current_rx = self.get_receiver(stream_name).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(100);

        ReduceFunction::new(func, current_rx, tx.clone());

        self.streams.insert(
            reduced_stream_name.to_string(),
            (reduced_stream_schema, tx, rx),
        );
    }
}

pub trait ReductionFnTypeSig:
    Fn(&Option<RecordBatch>, &RecordBatch) -> RecordBatch + Send + 'static
{
}

type ReductionFn = fn(&Option<RecordBatch>, &RecordBatch) -> RecordBatch;

pub struct ReduceFunction;

impl ReduceFunction {
    /// Create a new instance of a reduction function.
    pub fn new(func: ReductionFn, mut rx: Receiver, tx: Sender) {
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
