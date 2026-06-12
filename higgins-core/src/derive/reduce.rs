use super::utils::put_default_index_at_range;
use crate::derive::operation::OperationData;
use crate::functions::reduce::run_reduce_function;
use higgins_shared::{HigginsError, read_arrow};

pub struct ReduceOperation(pub OperationData);

impl ReduceOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        // tracing::debug!("Retrieved {} records for reduction.", self.records.len());

        let offsets = self.0.offsets.get().await?;

        self.0.offsets_setter.set(offsets.clone()).await;
        // In order to begin the reduction for these records, we need to
        // retrieve the first record's previous record.
        let mut prev_record = match offsets.start {
            0 => None,
            _ => {
                let mut broker_guard = self.0.broker.write().await;
                broker_guard
                    .get_at(
                        &self.0.stream,
                        &self.0.partition,
                        offsets.start - 1, // TODO: This should be impossible to fail as the invariant forces > 0, perhaps there is a better technique to be used here
                    )
                    .await
                    .inspect_err(|err| {
                        tracing::error!("Failed to retrieve offset with error: {:#?}", err)
                    })
                    .ok()
                    .flatten()
                    .map(|arrow_bytes| {
                        // tracing::trace!("bytes: {:#?}", arrow_bytes);
                        let mut batches = read_arrow(&arrow_bytes).ok()?;
                        tracing::trace!("batches: {:#?}", batches);
                        batches
                            .next()
                            .inspect(|val| {
                                tracing::trace!(
                                    "Correctly retrieved a value from the batches: {:#?}",
                                    val
                                );
                            })
                            .and_then(|result| result.ok())
                    })
            }
        }
        .flatten();

        let mut references = vec![];

        let records = self.0.records.get().await?;
        self.0.records_setter.set(records.clone()).await;

        for batch in records.iter() {
            tracing::trace!("[REDUCE] Awaiting the broker lock..");

            let broker_lock = self.0.broker.write().await;

            tracing::trace!("[REDUCE] We are reading the stream values in..");

            tracing::debug!("Retrieved current value: {:#?}", batch);
            tracing::debug!("Previous value: {:#?}", prev_record);

            match prev_record.as_ref() {
                Some(prev_record) => {
                    tracing::info!("Using previous record..");

                    let function_name = match self.0.definition.function_name.as_ref() {
                        Some(fn_name) => fn_name,
                        None => {
                            continue;
                        }
                    };

                    let module = match broker_lock
                        .wasm_modules
                        .iter()
                        .find(|(n, _)| n == function_name)
                        .map(|(_, m)| m)
                    {
                        Some(module) => module,
                        None => {
                            continue;
                        }
                    };

                    tracing::trace!("Applying the function..");

                    let reduced_record_batch =
                        run_reduce_function(batch, prev_record, &broker_lock.wasm_engine, module)?;

                    tracing::trace!("Reduced Record batch: {:#?}", reduced_record_batch);

                    {
                        // CREATE REFERENCE
                        let reference = broker_lock
                            .put_data_store(
                                self.0.stream.to_string(),
                                &self.0.partition,
                                reduced_record_batch,
                            )
                            .await?;

                        references.push(reference);
                    }
                }
                None => {
                    tracing::trace!(
                        "No previous index found. Producing to stream {} key {} ",
                        self.0.stream.to_string(),
                        &self.0.partition.to_string().unwrap_or("NO_KEY".to_string())
                    );

                    // CREATE REFERENCE
                    let reference = broker_lock
                        .put_data_store(self.0.stream.to_string(), &self.0.partition, batch.clone())
                        .await?;

                    references.push(reference);
                }
            }

            tracing::trace!("Setting previous record to current value.");
            prev_record = Some(batch.clone());
        }

        self.0.references = Some(references);

        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        tracing::trace!("Writing the values.");

        if let Some(references) = self.0.references.as_ref() {
            let offsets = self.0.offsets.get().await?;

            {
                let mut broker_guard = self.0.broker.write().await;

                tracing::trace!("Writing the offsets.");

                put_default_index_at_range(
                    self.0.stream.clone(),
                    &self.0.partition,
                    offsets.clone(),
                    &mut broker_guard,
                    references,
                )
                .await?;
            }
            tracing::trace!(
                "Wrote the offsets to {:#?}. References: {:#?}",
                offsets,
                references
            );

            if let Some(subscription) = self.0.subscription.as_ref() {
                let mut lock = subscription.write().await;

                lock.acknowledge(&self.0.partition, &offsets)?;
            }
        } else {
            tracing::error!("Attempt to commit without any referencs on Reduce stream.")
        }

        Ok(())
    }
}
