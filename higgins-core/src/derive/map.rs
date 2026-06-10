use super::utils::ColumnName;
use crate::derive::operation::OperationData;
use crate::{
    derive::utils::{get_partition_key_from_record_batch, put_default_index_at_range},
    functions::map::run_map_function,
};
use higgins_shared::{HigginsError, PartitionName};

pub struct MapOperation(pub OperationData);

impl MapOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        tracing::trace!("[MAP] Retrieved records: {:#?}", self.0.records);

        let mut references = vec![];

        let records = self.0.records.get().await?;

        tracing::debug!("[MAP] Received records: {:#?}", records);

        for record_batch in records.iter() {
            tracing::trace!("[MAP] Awaiting the broker lock..");

            let broker_lock = self.0.broker.write().await;

            tracing::trace!("[MAP] We are reading the stream values in..");

            for _ in 0..record_batch.num_rows() {
                let partition_val = get_partition_key_from_record_batch(
                    &record_batch,
                    &ColumnName::from(&self.0.definition),
                );

                let engine = &broker_lock.wasm_engine;
                let module = broker_lock
                    .wasm_modules
                    .iter()
                    .find(|(n, _)| n == self.0.definition.function_name.as_ref()?)
                    .map(|(_, m)| m)?;

                tracing::trace!("[MAP] We have fetched the module.");

                let mapped_record_batch = run_map_function(&record_batch, engine, module)?;

                tracing::trace!("[MAP] Result from mapping: {:#?}", mapped_record_batch);

                tracing::trace!("[MAP] Producing to the stream..");

                {
                    let stream = self.0.stream.to_string();
                    let partition = &PartitionName::try_from(&partition_val[..])?;

                    // CREATE REFERENCE
                    let reference = broker_lock
                        .put_data_store(stream.clone(), partition, mapped_record_batch)
                        .await?;

                    references.push(reference);
                }
            }

            drop(broker_lock);
        }

        self.0.references = Some(references);

        Ok(())

        // PREPARE
        // COMMIT
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        match self.0.references.as_ref() {
            Some(references) => {
                let mut broker_guard = self.0.broker.write().await;

                let offsets = self.0.offsets.get().await?;

                put_default_index_at_range(
                    self.0.stream.clone(),
                    &self.0.partition,
                    offsets.clone(),
                    &mut broker_guard,
                    references,
                )
                .await?;

                let mut lock = self.0.subscription.as_mut()?.write().await;

                lock.acknowledge(&self.0.partition, &offsets)?;

                drop(lock);

                Ok(())
            }
            None => Err(HigginsError::Arbitrary("No References found".to_string())),
        }
    }
}
