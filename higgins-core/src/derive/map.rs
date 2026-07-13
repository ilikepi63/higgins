use super::utils::ColumnName;
use crate::broker::Broker;
use crate::derive::operation::OperationData;
use crate::{
    derive::utils::{get_partition_key_from_record_batch, put_default_index_at_range},
    functions::map::run_map_function,
};
use higgins_shared::{HigginsError, PartitionName};

#[derive(Debug)]
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
                    record_batch,
                    &ColumnName::try_from(&self.0.definition)?,
                )?;

                let function_name = match self.0.definition.function_name.as_ref() {
                    Some(fn_name) => fn_name,
                    None => {
                        continue;
                    }
                };

                let engine = &broker_lock.wasm_engine;
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

                tracing::trace!("[MAP] We have fetched the module.");

                let mapped_record_batch = run_map_function(record_batch, engine, module)?;

                tracing::trace!("[MAP] Result from mapping: {:#?}", mapped_record_batch);

                tracing::trace!("[MAP] Producing to the stream..");

                {
                    let stream = self.0.stream.to_string();
                    let partition = &PartitionName::try_from(&partition_val[..])?;

                    let backing_store = broker_lock
                        .backing_store
                        .as_ref()
                        .ok_or(HigginsError::ObjectStoreNotConfigured)?
                        .clone();
                    // CREATE REFERENCE
                    let reference = Broker::put_data_store(
                        backing_store,
                        stream,
                        partition,
                        mapped_record_batch,
                    )
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

                // if let Some(subscription) = self.0.subscription.as_mut() {
                //     let mut lock = subscription.write().await;
                //     lock.acknowledge(&self.0.partition, &offsets)?;

                //     drop(lock);
                // }

                Ok(())
            }
            None => Err(HigginsError::Arbitrary("No References found".to_string())),
        }
    }
}
