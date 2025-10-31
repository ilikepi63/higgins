//! Algorithms related to joining two streams.
//!
//! There exists a  set of types of stream joins, akin to SQL joins:
//! - Inner Join        -> emits a value for each corresponding index value for both underlying streams.
//! - Left Outer Join   -> emits a value for every value on the left side of the join, regardless of whether they have a matching key on the alternate stream.
//! - Right Outer Join  -> Similar to Left Outer Join, except on the right side of the join.
//! - Full Join         -> Similar to Right or Left Outer, except will emit for all values.

// TODO: How do we chain multiple streams together?.

use std::sync::Arc;

use tokio::sync::RwLock;

pub mod join;
pub mod mapping;
mod opts;

use crate::{
    broker::Broker,
    client::ClientRef,
    derive::joining::{join::JoinDefinition, opts::create_join_operator},
    error::HigginsError,
};

#[allow(unused)]
pub async fn create_joined_stream_from_definition(
    definition: JoinDefinition,
    _broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    // Instantiate Operator on this definition.
    let _operator = create_join_operator(definition, broker_ref);

    // Add the operator to a referencable struct.
    // broker.add_operator();

    Ok(())
}
