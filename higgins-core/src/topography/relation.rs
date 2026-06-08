//! A {Relation} maps the processing relationship between one stream and another.

use std::sync::Arc;

use higgins_shared::StreamName;
use tokio::sync::RwLock;

use crate::{subscription::Subscription, topography::StreamDefinition};

#[derive(Debug, Clone)]
pub struct Relation {
    pub stream_name: StreamName,
    pub definition: StreamDefinition,
    pub subscription: Arc<RwLock<Subscription>>,
    pub join_index: Option<u64>,
}
