#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod arrow_ipc;
mod definitions;
mod stream_name;
mod unique_collection;
pub use arrow_ipc::{read_arrow, write_arrow};
pub use definitions::{PartitionName, PartitionNameError};
pub use stream_name::StreamName;
pub use unique_collection::UniqueCollection;
mod error;
pub use error::*;
