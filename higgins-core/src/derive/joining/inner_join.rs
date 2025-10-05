use crate::topography::{Key, StreamDefinition};

/// A structure representing an Inner Join.
///
/// This structure is primarily implemented for transporting inner join
/// configuration data.
pub struct InnerJoin {
    /// Name and definition of the stream that this is joined to.
    pub stream: (Key, StreamDefinition),
}
