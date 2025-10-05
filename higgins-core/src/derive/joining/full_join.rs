use crate::topography::{Key, StreamDefinition};

/// A structure representing an Full Join.
///
/// This structure is primarily implemented for transporting full join
/// configuration data.
pub struct FullJoin {
    /// Name and definition of the stream that this is joined to.
    pub stream: (Key, StreamDefinition),
}
