use crate::topography::{Key, StreamDefinition};

/// A structure representing an Full Join.
///
/// This structure is primarily implemented for transporting full join
/// configuration data.
pub struct FullJoin {
    /// Name and definition of the stream that these streams are joined to.
    stream: (Key, StreamDefinition),
    /// The first stream. There is no distinction between streams as a full join
    /// does not differentiate between two streams.
    first_stream: (Key, StreamDefinition),
    /// The second stream definition.
    second_stream: (Key, StreamDefinition),
}
