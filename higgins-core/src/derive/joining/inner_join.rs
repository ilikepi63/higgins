use crate::topography::{Key, StreamDefinition};

/// A structure representing an Inner Join.
///
/// This structure is primarily implemented for transporting inner join
/// configuration data.
pub struct InnerJoin {
    /// Name and definition of the stream that these streams are joined to.
    pub stream: (Key, StreamDefinition),
    /// The left (or derivative) stream that this stream is joined from.
    pub left_stream: (Key, StreamDefinition),
    /// The right or outer stream that this stream is joined from.
    pub right_stream: (Key, StreamDefinition),
}
