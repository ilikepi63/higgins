use crate::topography::{Key, StreamDefinition};

/// The side to which a stream is being joined.
pub enum OuterSide {
    Left,
    Right,
}

/// A structure representing an Outer Join.
///
/// This structure is primarily implemented for transporting join
/// configuration data.
pub struct OuterJoin {
    /// The side at which a stream is being joined.
    pub side: OuterSide,
    /// Name and definition of the stream that these streams are joined to.
    pub stream: (Key, StreamDefinition),
    /// The left (or derivative) stream that this stream is joined from.
    pub left_stream: (Key, StreamDefinition),
    /// The right or outer stream that this stream is joined from.
    pub right_stream: (Key, StreamDefinition),
}
