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
    /// Name and definition of the stream that this is joined to.
    pub stream: (Key, StreamDefinition),
}
