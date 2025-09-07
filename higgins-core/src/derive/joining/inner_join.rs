/// A structure representing an Inner Join.
///
/// This structure is primarily implemented for transporting inner join
/// configuration data.
pub struct InnerJoin {
    /// Name and definition of the stream that these streams are joined to.
    stream: (Key, StreamDefinition),
    /// The left (or derivative) stream that this stream is joined from.
    left_stream: (Key, StreamDefinition),
    /// The right or outer stream that this stream is joined from.
    right_stream: (Key, StreamDefinition),
}
