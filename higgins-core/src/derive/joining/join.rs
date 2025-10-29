use super::mapping::JoinMapping;
use crate::topography::{Key, StreamDefinition};

/// A {JoinDefinition} represents a definition as how it would like be represented in configuration with all of its
/// metadata.
#[derive(Clone)]
pub struct JoinDefinition {
    /// The base stream that this join definition comes from.
    pub base: (Key, StreamDefinition),
    /// The different joins that will
    pub joins: Vec<JoinWithStream>,
    /// The mapping of the given joins with the overarching joined stream.
    pub mapping: JoinMapping,
}

impl JoinDefinition {
    #[allow(unused)]
    pub fn joined_stream_from_index(&self, i: usize) -> Option<&(Key, StreamDefinition)> {
        self.joins.get(i).as_ref().map(|v| &v.stream)
    }
}

// impl TryFrom<(Key, StreamDefinition, &Broker)> for JoinDefinition {
//     type Error = HigginsError;

//     fn try_from(
//         (key, stream_definition, broker): (Key, StreamDefinition, &Broker),
//     ) -> Result<Self, Self::Error> {
//         let left = broker
//             .get_topography_stream(&stream_definition.clone().base.unwrap())
//             .map(|(key, def)| (key.clone(), def.clone()))
//             .unwrap();

//         let right = broker
//             .get_topography_stream(
//                 &stream_definition
//                     .clone()
//                     .join
//                     .map(|v| Key(v.key().to_owned()))
//                     .unwrap(),
//             )
//             .map(|(key, def)| (key.clone(), def.clone()))
//             .unwrap();

//         Ok(match stream_definition.clone().join.unwrap() {
//             Join::Inner(_) => JoinDefinition::Inner(InnerJoin {
//                 stream: (key, stream_definition),
//                 left_stream: left,
//                 right_stream: right,
//             }),
//             Join::LeftOuter(_) => JoinDefinition::Outer(OuterJoin {
//                 side: super::outer_join::OuterSide::Left,
//                 stream: (key, stream_definition),
//                 left_stream: left,
//                 right_stream: right,
//             }),

//             Join::RightOuter(_) => JoinDefinition::Outer(OuterJoin {
//                 side: super::outer_join::OuterSide::Right,
//                 stream: (key, stream_definition),
//                 left_stream: left,
//                 right_stream: right,
//             }),

//             Join::Full(_) => JoinDefinition::Full(FullJoin {
//                 stream: (key, stream_definition),
//                 first_stream: left,
//                 second_stream: right,
//             }),
//         })
//     }
// }

/// # JoinWithStream
///
/// Structure primarily used as a ADT over different join types.
#[derive(Clone)]
pub struct JoinWithStream {
    /// Name and definition of the stream that this is joined to.
    pub stream: (Key, StreamDefinition),
}
