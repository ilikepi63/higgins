use crate::broker::Broker;
use crate::error::HigginsError;
use crate::topography::{Join, Key, StreamDefinition};

use super::{full_join::FullJoin, inner_join::InnerJoin, outer_join::OuterJoin};

pub enum JoinDefinition {
    Inner(InnerJoin),
    Outer(OuterJoin),
    Full(FullJoin),
}

impl TryFrom<(Key, StreamDefinition, &Broker)> for JoinDefinition {
    type Error = HigginsError;

    fn try_from(
        (key, stream_definition, broker): (Key, StreamDefinition, &Broker),
    ) -> Result<Self, Self::Error> {
        let left = broker
            .get_topography_stream(&stream_definition.clone().base.unwrap())
            .map(|(key, def)| (key.clone(), def.clone()))
            .unwrap();

        let right = broker
            .get_topography_stream(
                &stream_definition
                    .clone()
                    .join
                    .map(|v| Key(v.key().to_owned()))
                    .unwrap(),
            )
            .map(|(key, def)| (key.clone(), def.clone()))
            .unwrap();

        Ok(match stream_definition.clone().join.unwrap() {
            Join::Inner(_) => JoinDefinition::Inner(InnerJoin {
                stream: (key, stream_definition),
                left_stream: left,
                right_stream: right,
            }),
            Join::LeftOuter(_) => JoinDefinition::Outer(OuterJoin {
                side: super::outer_join::OuterSide::Left,
                stream: (key, stream_definition),
                left_stream: left,
                right_stream: right,
            }),

            Join::RightOuter(_) => JoinDefinition::Outer(OuterJoin {
                side: super::outer_join::OuterSide::Right,
                stream: (key, stream_definition),
                left_stream: left,
                right_stream: right,
            }),

            Join::Full(_) => JoinDefinition::Full(FullJoin {
                stream: (key, stream_definition),
                first_stream: left,
                second_stream: right,
            }),
        })
    }
}
