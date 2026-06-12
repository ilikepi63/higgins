use higgins_shared::{StreamName, TopographyError};

use super::mapping::JoinMapping;
use crate::broker::Broker;
use crate::topography::StreamDefinition;

/// A {JoinDefinition} represents a definition as how it would like be represented in configuration with all of its
/// metadata.
#[derive(Clone, Debug)]
pub struct JoinDefinition {
    /// The base stream that this join definition comes from.
    pub base: (StreamName, StreamDefinition),
    /// The different joins that will
    pub joins: Vec<JoinWithStream>,
    /// The mapping of the given joins with the overarching joined stream.
    pub mapping: JoinMapping,
}

impl JoinDefinition {
    #[allow(unused)]
    pub fn joined_stream_from_index(&self, i: usize) -> Option<&(StreamName, StreamDefinition)> {
        self.joins.get(i).as_ref().map(|v| &v.stream)
    }
}

impl TryFrom<(StreamName, StreamDefinition, &Broker)> for JoinDefinition {
    type Error = TopographyError;

    fn try_from(
        (key, stream_definition, broker): (StreamName, StreamDefinition, &Broker),
    ) -> Result<Self, Self::Error> {
        let schema = broker
            .get_stream(&key)
            .map(|(schema, _, _)| schema.clone())
            .ok_or(TopographyError::SchemaNotFound(format!("{:#?}", key)))?;

        let join_streams = stream_definition
            .join
            .clone()
            .map(|joins| {
                joins.into_iter().map(|stream_name| {
                    broker
                        .get_topography_stream(&stream_name)
                        .map(JoinWithStream::from)
                })
            })
            .ok_or(TopographyError::NoJoinsInJoinDefinition)?
            .collect::<Result<Vec<_>, TopographyError>>()?;

        Ok(JoinDefinition {
            base: (key, stream_definition.clone()),
            joins: join_streams,
            mapping: stream_definition
                .map
                .clone()
                .map(|map| JoinMapping::from((schema, map)))
                .ok_or(TopographyError::JoinStreamWithoutMappingAttributes)?,
        })
    }
}

impl TryFrom<(StreamName, StreamDefinition, &mut Broker)> for JoinDefinition {
    type Error = TopographyError;

    fn try_from(
        (key, stream_definition, broker): (StreamName, StreamDefinition, &mut Broker),
    ) -> Result<Self, Self::Error> {
        let schema = broker
            .get_stream(&key)
            .map(|(schema, _, _)| schema.clone())
            .ok_or(TopographyError::SchemaNotFound(format!("{:#?}", key)))?;

        let join_streams = stream_definition
            .join
            .clone()
            .map(|joins| {
                joins.into_iter().map(|stream_name| {
                    broker
                        .get_topography_stream(&stream_name)
                        .map(JoinWithStream::from)
                })
            })
            .ok_or(TopographyError::NoJoinsInJoinDefinition)?
            .collect::<Result<Vec<_>, TopographyError>>()?;

        Ok(JoinDefinition {
            base: (key, stream_definition.clone()),
            joins: join_streams,
            mapping: stream_definition
                .map
                .clone()
                .map(|map| JoinMapping::from((schema, map)))
                .ok_or(TopographyError::JoinStreamWithoutMappingAttributes)?,
        })
    }
}

/// # JoinWithStream
///
/// Structure primarily used as a ADT over different join types.
#[derive(Clone, Debug)]
pub struct JoinWithStream {
    /// Name and definition of the stream that this is joined to.
    pub stream: (StreamName, StreamDefinition),
}

impl From<(StreamName, &StreamDefinition)> for JoinWithStream {
    fn from((key, def): (StreamName, &StreamDefinition)) -> Self {
        Self {
            stream: (key.clone(), def.clone()),
        }
    }
}
