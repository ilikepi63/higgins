use super::mapping::JoinMapping;
use crate::broker::Broker;
use crate::topography::errors::TopographyError;
use crate::topography::{Key, StreamDefinition};

/// A {JoinDefinition} represents a definition as how it would like be represented in configuration with all of its
/// metadata.
#[derive(Clone, Debug)]
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

impl TryFrom<(Key, StreamDefinition, &Broker)> for JoinDefinition {
    type Error = TopographyError;

    fn try_from(
        (key, stream_definition, broker): (Key, StreamDefinition, &Broker),
    ) -> Result<Self, Self::Error> {
        let schema = broker
            .get_stream(key.inner())
            .map(|(schema, _, _)| schema.clone())
            .ok_or(TopographyError::SchemaNotFound(format!("{:#?}", key)))?;

        let join_streams = stream_definition
            .join
            .clone()
            .map(|joins| {
                joins.into_iter().map(|stream_name| {
                    broker
                        .get_topography_stream(&Key(stream_name.into_bytes()))
                        .map(JoinWithStream::from)
                        .ok_or(TopographyError::JoinStreamDoesNotExist)
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
    pub stream: (Key, StreamDefinition),
}

impl From<(&Key, &StreamDefinition)> for JoinWithStream {
    fn from((key, def): (&Key, &StreamDefinition)) -> Self {
        Self {
            stream: (key.clone(), def.clone()),
        }
    }
}
