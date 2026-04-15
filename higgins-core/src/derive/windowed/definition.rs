use crate::{
    broker::Broker,
    error::HigginsError,
    topography::{Key, StreamDefinition, config::WindowDefinition, errors::TopographyError},
};

pub struct WindowedStreamDefinition {
    base_stream: StreamDefinition,
    window_type: WindowedStreamType,
}

pub enum WindowedStreamType {
    Count(u64),
    Timed(std::time::Duration),
}

impl From<&WindowDefinition> for WindowedStreamType {
    fn from(value: &WindowDefinition) -> Self {
        WindowedStreamType::Count(5)
    }
}

impl TryFrom<(Key, StreamDefinition, &Broker)> for WindowedStreamDefinition {
    type Error = HigginsError;

    fn try_from(
        (
            key,
            StreamDefinition {
                base,
                // stream_type,
                // partition_key,
                // schema,
                // join,
                // map,
                // function_name,
                window,
                ..
            },
            broker,
        ): (Key, StreamDefinition, &Broker),
    ) -> Result<Self, Self::Error> {
        let base_stream = broker
            .get_topography_stream(&base.ok_or(TopographyError::IncorrectStreamDefinition(
                format!("Base value non-present for windowed stream: {:#?}", key),
            ))?)
            .ok_or(TopographyError::DerivativeNotFound(format!(
                "Derivative stream not found.",
                // base.clone()
            )))?
            .1
            .clone();

        Ok(Self {
            base_stream,
            window_type: WindowedStreamType::from(&window.unwrap()),
        })
    }
}
