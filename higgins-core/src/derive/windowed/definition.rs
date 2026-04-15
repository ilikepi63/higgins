use std::time::Duration;

use arrow_schema::TimeUnit;
use nom::{IResult, Parser};

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
    Timed(WindowedTimeUnit),
}

impl From<&WindowDefinition> for WindowedStreamType {
    fn from(value: &WindowDefinition) -> Self {
        WindowedStreamType::Count(5)
    }
}

#[derive(Clone)]
pub enum WindowedTimeUnit {
    Min,
    Hour,
    Day,
    Sec,
    Ms,
    Us,
}

use nom::branch::alt;
use nom::bytes::tag;
use nom::combinator::value;

pub fn parse_window_time_unit(input: &str) -> IResult<&str, WindowedTimeUnit> {
    alt((
        value(WindowedTimeUnit::Sec, tag("s")),
        value(WindowedTimeUnit::Ms, tag("ms")),
        value(WindowedTimeUnit::Us, tag("us")),
        value(WindowedTimeUnit::Min, tag("m")),
        value(WindowedTimeUnit::Hour, tag("h")),
        value(WindowedTimeUnit::Day, tag("d")),
    ))
    .parse(input)
}

pub fn window_interval_parser(input: &str) -> Result<WindowedStreamType, HigginsError> {
    let (_, (n, time)) = (
        nom::bytes::take_while1(|n: char| n.is_numeric()),
        nom::combinator::opt(parse_window_time_unit),
    )
        .parse(input)
        .map_err(|e| {
            tracing::error!("{:#?}", e);
            HigginsError::Unknown
        })?;

    let n = str::parse::<u64>(n).map_err(|e| {
        tracing::error!("{:#?}", e);
        HigginsError::Unknown
    })?;

    Ok(match time {
        Some(val) => WindowedStreamType::Timed(val),
        None => WindowedStreamType::Count(n),
    })
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
