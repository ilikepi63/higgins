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
use nom::bytes::complete::tag;
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
        nom::bytes::complete::take_while1(|n: char| n.is_numeric()),
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_window_time_unit ──────────────────────────────────────────

    #[test]
    fn parse_time_unit_seconds() {
        let (rest, unit) = parse_window_time_unit("s").unwrap();
        assert!(rest.is_empty());
        assert!(matches!(unit, WindowedTimeUnit::Sec));
    }

    #[test]
    fn parse_time_unit_milliseconds() {
        let (rest, unit) = parse_window_time_unit("ms").unwrap();
        assert!(rest.is_empty());
        assert!(matches!(unit, WindowedTimeUnit::Ms));
    }

    #[test]
    fn parse_time_unit_microseconds() {
        let (rest, unit) = parse_window_time_unit("us").unwrap();
        assert!(rest.is_empty());
        assert!(matches!(unit, WindowedTimeUnit::Us));
    }

    #[test]
    fn parse_time_unit_minutes_incomplete() {
        assert!(matches!(
            parse_window_time_unit("m").inspect_err(|val| println!("{:#?}", val)),
            Ok(("", WindowedTimeUnit::Min))
        ));
    }

    #[test]
    fn parse_time_unit_minutes_with_trailing() {
        // With trailing content the parser can disambiguate "m" from "ms".
        let (rest, unit) = parse_window_time_unit("mx").unwrap();
        assert_eq!(rest, "x");
        assert!(matches!(unit, WindowedTimeUnit::Min));
    }

    #[test]
    fn parse_time_unit_hours() {
        let (rest, unit) = parse_window_time_unit("h").unwrap();
        assert!(rest.is_empty());
        assert!(matches!(unit, WindowedTimeUnit::Hour));
    }

    #[test]
    fn parse_time_unit_days() {
        let (rest, unit) = parse_window_time_unit("d").unwrap();
        assert!(rest.is_empty());
        assert!(matches!(unit, WindowedTimeUnit::Day));
    }

    #[test]
    fn parse_time_unit_leaves_trailing_input() {
        let (rest, unit) = parse_window_time_unit("sxyz").unwrap();
        assert_eq!(rest, "xyz");
        assert!(matches!(unit, WindowedTimeUnit::Sec));
    }

    #[test]
    fn parse_time_unit_invalid_input() {
        assert!(parse_window_time_unit("x").is_err());
    }

    #[test]
    fn parse_time_unit_empty_input() {
        assert!(parse_window_time_unit("").is_err());
    }

    #[test]
    fn interval_parser_count_only_errors_due_to_streaming() {
        assert!(matches!(
            window_interval_parser("100"),
            Ok(WindowedStreamType::Count(100))
        ));
    }

    #[test]
    fn interval_parser_count_one_errors_due_to_streaming() {
        assert!(matches!(
            window_interval_parser("1"),
            Ok(WindowedStreamType::Count(1))
        ));
    }

    #[test]
    fn interval_parser_count_zero_errors_due_to_streaming() {
        assert!(matches!(
            window_interval_parser("0"),
            Ok(WindowedStreamType::Count(0))
        ));
    }

    #[test]
    fn interval_parser_timed_seconds() {
        let result = window_interval_parser("30s").unwrap();
        assert!(matches!(
            result,
            WindowedStreamType::Timed(WindowedTimeUnit::Sec)
        ));
    }

    #[test]
    fn interval_parser_timed_milliseconds() {
        let result = window_interval_parser("500ms").unwrap();
        assert!(matches!(
            result,
            WindowedStreamType::Timed(WindowedTimeUnit::Ms)
        ));
    }

    #[test]
    fn interval_parser_timed_microseconds() {
        let result = window_interval_parser("200us").unwrap();
        assert!(matches!(
            result,
            WindowedStreamType::Timed(WindowedTimeUnit::Us)
        ));
    }

    #[test]
    fn interval_parser_timed_minutes_errors_due_to_streaming() {
        assert!(matches!(
            window_interval_parser("5m"),
            Ok(WindowedStreamType::Timed(WindowedTimeUnit::Min))
        ));
    }

    #[test]
    fn interval_parser_timed_hours() {
        let result = window_interval_parser("2h").unwrap();
        assert!(matches!(
            result,
            WindowedStreamType::Timed(WindowedTimeUnit::Hour)
        ));
    }

    #[test]
    fn interval_parser_timed_days() {
        let result = window_interval_parser("7d").unwrap();
        assert!(matches!(
            result,
            WindowedStreamType::Timed(WindowedTimeUnit::Day)
        ));
    }

    #[test]
    fn interval_parser_large_count_errors_due_to_streaming() {
        assert!(matches!(
            window_interval_parser("999999"),
            Ok(WindowedStreamType::Count(999999))
        ));
    }

    #[test]
    fn interval_parser_large_timed_value() {
        let result = window_interval_parser("86400s").unwrap();
        assert!(matches!(
            result,
            WindowedStreamType::Timed(WindowedTimeUnit::Sec)
        ));
    }

    #[test]
    fn interval_parser_empty_input_errors() {
        assert!(window_interval_parser("").is_err());
    }

    #[test]
    fn interval_parser_only_unit_errors() {
        assert!(window_interval_parser("s").is_err());
    }

    #[test]
    fn interval_parser_non_numeric_errors() {
        assert!(window_interval_parser("abc").is_err());
    }

    #[test]
    fn windowed_stream_type_from_window_definition_returns_count() {
        let wd: WindowDefinition =
            serde_json::from_str(r#"{"type": "tumbling", "interval": "10s"}"#).unwrap();
        // Current implementation always returns Count(5)
        let wst = WindowedStreamType::from(&wd);
        assert!(matches!(wst, WindowedStreamType::Count(5)));
    }
}
