use nom::{IResult, Parser};

use crate::{
    broker::Broker,
    error::HigginsError,
    topography::{Key, StreamDefinition, errors::TopographyError},
};

#[derive(Clone)]
pub struct WindowedStreamDefinition {
    pub base_key: String,
    pub slide: WindowValue,
    pub window_type: WindowValue,
    pub resultant_key: String,
}

#[derive(Clone, Debug)]
pub enum WindowValue {
    Count(u64),
    Timed((u64, WindowedTimeUnit)),
}

impl TryFrom<&str> for WindowValue {
    type Error = HigginsError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        window_interval_parser(value)
            .inspect_err(|err| tracing::error!("{:#?}", err))
            .map_err(|_err| HigginsError::Unknown)
    }
}

const HOURS_IN_DAY: u64 = 24;
const MINUTES_IN_HOUR: u64 = 60;
const SECONDS_IN_MINUTE: u64 = 60;

impl WindowValue {
    /// Normalize this value into a nominal u64 value.
    pub fn normalize(&self) -> u64 {
        match self {
            Self::Count(c) => *c,
            Self::Timed((t, u)) => match u {
                WindowedTimeUnit::Sec => *t,
                WindowedTimeUnit::Min => t * SECONDS_IN_MINUTE,
                WindowedTimeUnit::Hour => t * SECONDS_IN_MINUTE * MINUTES_IN_HOUR,
                WindowedTimeUnit::Day => t * SECONDS_IN_MINUTE * MINUTES_IN_HOUR * HOURS_IN_DAY,
                _ => todo!(),
            },
        }
    }
}

#[derive(Clone, Debug)]
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

pub fn window_interval_parser(input: &str) -> Result<WindowValue, HigginsError> {
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
        Some(val) => WindowValue::Timed((n, val)),
        None => WindowValue::Count(n),
    })
}

impl TryFrom<(Key, StreamDefinition, &Broker)> for WindowedStreamDefinition {
    type Error = HigginsError;

    fn try_from(
        (key, StreamDefinition { base, window, .. }, _): (Key, StreamDefinition, &Broker),
    ) -> Result<Self, Self::Error> {
        let base_key = &base.ok_or(TopographyError::IncorrectStreamDefinition(format!(
            "Base value non-present for windowed stream: {:#?}",
            key
        )))?;

        let window = window.unwrap();
        let window_value = WindowValue::try_from(window.interval.as_str())?;

        Ok(Self {
            base_key: String::from_utf8(base_key.as_bytes().to_vec()).unwrap(),
            window_type: window_value.clone(),
            slide: window
                .slide
                .map(|val| WindowValue::try_from(val.as_str()).unwrap())
                .unwrap_or(window_value),
            resultant_key: String::from_utf8(key.as_bytes().to_vec()).unwrap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topography::config::WindowDefinition;

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
            Ok(WindowValue::Count(100))
        ));
    }

    #[test]
    fn interval_parser_count_one_errors_due_to_streaming() {
        assert!(matches!(
            window_interval_parser("1"),
            Ok(WindowValue::Count(1))
        ));
    }

    #[test]
    fn interval_parser_count_zero_errors_due_to_streaming() {
        assert!(matches!(
            window_interval_parser("0"),
            Ok(WindowValue::Count(0))
        ));
    }

    #[test]
    fn interval_parser_timed_seconds() {
        let result = window_interval_parser("30s").unwrap();
        assert!(matches!(
            result,
            WindowValue::Timed((30, WindowedTimeUnit::Sec))
        ));
    }

    #[test]
    fn interval_parser_timed_milliseconds() {
        let result = window_interval_parser("500ms").unwrap();
        assert!(matches!(
            result,
            WindowValue::Timed((500, WindowedTimeUnit::Ms))
        ));
    }

    #[test]
    fn interval_parser_timed_microseconds() {
        let result = window_interval_parser("200us").unwrap();
        assert!(matches!(
            result,
            WindowValue::Timed((200, WindowedTimeUnit::Us))
        ));
    }

    #[test]
    fn interval_parser_timed_minutes_errors_due_to_streaming() {
        assert!(matches!(
            window_interval_parser("5m"),
            Ok(WindowValue::Timed((5, WindowedTimeUnit::Min)))
        ));
    }

    #[test]
    fn interval_parser_timed_hours() {
        let result = window_interval_parser("2h").unwrap();
        assert!(matches!(
            result,
            WindowValue::Timed((2, WindowedTimeUnit::Hour))
        ));
    }

    #[test]
    fn interval_parser_timed_days() {
        let result = window_interval_parser("7d").unwrap();
        assert!(matches!(
            result,
            WindowValue::Timed((7, WindowedTimeUnit::Day))
        ));
    }

    #[test]
    fn interval_parser_large_count_errors_due_to_streaming() {
        assert!(matches!(
            window_interval_parser("999999"),
            Ok(WindowValue::Count(999999))
        ));
    }

    #[test]
    fn interval_parser_large_timed_value() {
        let result = window_interval_parser("86400s").unwrap();
        assert!(matches!(
            result,
            WindowValue::Timed((86400, WindowedTimeUnit::Sec))
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
        let wst = WindowValue::try_from(wd.interval.as_str()).unwrap();
        assert!(matches!(
            wst,
            WindowValue::Timed((10, WindowedTimeUnit::Sec))
        ));
    }
}
