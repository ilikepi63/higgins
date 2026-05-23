use arrow::datatypes::{DataType, TimeUnit};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::{
        complete::{tag, take_while1},
        take_until,
    },
    character::{char, complete::multispace0},
    combinator::{opt, value},
    error::ErrorKind,
    multi::separated_list0,
    sequence::{delimited, preceded},
};

fn parse_simple(input: &str) -> IResult<&str, DataType> {
    alt((
        value(DataType::Utf8, tag("string")),
        value(DataType::LargeUtf8, tag("large_string")),
        // bytes
        value(DataType::Binary, tag("bytes")),
        value(DataType::LargeBinary, tag("large_bytes")),
        // signed integer types
        value(DataType::Int8, tag("int8")),
        value(DataType::Int16, tag("int16")),
        value(DataType::Int32, tag("int32")),
        value(DataType::Int64, tag("int64")),
        // unsigned integer types
        value(DataType::UInt8, tag("uint8")),
        value(DataType::UInt16, tag("uint16")),
        value(DataType::UInt32, tag("uint32")),
        value(DataType::UInt64, tag("uint64")),
        // floats
        value(DataType::Float16, tag("float16")),
        value(DataType::Float32, tag("float32")),
        value(DataType::Float64, tag("float64")),
        // boolean
        value(DataType::Boolean, tag("bool")),
        // dates
        value(DataType::Date32, tag("date32")),
        value(DataType::Date64, tag("date64")),
    ))
    .parse(input.trim())
}

fn parse_decimal(input: &str) -> IResult<&str, DataType> {
    let (_, (_, _, config_vector, _)) = (
        tag("decimal"),
        tag("["),
        separated_list0(char(','), take_while1(|n: char| n.is_numeric())),
        tag("]"),
    )
        .parse(input)?;

    let precision = config_vector
        .first()
        .and_then(|v| v.parse::<u8>().ok())
        .ok_or(nom::Err::Error(nom::error::Error::new(
            input,
            ErrorKind::IsNot,
        )))?;

    let scale = config_vector
        .get(1)
        .and_then(|v| v.parse::<i8>().ok())
        .ok_or(nom::Err::Error(nom::error::Error::new(
            input,
            ErrorKind::IsNot,
        )))?;

    let data_type = match config_vector.get(2) {
        Some(v) if *v == "256" => Ok(DataType::Decimal256(precision, scale)),
        Some(v) if *v == "128" => Ok(DataType::Decimal128(precision, scale)),
        None => Ok(DataType::Decimal128(precision, scale)),
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            ErrorKind::IsNot,
        ))),
    }?;

    IResult::Ok((input, data_type))
}

pub fn parse_time_unit(input: &str) -> IResult<&str, TimeUnit> {
    alt((
        value(TimeUnit::Second, tag("s")),
        value(TimeUnit::Nanosecond, tag("ns")),
        value(TimeUnit::Microsecond, tag("us")),
        value(TimeUnit::Millisecond, tag("ms")),
    ))
    .parse(input)
}

fn parse_time(input: &str) -> IResult<&str, DataType> {
    let (_, (_, _, timeunit, _)) =
        (tag("time"), tag("["), parse_time_unit, tag("]")).parse(input)?;

    let data_type = match timeunit {
        TimeUnit::Second | TimeUnit::Millisecond => DataType::Time32(timeunit),
        TimeUnit::Nanosecond | TimeUnit::Microsecond => DataType::Time64(timeunit),
    };

    IResult::Ok((input, data_type))
}

fn parse_timezone(input: &str) -> IResult<&str, &str> {
    take_until("]").parse(input)
}

fn parse_timestamp(input: &str) -> IResult<&str, DataType> {
    let (_, (_, (time_unit, timezone))) = (
        tag("timestamp"),
        delimited(
            char('['),
            nom::sequence::pair(
                alt((tag("ms"), tag("us"), tag("s"), tag("ns"))),
                opt(preceded(
                    preceded(multispace0, tag(",")),
                    preceded(multispace0, parse_timezone),
                )),
            ),
            char(']'),
        ),
    )
        .parse(input)?;

    let time_unit = match time_unit {
        "ns" => TimeUnit::Nanosecond,
        "ms" => TimeUnit::Millisecond,
        "us" => TimeUnit::Microsecond,
        _ => TimeUnit::Second,
    };

    let timezone: Option<std::sync::Arc<str>> = timezone.map(|s| (*s).into());

    IResult::Ok((input, DataType::Timestamp(time_unit, timezone)))
}

fn parse_fixed_size_binary(input: &str) -> IResult<&str, DataType> {
    let (_, (_, _, digits, _)) = (
        tag("fixed_bytes"),
        tag("["),
        take_while1(|n: char| n.is_numeric()),
        tag("]"),
    )
        .parse(input)?;

    let size = digits
        .parse::<i32>()
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, ErrorKind::IsNot)))?;

    IResult::Ok((input, DataType::FixedSizeBinary(size)))
}

pub fn parse(input: &str) -> IResult<&str, DataType> {
    alt((
        parse_simple,
        parse_fixed_size_binary,
        parse_decimal,
        parse_time,
        parse_timestamp,
    ))
    .parse(input)
}

#[cfg(test)]
mod test {

    use super::*;
    use arrow_schema::DataType;

    const VALUES: &[(&str, DataType)] = &[
        // byte array types.
        ("string", DataType::Utf8),
        ("large_string", DataType::LargeUtf8),
        ("bytes", DataType::Binary),
        ("large_bytes", DataType::LargeBinary),
        ("fixed_bytes[5]", DataType::FixedSizeBinary(5)),
        // signed integer types
        ("int8", DataType::Int8),
        ("int16", DataType::Int16),
        ("int32", DataType::Int32),
        ("int64", DataType::Int64),
        // unsigned integer types.
        ("uint8", DataType::UInt8),
        ("uint16", DataType::UInt16),
        ("uint32", DataType::UInt32),
        ("uint64", DataType::UInt64),
        // floats
        ("float16", DataType::Float16),
        ("float32", DataType::Float32),
        ("float64", DataType::Float64),
        // boolean
        ("bool", DataType::Boolean),
        // dates
        ("date32", DataType::Date32),
        ("date64", DataType::Date64),
        // decimal
        ("decimal[1,3]", DataType::Decimal128(1, 3)),
        ("decimal[7,8,256]", DataType::Decimal256(7, 8)),
        ("decimal[5,6,128]", DataType::Decimal128(5, 6)),
        // time
        ("time[ns]", DataType::Time64(TimeUnit::Nanosecond)),
        ("time[s]", DataType::Time32(TimeUnit::Second)),
        ("time[us]", DataType::Time64(TimeUnit::Microsecond)),
    ];

    #[test]
    fn can_parse() {
        for (input, output) in VALUES {
            assert_eq!(parse(input).unwrap().1, *output)
        }
    }

    #[test]
    fn can_parse_decimal() {
        let (_, result) = parse_decimal("decimal[1,3]")
            .inspect_err(|err| {
                dbg!(err);
            })
            .unwrap();

        assert_eq!(result, DataType::Decimal128(1, 3));
    }

    #[test]
    fn can_parse_timestamp() {
        let (_, result) = parse_timestamp("timestamp[s]").unwrap();

        assert_eq!(result, DataType::Timestamp(TimeUnit::Second, None));

        let (_, result) = parse_timestamp("timestamp[ms, America/New_York]").unwrap();

        assert_eq!(
            result,
            DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into()))
        );

        let (_, result) = parse_timestamp("timestamp[ms, +07:30]").unwrap();

        assert_eq!(
            result,
            DataType::Timestamp(TimeUnit::Millisecond, Some("+07:30".into()))
        );
    }
}
