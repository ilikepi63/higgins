use arrow::datatypes::{DataType, TimeUnit};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{cut, map, map_res, opt, recognize, value},
    error::{ErrorKind, context},
    multi::separated_list0,
    sequence::{delimited, preceded, tuple},
};
use std::str::FromStr;

// // Helper: parse usize from digits
// fn parse_usize(input: &str) -> IResult<&str, usize> {
//     map_res(digit1, usize::from_str)(input)
// }

// // Helper: parse quoted string (very naive – no escapes)
// fn quoted_string(input: &str) -> IResult<&str, &str> {
//     delimited(char('"'), take_while1(|c: char| c != '"'), char('"'))(input)
// }

// // Helper: parse optional timezone after comma
// fn maybe_timezone(input: &str) -> IResult<&str, Option<String>> {
//     opt(preceded(
//         multispace0,
//         preceded(char(','), preceded(multispace0, quoted_string)),
//     ))
//     .map(|s| s.map(ToString::to_string))(input)
// }

// // timestamp[unit]  or  timestamp[unit, "tz"]
// fn parse_timestamp(input: &str) -> IResult<&str, DataType> {
//     preceded(
//         tag_no_case("timestamp"),
//         delimited(
//             char('['),
//             cut(tuple((
//                 alt((
//                     value(TimeUnit::Second, tag_no_case("s")),
//                     value(TimeUnit::Millisecond, tag_no_case("ms")),
//                     value(TimeUnit::Microsecond, tag_no_case("us")),
//                     value(TimeUnit::Nanosecond, tag_no_case("ns")),
//                 )),
//                 maybe_timezone,
//             ))),
//             char(']'),
//         ),
//     )
//     .map(|(unit, tz)| DataType::Timestamp(unit, tz.map(|s| s.into())))
//     .parse(input)
// }

// // decimal(precision, scale)  – we produce Decimal128 for now (most common)
// fn parse_decimal(input: &str) -> IResult<&str, DataType> {
//     preceded(
//         alt((
//             tag_no_case("decimal"),
//             tag_no_case("decimal128"),
//             tag_no_case("decimal256"), // we can add logic later
//         )),
//         delimited(
//             char('('),
//             cut(tuple((
//                 parse_usize,
//                 preceded(tuple((multispace0, char(','), multispace0)), parse_usize),
//             ))),
//             char(')'),
//         ),
//     )
//     .map(|(precision, scale)| {
//         // In real code you might choose Decimal128 vs 256 depending on precision
//         // For simplicity → always Decimal128 here
//         DataType::Decimal128(precision as i8, scale as i8)
//     })
//     .parse(input)
// }

// fixed_size_binary(<len>)
// fn parse_fixed_size_binary(input: &str) -> IResult<&str, DataType> {
//     preceded(
//         tag_no_case("fixed_size_binary"),
//         delimited(char('('), cut(parse_usize), char(')')),
//     )
//     .map(DataType::FixedSizeBinary)
//     .parse(input)
// }
//
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

fn parse(input: &str) -> IResult<&str, DataType> {
    alt((parse_simple, parse_fixed_size_binary)).parse(input)
}

#[cfg(test)]
mod test {

    use super::*;
    use arrow_schema::DataType;

    const values: &[(&str, DataType)] = &[
        // byte array types.
        ("string", DataType::Utf8),
        ("large_string", DataType::LargeUtf8),
        ("bytes", DataType::Binary),
        ("large_bytes", DataType::LargeBinary),
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
        // ("string", DataType::Utf8),
        // ("string", DataType::Utf8),
        // ("string", DataType::Utf8),
        // ("string", DataType::Utf8),
        // ("string", DataType::Utf8),
        // ("string", DataType::Utf8),
        // ("string", DataType::Utf8),
        ("fixed_bytes[5]", DataType::FixedSizeBinary(5)),
    ];

    #[test]
    fn can_parse() {
        for (input, output) in values {
            assert_eq!(parse(input).unwrap().1, *output)
        }
    }
}
