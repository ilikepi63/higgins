use std::{borrow::Cow, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionMode},
    error::ArrowError,
};

use crate::{
    types::{WasmArrowSchema, WasmPtr},
    utils::{WasmAllocator, clone_struct, u32_to_u8},
};

// Bitflags for Arrow C Data Interface.
bitflags::bitflags! {
    /// Flags for [`FFI_ArrowSchema`]
    ///
    /// Old Workaround at <https://github.com/bitflags/bitflags/issues/356>
    /// is no longer required as `bitflags` [fixed the issue](https://github.com/bitflags/bitflags/pull/355).
    pub struct Flags: i64 {
        /// Indicates that the dictionary is ordered
        const DICTIONARY_ORDERED = 0b00000001;
        /// Indicates that the field is nullable
        const NULLABLE = 0b00000010;
        /// Indicates that the map keys are sorted
        const MAP_KEYS_SORTED = 0b00000100;
    }
}

/// Create Children from the given Datatype.
fn children_from_datatype(
    dtype: &DataType,
    allocator: &mut WasmAllocator,
) -> Result<Vec<WasmArrowSchema>, ArrowError> {
    let children = match dtype {
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::FixedSizeList(child, _)
        | DataType::Map(child, _) => {
            vec![try_from_field(child.as_ref(), allocator)?]
        }
        DataType::Union(fields, _) => fields
            .iter()
            .map(|(_, f)| try_from_field(f.as_ref(), allocator))
            .collect::<Result<Vec<_>, ArrowError>>()?,
        DataType::Struct(fields) => fields
            .iter()
            .map(|f| try_from_field(f.as_ref(), allocator))
            .collect::<Result<Vec<_>, ArrowError>>()?,
        DataType::RunEndEncoded(run_ends, values) => vec![
            try_from_field(run_ends.as_ref(), allocator)?,
            try_from_field(values.as_ref(), allocator)?,
        ],
        _ => vec![],
    };

    Ok(children)
}

pub fn copy_schema(
    dtype: &DataType,
    field: Arc<Field>,
    allocator: &mut WasmAllocator,
) -> Result<WasmPtr<WasmArrowSchema>, ArrowError> {
    let format = get_format_string(dtype)?;

    let format_ptr = allocator.copy(format.as_bytes());

    // allocate and hold the children
    let children = children_from_datatype(dtype, allocator)?;

    let dictionary = if let DataType::Dictionary(_, value_data_type) = dtype {
        Some(copy_schema(
            value_data_type.as_ref(),
            field.clone(),
            allocator,
        )?)
    } else {
        None
    };

    let flags = match dtype {
        DataType::Map(_, true) => Flags::MAP_KEYS_SORTED,
        _ => Flags::empty(),
    };

    // Allocation happens here.
    let children_box = children
        .into_iter()
        .map(|child_schema| clone_struct(child_schema, allocator))
        .collect::<Box<_>>();

    let n_children = children_box.len() as i64;

    // Malloc for the children pointers.
    let children_ptr = allocator.copy(u32_to_u8(&children_box));

    let dictionary_ptr = dictionary
        .map(|d| {
            let ptr = clone_struct(d, allocator);

            WasmPtr::new(ptr)
        })
        .unwrap_or(WasmPtr::null());

    let dictionary = dictionary_ptr;

    let name = allocator.copy(field.name().as_bytes());

    let schema = WasmArrowSchema {
        format: WasmPtr::new(format_ptr),
        name: WasmPtr::new(name),
        metadata: WasmPtr::null(),
        flags: flags.bits(),
        n_children,
        children: WasmPtr::new(children_ptr),
        dictionary,
        release: None,
        private_data: WasmPtr::null(),
    };

    let buffer: &[u8] = unsafe {
        &std::mem::transmute::<WasmArrowSchema, [u8; std::mem::size_of::<WasmArrowSchema>()]>(
            schema,
        )
    };

    let ptr = allocator.copy(buffer);

    println!("Copying Schema to ptr {ptr:#?}");

    Ok(WasmPtr::new(ptr))
}

pub fn try_from(
    dtype: &DataType,
    allocator: &mut WasmAllocator,
) -> Result<WasmArrowSchema, ArrowError> {
    let format = get_format_string(dtype)?;
    let format_ptr = allocator.copy(format.as_bytes());

    // allocate and hold the children
    let children = match dtype {
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::FixedSizeList(child, _)
        | DataType::Map(child, _) => {
            vec![try_from_field(child.as_ref(), allocator)?]
        }
        DataType::Union(fields, _) => fields
            .iter()
            .map(|(_, f)| try_from_field(f.as_ref(), allocator))
            .collect::<Result<Vec<_>, ArrowError>>()?,
        DataType::Struct(fields) => fields
            .iter()
            .map(|f| try_from_field(f.as_ref(), allocator))
            .collect::<Result<Vec<_>, ArrowError>>()?,
        DataType::RunEndEncoded(run_ends, values) => vec![
            try_from_field(run_ends.as_ref(), allocator)?,
            try_from_field(values.as_ref(), allocator)?,
        ],
        _ => vec![],
    };
    let dictionary = if let DataType::Dictionary(_, value_data_type) = dtype {
        Some(try_from(value_data_type.as_ref(), allocator)?)
    } else {
        None
    };

    let flags = match dtype {
        DataType::Map(_, true) => Flags::MAP_KEYS_SORTED,
        _ => Flags::empty(),
    };

    let children_box = children
        .into_iter()
        .map(|schema| clone_struct(schema, allocator))
        .collect::<Box<_>>();

    let children_ptr = allocator.copy(u32_to_u8(&children_box));

    let n_children = children_box.len() as i64;

    let dictionary_ptr = dictionary
        .map(|d| WasmPtr::new(clone_struct(d, allocator)))
        .unwrap_or(WasmPtr::null());

    let dictionary = dictionary_ptr;

    Ok(WasmArrowSchema {
        format: WasmPtr::new(format_ptr),
        name: WasmPtr::null(),
        metadata: WasmPtr::null(),
        flags: flags.bits(),
        n_children,
        children: WasmPtr::new(children_ptr),
        dictionary,
        release: None,
        private_data: WasmPtr::null(),
    })
}

/// Retrieves the format string for each DataType in line with C Data Interface.
fn get_format_string(dtype: &DataType) -> Result<Cow<'static, str>, ArrowError> {
    match dtype {
        DataType::Null => Ok("n".into()),
        DataType::Boolean => Ok("b".into()),
        DataType::Int8 => Ok("c".into()),
        DataType::UInt8 => Ok("C".into()),
        DataType::Int16 => Ok("s".into()),
        DataType::UInt16 => Ok("S".into()),
        DataType::Int32 => Ok("i".into()),
        DataType::UInt32 => Ok("I".into()),
        DataType::Int64 => Ok("l".into()),
        DataType::UInt64 => Ok("L".into()),
        DataType::Float16 => Ok("e".into()),
        DataType::Float32 => Ok("f".into()),
        DataType::Float64 => Ok("g".into()),
        DataType::BinaryView => Ok("vz".into()),
        DataType::Binary => Ok("z".into()),
        DataType::LargeBinary => Ok("Z".into()),
        DataType::Utf8View => Ok("vu".into()),
        DataType::Utf8 => Ok("u".into()),
        DataType::LargeUtf8 => Ok("U".into()),
        DataType::FixedSizeBinary(num_bytes) => Ok(Cow::Owned(format!("w:{num_bytes}"))),
        DataType::FixedSizeList(_, num_elems) => Ok(Cow::Owned(format!("+w:{num_elems}"))),
        DataType::Decimal128(precision, scale) => Ok(Cow::Owned(format!("d:{precision},{scale}"))),
        DataType::Decimal256(precision, scale) => {
            Ok(Cow::Owned(format!("d:{precision},{scale},256")))
        }
        DataType::Date32 => Ok("tdD".into()),
        DataType::Date64 => Ok("tdm".into()),
        DataType::Time32(TimeUnit::Second) => Ok("tts".into()),
        DataType::Time32(TimeUnit::Millisecond) => Ok("ttm".into()),
        DataType::Time64(TimeUnit::Microsecond) => Ok("ttu".into()),
        DataType::Time64(TimeUnit::Nanosecond) => Ok("ttn".into()),
        DataType::Timestamp(TimeUnit::Second, None) => Ok("tss:".into()),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok("tsm:".into()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok("tsu:".into()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok("tsn:".into()),
        DataType::Timestamp(TimeUnit::Second, Some(tz)) => Ok(Cow::Owned(format!("tss:{tz}"))),
        DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => Ok(Cow::Owned(format!("tsm:{tz}"))),
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => Ok(Cow::Owned(format!("tsu:{tz}"))),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => Ok(Cow::Owned(format!("tsn:{tz}"))),
        DataType::Duration(TimeUnit::Second) => Ok("tDs".into()),
        DataType::Duration(TimeUnit::Millisecond) => Ok("tDm".into()),
        DataType::Duration(TimeUnit::Microsecond) => Ok("tDu".into()),
        DataType::Duration(TimeUnit::Nanosecond) => Ok("tDn".into()),
        DataType::Interval(IntervalUnit::YearMonth) => Ok("tiM".into()),
        DataType::Interval(IntervalUnit::DayTime) => Ok("tiD".into()),
        DataType::Interval(IntervalUnit::MonthDayNano) => Ok("tin".into()),
        DataType::List(_) => Ok("+l".into()),
        DataType::LargeList(_) => Ok("+L".into()),
        DataType::Struct(_) => Ok("+s".into()),
        DataType::Map(_, _) => Ok("+m".into()),
        DataType::RunEndEncoded(_, _) => Ok("+r".into()),
        DataType::Dictionary(key_data_type, _) => get_format_string(key_data_type),
        DataType::Union(fields, mode) => {
            let formats = fields
                .iter()
                .map(|(t, _)| t.to_string())
                .collect::<Vec<_>>();
            match mode {
                UnionMode::Dense => Ok(Cow::Owned(format!("{}:{}", "+ud", formats.join(",")))),
                UnionMode::Sparse => Ok(Cow::Owned(format!("{}:{}", "+us", formats.join(",")))),
            }
        }
        other => Err(ArrowError::CDataInterface(format!(
            "The datatype \"{other:?}\" is still not supported in Rust implementation"
        ))),
    }
}

fn try_from_field(
    field: &Field,
    allocator: &mut WasmAllocator,
) -> Result<WasmArrowSchema, ArrowError> {
    let mut flags = if field.is_nullable() {
        Flags::NULLABLE
    } else {
        Flags::empty()
    };

    if let Some(true) = field.dict_is_ordered() {
        flags |= Flags::DICTIONARY_ORDERED;
    }

    let mut schema = try_from(field.data_type(), allocator)?;

    // Copy the name from the field.
    let name_ptr = allocator.copy(field.name().as_bytes());
    schema.name = WasmPtr::new(name_ptr);

    // Copy the bits.
    schema.flags = flags.bits();

    // Copy the metadata. -> TODO as this hashmap might be a little more difficult.
    // schema.metadata = field.metadata();

    Ok(schema)
}
